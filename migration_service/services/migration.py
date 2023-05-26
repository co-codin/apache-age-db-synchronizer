import asyncio
import logging
import itertools
import re

from typing import Sequence

import age
from age import Age
from fastapi import status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession

from migration_service.errors import MoreThanTwoFieldsMatchFKPattern
from migration_service.schemas.migrations import (
    MigrationPattern, HubToCreate, TableToAlter, ApplySchema
)
from migration_service.services.migration_formatter import ApplyMigrationFormatter

from migration_service.crud.migration import select_last_migration_tables_fields
from migration_service.utils.migration_utils import to_batches, get_highest_table_similarity_score

from migration_service.age_queries.hub_queries import create_hubs_query, construct_create_hubs_query
from migration_service.age_queries.sat_queries import (
    create_sats_with_hubs_query, construct_create_sats_query, create_sats_query
)
from migration_service.age_queries.link_queries import (
    create_links_query, create_links_with_hubs_query, construct_create_links_query
)
from migration_service.age_queries.node_queries import (
    delete_nodes_query, construct_delete_nodes_query, alter_nodes_query_create_fields, alter_nodes_query_delete_fields,
    alter_nodes_query_alter_fields, construct_delete_fields_query, construct_alter_fields_query,
    construct_create_fields_query
)


logger = logging.getLogger(__name__)


async def apply_migration(
        migration_pattern: MigrationPattern, session: SQLAlchemyAsyncSession, age_session
) -> str:
    last_migration = await select_last_migration_tables_fields(session)
    if not last_migration:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    guid = last_migration.guid
    last_migration = ApplyMigrationFormatter(
        last_migration, migration_pattern.fk_pattern, migration_pattern.pk_pattern
    ).format()

    logger.info(f"last migration: {last_migration}")
    for schema in last_migration.schemas:
        ns = f'{last_migration.db_source}.{schema.name}'
        ag = await asyncio.get_running_loop().run_in_executor(None, age_session.setGraph, ns)

        await _apply_delete_tables(schema, ag)
        await _apply_create_tables(schema, migration_pattern, ag)
        await _apply_alter_tables(schema, ag)
    return guid


async def _apply_delete_tables(apply_schema: ApplySchema, age_session: Age):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        _delete_nodes_tx,
        itertools.chain(apply_schema.hubs_to_delete, apply_schema.sats_to_delete, apply_schema.links_to_delete),
        age_session
    )


async def _apply_create_tables(
        apply_schema: ApplySchema, migration_pattern: MigrationPattern, age_session: Age
):
    hub_dicts_to_create = (hub.dict() for hub in apply_schema.hubs_to_create)
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(None, _add_hubs_tx, hub_dicts_to_create, age_session)
    await _add_links(apply_schema, migration_pattern, age_session)
    await _add_sats(apply_schema, migration_pattern, age_session)


async def _apply_alter_tables(apply_schema: ApplySchema, age_session: Age):
    nodes_to_alter = (
        node.dict()
        for node in itertools.chain(apply_schema.hubs_to_alter, apply_schema.sats_to_alter, apply_schema.links_to_alter)
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _alter_nodes_tx, nodes_to_alter, age_session)


def _delete_nodes_tx(nodes_to_delete: Sequence, age_session):
    for node_batch in to_batches(nodes_to_delete):
        constructed_query = construct_delete_nodes_query(node_batch)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execute(delete_nodes_query.format(nodes=str_query))
        age_session.commit()


def _add_hubs_tx(hubs_to_create: Sequence[HubToCreate], age_session: Age):
    for hub_batch in to_batches(hubs_to_create):
        constructed_query = construct_create_hubs_query(hub_batch)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(create_hubs_query.format(hubs=str_query))
        age_session.commit()


async def _add_sats(apply_schema: ApplySchema, migration_pattern: MigrationPattern, age_session: Age):
    sats_with_hub = []
    sats_without_hub = []

    sat_pattern = re.compile(migration_pattern.fk_table)
    tables_to_pks = apply_schema.tables_to_pks

    for sat in apply_schema.sats_to_create:
        table_prefix = sat_pattern.search(sat.name)
        if table_prefix:
            table_name = get_highest_table_similarity_score(table_prefix.group(1), tables_to_pks.keys(), sat.name)
        else:
            table_name = None

        try:
            sat.link.ref_table = table_name
            sat.link.ref_table_pk = tables_to_pks[table_name]
            sats_with_hub.append(sat.dict())
        except KeyError:
            sats_without_hub.append(sat.dict(exclude={'link'}))

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, _add_sats_tx, create_sats_with_hubs_query, sats_with_hub, True, age_session
    )
    await loop.run_in_executor(
        None, _add_sats_tx, create_sats_query, sats_without_hub, False, age_session
    )


def _add_sats_tx(add_sats_query: str, sats: list[dict], is_linked: bool, age_session: Age):
    for sat_batch in to_batches(sats):
        constructed_query = construct_create_sats_query(sat_batch, is_linked)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(add_sats_query.format(sats=str_query))
        age_session.commit()


async def _add_links(
        apply_schema: ApplySchema,
        migration_pattern: MigrationPattern,
        age_session: Age
):
    links_with_hubs = []
    links_without_hubs = []

    fk_pattern_compiled = re.compile(migration_pattern.fk_pattern)
    tables_to_pks = apply_schema.tables_to_pks

    for link in apply_schema.links_to_create:
        link.match_fks_to_fk_tables(fk_pattern_compiled, tables_to_pks.keys())
        try:
            link.main_link.ref_table_pk = tables_to_pks[link.main_link.ref_table]
            link.paired_link.ref_table_pk = tables_to_pks[link.paired_link.ref_table]
            links_with_hubs.append(link.dict())
        except (KeyError, AttributeError, MoreThanTwoFieldsMatchFKPattern):
            links_without_hubs.append(
                link.dict(exclude={'main_link', 'paired_link'})
            )

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, _add_links_tx, create_links_with_hubs_query, links_with_hubs, True, age_session
    )
    await loop.run_in_executor(
        None, _add_links_tx, create_links_query, links_without_hubs, False, age_session
    )


def _add_links_tx(add_links_query: str, links: list[dict], is_linked: bool, age_session: Age):
    for link_batch in to_batches(links):
        constructed_query = construct_create_links_query(link_batch, is_linked)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(add_links_query.format(links=str_query))
        age_session.commit()


def _alter_nodes_tx(nodes_to_alter: Sequence[TableToAlter], age_session: Age):
    for node_batch in to_batches(nodes_to_alter):
        constructed_query = construct_create_fields_query(node_batch)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(alter_nodes_query_create_fields.format(nodes=str_query))
        age_session.commit()

        constructed_query = construct_delete_fields_query(node_batch)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(alter_nodes_query_delete_fields.format(nodes=str_query))
        age_session.commit()

        constructed_query = construct_alter_fields_query(node_batch)
        str_query = constructed_query.as_string(age_session.connection)

        age_session.execCypher(alter_nodes_query_alter_fields.format(nodes=str_query))
        age_session.commit()
