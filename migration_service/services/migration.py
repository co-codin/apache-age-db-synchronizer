import asyncio
import logging
import itertools
import re

from typing import Sequence

from age import Age
from fastapi import status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession

from migration_service.cql_queries.sat_queries import create_sats_query, create_sats_with_hubs_query
from migration_service.errors import MoreThanTwoFieldsMatchFKPattern
from migration_service.schemas.migrations import (
    MigrationPattern, HubToCreate, TableToAlter, ApplySchema
)
from migration_service.services.migration_formatter import ApplyMigrationFormatter

from migration_service.crud.migration import select_last_migration_tables_fields
from migration_service.utils.migration_utils import to_batches, get_highest_table_similarity_score

from migration_service.cql_queries.node_queries import (
    delete_nodes_query, alter_nodes_query_create_fields, alter_nodes_query_delete_fields,
    alter_nodes_query_alter_fields
)
from migration_service.cql_queries.hub_queries import create_hubs_query

from migration_service.cql_queries.link_queries import (
    create_links_query, delete_links_query, create_links_with_hubs_query
)

logger = logging.getLogger(__name__)


async def apply_migration(
        migration_pattern: MigrationPattern, session: SQLAlchemyAsyncSession, age_session: Age
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
        await _apply_delete_tables(schema, age_session)
        await _apply_create_tables(schema, migration_pattern, last_migration.db_source, age_session)
        await _apply_alter_tables(schema, last_migration.db_source, age_session)
    return guid


async def _apply_delete_tables(apply_schema: ApplySchema, age_session: Age):
    hubs_sats_to_delete = (
        table
        for table in itertools.chain(apply_schema.hubs_to_delete, apply_schema.sats_to_delete)
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _delete_nodes_tx, hubs_sats_to_delete, delete_nodes_query, age_session)
    await loop.run_in_executor(None, _delete_nodes_tx, apply_schema.links_to_delete, delete_links_query, age_session)


async def _apply_create_tables(
        apply_schema: ApplySchema, migration_pattern: MigrationPattern, db_source: str, age_session: Age
):
    hub_dicts_to_create = (hub.dict() for hub in apply_schema.hubs_to_create)
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(None, _add_hubs_tx, hub_dicts_to_create, db_source, age_session)
    await _add_links(apply_schema, migration_pattern, db_source, age_session)
    await _add_sats(apply_schema, migration_pattern, db_source, age_session)


async def _apply_alter_tables(apply_schema: ApplySchema, db_source: str, age_session: Age):
    nodes_to_alter = (
        node.dict()
        for node in itertools.chain(apply_schema.hubs_to_alter, apply_schema.sats_to_alter, apply_schema.links_to_alter)
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _alter_nodes_tx, nodes_to_alter, db_source, age_session)


def _delete_nodes_tx(nodes_to_delete: Sequence[str], delete_query: str, age_session: Age):
    for node_batch in to_batches(nodes_to_delete):
        with age_session.connection.cursor() as cursor:
            age_session.cypher(cursor, delete_query, params=(node_batch,))
            age_session.commit()


def _add_hubs_tx(hubs_to_create: Sequence[HubToCreate], db_source: str, age_session: Age):
    age_session.setGraph(db_source)
    for hub_batch in to_batches(hubs_to_create):
        with age_session.connection.cursor() as cursor:
            age_session.cypher(cursor, create_hubs_query, params=(hub_batch,))
            age_session.commit()


async def _add_sats(apply_schema: ApplySchema, migration_pattern: MigrationPattern, db_source: str, age_session: Age):
    sats_with_hub = []
    sats_without_hub = []

    sat_pattern = re.compile(migration_pattern.fk_table)
    tables_to_pks = apply_schema.tables_to_pks

    for sat in apply_schema.sats_to_create:
        table_prefix = sat_pattern.search(sat.name)
        table_name = get_highest_table_similarity_score(table_prefix.group(1), tables_to_pks.keys(), sat.name)

        logger.info(f'table prefix: {table_prefix.group(1)}')
        logger.info(f'table: {table_name}')

        try:
            sat.link.ref_table = table_name
            sat.link.ref_table_pk = tables_to_pks[table_name]
            sats_with_hub.append(sat.dict())
        except KeyError:
            sats_without_hub.append(sat.dict(exclude={'link'}))

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, _add_sats_tx, create_sats_with_hubs_query, sats_with_hub, db_source, age_session
    )
    await loop.run_in_executor(
        None, _add_sats_tx, create_sats_query, sats_without_hub, db_source, age_session
    )


def _add_sats_tx(add_sats_query: str, sats: list[dict], db_source: str, age_session: Age):
    for sat_batch in to_batches(sats):
        with age_session.connection.cursor() as cursor:
            age_session.cypher(cursor, add_sats_query, params=(sat_batch, db_source))
            age_session.commit()


async def _add_links(
        apply_schema: ApplySchema,
        migration_pattern: MigrationPattern,
        db_source: str,
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
        None, _add_links_tx, create_links_with_hubs_query, links_with_hubs, db_source, age_session
    )
    await loop.run_in_executor(
        None, _add_links_tx, create_links_query, links_without_hubs, db_source, age_session
    )


def _add_links_tx(add_links_query: str, links: list[dict], db_source: str, age_session: Age):
    for sat_batch in to_batches(links):
        with age_session.connection.cursor() as cursor:
            age_session.cypher(cursor, add_links_query, params=(sat_batch, db_source))
            age_session.commit()


def _alter_nodes_tx(nodes_to_alter: Sequence[TableToAlter], db_source: str, age_session: Age):
    for node_batch in to_batches(nodes_to_alter):
        with age_session.connection.cursor() as cursor:
            age_session.cypher(cursor, alter_nodes_query_create_fields, params=(node_batch, db_source))
            age_session.commit()

            age_session.cypher(cursor, alter_nodes_query_delete_fields, params=(node_batch, db_source))
            age_session.commit()

            age_session.cypher(cursor, alter_nodes_query_alter_fields, params=(node_batch, db_source))
            age_session.commit()
