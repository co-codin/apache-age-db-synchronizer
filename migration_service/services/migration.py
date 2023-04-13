import logging
import itertools
import re

from typing import Iterable

from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession, AsyncManagedTransaction

from migration_service.cql_queries.sat_queries import create_sats_query, create_sats_with_hubs_query
from migration_service.errors import MoreThanTwoFieldsMatchFKPattern
from migration_service.models import migrations
from migration_service.schemas.migrations import MigrationPattern, HubToCreate, ApplyMigration
from migration_service.services.apply_migration_formatter import format_orm_migration

from migration_service.crud.migration import select_last_migration_tables_fields
from migration_service.utils.migration_utils import to_batches, match_fk_to_table

from migration_service.cql_queries.node_queries import delete_nodes_query
from migration_service.cql_queries.hub_queries import create_hubs_query
from migration_service.cql_queries.link_queries import (
    create_links_query, delete_links_query, create_links_with_hubs_query
)

logger = logging.getLogger(__name__)


async def apply_migration(
        migration_pattern: MigrationPattern, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession
):
    """
    1) Insert tables
    2) Alter tables
    3) Delete tables
    """
    last_migration = await select_last_migration_tables_fields(session)
    if not last_migration:
        return

    last_migration = format_orm_migration(
        last_migration, migration_pattern.fk_pattern, migration_pattern.pk_pattern
    )

    logger.info(f"last migration: {last_migration}")

    await _apply_delete_tables(last_migration, graph_session)
    await _apply_create_tables(last_migration, migration_pattern, graph_session)


async def _apply_create_tables(
        apply_migration: ApplyMigration,
        migration_pattern: MigrationPattern,
        graph_session: Neo4jAsyncSession
):
    hub_dicts_to_create = (hub.dict() for hub in apply_migration.hubs_to_create)

    await graph_session.execute_write(_add_hubs_tx, hub_dicts_to_create, apply_migration.db_source)
    await graph_session.execute_write(_add_links_tx, apply_migration, migration_pattern)
    await graph_session.execute_write(_add_sats_tax, apply_migration, migration_pattern)


async def _add_hubs_tx(tx: AsyncManagedTransaction, hubs_to_create: Iterable[HubToCreate], db_source: str):
    for hub_batch in to_batches(hubs_to_create):
        await tx.run(create_hubs_query, hubs=hub_batch, db_source=db_source)


async def _add_sats_tax(
        tx: AsyncManagedTransaction, apply_migration: ApplyMigration, migration_pattern: MigrationPattern
):
    sats_with_hub = []
    sats_without_hub = []

    sat_pattern = re.compile(migration_pattern.fk_table)
    tables_to_pks = apply_migration.tables_to_pks

    for sat in apply_migration.sats_to_create:
        table_prefix = sat_pattern.search(sat.name)
        table_name = match_fk_to_table(table_prefix, tables_to_pks.keys())
        if table_name:
            sat.link.ref_table = table_name
            sat.link.ref_table_pk = tables_to_pks[table_name]
            sats_with_hub.append(sat.dict())
        else:
            sats_without_hub.append(sat.dict(exclude={'link'}))

    for sat_batch in to_batches(sats_with_hub):
        await tx.run(create_sats_with_hubs_query, sats=sat_batch, db_source=apply_migration.db_source)
    for sat_batch in to_batches(sats_without_hub):
        await tx.run(create_sats_query, sats=sat_batch, db_source=apply_migration.db_source)


async def _add_links_tx(
        tx: AsyncManagedTransaction,
        apply_migration: ApplyMigration,
        migration_pattern: MigrationPattern
):
    links_with_hubs = []
    links_without_hubs = []

    fk_pattern_compiled = re.compile(migration_pattern.fk_pattern)
    tables_to_pks = apply_migration.tables_to_pks

    for link in apply_migration.links_to_create:
        link.match_fks_to_fk_tables(fk_pattern_compiled, tables_to_pks.keys())
        try:
            link.main_link.ref_table_pk = tables_to_pks[link.main_link.ref_table]
            link.paired_link.ref_table_pk = tables_to_pks[link.paired_link.ref_table]
            links_with_hubs.append(link.dict())
        except (KeyError, AttributeError, MoreThanTwoFieldsMatchFKPattern):
            links_without_hubs.append(
                link.dict(exclude={'main_link', 'paired_link'})
            )

    for link_batch in to_batches(links_with_hubs):
        await tx.run(create_links_with_hubs_query, links=link_batch, db_source=apply_migration.db_source)
    for link_batch in to_batches(links_without_hubs):
        await tx.run(create_links_query, links=link_batch, db_source=apply_migration.db_source)


async def _apply_alter_tables(last_migration_id: int, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession):
    tables_to_alter = await session.execute(
        select(migrations.Table)
        .options(selectinload(migrations.Table.fields))
        .filter(
            and_(
                migrations.Table.migration_id == last_migration_id,
                migrations.Table.old_name.is_not(None),
                migrations.Table.old_name == migrations.Table.new_name
            )
        )
    )
    tables_to_alter = tables_to_alter.scalars().all()
    return tables_to_alter


async def _apply_delete_tables(apply_migration: ApplyMigration, graph_session: Neo4jAsyncSession):
    hubs_sats_to_delete = (
        table
        for table in itertools.chain(apply_migration.hubs_to_delete, apply_migration.sats_to_delete)
    )
    await graph_session.execute_write(_delete_nodes_tx, hubs_sats_to_delete, delete_nodes_query)
    await graph_session.execute_write(_delete_nodes_tx, apply_migration.links_to_delete, delete_links_query)


async def _delete_nodes_tx(tx: AsyncManagedTransaction, nodes_to_delete: Iterable[str], delete_query: str):
    for node_batch in to_batches(nodes_to_delete):
        logger.info(f'nodes_batch: {node_batch}')
        await tx.run(delete_query, node_names=node_batch)
