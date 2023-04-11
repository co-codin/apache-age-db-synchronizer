import logging

from typing import List, Iterable

from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession, AsyncManagedTransaction

from migration_service.models import migrations
from migration_service.schemas.migrations import TableToCreate
from migration_service.services.migration_formatter import format_orm_migration
from migration_service.crud.migration import select_last_migration_tables_fields
from migration_service.utils.migration_utils import to_batches

from migration_service.cql_queries.node_queries import delete_nodes_query, delete_links_query, create_hubs_query


logger = logging.getLogger(__name__)


async def apply_migration(
        session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession
):
    """
    1) Insert tables
    2) Alter tables
    3) Delete tables
    """
    last_migration = await select_last_migration_tables_fields(session)
    if not last_migration:
        return

    db_source = last_migration.db_source
    last_migration = format_orm_migration(last_migration)
    await _apply_delete_tables(last_migration.tables_to_delete, graph_session)
    await _apply_create_tables(last_migration.tables_to_create, db_source, graph_session)


async def _apply_create_tables(tables_to_create: List[TableToCreate], db_source: str, graph_session: Neo4jAsyncSession):
    hubs_to_create = (table.dict() for table in tables_to_create if table.name.endswith('_hub'))
    await graph_session.execute_write(_add_hub_tx, hubs_to_create, db_source)


async def _add_hub_tx(tx: AsyncManagedTransaction, hubs_to_create: Iterable, db_source: str):
    for hub_batch in to_batches(hubs_to_create):
        await tx.run(create_hubs_query, hubs=hub_batch, db_source=db_source)


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


async def _apply_delete_tables(tables_to_delete: List[str], graph_session: Neo4jAsyncSession):
    hubs_sats_to_delete = (
        table
        for table in tables_to_delete
        if table.endswith('_hub') or table.endswith('_sat')
    )
    links_to_delete = (table for table in tables_to_delete if table.endswith('_link'))

    await graph_session.execute_write(_delete_nodes_tx, hubs_sats_to_delete, delete_nodes_query)
    await graph_session.execute_write(_delete_nodes_tx, links_to_delete, delete_links_query)


async def _delete_nodes_tx(tx: AsyncManagedTransaction, nodes_to_delete: Iterable[str], delete_query: str):
    for node_batch in to_batches(nodes_to_delete):
        logger.info(f'nodes_batch: {node_batch}')
        await tx.run(delete_query, node_names=node_batch)
