import logging
import psycopg

from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession
from typing import Set

from migration_service.models.migrations import Migration, Table, Field
from migration_service.settings import settings


logger = logging.getLogger(__name__)


async def scan_db_for_migration(session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession):
    name: str = 'test migration'
    db_source = 'dv_raw'

    db_tables = await _get_db_tables(db_source)
    neo4j_tables = await _get_neo4j_tables(graph_session, db_source)

    logger.info(f"db tables: {db_tables}")
    logger.info(f"neo4j tables: {neo4j_tables}")

    tables_to_delete = neo4j_tables - db_tables
    tables_to_create = db_tables - neo4j_tables
    tables_to_alter = neo4j_tables & db_tables

    logger.info(f'tables to create: {tables_to_create}')
    logger.info(f'tables to alter: {tables_to_alter}')
    logger.info(f'tables to delete: {tables_to_delete}')

    await _create_migration(name, tables_to_create, tables_to_alter, tables_to_delete, session, db_source)


async def _get_db_tables(db_source: str) -> Set[str]:
    conn_string = settings.db_sources[db_source]
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "select table_name "
                "from information_schema.tables "
                "where table_schema = %s;",
                (db_source,)
            )
            table_names = await cursor.fetchall()
            return {res[0] for res in table_names}


async def _get_neo4j_tables(graph_session: Neo4jAsyncSession, db_source: str) -> Set[str]:
    res = await graph_session.run(
        "MATCH (obj) "
        "WHERE (obj:Entity OR obj:Sat or obj:Link) and obj.db STARTS WITH $db_source "
        "RETURN split(obj.db, $db_source + '.')[1] as name;",
        db_source=db_source
    )
    table_names = await res.value('name')
    return set(table_names)


async def _create_migration(
        name: str,
        tables_to_create: Set[str],
        tables_to_alter: Set[str],
        tables_to_delete: Set[str],
        session: SQLAlchemyAsyncSession,
        db_source: str
):
    migration = Migration(name=name)
    await _delete_tables(tables_to_delete, migration)
    await _create_tables(tables_to_create, migration, db_source)
    await _alter_tables(tables_to_alter, migration)

    session.add(migration)


async def _create_tables(table_names: Set[str], migration: Migration, db_source: str):
    logger.info(f'table names tuple: {tuple(table_names)}')
    conn_string = settings.db_sources[db_source]
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cursor:
            params = {'db_source': db_source, 'tables': tuple(table_names)}
            logger.info(f"query params: {params}")
            await cursor.execute(
                "SELECT tabs.table_name, cols.column_name, cols.data_type "
                "FROM information_schema.columns AS cols "
                "JOIN information_schema.tables AS tabs "
                "ON tabs.table_name = cols.table_name "
                "WHERE tabs.table_schema = %(db_source)s AND tabs.table_name in %(tables)s;",
                params
            )
            records = await cursor.fetchall()
            logger.info(f'tables to create data: {records}')
#
#             table = records[0][0]
#             migration.tables.append(Table(new_name=table))
#
#             for record in records:
#                 if table == record[0]:
#                     field = Field(new_name=record[1], new_type=record[2])
#                     migration.tables[-1].fields.append(field)
#                 else:
#                     table = record[0]
#                     migration.tables.append(Table(new_name=table))


async def _alter_tables(table_names: Set[str], session: SQLAlchemyAsyncSession):
    ...


async def _delete_tables(table_names: Set[str], migration: Migration):
    for table in table_names:
        migration.tables.append(Table(old_name=table))
