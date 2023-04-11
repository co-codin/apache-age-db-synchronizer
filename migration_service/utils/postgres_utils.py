import psycopg

from typing import Set

from migration_service.settings import settings


async def get_db_tables(db_source: str) -> Set[str]:
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


async def get_table_col_type(table_names: Set[str], db_source: str):
    conn_string = settings.db_sources[db_source]
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT tabs.table_name, cols.column_name, cols.data_type "
                "FROM information_schema.columns AS cols "
                "JOIN information_schema.tables AS tabs "
                "ON tabs.table_name = cols.table_name "
                "WHERE tabs.table_schema = %s AND tabs.table_name = ANY(%s) "
                "ORDER BY tabs.table_name;",
                (db_source, list(table_names))
            )
            records = await cursor.fetchall()
    return records
