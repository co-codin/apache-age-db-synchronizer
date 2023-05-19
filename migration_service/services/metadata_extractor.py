import psycopg

from typing import Set, Tuple, Optional
from abc import ABC, abstractmethod


class MetadataExtractor(ABC):
    @abstractmethod
    def __init__(self, conn_string: str):
        self._conn_string = conn_string

    @property
    def conn_string(self):
        return self._conn_string

    @abstractmethod
    async def extract_table_names(self) -> dict[str, set[str]]:
        ...

    @abstractmethod
    async def extract_table_col_type(self, table_names: Set[str], ns: str) -> Tuple[str, str]:
        ...


class PostgresExtractor(MetadataExtractor):
    def __init__(self, conn_string: str):
        super().__init__(conn_string)

    async def extract_table_names(self) -> dict[str, set[str]]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    select table_schema, table_name
                    from information_schema.tables
                    where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema');
                    """
                )
                result = await cursor.fetchall()
                ns_to_tables: dict[str, set[str]] = {}
                db_source = self._conn_string.rsplit('/', maxsplit=1)[1]

                for res in result:
                    table_schema = res[0]
                    table_name = res[1]
                    ns = f'{db_source}.{table_schema}'
                    try:
                        ns_to_tables[ns].add(table_name)
                    except KeyError:
                        ns_to_tables[ns] = {table_name}

                return ns_to_tables

    async def extract_table_col_type(self, table_names: Set[str], ns: str) -> Tuple[str, str]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT tabs.table_name, cols.column_name, cols.data_type "
                    "FROM information_schema.columns AS cols "
                    "JOIN information_schema.tables AS tabs "
                    "ON tabs.table_name = cols.table_name "
                    "WHERE tabs.table_name = ANY(%s) "
                    "AND tabs.table_type = 'BASE TABLE' "
                    "AND tabs.table_schema = %s;",
                    (list(table_names), ns)
                )
                records = await cursor.fetchall()
        return records


class MetaDataExtractorFactory:
    _DRIVER_TO_METADATA_EXTRACTOR_TYPE = {
        'postgresql': PostgresExtractor
    }

    @classmethod
    def build(cls, conn_string: str) -> MetadataExtractor:
        driver = conn_string.split('://', maxsplit=1)[0]
        metadata_extractor_class = cls._DRIVER_TO_METADATA_EXTRACTOR_TYPE[driver]
        return metadata_extractor_class(conn_string)
