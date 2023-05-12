import psycopg

from typing import Set, Tuple
from abc import ABC, abstractmethod

from migration_service.settings import settings


class MetadataExtractor(ABC):
    @abstractmethod
    def __init__(self, conn_string: str):
        self._conn_string = conn_string

    @property
    def conn_string(self):
        return self._conn_string

    @abstractmethod
    async def extract_table_names(self) -> Set[str]:
        ...

    @abstractmethod
    async def extract_table_col_type(self, table_names: Set[str]) -> Tuple[str, str]:
        ...


class PostgresExtractor(MetadataExtractor):
    def __init__(self, conn_string: str):
        super().__init__(conn_string)

    async def extract_table_names(self) -> Set[str]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "select table_name "
                    "from information_schema.tables "
                    "where table_schema = %s;",
                    (settings.db_source,)
                )
                table_names = await cursor.fetchall()
                return {res[0] for res in table_names}

    async def extract_table_col_type(self, table_names: Set[str]) -> Tuple[str, str]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT tabs.table_name, cols.column_name, cols.data_type "
                    "FROM information_schema.columns AS cols "
                    "JOIN information_schema.tables AS tabs "
                    "ON tabs.table_name = cols.table_name "
                    "WHERE tabs.table_schema = %s AND tabs.table_name = ANY(%s) "
                    "ORDER BY tabs.table_name;",
                    (settings.db_source, list(table_names))
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
