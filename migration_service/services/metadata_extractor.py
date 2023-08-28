import psycopg
import base64

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
    async def extract_table_name(self, table_name: str, db_path: str | None) -> dict[str, set[str]]:
        ...

    @abstractmethod
    async def extract_table_col_type(self, table_names: set[str], ns: str) -> tuple[str, str]:
        ...

    @abstractmethod
    async def extract_table_count(self) -> int:
        ...


class PostgresExtractor(MetadataExtractor):
    def __init__(self, conn_string: str):
        super().__init__(conn_string)
        self._postgres_to_system_types = {
            'boolean': 'bool',

            'character varying': 'str',
            'character': 'str',
            'uuid': 'str',

            'text': 'str',

            'smallint': 'int',
            'integer': 'int',
            'bigint': 'int',

            'double precision': 'float',
            'real': 'float',
            'numeric': 'float',
            'decimal': 'float',

            'date': 'date',
            'timestamp without time zone': 'datetime',
            'timestamp with time zone': 'datetime',

            'jsonb': 'json',
            'xml': 'xml',
            'ARRAY': 'list'
        }

    async def extract_table_names(self) -> dict[str, set[str]]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    select table_schema, table_name
                    from information_schema.tables
                    where table_schema = 'dv_raw' 
                    and table_type = 'BASE TABLE' 
                    and table_schema not in ('pg_catalog', 'information_schema');
                    """
                )
                result = await cursor.fetchall()
                ns_to_tables: dict[str, set[str]] = {}
                db_source = self._conn_string.rsplit('/', maxsplit=1)[1]

                for res in result:
                    table_schema, table_name = res
                    ns = f'{db_source}.{table_schema}'
                    try:
                        ns_to_tables[ns].add(table_name)
                    except KeyError:
                        ns_to_tables[ns] = {table_name}
                return ns_to_tables

    async def extract_table_name(self, table_name: str, db_path: str | None) -> dict[str, set[str]]:
        if db_path:
            source, schema, name = db_path.split('.', maxsplit=2)
        else:
            source, schema, name = None, None, table_name
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    select table_schema, table_name
                    from information_schema.tables
                    where table_schema = 'dv_raw' and table_name = %s;
                    """,
                    (name, )
                )
                result = await cursor.fetchall()
                ns_to_tables: dict[str, set[str]] = {}
                db_source = self._conn_string.rsplit('/', maxsplit=1)[1]

                for res in result:
                    table_schema, table_name = res
                    ns = f'{db_source}.{table_schema}'
                    try:
                        ns_to_tables[ns].add(table_name)
                    except KeyError:
                        ns_to_tables[ns] = {table_name}

                if not ns_to_tables and source and schema:
                    ns_to_tables[f'{source}.{schema}'] = set()
                return ns_to_tables

    async def extract_table_col_type(self, table_names: set[str], ns: str) -> list[tuple[str, str, str, str]]:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT CONCAT(tabs.table_schema, '.', tabs.table_name) as full_name, tabs.table_name, cols.column_name, cols.data_type "
                    "FROM information_schema.tables as tabs "
                    "LEFT OUTER JOIN information_schema.columns as cols "
                    "ON tabs.table_name = cols.table_name "
                    "WHERE tabs.table_name = ANY(%s) "
                    "AND tabs.table_type = 'BASE TABLE' "
                    "AND tabs.table_schema = %s "
                    "ORDER BY full_name",
                    (list(table_names), ns)
                )
                records = await cursor.fetchall()
                result = []
                for row in records:
                    system_type = self.from_db_type_to_system_type(row[-1])
                    result.append((row[0], row[1], row[2], system_type))
        return result

    async def extract_table_count(self) -> int:
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    select count(*)
                    from information_schema.tables
                    where table_schema = 'dv_raw';
                    """,
                )
                result = await cursor.fetchall()
                return result[0][0]

    def from_db_type_to_system_type(self, var: str) -> str:
        system_type = self._postgres_to_system_types.get(var, '')

        if (system_type == 'str' or system_type == 'text') and self.is_b64(var):
            system_type = 'b64binary'

        return system_type

    @staticmethod
    def is_b64(string: str) -> bool:
        return base64.b64encode(base64.b64decode(string)) == string


class MongoExtractor(MetadataExtractor):
    def __init__(self, conn_string: str):
        super().__init__(conn_string)

    @property
    def conn_string(self):
        return self._conn_string

    async def extract_table_names(self) -> dict[str, set[str]]:
        ...

    async def extract_table_name(self, table_name: str, db_path: str | None) -> dict[str, set[str]]:
        ...

    async def extract_table_col_type(self, table_names: set[str], ns: str) -> tuple[str, str]:
        ...


class MetaDataExtractorFactory:
    _DRIVER_TO_METADATA_EXTRACTOR_TYPE = {
        'postgresql': PostgresExtractor,
        'mongodb': MongoExtractor
    }

    @classmethod
    def build(cls, conn_string: str) -> MetadataExtractor:
        driver = conn_string.split('://', maxsplit=1)[0]
        metadata_extractor_class = cls._DRIVER_TO_METADATA_EXTRACTOR_TYPE[driver]
        return metadata_extractor_class(conn_string)
