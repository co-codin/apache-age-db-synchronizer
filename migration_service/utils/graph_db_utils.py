from typing import Set
from psycopg2 import sql
from age import Age

from migration_service.utils.migration_utils import to_batches


def check_graph_created(graph_name: str, age_session: Age):
    with age_session.connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM ag_catalog.ag_graph WHERE name=%s", (graph_name,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("SELECT create_graph(%s);", (graph_name,))
            age_session.connection.commit()


def set_graph(graph_name: str, age_session: Age) -> Age:
    check_graph_created(graph_name, age_session)
    age_session.graphName = graph_name
    return age_session


def get_graph_db_tables(db_namespaces: set[str], age_session: Age) -> dict[str, set[str]]:
    graph_to_tables: dict[str, set[str]] = {}
    for db_ns in db_namespaces:
        ag = set_graph(db_ns, age_session)
        cursor = ag.execCypher(
            """
            MATCH (obj) 
            RETURN obj.name as name
            """,
            cols=['name']
        )
        graph_to_tables[db_ns] = {row[0] for row in cursor}
    return graph_to_tables


def get_graph_db_table(db_namespaces: set[str], table_name: str, age_session: Age) -> dict[str, set[str]]:
    graph_to_tables: dict[str, set[str]] = {}
    for db_ns in db_namespaces:
        ag = set_graph(db_ns, age_session)
        cursor = ag.execCypher(
            """
            MATCH (obj {name: %s}) 
            RETURN obj.name as name
            """,
            cols=['name'],
            params=(table_name,)
        )
        graph_to_tables[db_ns] = {row[0] for row in cursor}
    return graph_to_tables


def get_graph_db_table_col_type(
        db_source: str, ns: str, table_names: Set[str], age_session: Age
) -> list[tuple[str, str, str, str]]:
    ag = set_graph(f'{db_source}.{ns}', age_session)
    res = []
    for tables_batch in to_batches(table_names):

        params = sql.SQL(',').join(map(sql.Literal, tables_batch))
        params = sql.SQL('[{}]').format(params)
        params = params.as_string(ag.connection)

        cursor = ag.execCypher(
            """
            MATCH (obj)-[:ATTR]->(f:Field) 
            WHERE obj.name IN {} 
            RETURN obj.db, obj.name, f.db, f.dbtype 
            """.format(params),
            cols=['object_db', 'object_name', 'field_db', 'field_name']
        )
        res.extend(
            [(row[0], row[1], row[2], row[3]) for row in cursor]
        )
    return list(sorted(res, key=lambda row: row[0]))
