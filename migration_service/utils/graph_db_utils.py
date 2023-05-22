from typing import Set

from age import Age


def get_graph_db_tables(db_namespaces: set[str], age_session: Age) -> dict[str, set[str]]:
    graph_to_tables: dict[str, set[str]] = {}
    for db_ns in db_namespaces:
        ag = age_session.setGraph(db_ns)
        cursor = ag.execCypher(
            "MATCH (obj) "
            "WHERE (obj:Entity OR obj:Sat OR (obj:Link AND obj.main = True)) "
            "RETURN obj.name as name;",
            cols=['name']
        )
        graph_to_tables[db_ns] = {row[0] for row in cursor}
    return graph_to_tables


def get_graph_db_table_col_type(db_source: str, ns: str, table_names: Set[str], age_session: Age):
    ag = age_session.setGraph(f'{db_source}.{ns}')
    cursor = ag.execCypher(
        """
        MATCH (obj)-[:ATTR]->(f:Field) 
        WHERE (obj:Entity OR obj:Sat OR (obj:Link AND obj.main = True)) AND obj.db IN $tables 
        RETURN obj.db as db_name, obj.name as name, f.db as f_name, f.dbtype as f_type 
        ORDER BY db_name;
        """,
        params=(table_names,),
        cols=['db_name, name, f_name, f_type']
    )
    return [(row[0], row[1], row[2], row[3]) for row in cursor]
