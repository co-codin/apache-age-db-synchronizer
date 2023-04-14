import logging

from typing import Set

from neo4j import AsyncSession as Neo4jAsyncSession


async def get_neo4j_tables(graph_session: Neo4jAsyncSession, db_source: str) -> Set[str]:
    res = await graph_session.run(
        "MATCH (obj) "
        "WITH split(obj.db, $db_source + '.')[1] as db_name, obj "
        "WHERE (obj:Entity OR obj:Sat OR (obj:Link AND obj.main = True)) AND obj.db STARTS WITH $db_source "
        "RETURN db_name as name;",
        db_source=db_source
    )
    table_names = await res.value('name')
    return set(table_names)


async def get_neo4j_table_col_type(table_names: Set[str], db_source: str, graph_session: Neo4jAsyncSession):
    res = await graph_session.run(
        "MATCH (obj)-[:ATTR]->(f:Field) "
        "WITH split(obj.db, $db_source + '.')[1] AS db_name, obj, f "
        "WHERE (obj:Entity OR obj:Sat OR obj:Link) and obj.db STARTS WITH $db_source AND db_name IN $tables "
        "RETURN db_name as name, split(f.db, $db_source + '.' + db_name + '.')[1] as f_name, f.dbtype as f_type "
        "ORDER BY db_name;",
        db_source=db_source, tables=list(table_names)
    )
    table_names = await res.values('name', 'f_name', 'f_type')
    return table_names
