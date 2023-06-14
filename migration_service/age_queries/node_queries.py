from psycopg2 import sql

delete_nodes_query = """
                     WITH {nodes} as node_batch 
                     UNWIND node_batch as node_name 
                     
                     MATCH (node {{ name: node_name }}) 
                     OPTIONAL MATCH (node)-[:ATTR]->(f:Field)  

                     DETACH DELETE node, f
"""


alter_nodes_query_create_fields = """
                                  WITH {nodes} as node_batch 
                                  UNWIND node_batch as node_record 
                                  
                                  MATCH (node {{ name: node_record.name }}) 
                                  
                                  WITH node_record.fields_to_create as fields_to_create, node 
                                  UNWIND fields_to_create as field 
                                  CREATE (node)-[:ATTR]->(:Field {{ name: field.name, db: field.name, attrs: [], dbtype: field.db_type }})
"""

alter_nodes_query_delete_fields = """
                                  WITH {nodes} as node_batch 
                                  UNWIND node_batch as node_record 
                                  
                                  MATCH (node {{ name: node_record.name }}) 
                                  
                                  WITH node_record.fields_to_delete as fields_to_delete, node 
                                  UNWIND fields_to_delete as field 
                                  
                                  MATCH (node)-[:ATTR]->(f:Field {{ db: field }}) 
                                  DETACH DELETE f
"""


alter_nodes_query_alter_fields = """
                                 WITH {nodes} as node_batch 
                                 UNWIND node_batch as node_record 
                                 
                                 MATCH (node {{ name: node_record.name }}) 
                                 
                                 WITH node_record.fields_to_alter as fields_to_alter, node 
                                 UNWIND fields_to_alter as field 
                                 
                                 MATCH (node)-[:ATTR]->(f:Field {{ db: field.name }}) 
                                 SET f.dbtype=field.new_type
"""


def construct_delete_nodes_query(name_batch) -> sql.Composable:
    query = sql.SQL('{}').format(
        sql.SQL(',').join(
            map(sql.Literal, name_batch)
        )
    )
    query = sql.SQL('[{}]').format(query)
    return query


def construct_create_fields_query(nodes_batch) -> sql.Composable:
    query = []
    for node in nodes_batch:
        name = sql.SQL("name: {}").format(sql.Literal(node['name']))

        fields_query = []
        for field in node['fields_to_create']:
            f_name = sql.SQL("name: {}").format(sql.Literal(field['name']))
            f_dbtype = sql.SQL("db_type: {}").format(sql.Literal(field['db_type']))

            f_fields = sql.SQL(',').join((f_name, f_dbtype))
            f_fields = sql.SQL('{{{}}}').format(f_fields)

            fields_query.append(f_fields)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields_to_create: [{}]").format(fields_query)

        hub_query = sql.SQL(',').join((name, fields))
        hub_query = sql.SQL("{{{}}}").format(hub_query)
        query.append(hub_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query


def construct_delete_fields_query(nodes_batch) -> sql.Composable:
    query = []
    for node in nodes_batch:
        name = sql.SQL("name: {}").format(sql.Literal(node['name']))

        fields_query = []
        for field in node['fields_to_delete']:
            f_name = sql.SQL("{}").format(sql.Literal(field))

            fields_query.append(f_name)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields_to_delete: [{}]").format(fields_query)

        hub_query = sql.SQL(',').join((name, fields))
        hub_query = sql.SQL("{{{}}}").format(hub_query)
        query.append(hub_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query


def construct_alter_fields_query(nodes_batch) -> sql.Composable:
    query = []
    for node in nodes_batch:
        name = sql.SQL("name: {}").format(sql.Literal(node['name']))

        fields_query = []
        for field in node['fields_to_alter']:
            f_name = sql.SQL("name: {}").format(sql.Literal(field['name']))
            f_dbtype = sql.SQL("new_type: {}").format(sql.Literal(field['new_type']))

            f_fields = sql.SQL(',').join((f_name, f_dbtype))
            f_fields = sql.SQL('{{{}}}').format(f_fields)

            fields_query.append(f_fields)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields_to_alter: [{}]").format(fields_query)

        hub_query = sql.SQL(',').join((name, fields))
        hub_query = sql.SQL("{{{}}}").format(hub_query)
        query.append(hub_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query
