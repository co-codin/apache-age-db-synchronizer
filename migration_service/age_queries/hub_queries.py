from psycopg2 import sql


create_hubs_query = """
                    WITH {hubs} as hub_batch
                    UNWIND hub_batch as hub_record
                    
                    MERGE (hub:Table {{ name: hub_record.name }})
                    SET hub.db = hub_record.db
                    
                    WITH hub_record.fields as field_batch, hub
                    UNWIND field_batch as field
                    CREATE (hub)-[:ATTR]->(:Field {{name: field.name, db: field.name, attrs: [], dbtype: field.db_type}})
"""


def construct_create_hubs_query(hub_batch) -> sql.Composable:
    query = []
    for hub in hub_batch:
        name = sql.SQL("name: {}").format(sql.Literal(hub['name']))
        db = sql.SQL("db: {}").format(sql.Literal(hub['db']))

        fields_query = []
        for field in hub['fields']:
            f_name = sql.SQL("name: {}").format(sql.Literal(field['name']))
            f_dbtype = sql.SQL("db_type: {}").format(sql.Literal(field['db_type']))

            f_fields = sql.SQL(',').join((f_name, f_dbtype))
            f_fields = sql.SQL('{{{}}}').format(f_fields)

            fields_query.append(f_fields)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields: [{}]").format(fields_query)

        hub_query = sql.SQL(',').join((name, db, fields))
        hub_query = sql.SQL("{{{}}}").format(hub_query)
        query.append(hub_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query
