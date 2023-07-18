from psycopg2 import sql

create_sats_with_hubs_query = """
                WITH {sats} as sat_batch 
                UNWIND sat_batch as sat_record 

                MERGE (node:Table {{ name: sat_record.link.ref_table }}) 
                
                MERGE (sat:Table {{ name: sat_record.name }})
                SET sat.db = sat_record.db 
                
                CREATE (node)-[:ONE_TO_MANY {{on: [sat_record.link.ref_table_pk, sat_record.link.fk] }}]->(sat)-[:MANY_TO_ONE {{on: [sat_record.link.fk, sat_record.link.ref_table_pk] }}]->(node)

                WITH sat_record.fields as fields_batch, sat 
                UNWIND fields_batch as field 

                CREATE (sat)-[:ATTR]->(:Field {{name: field.name, db: sat.db + '.' + field.name, attrs: [], dbtype: field.db_type}}) 
"""

create_sats_query = """
                WITH {sats} as sat_batch 
                UNWIND sat_batch as sat_record 

                MERGE (sat:Table {{name: sat_record.name, db: sat_record.db}})

                WITH sat_record.fields as fields_batch, sat 
                UNWIND fields_batch as field 

                CREATE (sat)-[:ATTR]->(:Field {{name: field.name, db: sat.db + '.' + field.name, attrs: [], dbtype: field.db_type}}) 
"""


# def construct_create_sats_query(sat_batch) -> str:
#     query = []
#     for sat in sat_batch:
#         name = f"name: '{sat['name']}'"
#         db = f"db: '{sat['db']}'"
#         # sat_record.link.ref_table_pk, sat_record.link.fk
#         link = f"link: {{ ref_table: '{sat['link']['ref_table']}', ref_table_pk: '{sat['link']['ref_table_pk']}', fk: '{sat['link']['fk']}' }}"
#
#         fields_query = []
#         for field in sat['fields']:
#             f_name = f"name: '{field['name']}'"
#             f_dbtype = f"db_type: '{field['db_type']}'"
#
#             f_fields = ','.join((f_name, f_dbtype))
#             f_fields = f'{{{f_fields}}}'
#
#             fields_query.append(f_fields)
#
#         fields_query = ','.join(fields_query)
#         fields = f"fields: [{fields_query}]"
#
#         sat_query = ','.join((name, db, link, fields))
#         sat_query = f'{{{sat_query}}}'
#         query.append(sat_query)
#
#     query = ','.join(query)
#     query = f'[{query}]'
#     return query

def construct_create_sats_query(sat_batch, is_linked: bool) -> sql.Composable:
    query = []
    for sat in sat_batch:
        name = sql.SQL("name: {}").format(sql.Literal(sat['name']))
        db = sql.SQL("db: {}").format(sql.Literal(sat['db']))
        # sat_record.link.ref_table_pk, sat_record.link.fk
        if is_linked:
            link = sql.SQL("link: {{ref_table: {}, ref_table_pk: {}, fk: {}}}").format(
                sql.Literal(sat['link']['ref_table']),
                sql.Literal(sat['link']['ref_table_pk']),
                sql.Literal(sat['link']['fk'])
            )

        fields_query = []
        for field in sat['fields']:
            f_name = sql.SQL("name: {}").format(sql.Literal(field['name']))
            f_dbtype = sql.SQL("db_type: {}").format(sql.Literal(field['db_type']))

            f_fields = sql.SQL(',').join((f_name, f_dbtype))
            f_fields = sql.SQL('{{{}}}').format(f_fields)

            fields_query.append(f_fields)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields: [{}]").format(fields_query)

        if is_linked:
            sat_query = sql.SQL(',').join((name, db, link, fields))
        else:
            sat_query = sql.SQL(',').join((name, db, fields))

        sat_query = sql.SQL("{{{}}}").format(sat_query)
        query.append(sat_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query
