from psycopg2 import sql

create_links_query = """
                     WITH {links} as link_batch 
                     UNWIND link_batch as link_record 

                     MERGE (link:Table {{ name: link_record.name }})
                     SET link.db = link_record.db

                     WITH link_record.fields as fields_batch, link
                     UNWIND fields_batch as field  

                     CREATE (link)-[:ATTR]->(:Field {{name: field.name, db: field.name, attrs: [], dbtype: field.db_type}}) 
"""

create_links_with_hubs_query = """
                    WITH {links} as link_batch 
                    UNWIND link_batch as link_record 

                    MERGE (hub1:Table {{ name: link_record.main_link.ref_table }}) 
                    MERGE (hub2:Table {{ name: link_record.paired_link.ref_table }}) 
                    
                    MERGE (link:Table {{ name: link_record.name }})
                    SET link.db = link_record.db

                    CREATE (hub1)-[:ONE_TO_MANY {{on: [link_record.main_link.ref_table_pk, link_record.main_link.fk] }}]->(link)-[:MANY_TO_ONE {{ on: [link_record.paired_link.fk, link_record.paired_link.ref_table_pk] }}]->(hub2) 
                    CREATE (hub2)-[:ONE_TO_MANY {{on: [link_record.paired_link.ref_table_pk, link_record.paired_link.fk] }}]->(link)-[:MANY_TO_ONE {{ on: [link_record.main_link.fk, link_record.main_link.ref_table_pk] }}]->(hub1) 

                    WITH link_record.fields as fields_batch, link, link
                    UNWIND fields_batch as field 

                    CREATE (link)-[:ATTR]->(:Field {{ name: field.name, db: field.name, attrs: [], dbtype: field.db_type }}) 
"""

delete_links_query = """
                     WITH {nodes} as link_batch  
                     UNWIND link_batch as link_name  

                     MATCH (e1:Table)-[:MANY_TO_MANY]->(main_link:Link {{ name: link_name, main:'True' }})-[:MANY_TO_MANY]->(e2:Table)-[:MANY_TO_MANY]->(paired_link:Link {{ name: link_name, main: 'False' }})-[:MANY_TO_MANY]->(e1:Table)  

                     OPTIONAL MATCH (main_link)-[:ATTR]->(mlf:Field)  
                     OPTIONAL MATCH (paired_link)-[:ATTR]->(plf:Field)  
                     DETACH DELETE mlf, plf, main_link, paired_link
"""


def construct_create_links_query(link_batch, is_linked: bool) -> sql.Composable:
    query = []
    for link in link_batch:
        name = sql.SQL("name: {}").format(sql.Literal(link['name']))
        db = sql.SQL("db: {}").format(sql.Literal(link['db']))

        if is_linked:
            main_link = sql.SQL("main_link: {{ref_table: {}, ref_table_pk: {}, fk: {}}}").format(
                sql.Literal(link['main_link']['ref_table']),
                sql.Literal(link['main_link']['ref_table_pk']),
                sql.Literal(link['main_link']['fk'])
            )
            paired_link = sql.SQL("paired_link: {{ref_table: {}, ref_table_pk: {}, fk: {}}}").format(
                sql.Literal(link['paired_link']['ref_table']),
                sql.Literal(link['paired_link']['ref_table_pk']),
                sql.Literal(link['paired_link']['fk'])
            )

        fields_query = []
        for field in link['fields']:
            f_name = sql.SQL("name: {}").format(sql.Literal(field['name']))
            f_dbtype = sql.SQL("db_type: {}").format(sql.Literal(field['db_type']))

            f_fields = sql.SQL(',').join((f_name, f_dbtype))
            f_fields = sql.SQL('{{{}}}').format(f_fields)

            fields_query.append(f_fields)

        fields_query = sql.SQL(',').join(fields_query)
        fields = sql.SQL("fields: [{}]").format(fields_query)

        if is_linked:
            link_query = sql.SQL(',').join((name, db, main_link, paired_link, fields))
        else:
            link_query = sql.SQL(',').join((name, db, fields))

        link_query = sql.SQL("{{{}}}").format(link_query)
        query.append(link_query)

    query = sql.SQL(',').join(query)
    query = sql.SQL('[{}]').format(query)
    return query
