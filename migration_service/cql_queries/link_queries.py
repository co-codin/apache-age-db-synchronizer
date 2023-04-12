create_links_query = "WITH $links as link_batch " \
                     "UNWIND link_batch as link_record " \
                     "CREATE (link1:Link {uuid: randomUUID(), name: link_record.name, db: $db_source + '.' + link_record.name}, main: True) " \
                     "CREATE (link2:Link {name: link_record.name + '.paired', db: $db_source + '.' + link_record.name + '.paired', main: False}) " \
                     "WITH $fields as fields_batch, link1, link2 " \
                     "UNWIND fields_batch as field " \
                     "CREATE (link1)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + field.name, attrs: [], dbtype: field.db_type}) " \
                     "RETURN link1.uuid as uuid;"

create_links_with_hubs_query = "WITH $links as link_batch " \
                               "UNWIND link_batch as link_record " \
                               "MATCH (hub1:Entity {db: $db_source + '.' + link_record.main_link.hub }) " \
                               "MATCH (hub2:Entity {db: $db_source + '.' + link_record.paired_link.hub }) " \
                               "CREATE (hub1)-[:LINK {on: [link_record.main_link.ref_table_pk, link_record.main_link.fk]}]->(link1:Link {uuid: randomUUID(), name: link_record.name, db: $db_source + '.' + link_record.name, main: True})-[:LINK {on: [link_record.paired_link.fk, link_record.paired_link.ref_table_pk]}]->(hub2) " \
                               "CREATE (hub2)-[:LINK {on: [link_record.paired_link.ref_table_pk, link_record.paired_link.fk]}]->(link2:Link {name: link_record.name + '.paired', db: $db_source + '.' + link_record.name + '.paired', main: False})-[:LINK {on: [link_record.main_link.fk, link_record.main_link.ref_table_pk]}]->(hub1) " \
                               "WITH link_record.fields as fields_batch, link1, link2 " \
                               "UNWIND fields_batch as field " \
                               "CREATE (link1)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + field.db, attrs: [], dbtype: field.db_type}) " \
                               "RETURN link1.uuid as uuid;"


delete_links_query = "WITH $node_names as link_batch " \
                     "UNWIND link_batch as link_name " \
                     "MATCH (e1:Entity)-[:LINK]->(main_link:Link {main:True})-[:LINK]->(e2:Entity)-[:LINK]->(paired_link:Link {main: False})-[:LINK]->(e1:Entity) " \
                     "WITH split(main_link.db, '.')[1] as table_name, link_name, main_link, paired_link " \
                     "WHERE table_name=link_name " \
                     "OPTIONAL MATCH (main_link)-[:ATTR]->(mlf:Field) " \
                     "OPTIONAL MATCH (paired_link)-[:ATTR]->(plf:Field) " \
                     "DETACH DELETE mlf, plf, main_link, paired_link;"
