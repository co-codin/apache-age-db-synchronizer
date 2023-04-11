delete_nodes_query = "WITH $node_names as node_batch " \
                     "UNWIND node_batch as node_name " \
                     "MATCH (node) " \
                     "WITH split(node.db, '.')[1] as table_name, node_name, node " \
                     "WHERE table_name=node_name " \
                     "OPTIONAL MATCH (node)-[:ATTR]->(f:Field) " \
                     "WITH node.uuid as uuid, node, f " \
                     "DETACH DELETE node, f " \
                     "RETURN uuid;"

delete_links_query = "WITH $node_names as link_batch " \
                     "UNWIND link_batch as link_name " \
                     "MATCH (e1:Entity)-[:LINK]->(main_link:Link {main:True})-[:LINK]->(e2:Entity)-[:LINK]->(paired_link:Link {main: False})-[:LINK]->(e1:Entity) " \
                     "WITH split(main_link.db, '.')[1] as table_name, link_name, main_link, paired_link " \
                     "WHERE table_name=link_name " \
                     "OPTIONAL MATCH (main_link)-[:ATTR]->(mlf:Field) " \
                     "OPTIONAL MATCH (paired_link)-[:ATTR]->(plf:Field) " \
                     "DETACH DELETE mlf, plf, main_link, paired_link;"


create_hubs_query = "WITH $hubs as hub_batch " \
                    "UNWIND hub_batch as hub_record " \
                    "CREATE (hub:Entity {uuid: randomUUID(), name: hub_record.name, db: $db_source + '.' + hub_record.name}) " \
                    "WITH hub_record.fields as fields_batch, hub " \
                    "UNWIND fields_batch as field " \
                    "CREATE (hub)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + field.name, attrs: [], dbtype: field.db_type}) " \
                    "RETURN hub.uuid as uuid;"
