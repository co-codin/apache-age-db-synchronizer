delete_nodes_query = "WITH $node_names as node_batch " \
                     "UNWIND node_batch as node_name " \
                     "MATCH (node) " \
                     "WITH split(node.db, '.')[1] as table_name, node_name, node " \
                     "WHERE table_name=node_name " \
                     "OPTIONAL MATCH (node)-[:ATTR]->(f:Field) " \
                     "WITH node.uuid as uuid, node, f " \
                     "DETACH DELETE node, f " \
                     "RETURN uuid;"
