delete_nodes_query = """
                     WITH $node_names as node_batch 
                     UNWIND node_batch as node_name 
                     MATCH (node) 
                     WITH split(node.db, '.')[1] as table_name, node_name, node  
                     WHERE table_name=node_name 
                     OPTIONAL MATCH (node)-[:ATTR]->(f:Field)  
                     WITH node.uuid as uuid, node, f  
                     
                     DETACH DELETE node, f  
                     RETURN uuid;
"""

alter_nodes_query_create_fields = """
                                  WITH $nodes as node_batch 
                                  UNWIND node_batch as node_record 
                                  MATCH (node {db: node_record.db}) 
                                  WITH node_record.fields_to_create as fields_to_create, node 
                                  UNWIND fields_to_create as field 
                                  CREATE (node)-[:ATTR]->(:Field {name: field.name, db: field.name, attrs: [], dbtype: field.db_type});
"""

alter_nodes_query_delete_fields = """
                                  WITH $nodes as node_batch 
                                  UNWIND node_batch as node_record 
                                  MATCH (node {db: node_record.db}) 
                                  WITH node_record.fields_to_delete as fields_to_delete, node 
                                  UNWIND fields_to_delete as field " 
                                  MATCH (node)-[:ATTR]->(f:Field {db: field}) 
                                  DETACH DELETE f;
"""


alter_nodes_query_alter_fields = """
                                 WITH $nodes as node_batch 
                                 UNWIND node_batch as node_record 
                                 MATCH (node {db: node_record.db}) 
                                 WITH node_record.fields_to_alter as fields_to_alter, node 
                                 UNWIND fields_to_alter as field 
                                 MATCH (node)-[:ATTR]->(f:Field {db: field.name}) 
                                 SET f.dbtype=field.new_type;
"""
