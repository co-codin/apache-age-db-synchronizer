create_hubs_query = """
                    WITH %s as hub_batch
                    UNWIND hub_batch as hub_record
                    CREATE (hub:Entity {uuid: randomUUID(), name: hub_record.name, db: hub_record.db})
                    UNWIND hub as field
                    CREATE (hub)-[:ATTR]->(:Field {name: field.name, db: field.name, attrs: [], dbtype: field.db_type})
                    RETURN hub
"""

alter_hubs_query_create_fields = "WITH $hubs as hub_batch " \
                                 "UNWIND hub_batch as hub_record " \
                                 "MATCH (hub:Entity {db: $db_source + '.' + hub_record.name}) " \
                                 "WITH hub_record.fields_to_create as fields_to_create, hub " \
                                 "UNWIND fields_to_create as field " \
                                 "CREATE (hub)-[:ATTR]->(:Field {name: field.name, db: field.name, attrs: [], dbtype: field.db_type});"

alter_hubs_query_delete_fields = "WITH $hubs as hub_batch " \
                                 "UNWIND hub_batch as hub_record " \
                                 "MATCH (hub:Entity {db: $db_source + '.' + hub_record.name}) " \
                                 "WITH hub_record.fields_to_delete as fields_to_delete, hub " \
                                 "UNWIND fields_to_delete as field " \
                                 "MATCH (hub)-[:ATTR]->(f:Field {db: hub.db + '.' + field}) " \
                                 "DETACH DELETE f;"


alter_hubs_query_alter_fields = "WITH $hubs as hub_batch " \
                                "UNWIND hub_batch as hub_record " \
                                "MATCH (hub:Entity {db: $db_source + '.' + hub_record.name}) " \
                                "WITH hub_record.fields_to_alter as fields_to_alter, hub " \
                                "UNWIND fields_to_alter as field " \
                                "MATCH (hub)-[:ATTR]->(f:Field {db: field.name}) " \
                                "SET f.dbtype=field.new_type;"
