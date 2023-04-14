create_hubs_query = "WITH $hubs as hub_batch " \
                    "UNWIND hub_batch as hub_record " \
                    "CREATE (hub:Entity {uuid: randomUUID(), name: hub_record.name, db: $db_source + '.' + hub_record.name}) " \
                    "WITH hub_record.fields as fields_batch, hub " \
                    "UNWIND fields_batch as field " \
                    "CREATE (hub)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + hub.name + '.' + field.name, attrs: [], dbtype: field.db_type}) " \
                    "RETURN hub.uuid as uuid;"
