create_sats_query = "WITH $sats as sat_batch " \
                    "UNWIND sat_batch as sat_record " \
                    "CREATE (sat:Sat {uuid: randomUUID(), name: sat_record.name, db: $db_source + '.' + sat_record.name}) " \
                    "WITH sat_record.fields as fields_batch, sat " \
                    "UNWIND fields_batch as field " \
                    "CREATE (sat)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + field.name, attrs: [], dbtype: field.db_type}) " \
                    "RETURN sat.uuid as uuid;"

create_sats_with_hubs_query = "WITH $sats as sat_batch " \
                              "UNWIND sat_batch as sat_record " \
                              "MATCH (hub:Entity {db: $db_source + '.' + sat_record.link.ref_table }) " \
                              "CREATE (hub)-[:SAT {on: [sat_record.link.ref_table_pk, sat_record.link.fk]}]->(sat:Sat {uuid: randomUUID(), name: sat_record.name, db: $db_source + '.' + sat_record.name}) " \
                              "WITH sat_record.fields as fields_batch, sat " \
                              "UNWIND fields_batch as field " \
                              "CREATE (sat)-[:ATTR]->(:Field {name: field.name, db: $db_source + '.' + sat.name + '.' + field.name, attrs: [], dbtype: field.db_type}) " \
                              "RETURN sat.uuid as uuid;"