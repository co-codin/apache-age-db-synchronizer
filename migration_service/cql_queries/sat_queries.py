create_sats_query = """
                    WITH $sats as sat_batch  
                    UNWIND sat_batch as sat_record  
                    
                    CREATE (sat:Sat {uuid: randomUUID(), name: sat_record.name, db: sat_record.db})  
                    
                    WITH sat_record.fields as fields_batch, sat  
                    UNWIND fields_batch as field  
                    
                    CREATE (sat)-[:ATTR]->(:Field {name: field.name, db: field.name, attrs: [], dbtype: field.db_type})  
                    RETURN sat.uuid as uuid;
"""

create_sats_with_hubs_query = """
                WITH $sats as sat_batch 
                UNWIND sat_batch as sat_record 
                
                MERGE (node {name: sat_record.link.ref_table}) 
                CREATE (node)-[:SAT {on: [sat_record.link.ref_table_pk, sat_record.link.fk]}]->(sat:Sat {uuid: randomUUID(), name: sat_record.name, db: sat_record.db}) 
                
                WITH sat_record.fields as fields_batch, sat 
                UNWIND fields_batch as field 
                
                CREATE (sat)-[:ATTR]->(:Field {name: field.name, db: field.name, attrs: [], dbtype: field.db_type}) 
                RETURN sat.uuid as uuid;
"""