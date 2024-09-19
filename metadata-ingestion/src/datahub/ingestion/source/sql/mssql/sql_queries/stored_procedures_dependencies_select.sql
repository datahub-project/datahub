SELECT DISTINCT
    id                      
  , procedure_id            
  , REPLACE(REPLACE(current_db, '[', ''), ']', '')               AS current_db             
  , REPLACE(REPLACE(procedure_schema, '[', ''), ']', '')         AS procedure_schema
  , REPLACE(REPLACE(procedure_name, '[', ''), ']', '')           AS procedure_name
  , type                    
  , REPLACE(REPLACE(referenced_server_name, '[', ''), ']', '')   AS referenced_server_name
  , REPLACE(REPLACE(referenced_database_name, '[', ''), ']', '') AS referenced_database_name
  , REPLACE(REPLACE(referenced_schema_name, '[', ''), ']', '')   AS referenced_schema_name
  , REPLACE(REPLACE(referenced_entity_name, '[', ''), ']', '')   AS referenced_entity_name
  , referenced_id           
  , referenced_object_type  
  , is_selected             
  , is_updated              
  , is_select_all           
  , is_all_columns_found
FROM #ProceduresDependencies
WHERE referenced_object_type IN ('U ', 'V ', 'P ');