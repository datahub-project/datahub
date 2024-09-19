SELECT
    DB_NAME()                                                                 AS cur_database_name
  , OBJECT_SCHEMA_NAME(ed.referencing_id)                                     AS dst_schema_name
  , OBJECT_NAME(ed.referencing_id)                                            AS dst_object_name
  , so_dst.type                                                               AS dst_object_type
  , ed.referenced_server_name                                                 AS src_server_name
  , ed.referenced_database_name                                               AS src_database_name
  , COALESCE(OBJECT_SCHEMA_NAME(ed.referenced_id), ed.referenced_schema_name) AS src_schema_name
  , COALESCE(OBJECT_NAME(ed.referenced_id), ed.referenced_entity_name)        AS src_object_name
  , so_src.type                                                               AS src_object_type
FROM sys.sql_expression_dependencies AS ed
LEFT JOIN sys.objects                AS so_dst 
    ON so_dst.object_id = ed.referencing_id
LEFT JOIN sys.objects                AS so_src 
    ON so_src.object_id = ed.referenced_id
WHERE so_dst.type   in ('U ', 'V ')
    AND so_src.type in ('U ', 'V ');