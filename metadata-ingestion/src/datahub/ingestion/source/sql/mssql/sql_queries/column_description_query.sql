SELECT
    SCHEMA_NAME(T.SCHEMA_ID) AS schema_name
  , T.NAME                   AS table_name
  , C.NAME                   AS column_name
  , EP.VALUE                 AS column_description
FROM sys.tables                    AS T
INNER JOIN sys.all_columns         AS C
    ON C.OBJECT_ID = T.[OBJECT_ID]
INNER JOIN sys.extended_properties AS EP
     ON EP.MAJOR_ID = T.[OBJECT_ID]
    AND EP.MINOR_ID = C.COLUMN_ID
    AND EP.NAME     = 'MS_Description'
    AND EP.[CLASS] = 1;