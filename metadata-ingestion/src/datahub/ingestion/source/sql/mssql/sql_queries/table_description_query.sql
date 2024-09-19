SELECT
    SCHEMA_NAME(T.SCHEMA_ID) AS schema_name
  , T.NAME                   AS table_name
  , EP.VALUE                 AS table_description
FROM sys.tables                    AS T
INNER JOIN sys.extended_properties AS EP
     ON EP.MAJOR_ID = T.OBJECT_ID
    AND EP.MINOR_ID = 0
    AND EP.NAME     = 'MS_Description'
    AND EP.CLASS    = 1;
