SELECT
    pr.name AS procedure_name
  , s.name  AS schema_name
FROM [{db_name}].[sys].[procedures]    AS pr
INNER JOIN [{db_name}].[sys].[schemas] AS s
    ON pr.schema_id = s.schema_id
WHERE s.name = '{schema}';