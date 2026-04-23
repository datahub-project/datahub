-- ============================================================================
-- Snowflake Tags Extraction - Account-Level Query
-- ============================================================================
-- Extracts ALL tags across the entire account using SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
-- Covers: database_tags, schema_tags, table_tags, column_tags
--
-- USAGE:
--   Executed once per account (not per-database)
--   Results can be filtered by object_database in Python if needed
--
-- PERFORMANCE: Single query replaces 268 per-database tag queries (4 × 67 dbs)
-- LATENCY: ACCOUNT_USAGE has ~45 minute metadata latency (acceptable for inventory)
--
-- REQUIRES: SNOWFLAKE.ACCOUNT_USAGE grant (typically IMPORTED PRIVILEGES)
-- ============================================================================

SELECT
    tag_database::VARCHAR AS tag_database,
    tag_schema::VARCHAR AS tag_schema,
    tag_name::VARCHAR AS tag_name,
    tag_value::VARCHAR AS tag_value,
    object_database::VARCHAR AS object_database,
    object_schema::VARCHAR AS object_schema,
    object_name::VARCHAR AS object_name,
    column_name::VARCHAR AS column_name,
    domain::VARCHAR AS domain,
    object_id::VARCHAR AS object_id,
    apply_method::VARCHAR AS apply_method,
    level::VARCHAR AS level,
    'TAG' AS metadata_source
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE domain IN ('DATABASE', 'SCHEMA', 'TABLE', 'COLUMN')
    AND object_deleted IS NULL
ORDER BY
    object_database,
    object_schema,
    object_name,
    column_name,
    tag_database,
    tag_schema,
    tag_name
;
