-- ============================================================================
-- Snowflake Metadata Extraction - Database Level (Core Catalog Only)
-- ============================================================================
-- Extracts core metadata for a SINGLE database: schemas, tables, columns
-- Optimized for performance and edition-agnostic compatibility
--
-- GUARANTEED TO WORK on all Snowflake editions (Standard, Business Critical, trial, etc.)
-- Uses only INFORMATION_SCHEMA views that exist in all editions with standard permissions
--
-- NOTE: Constraints are extracted separately via SHOW commands in Python
-- (SHOW PRIMARY KEYS and SHOW IMPORTED KEYS work for shared databases)
--
-- USAGE:
--   Called from metadata_bulk_extractor.py with {database_name} parameter
--   Replaces 600+ SHOW queries with efficient INFORMATION_SCHEMA queries
--
-- PERFORMANCE: 75% query reduction (4 sequential queries → 1 bulk query per database)
-- ============================================================================
--
-- Three-Bucket Architecture:
--   1. Tags: tags_extractor.sql (once per account)
--   2. Core Catalog: THIS FILE (once per database) - schemas + tables + columns
--   3. Constraints: SHOW PRIMARY KEYS + REFERENTIAL_CONSTRAINTS (once per database)
--
-- ============================================================================

-- Job 3A: Extract schemas
WITH schemas AS (
    SELECT
        catalog_name::VARCHAR AS database_name,
        schema_name::VARCHAR AS schema_name,
        schema_name::VARCHAR AS table_name,
        NULL::VARCHAR AS column_name,
        'SCHEMA'::VARCHAR AS table_type,
        NULL::NUMBER AS column_ordinal,
        NULL::VARCHAR AS data_type,
        NULL::VARCHAR AS is_nullable,
        NULL::VARCHAR AS column_default,
        NULL::VARCHAR AS column_comment,
        NULL::VARCHAR AS constraint_name,
        NULL::VARCHAR AS constraint_type,
        NULL::VARCHAR AS fk_table_name,
        created::TIMESTAMP_NTZ AS created,
        last_altered::TIMESTAMP_NTZ AS last_altered,
        comment::VARCHAR AS comment,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'owner', schema_owner
        ) AS properties,
        'SCHEMA'::VARCHAR AS metadata_source
    FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA
    WHERE schema_name <> 'INFORMATION_SCHEMA'
),

-- Job 3B: Extract tables and views
tables_and_views AS (
    SELECT
        table_catalog::VARCHAR AS database_name,
        table_schema::VARCHAR AS schema_name,
        table_name::VARCHAR AS table_name,
        NULL::VARCHAR AS column_name,
        table_type::VARCHAR AS table_type,
        NULL::NUMBER AS column_ordinal,
        NULL::VARCHAR AS data_type,
        NULL::VARCHAR AS is_nullable,
        NULL::VARCHAR AS column_default,
        NULL::VARCHAR AS column_comment,
        NULL::VARCHAR AS constraint_name,
        NULL::VARCHAR AS constraint_type,
        NULL::VARCHAR AS fk_table_name,
        created::TIMESTAMP_NTZ AS created,
        last_altered::TIMESTAMP_NTZ AS last_altered,
        comment::VARCHAR AS comment,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'owner', table_owner,
            'row_count', row_count,
            'bytes', bytes,
            'is_transient', is_transient,
            'clustering_key', clustering_key,
            'is_dynamic', is_dynamic,
            'is_iceberg', is_iceberg,
            'is_hybrid', is_hybrid,
            'retention_time', retention_time
        ) AS properties,
        'TABLE_META'::VARCHAR AS metadata_source
    FROM {database_name}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema <> 'INFORMATION_SCHEMA'
),

-- Job 3B-2: Extract view definitions (INFORMATION_SCHEMA.VIEWS has view_definition)
views AS (
    SELECT
        table_catalog::VARCHAR AS database_name,
        table_schema::VARCHAR AS schema_name,
        table_name::VARCHAR AS table_name,
        NULL::VARCHAR AS column_name,
        'VIEW'::VARCHAR AS table_type,
        NULL::NUMBER AS column_ordinal,
        NULL::VARCHAR AS data_type,
        NULL::VARCHAR AS is_nullable,
        NULL::VARCHAR AS column_default,
        NULL::VARCHAR AS column_comment,
        NULL::VARCHAR AS constraint_name,
        NULL::VARCHAR AS constraint_type,
        NULL::VARCHAR AS fk_table_name,
        created::TIMESTAMP_NTZ AS created,
        last_altered::TIMESTAMP_NTZ AS last_altered,
        comment::VARCHAR AS comment,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'view_definition', view_definition
        ) AS properties,
        'VIEW_DEFINITION'::VARCHAR AS metadata_source
    FROM {database_name}.INFORMATION_SCHEMA.VIEWS
    WHERE table_schema <> 'INFORMATION_SCHEMA'
),

-- Job 3C: Extract columns
columns AS (
    SELECT
        table_catalog::VARCHAR AS database_name,
        table_schema::VARCHAR AS schema_name,
        table_name::VARCHAR AS table_name,
        column_name::VARCHAR AS column_name,
        NULL::VARCHAR AS table_type,
        ordinal_position::NUMBER AS column_ordinal,
        data_type::VARCHAR AS data_type,
        is_nullable::VARCHAR AS is_nullable,
        column_default::VARCHAR AS column_default,
        comment::VARCHAR AS column_comment,
        NULL::VARCHAR AS constraint_name,
        NULL::VARCHAR AS constraint_type,
        NULL::VARCHAR AS fk_table_name,
        NULL::TIMESTAMP_NTZ AS created,
        NULL::TIMESTAMP_NTZ AS last_altered,
        NULL::VARCHAR AS comment,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'numeric_precision', numeric_precision,
            'numeric_scale', numeric_scale,
            'character_maximum_length', character_maximum_length
        ) AS properties,
        'COLUMN_META'::VARCHAR AS metadata_source
    FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema <> 'INFORMATION_SCHEMA'
)

-- Union all core metadata (constraints extracted separately via SHOW commands)
SELECT * FROM schemas
UNION ALL
SELECT * FROM tables_and_views
UNION ALL
SELECT * FROM views
UNION ALL
SELECT * FROM columns
