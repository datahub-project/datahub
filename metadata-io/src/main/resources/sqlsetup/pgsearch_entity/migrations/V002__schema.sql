-- DataHub Postgres entity search: schema + registry + hybrid row template (setup framework only).
-- Session GUC set by SqlSetup: datahub.pgsearch_entity_fulltext_language (from fulltext.defaultLanguage).
-- Session: SET search_path + token __PGSEARCH_PREFIX__. Schema COMMENT applied from Java.
--
-- Non-goals: graph, system metadata store, timeseries, MAE/reindex, SearchService, tsvector triggers,
-- hybrid RRF / rank fusion (query layer).
--
-- Elasticsearch entity index v3 parity (read-only design reference):
--   - One v3 physical index per entity-registry search group; here search_group maps to a physical
--     table name via {prefix}_group_registry.
--   - v1 uses one shared row table for all groups; each URN appears at most once (PK is urn).
--     search_group is denormalized for filtering (same entity cannot span multiple groups).
--     Registry rows are inserted by SqlSetup Java (PgSearchEntitySchemaStep) from EntityRegistry
--     search groups (same basis as ES v3 per-group indices), plus the PDL @Entity default search
--     group when absent.
--   - Tier N: co-located plain text, tsvector, and (optional) embedding for @Searchable searchTier N.
--   - search_extras: JSONB for Elasticsearch _search content other than tier_* consolidated text.

CREATE TABLE IF NOT EXISTS __PGSEARCH_PREFIX___group_registry (
    search_group TEXT PRIMARY KEY,
    physical_table_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS __PGSEARCH_PREFIX___search_row (
    urn TEXT NOT NULL,
    search_group TEXT NOT NULL REFERENCES __PGSEARCH_PREFIX___group_registry (search_group),
    entity_type TEXT NOT NULL,
    document JSONB,
    search_extras JSONB,
    systemmetadata JSONB,
__PGSEARCH_TIER_TEXT_COLUMNS__
__PGSEARCH_TIER_TSVECTOR_COLUMNS__
__PGSEARCH_TIER_EMBEDDING_VECTOR_COLUMNS__
    PRIMARY KEY (urn)
);

__PGSEARCH_TIER_TSVECTOR_INDEXES__

CREATE INDEX IF NOT EXISTS idx___PGSEARCH_PREFIX___search_row_search_group_entity_type
    ON __PGSEARCH_PREFIX___search_row (search_group, entity_type);

CREATE INDEX IF NOT EXISTS idx___PGSEARCH_PREFIX___search_row_entity_type
    ON __PGSEARCH_PREFIX___search_row (entity_type);

CREATE INDEX IF NOT EXISTS idx___PGSEARCH_PREFIX___search_row_systemmetadata_gin
    ON __PGSEARCH_PREFIX___search_row USING gin (systemmetadata jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx___PGSEARCH_PREFIX___search_row_search_extras_gin
    ON __PGSEARCH_PREFIX___search_row USING gin (search_extras jsonb_path_ops);

COMMENT ON TABLE __PGSEARCH_PREFIX___search_row IS
    'Postgres entity search DDL (SqlSetup); PK is urn (one row per entity). search_group filters the ES-style scope; tiered lexical/vector channels; non-tier JSON columns.';

COMMENT ON COLUMN __PGSEARCH_PREFIX___search_row.entity_type IS
    'Denormalized document._entityType for btree filters (prefer over JSON on document).';

COMMENT ON COLUMN __PGSEARCH_PREFIX___search_row.search_extras IS
    'JSONB: Elasticsearch _search object minus tier_* consolidated text (e.g. searchLabel channels); population is incremental.';

COMMENT ON COLUMN __PGSEARCH_PREFIX___search_row.systemmetadata IS
    'JSONB: map aspect name -> SystemMetadata object; extracted from document._aspects.<aspect>._systemmetadata.';

COMMENT ON COLUMN __PGSEARCH_PREFIX___search_row.search_text_tier1 IS
    'Plain text for search tier 1 (co-located with search_vector_tier1 and optional per-tier pgvector embedding when enabled); see postgres.pgSearch.entity.fulltext.tierTsvectorColumnCount.';

COMMENT ON COLUMN __PGSEARCH_PREFIX___search_row.search_vector_tier1 IS
    'tsvector for search tier 1; see postgres.pgSearch.entity.fulltext.tierTsvectorColumnCount.';
