-- =============================================================================
-- DataHub Graph Schema for pgRouting Integration
-- =============================================================================
-- SqlSetup replaces __PGGRAPH_PREFIX__ with postgres.pgGraph.tablePrefix (default metadata_graph).
-- 
-- This file creates the complete graph database schema that enables DataHub to
-- leverage pgRouting for advanced graph analytics.
-- 
-- DESIGN OVERVIEW:
-- The schema implements a partitioned graph structure optimized for:
-- - High-performance graph traversal using pgRouting algorithms
-- - Efficient storage and retrieval of large-scale metadata graphs
-- - Flexible property storage using JSONB for extensible metadata
-- - Soft deletion support for maintaining graph integrity during analysis
-- - Automatic edge state management derived from vertex removal states
--
-- CONSISTENCY DESIGN:
-- The schema enforces strict logical consistency between vertices and edges through
-- a trigger-based architecture with the following principles:
--
-- 1. VERTEX AUTHORITY: Only vertices control their removal state (removed=TRUE/FALSE)
-- 2. AUTOMATIC EDGE CONSISTENCY: Edges automatically follow vertex states via triggers
-- 3. NO MANUAL EDGE STATE MANAGEMENT: Edge removal status is never set explicitly
-- 4. BIDIRECTIONAL CASCADE: 
--    - Vertex deletion → All connected edges are automatically soft-deleted
--    - Vertex restoration → Edges are restored only when both vertices are active
-- 5. SAFETY GUARANTEES:
--    - An edge can never be active if either vertex is removed
--    - No orphaned edges or inconsistent graph states
--    - All edge operations are deterministic based on vertex states
--
-- KEY COMPONENTS:
-- - Core graph tables (vertices, edges, edge types)
-- - Performance optimization (partitioning, indexing, views)
-- - pgRouting integration views
-- - SQLG compatibility layer for existing DataHub code
-- - Automatic consistency triggers (cascade_vertex_soft_delete, cascade_vertex_soft_restore)
--
-- DEPENDENCIES:
-- - postgis extension (for spatial operations if needed)
-- - pgrouting extension (for graph algorithms)
-- - datahub.pgrouting_partition_count setting (for edge table partitioning)
-- =============================================================================


DO $$
DECLARE
    partition_count INTEGER;
    i INTEGER;
BEGIN
    -- Set default partition count if not configured
    BEGIN
        partition_count := current_setting('datahub.pgrouting_partition_count')::integer;
    EXCEPTION WHEN OTHERS THEN
        partition_count := 2; -- Default to 2 partitions
        RAISE NOTICE 'Using default partition count: %', partition_count;
    END;
    -- =============================================================================
    -- EXTENSION VALIDATION
    -- =============================================================================
    -- Ensure required extensions are available before proceeding
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'postgis'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pgrouting'
    ) THEN
        RAISE EXCEPTION 'Required extensions postgis and pgrouting must be installed first';
    END IF;

    RAISE NOTICE 'Creating graph schema with partition count: %', partition_count;
    
    -- =============================================================================
    -- CORE GRAPH STRUCTURE TABLES
    -- =============================================================================
    
    -- EDGE TYPES TABLE
    -- Defines the semantic meaning of different relationship types in the graph
    -- Used for filtering edges during graph traversal and analysis
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___edge_types (
        id SMALLINT PRIMARY KEY,
        type_name VARCHAR(200) UNIQUE NOT NULL
    );
    
    -- VERTICES TABLE
    -- Represents entities in the metadata graph (datasets, users, etc.)
    -- Uses xxhash64 IDs for efficient storage and consistent hashing
    -- Supports soft deletion and flexible property storage via JSONB
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___vertices (
        xxhash64_id BIGINT PRIMARY KEY,  -- Provided by client
        urn VARCHAR(1024) UNIQUE NOT NULL,  -- DataHub URN identifier
        removed BOOLEAN DEFAULT FALSE,      -- Soft deletion flag
        properties JSONB DEFAULT '{}'::jsonb  -- Flexible metadata storage
    );

    -- VERTICES INDEXES
    -- Optimized for common query patterns in graph operations
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___vertices_urn_hash ON __PGGRAPH_PREFIX___vertices USING HASH (urn);
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___vertices_urn_btree ON __PGGRAPH_PREFIX___vertices(urn varchar_pattern_ops) WHERE removed = FALSE;
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___vertices_removed ON __PGGRAPH_PREFIX___vertices(removed) WHERE removed = FALSE;

    -- EDGES TABLE (Partitioned by source_id hash)
    -- Represents relationships between entities in the graph
    -- Partitioned for performance on large graphs
    -- Composite primary key ensures uniqueness of relationships
    
    -- Check if table exists before creating it
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '__PGGRAPH_PREFIX___edges') THEN
        RAISE NOTICE 'Creating __PGGRAPH_PREFIX___edges table and partitions...';
        
        -- Create the partitioned table
        CREATE TABLE __PGGRAPH_PREFIX___edges (
            source_id BIGINT NOT NULL REFERENCES __PGGRAPH_PREFIX___vertices(xxhash64_id) ON DELETE CASCADE,
            target_id BIGINT NOT NULL REFERENCES __PGGRAPH_PREFIX___vertices(xxhash64_id) ON DELETE CASCADE,
            edge_type SMALLINT NOT NULL REFERENCES __PGGRAPH_PREFIX___edge_types(id) ON DELETE CASCADE,
            owner_id BIGINT NOT NULL DEFAULT 0,  -- For ownership tracking
            removed BOOLEAN DEFAULT FALSE,       -- Soft deletion flag
            created_at BIGINT NOT NULL,         -- Unix timestamp
            updated_at BIGINT NOT NULL,         -- Unix timestamp
            properties JSONB NOT NULL DEFAULT '{}'::jsonb,  -- Edge metadata
            PRIMARY KEY (source_id, edge_type, target_id, owner_id)
        ) PARTITION BY HASH (source_id);
        
        -- CREATE EDGE PARTITIONS
        -- Hash partitioning improves query performance on large edge sets
        -- Each partition handles a subset of edges based on source_id hash
        FOR i IN 0..(partition_count-1) LOOP
            BEGIN
                EXECUTE format('CREATE TABLE __PGGRAPH_PREFIX___edges_%s PARTITION OF __PGGRAPH_PREFIX___edges 
                            FOR VALUES WITH (modulus %s, remainder %s)', i, partition_count, i);
                RAISE NOTICE 'Created partition __PGGRAPH_PREFIX___edges_%', i;
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'Partition __PGGRAPH_PREFIX___edges_% already exists, skipping', i;
            WHEN OTHERS THEN
                RAISE NOTICE 'Error creating partition __PGGRAPH_PREFIX___edges_%: %', i, SQLERRM;
            END;
        END LOOP;
        RAISE NOTICE 'Created % partitions for __PGGRAPH_PREFIX___edges table', partition_count;
    ELSE
        RAISE NOTICE 'Table __PGGRAPH_PREFIX___edges already exists, checking for partitions...';
        
        -- Check if partitions exist, create them if they don't
        IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname LIKE '__PGGRAPH_PREFIX___edges_%' LIMIT 1) THEN
            RAISE NOTICE 'No partitions found, creating partitions for existing table...';
            FOR i IN 0..(partition_count-1) LOOP
                BEGIN
                    EXECUTE format('CREATE TABLE __PGGRAPH_PREFIX___edges_%s PARTITION OF __PGGRAPH_PREFIX___edges 
                                FOR VALUES WITH (modulus %s, remainder %s)', i, partition_count, i);
                    RAISE NOTICE 'Created partition __PGGRAPH_PREFIX___edges_%', i;
                EXCEPTION WHEN duplicate_table THEN
                    RAISE NOTICE 'Partition __PGGRAPH_PREFIX___edges_% already exists, skipping', i;
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error creating partition __PGGRAPH_PREFIX___edges_%: %', i, SQLERRM;
                END;
            END LOOP;
            RAISE NOTICE 'Created % partitions for existing __PGGRAPH_PREFIX___edges table', partition_count;
        ELSE
            RAISE NOTICE 'Partitions already exist for __PGGRAPH_PREFIX___edges table';
        END IF;
    END IF;

    -- EDGES INDEXES
    -- Optimized for graph traversal queries
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___edges_active_source ON __PGGRAPH_PREFIX___edges(source_id, edge_type) WHERE removed = FALSE;
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___edges_active_target ON __PGGRAPH_PREFIX___edges(target_id, edge_type) WHERE removed = FALSE;
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___edges_temporal_brin ON __PGGRAPH_PREFIX___edges USING BRIN (created_at, updated_at) WITH (pages_per_range = 128);

    -- =============================================================================
    -- PGROUTING INTEGRATION VIEWS
    -- =============================================================================
    -- These views transform the DataHub graph structure into formats compatible
    -- with pgRouting algorithms and existing DataHub code
    
    -- MAIN PGROUTING NETWORK VIEW
    -- Primary interface for pgRouting algorithms
    -- Maps DataHub graph structure to pgRouting's expected format
    -- Stable BIGINT edge id for pgRouting (same composite key as graph PK; deterministic across queries).
    CREATE OR REPLACE VIEW __PGGRAPH_PREFIX___pgrouting_network AS
    SELECT 
        (abs(('x' || substr(md5(source_id::text || '|' || target_id::text || '|' || edge_type::text || '|' || owner_id::text), 1, 16))::bit(64)::bigint)) AS id,
        source_id AS source,                 -- xxhash64 of source vertex
        target_id AS target,                 -- xxhash64 of target vertex
        1.0::double precision AS cost,                  -- Constant cost (pgRouting uses double precision)
        1.0::double precision AS reverse_cost,          -- Constant reverse cost
        edge_type,                           -- Edge type for filtering
        owner_id,                            -- Ownership information
        properties                           -- Edge metadata
    FROM __PGGRAPH_PREFIX___edges
    WHERE removed = FALSE;

    -- SQLG COMPATIBILITY VIEW - EDGES
    -- Provides compatibility with existing DataHub SQLG-based code
    -- Transforms internal IDs to human-readable URNs and type names
    CREATE OR REPLACE VIEW __PGGRAPH_PREFIX___sqlg_edges AS
    SELECT 
        -- Composite ID using xxhash64 values for compatibility
        source_id || ':::' || target_id || ':::' || edge_type || ':::' || owner_id AS ID,
        vs.urn AS out_vertex,                -- Source vertex URN
        vt.urn AS in_vertex,                 -- Target vertex URN
        et.type_name AS relationship_type,   -- Human-readable edge type
        owner_id AS lifecycle_owner_id,      -- Ownership tracking
        e.properties,                        -- Edge metadata
        e.created_at,                        -- Creation timestamp
        e.updated_at                         -- Update timestamp
    FROM __PGGRAPH_PREFIX___edges e
    JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
    JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
    JOIN __PGGRAPH_PREFIX___edge_types et ON e.edge_type = et.id
    WHERE e.removed = FALSE 
    AND vs.removed = FALSE 
    AND vt.removed = FALSE;

    -- SQLG COMPATIBILITY VIEW - VERTICES
    -- Provides compatibility with existing DataHub SQLG-based code
    -- Maps internal structure to expected SQLG format
    CREATE OR REPLACE VIEW __PGGRAPH_PREFIX___sqlg_vertices AS
    SELECT 
        xxhash64_id AS ID,                   -- Vertex identifier
        urn,                                 -- DataHub URN
        properties,                          -- Vertex metadata
        removed                              -- Deletion status
    FROM __PGGRAPH_PREFIX___vertices;

    -- VERTEX NEIGHBORS VIEW
    -- Optimized view for graph traversal and neighbor discovery
    -- Combines outgoing and incoming edges for comprehensive neighbor analysis
    CREATE OR REPLACE VIEW __PGGRAPH_PREFIX___vertex_neighbors AS
    SELECT 
        v.urn AS vertex_urn,                 -- Current vertex URN
        v.xxhash64_id AS vertex_id,          -- Current vertex ID
        'outgoing' AS direction,             -- Edge direction
        et.type_name AS edge_type,           -- Relationship type
        vt.urn AS neighbor_urn,              -- Neighbor vertex URN
        vt.xxhash64_id AS neighbor_id,       -- Neighbor vertex ID
        e.properties AS edge_properties      -- Edge metadata
    FROM __PGGRAPH_PREFIX___vertices v
    JOIN __PGGRAPH_PREFIX___edges e ON v.xxhash64_id = e.source_id
    JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
    JOIN __PGGRAPH_PREFIX___edge_types et ON e.edge_type = et.id
    WHERE v.removed = FALSE 
    AND vt.removed = FALSE 
    AND e.removed = FALSE
    UNION ALL
    SELECT 
        v.urn AS vertex_urn,                 -- Current vertex URN
        v.xxhash64_id AS vertex_id,          -- Current vertex ID
        'incoming' AS direction,             -- Edge direction
        et.type_name AS edge_type,           -- Relationship type
        vs.urn AS neighbor_urn,              -- Neighbor vertex URN
        vs.xxhash64_id AS neighbor_id,       -- Neighbor vertex ID
        e.properties AS edge_properties      -- Edge metadata
    FROM __PGGRAPH_PREFIX___vertices v
    JOIN __PGGRAPH_PREFIX___edges e ON v.xxhash64_id = e.target_id
    JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
    JOIN __PGGRAPH_PREFIX___edge_types et ON e.edge_type = et.id
    WHERE v.removed = FALSE 
    AND vs.removed = FALSE 
    AND e.removed = FALSE;

    RAISE NOTICE 'Graph schema created successfully';
END $$;

-- =============================================================================
-- DataHub Graph Functions for pgRouting Integration
-- =============================================================================
-- 
-- This section contains the core set of database functions that implement
-- DataHub's graph analytics capabilities.
--
-- DESIGN OVERVIEW:
-- The functions provide a comprehensive API for:
-- 1. Graph structure management (vertices, edges, edge types)
-- 2. Performance monitoring and optimization
-- 3. Bulk operations for efficient data processing
--
-- FUNCTION CATEGORIES:
-- - Core Graph Operations: CRUD operations for vertices and edges
-- - Performance Utilities: Statistics, monitoring, and optimization
-- - Bulk Operations: Efficient batch processing for large datasets
-- =============================================================================

-- =============================================================================
-- CORE GRAPH STRUCTURE MANAGEMENT FUNCTIONS
-- =============================================================================
-- Functions for managing the fundamental graph elements: vertices and edges
-- 
-- IMPORTANT: These functions only manage vertex states directly. Edge states are
-- automatically managed by triggers based on vertex removal status. This ensures
-- consistent graph state without manual edge state management.

-- FUNCTION: dh_get_graph_statistics
-- Purpose: Provides comprehensive statistics about the graph structure and performance
-- Parameters: None
-- Returns: TABLE with graph metrics (vertices, edges, types, partition sizes)
-- Usage: Monitoring graph health, performance analysis, and capacity planning
CREATE OR REPLACE FUNCTION dh_get_graph_statistics()
RETURNS TABLE(
    total_vertices BIGINT,
    active_vertices BIGINT,
    total_edges BIGINT,
    active_edges BIGINT,
    edge_types_count INTEGER,
    largest_partition_size BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM __PGGRAPH_PREFIX___vertices),
        (SELECT COUNT(*) FROM __PGGRAPH_PREFIX___vertices WHERE removed = FALSE),
        (SELECT COUNT(*) FROM __PGGRAPH_PREFIX___edges),
        (SELECT COUNT(*) FROM __PGGRAPH_PREFIX___edges WHERE removed = FALSE),
        (SELECT COUNT(*) FROM __PGGRAPH_PREFIX___edge_types)::integer,
        (SELECT MAX(pg_relation_size(c.oid))
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' 
        AND c.relname LIKE '__PGGRAPH_PREFIX___edges_%');
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- GRAPH EDGE REMOVAL FUNCTIONS
-- =============================================================================
-- Functions for removing edges from the graph with proper cleanup

-- FUNCTION: dh_bulk_remove_edges
-- Purpose: Efficiently removes multiple edges in a single operation with support for both hard and soft deletion
-- Parameters: edge_filters (JSONB array of edge specifications), hard_delete (default FALSE)
-- Returns: INTEGER (number of edges removed)
-- Usage: Batch removal of relationships, cleanup operations, and data migration
-- Note: Uses a single statement for optimal performance, supports both deletion modes
-- Note: This function is an exception to the "edges follow vertices" rule - it's used for
--       explicit edge cleanup operations and data migration scenarios
CREATE OR REPLACE FUNCTION dh_bulk_remove_edges(
    edge_filters JSONB[],  -- Array of {source_id, target_id, edge_type_id, owner_id}
    hard_delete BOOLEAN DEFAULT FALSE  -- TRUE for hard delete, FALSE for soft delete
) RETURNS INTEGER AS $$
DECLARE
    total_removed INTEGER := 0;
BEGIN
    IF hard_delete THEN
        -- Use DELETE for hard removal
        DELETE FROM __PGGRAPH_PREFIX___edges
        WHERE (source_id, target_id, edge_type, owner_id) IN (
            SELECT 
                (e->>'source_id')::bigint,
                (e->>'target_id')::bigint,
                (e->>'edge_type_id')::smallint,
                COALESCE((e->>'owner_id')::bigint, 0)
            FROM unnest(edge_filters) as e
        );
    ELSE
        -- Use UPDATE for soft removal
        UPDATE __PGGRAPH_PREFIX___edges
        SET 
            removed = TRUE,
            updated_at = extract(epoch from now())::bigint
        WHERE (source_id, target_id, edge_type, owner_id) IN (
            SELECT 
                (e->>'source_id')::bigint,
                (e->>'target_id')::bigint,
                (e->>'edge_type_id')::smallint,
                COALESCE((e->>'owner_id')::bigint, 0)
            FROM unnest(edge_filters) as e
        )
        AND removed = FALSE;
    END IF;
    
    GET DIAGNOSTICS total_removed = ROW_COUNT;
    RETURN total_removed;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VERTEX MANAGEMENT FUNCTIONS
-- =============================================================================
-- Functions for managing vertices and their relationships in the graph

-- FUNCTION: dh_bulk_insert_edges
-- Purpose: Efficiently inserts multiple edges with vertex creation using optimized batch operations
-- Parameters: edges_data (JSONB array of edge specifications including vertex details)
-- Returns: INTEGER (number of edges processed)
-- 
-- How it works:
-- 1. Creates temporary tables for vertices and edges to batch process data
-- 2. Extracts all vertex and edge data from the JSONB array into temporary tables
-- 3. Performs batch INSERT operations with ON CONFLICT handling for both vertices and edges
-- 4. Uses DISTINCT operations to avoid duplicate vertex insertions
-- 5. Automatically derives edge soft delete status from vertex states
-- 
-- Performance benefits:
-- - Eliminates row-by-row processing (was the main bottleneck)
-- - Uses temporary tables for efficient batch operations
-- - Reduces individual INSERT overhead by processing in bulk
-- - Minimizes memory usage through controlled batch sizes
-- 
-- Usage: Bulk loading of graph relationships, data migration, and batch operations
-- Note: Edge soft delete status is automatically derived from vertex states via triggers
--       This function only sets initial edge states; ongoing consistency is maintained by triggers
CREATE OR REPLACE FUNCTION dh_bulk_insert_edges(
    edges_data JSONB[]  -- Array of {source_id, target_id, source_urn, target_urn, source_removed, target_removed, edge_type_id, owner_id, properties}
) RETURNS INTEGER AS $$
DECLARE
    e_count INTEGER := 0;
    ts_now BIGINT;
    temp_vertices_table TEXT := 'temp_vertices_' || floor(random() * 1000000)::text || '_' || extract(epoch from clock_timestamp())::bigint;
    temp_edges_table TEXT := 'temp_edges_' || floor(random() * 1000000)::text || '_' || extract(epoch from clock_timestamp())::bigint;
    vertex_insert_sql TEXT;
    edge_insert_sql TEXT;
BEGIN
    ts_now := extract(epoch from now())::bigint;
    
    -- Create temporary tables for batch operations
    EXECUTE format('CREATE TEMP TABLE %I (
        xxhash64_id BIGINT PRIMARY KEY,
        urn VARCHAR NOT NULL,
        removed BOOLEAN NOT NULL DEFAULT FALSE,
        properties JSONB DEFAULT ''{}''::jsonb
    ) ON COMMIT DROP', temp_vertices_table);
    
    EXECUTE format('CREATE TEMP TABLE %I (
        source_id BIGINT NOT NULL,
        target_id BIGINT NOT NULL,
        edge_type SMALLINT NOT NULL,
        owner_id BIGINT NOT NULL DEFAULT 0,
        properties JSONB DEFAULT ''{}''::jsonb,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        removed BOOLEAN NOT NULL DEFAULT FALSE
    ) ON COMMIT DROP', temp_edges_table);
    
    -- Batch insert vertices into temporary table
    EXECUTE format('INSERT INTO %I (xxhash64_id, urn, removed, properties)
        WITH deduplicated_vertices AS (
            SELECT DISTINCT ON (xxhash64_id)
                xxhash64_id,
                urn,
                removed,
                properties
            FROM (
                SELECT
                    (value->>''source_id'')::bigint as xxhash64_id,
                    value->>''source_urn'' as urn,
                    (value->>''source_removed'')::boolean as removed,
                    COALESCE(value->''properties'', ''{}''::jsonb) as properties
                FROM unnest((%L)::jsonb[]) AS value
                WHERE value->>''source_id'' IS NOT NULL
                UNION ALL
                SELECT
                    (value->>''target_id'')::bigint as xxhash64_id,
                    value->>''target_urn'' as urn,
                    (value->>''target_removed'')::boolean as removed,
                    COALESCE(value->''properties'', ''{}''::jsonb) as properties
                FROM unnest((%L)::jsonb[]) AS value
                    WHERE value->>''target_id'' IS NOT NULL
            ) all_vertices
        )
        SELECT * FROM deduplicated_vertices', temp_vertices_table, edges_data, edges_data);
    
    -- Batch insert edges into temporary table
    EXECUTE format('INSERT INTO %I (source_id, target_id, edge_type, owner_id, properties, created_at, updated_at, removed)
        WITH deduplicated_edges AS (
            SELECT DISTINCT ON (source_id, edge_type, target_id, owner_id)
                source_id,
                target_id,
                edge_type,
                owner_id,
                properties,
                created_at,
                updated_at,
                removed
            FROM (
                SELECT 
                    (value->>''source_id'')::bigint as source_id,
                    (value->>''target_id'')::bigint as target_id,
                    (value->>''edge_type_id'')::smallint as edge_type,
                    COALESCE((value->>''owner_id'')::bigint, 0) as owner_id,
                    COALESCE(value->''properties'', ''{}''::jsonb) as properties,
                    (%L)::bigint as created_at,
                    (%L)::bigint as updated_at,
                    CASE 
                        WHEN (value->>''source_removed'')::boolean OR (value->>''target_removed'')::boolean 
                        THEN TRUE 
                        ELSE FALSE 
                    END as removed
                FROM unnest((%L)::jsonb[]) AS value
            ) all_edges
        )
        SELECT * FROM deduplicated_edges', temp_edges_table, ts_now, ts_now, edges_data);
    
    -- Batch insert vertices with conflict resolution
    vertex_insert_sql := format('
        INSERT INTO __PGGRAPH_PREFIX___vertices (xxhash64_id, urn, removed, properties)
        SELECT xxhash64_id, urn, removed, properties
        FROM %I
        ON CONFLICT (xxhash64_id) DO UPDATE SET
            removed = EXCLUDED.removed,
            properties = EXCLUDED.properties
        WHERE __PGGRAPH_PREFIX___vertices.removed != EXCLUDED.removed
           OR __PGGRAPH_PREFIX___vertices.properties != EXCLUDED.properties', temp_vertices_table);
    
    EXECUTE vertex_insert_sql;
    
    -- Batch insert edges with conflict resolution
    edge_insert_sql := format('
        INSERT INTO __PGGRAPH_PREFIX___edges (source_id, target_id, edge_type, owner_id, properties, created_at, updated_at, removed)
        SELECT source_id, target_id, edge_type, owner_id, properties, created_at, updated_at, removed
        FROM %I
        ON CONFLICT (source_id, edge_type, target_id, owner_id) DO UPDATE SET
            removed = EXCLUDED.removed,
            updated_at = EXCLUDED.updated_at,
            properties = EXCLUDED.properties
        WHERE __PGGRAPH_PREFIX___edges.removed != EXCLUDED.removed
           OR __PGGRAPH_PREFIX___edges.updated_at != EXCLUDED.updated_at
           OR __PGGRAPH_PREFIX___edges.properties != EXCLUDED.properties', temp_edges_table);
    
    EXECUTE edge_insert_sql;
    
    -- Get count of processed edges
    EXECUTE format('SELECT COUNT(*) FROM %I', temp_edges_table) INTO e_count;
    
    RETURN e_count;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: dh_bulk_remove_vertices
-- Purpose: Efficiently removes multiple vertices from the graph with options for soft/hard deletion
-- Parameters: vertex_ids (BIGINT array), hard_delete (default FALSE)
-- Returns: TABLE with deletion results (vertices_removed, deletion_type)
-- Usage: Bulk vertex cleanup, data migration, and graph maintenance operations
-- Note: Foreign key constraints automatically handle edge cascading for hard deletes
-- Note: Soft deletion automatically cascades to edges via the cascade_vertex_soft_delete trigger
CREATE OR REPLACE FUNCTION dh_bulk_remove_vertices(
    vertex_ids BIGINT[],
    hard_delete BOOLEAN DEFAULT FALSE
) RETURNS TABLE(vertices_removed INTEGER, deletion_type TEXT) AS $$
DECLARE
    v_removed INTEGER := 0;
    del_type TEXT;
BEGIN
    IF hard_delete THEN
        -- Hard delete vertices - edges are automatically removed via FK CASCADE
        DELETE FROM __PGGRAPH_PREFIX___vertices
        WHERE xxhash64_id = ANY(vertex_ids);

        GET DIAGNOSTICS v_removed = ROW_COUNT;
        del_type := 'hard';
    ELSE
        -- Soft delete vertices - edges are automatically soft deleted via cascade trigger
        UPDATE __PGGRAPH_PREFIX___vertices
        SET
            removed = TRUE,
            properties = properties || jsonb_build_object('removed_at', extract(epoch from now())::bigint)
        WHERE xxhash64_id = ANY(vertex_ids)
            AND removed = FALSE;

        GET DIAGNOSTICS v_removed = ROW_COUNT;
        del_type := 'soft';
    END IF;

    RETURN QUERY SELECT v_removed, del_type;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: dh_get_vertex_edges
-- Purpose: Retrieves edges connected to a specific vertex with filtering options
-- Parameters: vertex_id_param, edge_type_ids, direction
-- Returns: TABLE with edge details (source, target, type, properties, timestamps)
-- Usage: Graph traversal, neighbor discovery, and relationship analysis
-- Note: Supports directional filtering (outgoing, incoming, both) and edge type filtering
CREATE OR REPLACE FUNCTION dh_get_vertex_edges(
    vertex_id_param BIGINT,
    edge_type_ids SMALLINT[] DEFAULT NULL,
    direction VARCHAR DEFAULT 'both'  -- 'outgoing', 'incoming', 'both'
) RETURNS TABLE(
    source_id BIGINT,
    target_id BIGINT,
    edge_type SMALLINT,
    owner_id BIGINT,
    properties JSONB,
    created_at BIGINT,
    updated_at BIGINT
) AS $$
BEGIN
    IF direction IN ('outgoing', 'both') THEN
        RETURN QUERY
        SELECT e.source_id, e.target_id, e.edge_type, e.owner_id, 
            e.properties, e.created_at, e.updated_at
        FROM __PGGRAPH_PREFIX___edges e
        WHERE e.source_id = vertex_id_param
        AND e.removed = FALSE
        AND (edge_type_ids IS NULL OR e.edge_type = ANY(edge_type_ids));
    END IF;
    
    IF direction IN ('incoming', 'both') THEN
        RETURN QUERY
        SELECT e.source_id, e.target_id, e.edge_type, e.owner_id, 
            e.properties, e.created_at, e.updated_at
        FROM __PGGRAPH_PREFIX___edges e
        WHERE e.target_id = vertex_id_param
        AND e.removed = FALSE
        AND (edge_type_ids IS NULL OR e.edge_type = ANY(edge_type_ids))
        AND NOT (direction = 'both' AND e.source_id = vertex_id_param); -- Avoid duplicates
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- DataHub Graph Schema Triggers for pgRouting Integration
-- =============================================================================
--
-- This section creates database triggers that maintain graph consistency in the DataHub graph system.
--
-- DESIGN OVERVIEW:
-- The triggers implement an event-driven architecture that:
-- 1. Automatically cascades soft deletions from vertices to their edges
-- 2. Automatically restores edges when vertices are restored (if safe to do so)
-- 3. Maintains referential integrity during graph modifications
-- 4. Optimizes performance through efficient batch operations
-- 5. Supports both soft deletion (removed=TRUE) and hard deletion scenarios
--
-- CORE PRINCIPLE:
-- Edge soft delete status is NEVER set explicitly - it is ALWAYS derived from vertex states.
-- Vertices control their removal state, and edges automatically follow via triggers.
-- This ensures consistent graph state and eliminates manual edge state management.
--
-- TRIGGER STRATEGY:
-- - Performance is optimized by batching operations and using efficient lookups
-- - Triggers fire only when necessary to minimize overhead
-- - All edge state changes are automatic responses to vertex state changes
-- =============================================================================

-- =============================================================================
-- CASCADE TRIGGERS FOR VERTEX SOFT DELETION/RESTORATION
-- =============================================================================
-- Automatically manages edge removal/restoration when vertices are soft-deleted or restored
-- Purpose: Maintains graph consistency by ensuring edges respect vertex removal states
-- Trigger: AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___vertices
-- Benefits: Prevents orphaned edges and maintains referential integrity

-- FUNCTION: cascade_vertex_soft_delete
-- Handles edge removal when a vertex is soft-deleted
CREATE OR REPLACE FUNCTION cascade_vertex_soft_delete()
RETURNS TRIGGER AS $$
BEGIN
    -- When a vertex is soft-deleted, mark all its edges as removed
    IF OLD.removed = FALSE AND NEW.removed = TRUE THEN
        UPDATE __PGGRAPH_PREFIX___edges
        SET
            removed = TRUE,
            updated_at = extract(epoch from now())::bigint
        WHERE (source_id = NEW.xxhash64_id OR target_id = NEW.xxhash64_id)
        AND removed = FALSE;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: cascade_vertex_soft_restore  
-- Handles edge restoration when a vertex is restored, but only if the other vertex is also active
CREATE OR REPLACE FUNCTION cascade_vertex_soft_restore()
RETURNS TRIGGER AS $$
BEGIN
    -- When a vertex is restored, check if we can restore its edges
    IF OLD.removed = TRUE AND NEW.removed = FALSE THEN
        -- Restore edges where BOTH vertices are now active
        UPDATE __PGGRAPH_PREFIX___edges
        SET
            removed = FALSE,
            updated_at = extract(epoch from now())::bigint
        WHERE (source_id = NEW.xxhash64_id OR target_id = NEW.xxhash64_id)
        AND removed = TRUE
        AND EXISTS (
            -- Check that the other vertex is also active
            SELECT 1 FROM __PGGRAPH_PREFIX___vertices v
            WHERE (
                (__PGGRAPH_PREFIX___edges.source_id = NEW.xxhash64_id AND v.xxhash64_id = __PGGRAPH_PREFIX___edges.target_id)
                OR 
                (__PGGRAPH_PREFIX___edges.target_id = NEW.xxhash64_id AND v.xxhash64_id = __PGGRAPH_PREFIX___edges.source_id)
            )
            AND v.removed = FALSE
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the triggers
DROP TRIGGER IF EXISTS cascade_vertex_soft_delete_trigger ON __PGGRAPH_PREFIX___vertices;
CREATE TRIGGER cascade_vertex_soft_delete_trigger
AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___vertices
FOR EACH ROW
WHEN (OLD.removed = FALSE AND NEW.removed = TRUE)
EXECUTE FUNCTION cascade_vertex_soft_delete();

DROP TRIGGER IF EXISTS cascade_vertex_soft_restore_trigger ON __PGGRAPH_PREFIX___vertices;
CREATE TRIGGER cascade_vertex_soft_restore_trigger
AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___vertices
FOR EACH ROW
WHEN (OLD.removed = TRUE AND NEW.removed = FALSE)
EXECUTE FUNCTION cascade_vertex_soft_restore();
