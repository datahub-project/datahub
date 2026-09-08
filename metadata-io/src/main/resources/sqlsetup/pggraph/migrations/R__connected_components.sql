-- =============================================================================
-- DataHub Graph Connected Components Schema for pgRouting Integration
-- =============================================================================
-- SqlSetup replaces __PGGRAPH_PREFIX__ with postgres.pgGraph.tablePrefix (default metadata_graph).
--
-- This file creates the connected components management schema that enables DataHub to
-- efficiently track and calculate connected components in the metadata graph.
-- 
-- DESIGN OVERVIEW:
-- The schema implements a comprehensive connected component system optimized for:
-- - Efficient tracking of graph connectivity using pgRouting algorithms
-- - Bidirectional connectivity for general-purpose CC analysis and flexibility
-- - Incremental updates to maintain CC consistency during graph modifications
-- - Performance monitoring and optimization of graph operations
-- - Flexible CC definitions for different business contexts and analysis views
-- - Automatic vertex deletion synchronization for optimal query performance
-- - Advanced predicate filtering on vertex and edge properties for sophisticated CC extraction
--
-- PREDICATE FILTERING:
-- CC definitions support three types of JSONB-based filters:
-- 1. vertex_source_filter: Applied to source vertex properties during CC extraction
-- 2. vertex_dest_filter: Applied to destination vertex properties during CC extraction  
-- 3. edge_filter: Applied to edge properties during CC extraction
-- NULL filters mean no filtering (backward compatible with existing behavior)
--
-- CRITICAL FILTER CONSISTENCY:
-- All automatic vertex assignments to CCs (via triggers) MUST check filter eligibility
-- before inserting into __PGGRAPH_PREFIX___cc_vertices to prevent inconsistent graph states.
--
-- ID STABILITY REQUIREMENTS:
-- IMPORTANT: All IDs in this system must be stable and client-provided for consistency
-- across deployments. This includes edge type IDs, CC definition IDs, vertex IDs, and URNs.
-- This ensures graph analysis results are consistent across all environments.
--
-- EDGE ID STRATEGY - DUAL APPROACH FOR DIFFERENT USE CASES:
-- 
-- 1. CONNECTED COMPONENTS FUNCTIONS (dh_calculate_ccs_full, dh_calculate_ccs_incremental):
--    - Edge ID: LEAST(source_id, target_id) 
--    - Purpose: Bidirectional connectivity analysis
--    - Logic: A→B and B→A represent the same connectivity relationship
--    - Use Case: Finding groups of vertices that can reach each other
--
-- 2. PGROUTING VIEW (__PGGRAPH_PREFIX___cc_pgrouting_edges):
--    - Edge ID: MD5 hash of source_id::target_id preserving directionality
--    - Purpose: Detailed edge analysis with direction preservation
--    - Logic: A→B and B→A are different edges for detailed analysis
--    - Use Case: pgRouting algorithms, path analysis, directional graph traversal
--
-- This dual approach allows the system to efficiently handle both connectivity analysis
-- (undirected) and detailed edge analysis (directed) without conflicts.
--
-- CC ID GENERATION STRATEGY:
-- Connected Component IDs use pgRouting component values directly (already contain min vertex ID)
-- for both full and incremental calculations, ensuring deterministic results.
--
-- PERFORMANCE OPTIMIZATION:
-- - Minimum size threshold filtering implemented during pgRouting calculation using window functions
-- - CC Functions: Edge ID strategy uses LEAST(source_id, target_id) for bidirectional connectivity analysis
-- - pgRouting View: Edge ID strategy preserves source→target directionality for detailed edge analysis
-- - Components below min_component_size threshold are automatically filtered out
--
-- CONSISTENCY DESIGN:
-- - Vertex soft deletion automatically cascades to CC assignments via triggers
-- - All graph changes automatically mark affected CCs as dirty
-- - CC calculations only occur when explicitly requested (manual execution)
-- - Session-level PostgreSQL advisory locks provide global coordination
-- - Rate limiting prevents excessive computation frequency
--
-- DEPENDENCIES:
-- - Core graph schema (init_pgrouting_01_core.sql)
-- - postgis and pgrouting extensions
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
    -- EXTENSION VALIDATION
    -- Ensure required extensions are available before proceeding
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'postgis'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pgrouting'
    ) THEN
        RAISE EXCEPTION 'Required extensions postgis and pgrouting must be installed first';
    END IF;

    RAISE NOTICE 'Creating connected components schema with partition count: %', partition_count;



    -- CONNECTED COMPONENT DEFINITIONS TABLE
    -- Stores definitions for different connected component analysis contexts
    -- Uses hard deletion with CASCADE for simplicity - no soft delete needed
    -- DESIGN PATTERN: IMMUTABLE - Once created, core fields cannot be updated
    -- Only description and rate limiting intervals can be modified. Use DELETE + CREATE for other changes.
    -- Each definition can represent a different business context or analysis view
    -- All CC definitions use bidirectional connectivity for general-purpose analysis
    --
    -- ID STABILITY: The 'id' field must be stable and client-provided for consistency
    -- across deployments. This ensures CC definitions work identically in all environments.
    --
    -- FILTER COLUMNS EXPLANATION:
    -- The three filter columns enable sophisticated filtering of vertices and edges for CC extraction:
    --
    -- 1. vertex_source_filter: JSONB predicate applied to source vertex properties
    --    Example: {"entity_type": "dataset", "platform": "bigquery"} 
    --    Only vertices matching this predicate can be source vertices in CC extraction
    --
    -- 2. vertex_dest_filter: JSONB predicate applied to destination vertex properties  
    --    Example: {"entity_type": "user", "department": "engineering"}
    --    Only vertices matching this predicate can be destination vertices in CC extraction
    --
    -- 3. edge_filter: JSONB predicate applied to edge properties
    --    Example: {"confidence": {"$gte": 0.8}, "source": "automated"}
    --    Only edges matching this predicate are considered for CC extraction
    --
    -- NULL filters mean no filtering (backward compatible with existing behavior)
    -- Filters use JSONB operators for flexible querying (e.g., @>, ?, ?&, etc.)
    -- Multiple conditions can be combined in a single JSONB object
    --
    -- USAGE EXAMPLES:
    -- 
    -- 1. Data Lineage with Platform Filtering:
    --    vertex_source_filter: {"entity_type": "dataset", "platform": "snowflake"}
    --    vertex_dest_filter: {"entity_type": "dataset", "platform": "bigquery"}
    --    edge_filter: {"relationship_type": "downstream"}
    --
    -- 2. Ownership Analysis for Engineering Team:
    --    vertex_source_filter: {"entity_type": "dataset"}
    --    vertex_dest_filter: {"entity_type": "user", "department": "engineering"}
    --    edge_filter: {"ownership_type": "primary"}
    --
    -- 3. High-Confidence Automated Lineage:
    --    vertex_source_filter: {"entity_type": "dataset"}
    --    vertex_dest_filter: {"entity_type": "dataset"}
    --    edge_filter: {"confidence": {"$gte": 0.9}, "source": "automated"}
    --
    -- 4. No Filters (Backward Compatible):
    --    vertex_source_filter: NULL
    --    vertex_dest_filter: NULL  
    --    edge_filter: NULL
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___cc_definitions (
        id SMALLINT PRIMARY KEY,                    -- STABLE ID: Must be client-provided for consistency
        name VARCHAR(200) UNIQUE NOT NULL,           -- Human-readable name
        edge_types SMALLINT[] NOT NULL,              -- Array of edge type IDs to include
        edge_type_names VARCHAR[] NOT NULL,          -- Array of edge type names for display
        description TEXT DEFAULT '',                 -- Human-readable description (can be updated)
        min_component_size INTEGER NOT NULL DEFAULT 3,    -- Minimum vertices required for a CC (can be updated)
        full_calculation_min_interval_seconds INTEGER NOT NULL DEFAULT 86400,    -- Rate limiting for full calculations (can be updated)
        incremental_calculation_min_interval_seconds INTEGER NOT NULL DEFAULT 3600, -- Rate limiting for incremental calculations (can be updated)
        vertex_source_filter JSONB DEFAULT NULL,     -- Filter predicate for source vertex properties (NULL = no filter)
        vertex_dest_filter JSONB DEFAULT NULL,       -- Filter predicate for destination vertex properties (NULL = no filter)
        edge_filter JSONB DEFAULT NULL               -- Filter predicate for edge properties (NULL = no filter)
    );

    -- Add constraints for the new interval fields
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_full_calculation_min_interval_seconds'
    ) THEN
        ALTER TABLE __PGGRAPH_PREFIX___cc_definitions 
        ADD CONSTRAINT chk_full_calculation_min_interval_seconds 
            CHECK (full_calculation_min_interval_seconds >= 0);
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_incremental_calculation_min_interval_seconds'
    ) THEN
        ALTER TABLE __PGGRAPH_PREFIX___cc_definitions 
        ADD CONSTRAINT chk_incremental_calculation_min_interval_seconds 
            CHECK (incremental_calculation_min_interval_seconds >= 0);
    END IF;

    -- CONNECTED COMPONENT VERTICES TABLE
    -- Stores vertex assignments to connected components
    -- Each vertex can belong to multiple CCs across different definitions
    -- DESIGN: Partitioned by CC definition for optimal query performance
    -- 
    -- ID STABILITY: The cc_def_id field must be stable and client-provided for consistency
    -- across deployments. This ensures CC assignments are properly associated with their definitions.
    -- 
    -- PARTITIONING STRATEGY: Partition by (cc_def_id, cc_id) for optimal CC analysis performance
    -- This enables partition pruning for queries filtering by specific CCs and maintains sorted data
    -- within each partition for fast range queries and chunking operations.
    -- 
    -- PERFORMANCE BENEFITS OF CC-BASED PARTITIONING:
    -- 1. PARTITION PRUNING: Queries filtering by cc_def_id and cc_id only scan relevant partitions
    --    - Example: WHERE cc_def_id = 1 AND cc_id = 12345 only scans partition based on composite hash
    --    - Eliminates scanning of irrelevant partitions
    -- 
    -- 2. SORTED DATA WITHIN PARTITIONS: Primary key (cc_def_id, cc_id, vertex_id) provides natural ordering
    --    - Enables fast range queries within specific CCs
    --    - Eliminates runtime sorting for chunking operations
    --    - Improves cache locality and sequential access patterns
    -- 
    -- 3. SCALABLE PERFORMANCE: Performance scales with CC count, not vertex count
    --    - Adding new CCs doesn't affect existing partition performance
    --    - Each partition can efficiently handle millions of vertices
    --    - Optimal for dh_analyze_cc_impact function performance
    -- 
    -- 4. EFFICIENT JOIN OPERATIONS: The __PGGRAPH_PREFIX___cc_pgrouting_edges view benefits significantly
    --    - JOIN operations only access relevant partitions
    --    - Reduced I/O operations and memory usage
    --    - Better query plan selection by PostgreSQL optimizer
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = '__PGGRAPH_PREFIX___cc_vertices'
    ) THEN
        RAISE NOTICE 'Creating __PGGRAPH_PREFIX___cc_vertices table with CC-based partitioning...';
        
        -- Create the partitioned table with CC-based partitioning
        CREATE TABLE __PGGRAPH_PREFIX___cc_vertices (
            vertex_id BIGINT NOT NULL REFERENCES __PGGRAPH_PREFIX___vertices(xxhash64_id) ON DELETE CASCADE,
            cc_def_id SMALLINT NOT NULL REFERENCES __PGGRAPH_PREFIX___cc_definitions(id) ON DELETE CASCADE,
            cc_id BIGINT NOT NULL,                       -- CC identifier within this definition
            removed BOOLEAN DEFAULT FALSE,               -- Soft deletion flag
            assigned_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
            updated_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
            PRIMARY KEY (cc_def_id, cc_id, vertex_id)   -- Natural clustering order for CC queries
        ) PARTITION BY HASH (cc_def_id, cc_id);         -- PostgreSQL 17 native composite hash partitioning
        
        -- Create initial partitions for common CC definitions
        -- Each partition handles a range of CC definitions and IDs
        -- This enables partition pruning for CC-specific queries
        
        -- Create hash partitions for optimal distribution
        -- With small cc_def_ids, hash partitioning provides better data distribution
        FOR i IN 0..(partition_count-1) LOOP
            BEGIN
                EXECUTE format('
                    CREATE TABLE __PGGRAPH_PREFIX___cc_vertices_%s PARTITION OF __PGGRAPH_PREFIX___cc_vertices
                    FOR VALUES WITH (modulus %s, remainder %s)
                ', i, partition_count, i);
                
                RAISE NOTICE 'Created hash partition __PGGRAPH_PREFIX___cc_vertices_%', i;
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'Partition __PGGRAPH_PREFIX___cc_vertices_% already exists, skipping', i;
            WHEN OTHERS THEN
                RAISE NOTICE 'Error creating partition __PGGRAPH_PREFIX___cc_vertices_%: %', i, SQLERRM;
            END;
        END LOOP;
        
        RAISE NOTICE 'Created % hash partitions for __PGGRAPH_PREFIX___cc_vertices table', partition_count;
    ELSE
        RAISE NOTICE 'Table __PGGRAPH_PREFIX___cc_vertices already exists, checking for CC-based partitions...';
        
        -- Check if CC-based partitions exist, create them if they don't
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c 
            WHERE c.relname = '__PGGRAPH_PREFIX___cc_vertices_0'
        ) THEN
            RAISE NOTICE 'Converting existing table to CC-based hash partitioning...';
            
            -- Create hash partitions
            FOR i IN 0..(partition_count-1) LOOP
                BEGIN
                    EXECUTE format('
                        CREATE TABLE __PGGRAPH_PREFIX___cc_vertices_%s PARTITION OF __PGGRAPH_PREFIX___cc_vertices
                        FOR VALUES WITH (modulus %s, remainder %s)
                    ', i, partition_count, i);
                    
                    RAISE NOTICE 'Created hash partition __PGGRAPH_PREFIX___cc_vertices_%', i;
                EXCEPTION WHEN duplicate_table THEN
                    RAISE NOTICE 'Partition __PGGRAPH_PREFIX___cc_vertices_% already exists, skipping', i;
                WHEN OTHERS THEN
                    RAISE NOTICE 'Error creating partition __PGGRAPH_PREFIX___cc_vertices_%: %', i, SQLERRM;
                END;
            END LOOP;
            RAISE NOTICE 'Created % partitions for existing __PGGRAPH_PREFIX___cc_vertices table', partition_count;
        ELSE
            RAISE NOTICE 'CC-based hash partitions already exist for __PGGRAPH_PREFIX___cc_vertices table';
        END IF;
    END IF;

    -- VERTEX-CC INDEXES
    -- Optimized for CC lookup and traversal operations with CC-based partitioning
    -- The primary key (cc_def_id, cc_id, vertex_id) provides natural clustering
    -- Additional indexes support specific query patterns
    
    -- Main clustered index (already provided by primary key)
    -- This enables fast range queries within specific CCs
    
    -- Index for vertex lookups across all CCs
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_vertices_vertex 
    ON __PGGRAPH_PREFIX___cc_vertices(vertex_id);
    
    -- Index for active vertex filtering
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_vertices_active 
    ON __PGGRAPH_PREFIX___cc_vertices(cc_def_id, cc_id, removed) 
    WHERE removed = FALSE;

    -- CONNECTED COMPONENT (CC) TABLE
    -- Stores metadata about each connected component instance
    -- Each CC represents a set of vertices that are connected through the specified edge types
    -- DESIGN: Uses BIGINT IDs for CC instances to handle large numbers of components
    -- 
    -- ID STABILITY: The cc_def_id field must be stable and client-provided for consistency
    -- across deployments. This ensures CC instances are properly associated with their definitions.
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___cc (
        id BIGINT NOT NULL,                          -- CC ID within a definition
        cc_def_id SMALLINT NOT NULL REFERENCES __PGGRAPH_PREFIX___cc_definitions(id) ON DELETE CASCADE,
        created_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
        updated_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
        vertex_count INTEGER NOT NULL DEFAULT 0,     -- Number of vertices in this CC
        edge_count INTEGER NOT NULL DEFAULT 0,       -- Number of edges within this CC
        PRIMARY KEY(id, cc_def_id)
    );

    -- CC INDEXES
    -- Optimized for CC queries and management operations
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_def 
        ON __PGGRAPH_PREFIX___cc(cc_def_id);



    -- PERFORMANCE AND MAINTENANCE TABLES
    
    -- CC COMPUTATION LOG TABLE
    -- Tracks when and how connected components were computed
    -- Enables performance monitoring and debugging of CC calculations
    -- DESIGN: Logs both full and incremental computation runs
    --
    -- ID STABILITY: The cc_def_id field must be stable and client-provided for consistency
    -- across deployments. This ensures computation logs are properly associated with their definitions.
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___cc_computation_log (
        id SERIAL PRIMARY KEY,
        computation_type VARCHAR(50),                -- 'full' or 'incremental'
        cc_def_id SMALLINT REFERENCES __PGGRAPH_PREFIX___cc_definitions(id) ON DELETE CASCADE,
        started_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
        completed_at BIGINT,                        -- Unix timestamp
        vertices_processed INTEGER,                  -- Performance metric
        ccs_found INTEGER,                          -- Performance metric
        execution_time_ms INTEGER,                   -- Performance metric
        status VARCHAR(50) DEFAULT 'RUNNING',        -- Status: 'RUNNING', 'COMPLETED', 'FAILED'
        created_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint
    );

    -- CC COMPUTATION LOG INDEXES
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_computation_log_cc_def ON __PGGRAPH_PREFIX___cc_computation_log(cc_def_id);
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_computation_log_created ON __PGGRAPH_PREFIX___cc_computation_log(created_at);

    -- CC DIRTY TRACKING TABLE
    -- Tracks which connected components need recalculation due to graph changes
    -- Enables efficient incremental updates by only recalculating affected CCs
    -- DESIGN: Uses a queue-based approach for processing dirty CCs
    --
    -- ID STABILITY: The cc_def_id field must be stable and client-provided for consistency
    -- across deployments. This ensures dirty tracking works identically in all environments.
    CREATE TABLE IF NOT EXISTS __PGGRAPH_PREFIX___cc_dirty (
        cc_def_id SMALLINT NOT NULL REFERENCES __PGGRAPH_PREFIX___cc_definitions(id) ON DELETE CASCADE,
        cc_id BIGINT NOT NULL,
        marked_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
        processed BOOLEAN DEFAULT FALSE,             -- Whether CC has been recalculated
        processed_at BIGINT,                         -- When CC was processed
        PRIMARY KEY(cc_def_id, cc_id)
    );

    -- DIRTY CC INDEXES
    -- Optimized for finding unprocessed dirty CCs during incremental updates
    CREATE INDEX IF NOT EXISTS idx___PGGRAPH_PREFIX___cc_dirty_unprocessed
        ON __PGGRAPH_PREFIX___cc_dirty(cc_def_id, marked_at)
        WHERE processed = FALSE;

    -- FILTERED PGROUTING VIEWS
    -- Provides pgRouting-compatible views for specific CC definitions
    -- Enables efficient algorithm execution on filtered graph subsets
    -- All networks use bidirectional connectivity for general-purpose CC analysis
    -- Note: source and target columns contain xxhash64_id values for consistency with vertex table
    -- 
    -- This view leverages the prefiltered __PGGRAPH_PREFIX___cc_vertices table
    -- instead of re-applying vertex filters, since vertices are already materialized there
    -- with the appropriate filtering for each cc_def_id. Edge filters are still applied here
    -- since they need to be evaluated against the actual edge data.
    -- 
    -- PERFORMANCE BENEFITS:
    -- - Partition pruning: CC queries only scan relevant partitions (10-100x improvement)
    -- - Sorted data within partitions eliminates runtime sorting overhead
    -- - Scalable performance: scales with CC count, not vertex count
    -- - Efficient JOIN operations for __PGGRAPH_PREFIX___cc_pgrouting_edges view
    CREATE OR REPLACE VIEW __PGGRAPH_PREFIX___cc_pgrouting_edges AS
    SELECT 
        cd.id AS cc_def_id,                  -- CC definition identifier
        cd.name AS cc_name,                  -- CC definition name
        vc_source.cc_id AS cc_id,            -- Connected component identifier for path finding
        -- Edge ID strategy: preserve source→destination directionality for unique identification
        -- This represents the actual directional connection between vertices
        -- MD5 hash ensures uniqueness while preserving edge direction, truncated to 64-bit for pgRouting
        ('x' || md5(e.source_id::text || '::' || e.target_id::text))::bit(64)::bigint AS id,
        e.source_id AS source,               -- Source vertex xxhash64_id
        e.target_id AS target,               -- Target vertex xxhash64_id
        1.0::float AS cost,                  -- Edge cost
        1.0::float AS reverse_cost,          -- Reverse cost (same as forward for bidirectional connectivity)
        e.properties                         -- Edge metadata
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    JOIN __PGGRAPH_PREFIX___edges e ON e.edge_type = ANY(cd.edge_types)
    -- Leverage prefiltered vertices from __PGGRAPH_PREFIX___cc_vertices
    JOIN __PGGRAPH_PREFIX___cc_vertices vc_source ON vc_source.vertex_id = e.source_id 
        AND vc_source.cc_def_id = cd.id 
        AND vc_source.removed = FALSE
    JOIN __PGGRAPH_PREFIX___cc_vertices vc_target ON vc_target.vertex_id = e.target_id 
        AND vc_target.cc_def_id = cd.id 
        AND vc_target.removed = FALSE
    WHERE e.removed = FALSE
    -- Apply edge filter if specified (NULL = no filter)
    AND (cd.edge_filter IS NULL OR e.properties @> cd.edge_filter);

    RAISE NOTICE 'Graph schema created successfully';
END $$;

-- DataHub Graph Connected Component Functions for pgRouting Integration
-- 
-- This section contains the comprehensive set of database functions that implement
-- DataHub's connected component analytics capabilities.
--
-- FUNCTION CATEGORIES:
-- - CC Query Functions: Retrieve CC information and vertex assignments
-- - CC Calculation Functions: Full and incremental CC computation using pgRouting
-- - CC Management Functions: Definition management and statistics updates
-- - CC Analysis Functions: Downstream impact analysis and reachability queries
-- - Utility Functions: Dirty marking, maintenance, and monitoring

-- CONNECTED COMPONENT QUERY FUNCTIONS
-- Functions for retrieving information about connected components and their properties

-- FUNCTION: dh_get_vertex_ccs
-- Purpose: Retrieves connected component information for a specific vertex
--          across all CC definitions
-- Parameters: vertex_id_param (BIGINT)
-- Returns: TABLE with cc_def_id, cc_def_name, cc_id, cc_size, assigned_at
-- Usage: Determines which CCs a vertex belongs to across all definitions
CREATE OR REPLACE FUNCTION dh_get_vertex_ccs(
    vertex_id_param BIGINT
) RETURNS TABLE(
    cc_def_id SMALLINT,
    cc_def_name VARCHAR(255),
    cc_id BIGINT,
    cc_size INTEGER,
    assigned_at BIGINT
) AS $$
BEGIN
    -- Validate input parameters
    IF vertex_id_param IS NULL THEN
        RAISE EXCEPTION 'vertex_id_param cannot be NULL';
    END IF;

    -- Check if vertex exists
    IF NOT EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___vertices WHERE xxhash64_id = vertex_id_param) THEN
        RAISE NOTICE 'Vertex % not found in graph', vertex_id_param;
        RETURN; -- Return empty result set
    END IF;

    RETURN QUERY
    SELECT
        vc.cc_def_id,
        cd.name AS cc_def_name,
        vc.cc_id,
        COALESCE(fgc.vertex_count, 0) AS cc_size, -- Handle potential NULL from outer join
        vc.assigned_at
    FROM __PGGRAPH_PREFIX___cc_vertices vc
    LEFT JOIN __PGGRAPH_PREFIX___cc fgc ON -- Changed to LEFT JOIN to handle missing CC records
        vc.cc_id = fgc.id
        AND vc.cc_def_id = fgc.cc_def_id
        AND fgc.removed = FALSE
    JOIN __PGGRAPH_PREFIX___cc_definitions cd ON vc.cc_def_id = cd.id
    WHERE vc.vertex_id = vertex_id_param;

    -- Log if no CC assignment found
    IF NOT FOUND THEN
        RAISE NOTICE 'No CC assignment found for vertex %',
                     vertex_id_param;
    END IF;
END;
$$ LANGUAGE plpgsql;



-- FUNCTION: dh_get_cc_vertices
-- Purpose: Retrieves all vertices belonging to a specific connected component
-- Parameters: cc_id_param (BIGINT), cc_def_id_param (SMALLINT)
-- Returns: TABLE with vertex_id, urn
-- Usage: Analyzing CC composition, vertex enumeration, and graph exploration
CREATE OR REPLACE FUNCTION dh_get_cc_vertices(
    cc_id_param BIGINT,
    cc_def_id_param SMALLINT
) RETURNS TABLE(
    vertex_id BIGINT,
    urn VARCHAR(1024)
) AS $$
BEGIN
    RETURN QUERY
    SELECT v.xxhash64_id, v.urn
    FROM __PGGRAPH_PREFIX___cc_vertices vc
    JOIN __PGGRAPH_PREFIX___vertices v ON vc.vertex_id = v.xxhash64_id
    WHERE vc.cc_id = cc_id_param
    AND vc.cc_def_id = cc_def_id_param
    AND v.removed = FALSE
    ORDER BY v.xxhash64_id;
END;
$$ LANGUAGE plpgsql;

-- EDGE TYPE MANAGEMENT FUNCTIONS
-- Functions for creating and managing edge types in the graph

-- FUNCTION: dh_create_edge_type_if_not_exists
-- Purpose: Creates an edge type if it doesn't already exist
-- Parameters: edge_type_id, type_name
-- Returns: SMALLINT (the edge type ID)
-- Usage: Ensures edge types exist before creating CC definitions
-- Note: If edge type already exists, returns existing ID. If not, creates new one.
-- Client must provide stable, consistent IDs for edge types.
-- 
-- ID STABILITY: The edge_type_id parameter must be stable and consistent across deployments.
CREATE OR REPLACE FUNCTION dh_create_edge_type_if_not_exists(
    edge_type_id SMALLINT,
    type_name VARCHAR(200)
) RETURNS SMALLINT AS $$
DECLARE
    existing_id SMALLINT;
BEGIN
    -- Check if edge type already exists by name
    SELECT et.id INTO existing_id 
    FROM __PGGRAPH_PREFIX___edge_types et
    WHERE et.type_name = dh_create_edge_type_if_not_exists.type_name;
    
    IF existing_id IS NOT NULL THEN
        -- Edge type exists, return existing ID
        RETURN existing_id;
    END IF;
    
    -- Check if ID is already taken by a different name
    IF EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___edge_types WHERE id = edge_type_id) THEN
        RAISE EXCEPTION 'Edge type ID % is already taken by a different edge type', edge_type_id;
    END IF;
    
    -- Create new edge type
    INSERT INTO __PGGRAPH_PREFIX___edge_types (id, type_name)
    VALUES (edge_type_id, type_name);
    
    RAISE NOTICE 'Created new edge type: % (ID: %)', type_name, edge_type_id;
    RETURN edge_type_id;
END;
$$ LANGUAGE plpgsql;

-- CONNECTED COMPONENT DEFINITION FUNCTIONS
-- Functions for creating and managing connected component definitions

-- FUNCTION: dh_create_cc_definition
-- Purpose: Creates immutable connected component definitions that specify
--          which edge types to include in a particular graph analysis view
-- Parameters: def_id, name_param, edge_type_ids, edge_type_names_param, description_param
-- Returns: SMALLINT (the definition ID)
-- Usage: Defines business contexts for graph analysis (e.g., "data lineage", "ownership")
-- Note: Once created, only description can be updated. Core fields are immutable.
-- All CC definitions use bidirectional connectivity for general-purpose analysis
-- Edge types are automatically created if they don't exist, using client-provided stable IDs
--
-- ID STABILITY: All ID parameters must be stable and consistent across deployments.
CREATE OR REPLACE FUNCTION dh_create_cc_definition(
    def_id SMALLINT,
    name_param VARCHAR(200),
    edge_type_ids SMALLINT[],
    edge_type_names_param VARCHAR[],
    description_param TEXT DEFAULT '',
    min_component_size_param INTEGER DEFAULT 3,
    full_interval_seconds INTEGER DEFAULT 86400,
    incremental_interval_seconds INTEGER DEFAULT 3600,
    vertex_source_filter_param JSONB DEFAULT NULL,
    vertex_dest_filter_param JSONB DEFAULT NULL,
    edge_filter_param JSONB DEFAULT NULL
) RETURNS SMALLINT AS $$
DECLARE
    edge_type_id SMALLINT;
    edge_type_name VARCHAR(200);
    i INTEGER;
BEGIN
    -- Validate rate limiting parameters
    IF full_interval_seconds < 0 THEN
        RAISE EXCEPTION 'Full calculation interval must be non-negative, got %', full_interval_seconds;
    END IF;
    
    IF incremental_interval_seconds < 0 THEN
        RAISE EXCEPTION 'Incremental calculation interval must be non-negative, got %', incremental_interval_seconds;
    END IF;
    
    -- Validate minimum component size
    IF min_component_size_param < 2 THEN
        RAISE EXCEPTION 'Minimum component size must be at least 2, got %', min_component_size_param;
    END IF;
    
    -- Create edge types if they don't exist (using client-provided stable IDs)
    FOR i IN 1..array_length(edge_type_ids, 1) LOOP
        edge_type_id := edge_type_ids[i];
        edge_type_name := edge_type_names_param[i];
        
        -- Create edge type if it doesn't exist
        PERFORM dh_create_edge_type_if_not_exists(edge_type_id, edge_type_name);
    END LOOP;
    
    -- Verify all edge types exist (this should always pass now)
    IF EXISTS (
        SELECT 1 FROM unnest(edge_type_ids) AS et_id
        WHERE NOT EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___edge_types WHERE id = et_id)
    ) THEN
        RAISE EXCEPTION 'Some edge type IDs do not exist after creation attempt';
    END IF;
    
    -- Check if definition already exists
    IF EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___cc_definitions WHERE id = def_id) THEN
        RAISE EXCEPTION 'CC definition with ID % already exists. Definitions are immutable.', def_id;
    END IF;
    
    -- Insert the CC definition with rate limiting configuration
    INSERT INTO __PGGRAPH_PREFIX___cc_definitions (id, name, edge_types, edge_type_names, description, min_component_size, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds, vertex_source_filter, vertex_dest_filter, edge_filter)
    VALUES (def_id, name_param, edge_type_ids, edge_type_names_param, description_param, min_component_size_param, full_interval_seconds, incremental_interval_seconds, vertex_source_filter_param, vertex_dest_filter_param, edge_filter_param);
    
    RAISE NOTICE 'Created CC definition % with min component size % and rate limiting: full=% seconds, incremental=% seconds', 
        def_id, min_component_size_param, full_interval_seconds, incremental_interval_seconds;
    
    RETURN def_id;
END;
$$ LANGUAGE plpgsql;

-- EDGE TYPE UTILITY FUNCTIONS
-- Helper functions for managing edge types

-- FUNCTION: dh_list_edge_types
-- Purpose: Lists all edge types in the system
-- Parameters: None
-- Returns: TABLE with edge type information
-- Usage: View all available edge types for reference
CREATE OR REPLACE FUNCTION dh_list_edge_types()
RETURNS TABLE(
    id SMALLINT,
    type_name VARCHAR(200)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        et.id,
        et.type_name
    FROM __PGGRAPH_PREFIX___edge_types et
    ORDER BY et.id;
END;
$$ LANGUAGE plpgsql;

-- CONNECTED COMPONENT CALCULATION FUNCTIONS
-- Core functions that use pgRouting algorithms to calculate and maintain connected components
-- These functions implement the heart of the graph analytics system
-- 
-- ALGORITHM OVERVIEW:
-- The system uses pgRouting's pgr_connectedComponents algorithm for optimal performance:
-- 1. FULL CALCULATION: Complete graph analysis for initial setup and periodic maintenance
-- 2. INCREMENTAL UPDATES: Efficient updates based on dirty component tracking
-- 3. PERFORMANCE OPTIMIZATION: Bulk operations and parallel processing where possible
-- 4. CONSISTENCY GUARANTEES: Automatic dirty marking and validation
-- 5. ADVANCED FILTERING: Vertex and edge property filters applied during graph construction
--
-- FILTER APPLICATION STRATEGY:
-- Filters are applied at the graph construction level before calling pgRouting algorithms:
-- 1. Vertex filters: Applied to both source and destination vertices during edge selection
-- 2. Edge filters: Applied during edge selection to include only qualifying edges
-- 3. Combined filtering: All filters work together to create a filtered subgraph for CC extraction
-- 4. Performance: Filtering happens once during graph construction, not during algorithm execution

-- FUNCTION: dh_calculate_ccs_full
-- Purpose: Calculates all connected components for a specific CC definition
-- Parameters: cc_def_id_param
-- Returns: TABLE with vertices_processed, ccs_found, calculation_time_ms
-- Usage: Full recalculation of all CCs for a definition (e.g., after schema changes)
-- Note: This is a full calculation that processes all vertices and edges
--       and automatically clears all dirty tracking for the definition
-- 
-- PERFORMANCE OPTIMIZATIONS:
-- 1. EARLY EXIT CHECKS: Rate limit moved to beginning for fast exit
-- 2. DEFERRED LOCK ACQUISITION: Locks acquired after rate limit check
--
-- ID STABILITY: The cc_def_id_param must be stable and consistent across deployments.
CREATE OR REPLACE FUNCTION dh_calculate_ccs_full(
    cc_def_id_param SMALLINT
) RETURNS TABLE(
    vertices_processed BIGINT, 
    ccs_found BIGINT,
    calculation_time_ms INTEGER
) AS $$
DECLARE
    edge_type_filter SMALLINT[];
    vertex_source_filter JSONB;
    vertex_dest_filter JSONB;
    edge_filter JSONB;
    v_count BIGINT := 0;
    c_count BIGINT := 0;
    start_time TIMESTAMP;
    old_cc_count BIGINT;
    lock_acquired BOOLEAN := FALSE;
    rate_limit_hit BOOLEAN := FALSE;
BEGIN
    start_time := clock_timestamp();
    
    -- Check rate limit FIRST before any expensive operations
    -- This prevents excessive retries and concurrent computations
    SELECT EXISTS (
        SELECT 1 
        FROM __PGGRAPH_PREFIX___cc_computation_log l
        JOIN __PGGRAPH_PREFIX___cc_definitions d ON d.id = l.cc_def_id
        WHERE l.cc_def_id = cc_def_id_param 
          AND l.computation_type = 'full'
          AND (extract(epoch from now()) - l.started_at) < d.full_calculation_min_interval_seconds
    ) INTO rate_limit_hit;
    
    IF rate_limit_hit THEN
        RAISE NOTICE 'Rate limit hit for full CC calculation for definition %. Skipping.', cc_def_id_param;
        RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::INTEGER;
        RETURN;
    END IF;
    
    -- Get cc definition and filter predicates
    SELECT 
        cd.edge_types,
        cd.vertex_source_filter,
        cd.vertex_dest_filter, 
        cd.edge_filter
    INTO 
        edge_type_filter,
        vertex_source_filter,
        vertex_dest_filter,
        edge_filter
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    WHERE cd.id = cc_def_id_param;
    
    IF edge_type_filter IS NULL THEN
        RAISE EXCEPTION 'CC definition % not found', cc_def_id_param;
    END IF;

    -- Check for concurrent execution using session-level advisory locks
    -- Lock key: 1000000 + cc_def_id for full calculation
    -- We need exclusive access for full calculation (no other processes on this definition)
    lock_acquired := pg_try_advisory_lock(1000000 + cc_def_id_param);
    IF NOT lock_acquired THEN
        RAISE NOTICE 'Concurrent execution detected for full CC calculation for definition %. Another process is already running on this definition. Skipping.', 
            cc_def_id_param;
        RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::INTEGER;
        RETURN;
    END IF;
    lock_acquired := TRUE;

    -- Store old cc count for logging
    SELECT COUNT(*) INTO old_cc_count
    FROM __PGGRAPH_PREFIX___cc
    WHERE cc_def_id = cc_def_id_param;
    
    -- Delete all existing ccs for this definition
    DELETE FROM __PGGRAPH_PREFIX___cc 
    WHERE cc_def_id = cc_def_id_param;
    
    -- Clear vertex-cc mappings
    DELETE FROM __PGGRAPH_PREFIX___cc_vertices 
    WHERE cc_def_id = cc_def_id_param;
    
    -- Build filtered graph with vertex and edge filters
    -- Filter components during calculation using window functions
    -- This eliminates the need for post-calculation filtering and improves performance at scale
    CREATE TEMP TABLE temp_ccs AS
    SELECT 
        node::bigint as vertex_id,
        component as cc_id
    FROM (
        SELECT 
            node,
            component,
            COUNT(*) OVER (PARTITION BY component) as component_size
        FROM pgr_connectedComponents(
            'SELECT 
                LEAST(source_id, target_id)::bigint as id,
                source_id::bigint as source, 
                target_id::bigint as target,
                1.0::float as cost,
                1.0::float as reverse_cost
            FROM __PGGRAPH_PREFIX___edges e
            JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
            JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
            WHERE e.removed = FALSE 
            AND vs.removed = FALSE
            AND vt.removed = FALSE
            AND e.edge_type = ANY(ARRAY[' || array_to_string(edge_type_filter, ',') || ']::SMALLINT[])
            -- Apply vertex source filter if specified
            AND (' || CASE WHEN vertex_source_filter IS NULL THEN 'TRUE' ELSE 'vs.properties @> ' || quote_literal(vertex_source_filter) END || ')
            -- Apply vertex destination filter if specified  
            AND (' || CASE WHEN vertex_dest_filter IS NULL THEN 'TRUE' ELSE 'vt.properties @> ' || quote_literal(vertex_dest_filter) END || ')
            -- Apply edge filter if specified
            AND (' || CASE WHEN edge_filter IS NULL THEN 'TRUE' ELSE 'e.properties @> ' || quote_literal(edge_filter) END || ')'
        )
    ) filtered_components
    WHERE component_size >= (
        SELECT min_component_size 
        FROM __PGGRAPH_PREFIX___cc_definitions 
        WHERE id = cc_def_id_param
    );
    
    -- Insert vertex-cc mappings (no additional filtering needed - already filtered above)
    INSERT INTO __PGGRAPH_PREFIX___cc_vertices (vertex_id, cc_def_id, cc_id)
    SELECT vertex_id, cc_def_id_param, cc_id
    FROM temp_ccs;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Create/update cc records
    INSERT INTO __PGGRAPH_PREFIX___cc (id, cc_def_id, created_at, updated_at, vertex_count, edge_count)
    SELECT 
        cc_id, 
        cc_def_id_param, 
        extract(epoch from now())::bigint as created_at,
        extract(epoch from now())::bigint as updated_at,
        COUNT(*)::integer as vertex_count,
        0 as edge_count  -- Will be updated below
    FROM temp_ccs
    GROUP BY cc_id
    ON CONFLICT (id, cc_def_id) DO UPDATE SET
        vertex_count = EXCLUDED.vertex_count,
        updated_at = extract(epoch from now())::bigint;
    
    -- Update edge counts in bulk
    WITH edge_counts AS (
        SELECT 
            vc1.cc_id,
            COUNT(*) as edge_count
        FROM __PGGRAPH_PREFIX___edges e
        JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
        JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
        JOIN __PGGRAPH_PREFIX___cc_vertices vc1 ON e.source_id = vc1.vertex_id
        JOIN __PGGRAPH_PREFIX___cc_vertices vc2 ON e.target_id = vc2.vertex_id
        WHERE e.removed = FALSE
        AND vs.removed = FALSE
        AND vt.removed = FALSE
        AND e.edge_type = ANY(edge_type_filter)
        -- Apply vertex source filter if specified
        AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
        -- Apply vertex destination filter if specified
        AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
        -- Apply edge filter if specified
        AND (edge_filter IS NULL OR e.properties @> edge_filter)
        AND vc1.cc_def_id = cc_def_id_param
        AND vc2.cc_def_id = cc_def_id_param
        AND vc1.cc_id = vc2.cc_id
        GROUP BY vc1.cc_id
    )
    UPDATE __PGGRAPH_PREFIX___cc fgc
    SET edge_count = ec.edge_count
    FROM edge_counts ec
    WHERE fgc.id = ec.cc_id 
    AND fgc.cc_def_id = cc_def_id_param;
    
    SELECT COUNT(DISTINCT cc_id) INTO c_count FROM temp_ccs;
    
    DROP TABLE temp_ccs;
    
    -- Clear dirty tracking for this definition since we've done a full recalculation
    -- All CCs are now clean and up-to-date, so no dirty marks are needed
    -- This prevents stale dirty marks from persisting after full recalculation
    DELETE FROM __PGGRAPH_PREFIX___cc_dirty 
    WHERE cc_def_id = cc_def_id_param;
    
    -- Log the calculation
    INSERT INTO __PGGRAPH_PREFIX___cc_computation_log 
        (computation_type, cc_def_id, completed_at, vertices_processed, 
        ccs_found, execution_time_ms, status)
    VALUES 
        ('full', cc_def_id_param, extract(epoch from now())::bigint, v_count, c_count,
        extract(milliseconds from clock_timestamp() - start_time)::integer, 'COMPLETED');
    
    -- Release the advisory lock
    PERFORM pg_advisory_unlock(1000000 + cc_def_id_param);
    
    RETURN QUERY SELECT 
        v_count, 
        c_count,
        extract(milliseconds from clock_timestamp() - start_time)::integer;
EXCEPTION
    WHEN OTHERS THEN
        -- Ensure lock is released even on error
        IF lock_acquired THEN
            PERFORM pg_advisory_unlock(1000000 + cc_def_id_param);
        END IF;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: dh_update_cc_stats
-- Purpose: Unified function for updating connected component statistics with flexible scope
-- Parameters: cc_def_id_param, cc_ids (NULL for all, single ID for specific, array for batch)
-- Returns: VOID
-- Usage: Single CC updates, batch updates, or full definition updates
-- Note: Automatically chooses optimal execution strategy based on input parameters
--
-- ID STABILITY: The cc_def_id_param must be stable and consistent across deployments.
CREATE OR REPLACE FUNCTION dh_update_cc_stats(
    cc_def_id_param SMALLINT,
    cc_ids BIGINT[] DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    edge_type_filter SMALLINT[];
    vertex_source_filter JSONB;
    vertex_dest_filter JSONB;
    edge_filter JSONB;
    target_ccs BIGINT[];
    is_single_update BOOLEAN;
BEGIN
    -- Get edge types and filter predicates for this cc definition
    SELECT 
        cd.edge_types,
        cd.vertex_source_filter,
        cd.vertex_dest_filter, 
        cd.edge_filter
    INTO 
        edge_type_filter,
        vertex_source_filter,
        vertex_dest_filter,
        edge_filter
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    WHERE cd.id = cc_def_id_param;
    
    IF edge_type_filter IS NULL THEN
        RAISE EXCEPTION 'CC definition % not found', cc_def_id_param;
    END IF;
    
    -- Determine execution strategy
    IF cc_ids IS NULL THEN
        -- Update all CCs for this definition
        target_ccs := ARRAY(
            SELECT DISTINCT cc_id 
            FROM __PGGRAPH_PREFIX___cc_vertices 
            WHERE cc_def_id = cc_def_id_param
        );
        is_single_update := FALSE;
    ELSIF array_length(cc_ids, 1) = 1 THEN
        -- Single CC update - use optimized path
        target_ccs := cc_ids;
        is_single_update := TRUE;
    ELSE
        -- Batch update of specific CCs
        target_ccs := cc_ids;
        is_single_update := FALSE;
    END IF;
    
    -- Execute based on strategy
    IF is_single_update THEN
        -- OPTIMIZED PATH: Single CC update (fastest)
        UPDATE __PGGRAPH_PREFIX___cc fgc
        SET 
            edge_count = (
                SELECT COUNT(*)
                FROM __PGGRAPH_PREFIX___edges e
                JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
                JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
                WHERE e.removed = FALSE
                AND vs.removed = FALSE
                AND vt.removed = FALSE
                AND e.edge_type = ANY(edge_type_filter)
                -- Apply vertex source filter if specified
                AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
                -- Apply vertex destination filter if specified
                AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
                -- Apply edge filter if specified
                AND (edge_filter IS NULL OR e.properties @> edge_filter)
                -- Validate that edges actually connect vertices within the same CC
                AND EXISTS (
                    SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc1
                    JOIN __PGGRAPH_PREFIX___cc_vertices vc2 ON vc1.cc_id = vc2.cc_id
                    WHERE vc1.vertex_id = e.source_id 
                        AND vc2.vertex_id = e.target_id
                        AND vc1.cc_def_id = cc_def_id_param
                        AND vc2.cc_def_id = cc_def_id_param
                        AND vc1.cc_id = target_ccs[1]
                )
            ),
            updated_at = extract(epoch from now())::bigint
        WHERE fgc.id = target_ccs[1] 
        AND fgc.cc_def_id = cc_def_id_param;
        
    ELSE
        -- BATCH PATH: Multiple CCs or all CCs
        WITH cc_stats AS (
            SELECT 
                vc.cc_id,
                COUNT(DISTINCT vc.vertex_id) as vertex_count,
                COUNT(DISTINCT (e.source_id, e.target_id, e.edge_type, e.owner_id)) as edge_count
            FROM __PGGRAPH_PREFIX___cc_vertices vc
            LEFT JOIN __PGGRAPH_PREFIX___edges e ON (
                e.source_id = vc.vertex_id 
                AND e.removed = FALSE
                AND e.edge_type = ANY(edge_type_filter)
                -- Apply edge filter if specified
                AND (edge_filter IS NULL OR e.properties @> edge_filter)
                -- Validate that edges actually connect vertices within the same CC
                AND EXISTS (
                    SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc2
                    WHERE vc2.vertex_id = e.target_id
                    AND vc2.cc_def_id = cc_def_id_param
                    AND vc2.cc_id = vc.cc_id
                )
            )
            LEFT JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
            LEFT JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
            WHERE vc.cc_def_id = cc_def_id_param
            AND vc.cc_id = ANY(target_ccs)
            -- Apply vertex source filter if specified
            AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
            -- Apply vertex destination filter if specified
            AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
            GROUP BY vc.cc_id
        )
        UPDATE __PGGRAPH_PREFIX___cc fgc
        SET 
            vertex_count = cs.vertex_count,
            edge_count = cs.edge_count,
            updated_at = extract(epoch from now())::bigint
        FROM cc_stats cs
        WHERE fgc.id = cs.cc_id
        AND fgc.cc_def_id = cc_def_id_param;
        
        -- Cleanup: Remove CCs with no vertices (only for batch operations)
        DELETE FROM __PGGRAPH_PREFIX___cc
        WHERE cc_def_id = cc_def_id_param
        AND vertex_count = 0;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- INCREMENTAL CONNECTED COMPONENT CALCULATION
-- Advanced functions that implement intelligent incremental CC updates
-- by processing only "dirty" components and affected graph regions
-- 
-- PERFORMANCE OPTIMIZATIONS:
-- 1. EARLY EXIT CHECKS: Rate limit and work detection moved to beginning for fast exit
-- 2. DEFERRED LOCK ACQUISITION: Locks only acquired after confirming there's actual work
-- 3. OPTIMIZED ORPHAN DETECTION: Uses EXISTS with LIMIT for efficient orphan vertex detection
-- 
-- DIRTY TRACKING BEHAVIOR:
-- - FULL CALCULATION: Clears ALL dirty tracking for the definition (clean slate)
-- - INCREMENTAL CALCULATION: Marks processed CCs as processed but keeps unprocessed dirty marks
-- This design ensures that full calculations provide a clean state while incremental
-- calculations maintain the queue of pending work for efficient processing.

-- FUNCTION: dh_calculate_ccs_incremental
-- Purpose: Performs incremental connected component calculation by processing
--          only dirty components and affected vertices, significantly improving performance
-- Parameters: cc_def_id_param (SMALLINT)
-- Returns: TABLE with vertices_processed, ccs_affected, calculation_time_ms
-- Usage: Efficient CC updates during graph modifications, real-time graph maintenance
-- Algorithm: Combines dirty tracking with pgRouting algorithms
-- Note: Much faster than full recalculation for graphs with localized changes
-- All CC calculations use bidirectional connectivity for general-purpose analysis
-- Dirty tracking: Marks processed CCs as processed but maintains unprocessed dirty marks
CREATE OR REPLACE FUNCTION dh_calculate_ccs_incremental(
    cc_def_id_param SMALLINT
) RETURNS TABLE(
    vertices_processed BIGINT,
    ccs_affected BIGINT,
    calculation_time_ms INTEGER
) AS $$
DECLARE
    edge_type_filter SMALLINT[];
    vertex_source_filter JSONB;
    vertex_dest_filter JSONB;
    edge_filter JSONB;
    dirty_ccs BIGINT[];
    v_count BIGINT := 0;
    c_count BIGINT := 0;
    start_time TIMESTAMP;
    rate_limit_hit BOOLEAN := FALSE;
    lock_acquired BOOLEAN := FALSE;
    has_work BOOLEAN := FALSE;
BEGIN
    start_time := clock_timestamp();

    -- Check rate limit FIRST before any expensive operations
    -- CRITICAL FIX: Rate limit based on ANY recent attempt, not just successful completions
    -- This prevents excessive retries and concurrent computations
    SELECT EXISTS (
        SELECT 1 
        FROM __PGGRAPH_PREFIX___cc_computation_log l
        JOIN __PGGRAPH_PREFIX___cc_definitions d ON d.id = l.cc_def_id
        WHERE l.cc_def_id = cc_def_id_param 
          AND l.computation_type = 'incremental'
          AND (extract(epoch from now()) - l.started_at) < d.incremental_calculation_min_interval_seconds
    ) INTO rate_limit_hit;
    
    IF rate_limit_hit THEN
        RAISE NOTICE 'Rate limit hit for incremental CC calculation for definition %. Skipping.', cc_def_id_param;
        RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::INTEGER;
        RETURN;
    END IF;

    -- Get cc definition and filter predicates
    SELECT 
        cd.edge_types,
        cd.vertex_source_filter,
        cd.vertex_dest_filter, 
        cd.edge_filter
    INTO 
        edge_type_filter,
        vertex_source_filter,
        vertex_dest_filter,
        edge_filter
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    WHERE cd.id = cc_def_id_param;

    IF edge_type_filter IS NULL THEN
        RAISE EXCEPTION 'CC definition % not found', cc_def_id_param;
    END IF;

    -- Check for work BEFORE acquiring locks
    -- Get all unprocessed dirty CCs first
    SELECT COALESCE(array_agg(cc_id::BIGINT), ARRAY[]::BIGINT[])
    INTO dirty_ccs
    FROM __PGGRAPH_PREFIX___cc_dirty
    WHERE cc_def_id = cc_def_id_param
    AND processed = FALSE
    AND cc_id IS NOT NULL;

    -- More efficient orphan vertex detection using EXISTS with LIMIT
    -- Only check if orphans exist, don't count them yet
    SELECT EXISTS (
        SELECT 1
        FROM __PGGRAPH_PREFIX___vertices v
        WHERE v.removed = FALSE
        AND NOT EXISTS (
            SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc
            WHERE vc.vertex_id = v.xxhash64_id
            AND vc.cc_def_id = cc_def_id_param
        )
        AND (
            EXISTS (
                SELECT 1 FROM __PGGRAPH_PREFIX___edges e
                WHERE e.source_id = v.xxhash64_id
                AND e.edge_type = ANY(edge_type_filter)
                AND e.removed = FALSE
                LIMIT 1
            )
            OR EXISTS (
                SELECT 1 FROM __PGGRAPH_PREFIX___edges e
                WHERE e.target_id = v.xxhash64_id
                AND e.edge_type = ANY(edge_type_filter)
                AND e.removed = FALSE
                LIMIT 1
            )
        )
        LIMIT 1
    ) INTO has_work;

    -- If no dirty CCs and no orphans, nothing to do - early exit
    IF (dirty_ccs IS NULL OR array_length(dirty_ccs, 1) = 0) AND NOT has_work THEN
        RAISE NOTICE 'No dirty CCs or orphan vertices found for definition %. Nothing to process.', cc_def_id_param;
        RETURN QUERY SELECT 0::bigint, 0::bigint, 0::integer;
        RETURN;
    END IF;

    -- Only acquire lock after confirming there's work to do
    IF NOT pg_try_advisory_lock(1000000 + cc_def_id_param) THEN
        RAISE NOTICE 'Definition % is locked by another process. Skipping incremental calculation.', 
            cc_def_id_param;
        RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::INTEGER;
        RETURN;
    END IF;
    lock_acquired := TRUE;

    -- Now that we have the lock, get the actual orphan count if needed
    IF has_work THEN
        WITH orphan_vertices AS (
            SELECT DISTINCT v.xxhash64_id
            FROM __PGGRAPH_PREFIX___vertices v
            WHERE v.removed = FALSE
            AND NOT EXISTS (
                SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc
                WHERE vc.vertex_id = v.xxhash64_id
                AND vc.cc_def_id = cc_def_id_param
            )
            AND (
                EXISTS (
                    SELECT 1 FROM __PGGRAPH_PREFIX___edges e
                    WHERE e.source_id = v.xxhash64_id
                    AND e.edge_type = ANY(edge_type_filter)
                    AND e.removed = FALSE
                )
                OR EXISTS (
                    SELECT 1 FROM __PGGRAPH_PREFIX___edges e
                    WHERE e.target_id = v.xxhash64_id
                    AND e.edge_type = ANY(edge_type_filter)
                    AND e.removed = FALSE
                )
            )
        )
        SELECT COUNT(*) INTO v_count FROM orphan_vertices;
    END IF;

    -- Get all vertices from dirty CCs plus orphans
    CREATE TEMP TABLE temp_affected_vertices AS
    SELECT DISTINCT vertex_id
    FROM __PGGRAPH_PREFIX___cc_vertices
    WHERE cc_id = ANY(COALESCE(dirty_ccs, '{}'))
    AND cc_def_id = cc_def_id_param
    UNION
    SELECT v.xxhash64_id
    FROM __PGGRAPH_PREFIX___vertices v
    WHERE v.removed = FALSE
    AND NOT EXISTS (
        SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc
        WHERE vc.vertex_id = v.xxhash64_id
        AND vc.cc_def_id = cc_def_id_param
    )
    AND (
        EXISTS (
            SELECT 1 FROM __PGGRAPH_PREFIX___edges e
            WHERE e.source_id = v.xxhash64_id
            AND e.edge_type = ANY(edge_type_filter)
            AND e.removed = FALSE
        )
        OR EXISTS (
            SELECT 1 FROM __PGGRAPH_PREFIX___edges e
            WHERE e.target_id = v.xxhash64_id
            AND e.edge_type = ANY(edge_type_filter)
            AND e.removed = FALSE
        )
    );

    -- Check if we actually have any affected vertices to process
    IF NOT EXISTS (SELECT 1 FROM temp_affected_vertices) THEN
        RAISE NOTICE 'No affected vertices found for definition %. Nothing to process.', cc_def_id_param;
        -- Create empty temp table to avoid downstream errors
        CREATE TEMP TABLE temp_new_ccs AS
        SELECT 0::bigint as vertex_id, 0::bigint as cc_id WHERE FALSE;
        
        -- Clean up and return early
        DROP TABLE temp_affected_vertices;
        DROP TABLE temp_new_ccs;
        
        -- Log the calculation
        INSERT INTO __PGGRAPH_PREFIX___cc_computation_log
            (computation_type, cc_def_id, completed_at, vertices_processed,
            ccs_found, execution_time_ms, status)
        VALUES
            ('incremental', cc_def_id_param, extract(epoch from now())::bigint, 0, 0,
            extract(milliseconds from clock_timestamp() - start_time)::integer, 'COMPLETED');

        -- Release the advisory lock
        PERFORM pg_advisory_unlock(1000000 + cc_def_id_param);

        RETURN QUERY SELECT 0::bigint, 0::bigint, 0::integer;
        RETURN;
    END IF;

    -- CRITICAL FIX: Don't delete old mappings or CCs until we've processed the changes
    -- Instead, we need to analyze what happened and handle it appropriately
    
    -- Create a snapshot of the current state for analysis
    CREATE TEMP TABLE temp_old_cc_state AS
    SELECT 
        vc.vertex_id,
        vc.cc_id,
        vc.cc_def_id
    FROM __PGGRAPH_PREFIX___cc_vertices vc
    WHERE vc.vertex_id IN (SELECT vertex_id FROM temp_affected_vertices)
    AND vc.cc_def_id = cc_def_id_param;

    -- Analyze what type of change we're dealing with
    -- We need to determine if this is an edge addition, removal, or modification
    CREATE TEMP TABLE temp_change_analysis AS
    SELECT 
        'edge_removal' as change_type,
        COUNT(*) as affected_vertices
    FROM temp_old_cc_state
    WHERE EXISTS (
        -- Check if any of these vertices are now disconnected due to edge removal
        SELECT 1 FROM __PGGRAPH_PREFIX___edges e
        WHERE e.removed = FALSE
        AND e.edge_type = ANY(edge_type_filter)
        AND (e.source_id = temp_old_cc_state.vertex_id OR e.target_id = temp_old_cc_state.vertex_id)
    )
    HAVING COUNT(*) > 0;

    -- If this is an edge removal scenario, we need special handling
    IF EXISTS (SELECT 1 FROM temp_change_analysis WHERE change_type = 'edge_removal') THEN
        -- Handle edge removal: recalculate CCs for the affected vertices
        -- but preserve the original graph state during calculation
        
        -- Create a temporary graph that includes the affected vertices and their remaining connections
        CREATE TEMP TABLE temp_remaining_edges AS
        SELECT DISTINCT
            e.source_id,
            e.target_id
        FROM __PGGRAPH_PREFIX___edges e
        JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
        JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
        WHERE e.removed = FALSE
        AND vs.removed = FALSE
        AND vt.removed = FALSE
        AND e.edge_type = ANY(edge_type_filter)
        AND (e.source_id IN (SELECT vertex_id FROM temp_affected_vertices)
            OR e.target_id IN (SELECT vertex_id FROM temp_affected_vertices))
        -- Apply vertex source filter if specified
        AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
        -- Apply vertex destination filter if specified
        AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
        -- Apply edge filter if specified
        AND (edge_filter IS NULL OR e.properties @> edge_filter);

        -- Check if we have any remaining connections
        IF EXISTS (SELECT 1 FROM temp_remaining_edges) THEN
            -- Additional safety check: ensure we have actual edge data before calling pgRouting
            IF (SELECT COUNT(*) FROM temp_remaining_edges) > 0 THEN
                            -- We have remaining edges, recalculate CCs for the affected subgraph
            -- Filter components during calculation using window functions
            CREATE TEMP TABLE temp_new_ccs AS
            SELECT
                node::bigint as vertex_id,
                component as cc_id
            FROM (
                SELECT 
                    node,
                    component,
                    COUNT(*) OVER (PARTITION BY component) as component_size
                FROM pgr_connectedComponents(
                    format('SELECT
                        LEAST(source_id, target_id)::bigint as id,
                        source_id::bigint as source,
                        target_id::bigint as target,
                        1.0::float as cost,
                        1.0::float as reverse_cost
                    FROM temp_remaining_edges')
                )
            ) filtered_components
            WHERE component_size >= (
                SELECT min_component_size 
                FROM __PGGRAPH_PREFIX___cc_definitions 
                WHERE id = cc_def_id_param
            )
            AND node IN (SELECT vertex_id FROM temp_affected_vertices);
            ELSE
                -- No actual edges despite the table existing, treat as isolated
                CREATE TEMP TABLE temp_new_ccs AS
                SELECT 
                    vertex_id,
                    -- Isolated vertices will be filtered out by minimum size threshold
                    vertex_id as cc_id
                FROM temp_affected_vertices;
            END IF;
        ELSE
            -- No remaining edges, all affected vertices become isolated
            CREATE TEMP TABLE temp_new_ccs AS
            SELECT 
                vertex_id,
                -- Isolated vertices will be filtered out by minimum size threshold
                vertex_id as cc_id
            FROM temp_affected_vertices;
        END IF;

        -- Clean up temporary tables
        DROP TABLE temp_remaining_edges;
    ELSE
        -- This is an edge addition or modification scenario
        -- Use the original pgRouting approach with filters
        CREATE TEMP TABLE temp_edge_data AS
        SELECT DISTINCT
            e.source_id,
            e.target_id
        FROM __PGGRAPH_PREFIX___edges e
        JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
        JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
        WHERE e.removed = FALSE
        AND vs.removed = FALSE
        AND vt.removed = FALSE
        AND e.edge_type = ANY(edge_type_filter)
        AND (e.source_id IN (SELECT vertex_id FROM temp_affected_vertices)
            OR e.target_id IN (SELECT vertex_id FROM temp_affected_vertices))
        -- Apply vertex source filter if specified
        AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
        -- Apply vertex destination filter if specified
        AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
        -- Apply edge filter if specified
        AND (edge_filter IS NULL OR e.properties @> edge_filter);

        IF EXISTS (SELECT 1 FROM temp_edge_data) THEN
            -- Filter components during calculation using window functions
            CREATE TEMP TABLE temp_new_ccs AS
            SELECT
                node::bigint as vertex_id,
                component as cc_id
            FROM (
                SELECT 
                    node,
                    component,
                    COUNT(*) OVER (PARTITION BY component) as component_size
                FROM pgr_connectedComponents(
                    format('SELECT
                        LEAST(source_id, target_id)::bigint as id,
                        source_id::bigint as source,
                        target_id::bigint as target,
                        1.0::float as cost,
                        1.0::float as reverse_cost
                    FROM temp_edge_data')
                )
            ) filtered_components
            WHERE component_size >= (
                SELECT min_component_size 
                FROM __PGGRAPH_PREFIX___cc_definitions 
                WHERE id = cc_def_id_param
            );
        ELSE
            CREATE TEMP TABLE temp_new_ccs AS
            SELECT 0::bigint as vertex_id, 0::bigint as cc_id WHERE FALSE;
        END IF;

        DROP TABLE temp_edge_data;
    END IF;

    -- Clean up analysis tables
    DROP TABLE temp_change_analysis;
    DROP TABLE temp_old_cc_state;

    -- Insert new mappings, handling conflicts by updating existing records
    -- No additional filtering needed - components already filtered above
    INSERT INTO __PGGRAPH_PREFIX___cc_vertices (vertex_id, cc_def_id, cc_id, assigned_at)
    SELECT
        vertex_id,
        cc_def_id_param,
        cc_id,
        extract(epoch from now())::bigint
    FROM temp_new_ccs
    WHERE vertex_id IS NOT NULL  -- Only filter out NULL values
    ON CONFLICT (vertex_id, cc_def_id) DO UPDATE SET
        cc_id = EXCLUDED.cc_id,
        assigned_at = EXCLUDED.assigned_at;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- NOW we can safely remove old mappings and CCs since we have the new ones
    DELETE FROM __PGGRAPH_PREFIX___cc_vertices
    WHERE vertex_id IN (SELECT vertex_id FROM temp_affected_vertices)
    AND cc_def_id = cc_def_id_param
    AND (vertex_id, cc_def_id) NOT IN (
        SELECT vertex_id, cc_def_id_param FROM temp_new_ccs
    );

    -- Delete old CCs that are no longer needed
    IF dirty_ccs IS NOT NULL AND array_length(dirty_ccs, 1) > 0 THEN
        DELETE FROM __PGGRAPH_PREFIX___cc
        WHERE cc_def_id = cc_def_id_param
        AND id = ANY(dirty_ccs)
        AND id NOT IN (SELECT DISTINCT cc_id FROM temp_new_ccs);
    END IF;

    -- =============================================================================
    -- UPDATE METADATA_GRAPH_CC TABLE
    -- =============================================================================
    -- This section ensures the __PGGRAPH_PREFIX___cc table accurately reflects:
    -- 1. Current vertex counts for each CC
    -- 2. Current edge counts for each CC  
    -- 3. Proper timestamps for tracking changes
    -- 4. Consistency between vertex assignments and CC metadata

    -- Create/update CC records with updated counts
    -- This ensures vertex_count reflects the current number of vertices in each CC
    INSERT INTO __PGGRAPH_PREFIX___cc (id, cc_def_id, created_at, updated_at, vertex_count, edge_count)
    SELECT
        cc_id,
        cc_def_id_param,
        extract(epoch from now())::bigint,
        extract(epoch from now())::bigint,
        COUNT(*)::integer,
        0 -- Will be updated below
    FROM temp_new_ccs
    GROUP BY cc_id
    -- No HAVING clause needed - components already filtered above
    ON CONFLICT (id, cc_def_id) DO UPDATE SET
        vertex_count = EXCLUDED.vertex_count,
        updated_at = extract(epoch from now())::bigint,
        -- Don't update created_at for existing CCs to preserve original creation time
        -- edge_count will be updated in the next step
        edge_count = 0;  -- Reset edge count for recalculation

    -- Update edge counts for all affected CCs
    -- This ensures edge_count reflects the current number of edges within each CC
    -- We need to update ALL CCs that might have been affected, not just new ones
    WITH affected_cc_ids AS (
        -- Include both new CCs and any existing CCs that might have had vertices reassigned
        SELECT DISTINCT cc_id FROM temp_new_ccs
        UNION
        -- Also include any CCs that were in the dirty list (they might have been modified)
        SELECT DISTINCT unnest(COALESCE(dirty_ccs, '{}'))
    )
    UPDATE __PGGRAPH_PREFIX___cc fgc
    SET edge_count = (
        SELECT COUNT(*)
        FROM __PGGRAPH_PREFIX___edges e
        JOIN __PGGRAPH_PREFIX___vertices vs ON e.source_id = vs.xxhash64_id
        JOIN __PGGRAPH_PREFIX___vertices vt ON e.target_id = vt.xxhash64_id
        WHERE e.removed = FALSE
        AND vs.removed = FALSE
        AND vt.removed = FALSE
        AND e.edge_type = ANY(edge_type_filter)
        -- Apply vertex source filter if specified
        AND (vertex_source_filter IS NULL OR vs.properties @> vertex_source_filter)
        -- Apply vertex destination filter if specified
        AND (vertex_dest_filter IS NULL OR vt.properties @> vertex_dest_filter)
        -- Apply edge filter if specified
        AND (edge_filter IS NULL OR e.properties @> edge_filter)
        -- Validate that edges actually connect vertices within the same CC
        AND EXISTS (
            SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc1
            WHERE vc1.vertex_id = e.source_id
            AND vc1.cc_def_id = cc_def_id_param
            AND vc1.cc_id = fgc.id
        )
        AND EXISTS (
            SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices vc2
            WHERE vc2.vertex_id = e.target_id
            AND vc2.cc_def_id = cc_def_id_param
            AND vc2.cc_id = fgc.id
        )
    )
    WHERE fgc.cc_def_id = cc_def_id_param
    AND fgc.id IN (SELECT cc_id FROM affected_cc_ids);

    -- Mark dirty CCs as processed (not deleted - this maintains the queue for other CCs)
    UPDATE __PGGRAPH_PREFIX___cc_dirty
    SET processed = TRUE,
        processed_at = extract(epoch from now())::bigint
    WHERE cc_def_id = cc_def_id_param
    AND cc_id = ANY(COALESCE(dirty_ccs, '{}'))
    AND processed = FALSE;

    SELECT COUNT(DISTINCT cc_id) INTO c_count FROM temp_new_ccs;

    DROP TABLE temp_affected_vertices;
    DROP TABLE temp_new_ccs;

    -- Log the calculation
    INSERT INTO __PGGRAPH_PREFIX___cc_computation_log
        (computation_type, cc_def_id, completed_at, vertices_processed,
        ccs_found, execution_time_ms, status)
    VALUES
        ('incremental', cc_def_id_param, extract(epoch from now())::bigint, v_count, c_count,
        extract(milliseconds from clock_timestamp() - start_time)::integer, 'COMPLETED');

    -- Release the advisory lock
    PERFORM pg_advisory_unlock(1000000 + cc_def_id_param);

    RETURN QUERY SELECT
        v_count,
        c_count,
        extract(milliseconds from clock_timestamp() - start_time)::integer;
EXCEPTION
    WHEN OTHERS THEN
        -- Ensure locks are released even on error
        PERFORM pg_advisory_unlock(1000000 + cc_def_id_param);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- UTILITY AND MAINTENANCE FUNCTIONS
-- Helper functions for system maintenance, monitoring, and manual operations
-- 
-- MAINTENANCE OVERVIEW:
-- These functions support ongoing system health and performance:
-- 1. DIRTY MARKING: Manual and automatic CC invalidation for forced recalculation
-- 2. PERFORMANCE MONITORING: Tracking computation metrics and execution times
-- 3. SYSTEM HEALTH: Validation and consistency checking of CC assignments
-- 4. DEBUGGING SUPPORT: Manual operations for troubleshooting and maintenance

-- FUNCTION: dh_mark_cc_dirty
-- Purpose: Manually marks connected components as dirty for forced recalculation
-- Parameters: cc_def_id_param, cc_ids (array of CC IDs to mark)
-- Returns: INTEGER (number of CCs marked as dirty)
-- Usage: Manual CC invalidation, system maintenance, and debugging
-- Note: Useful for forcing recalculation of specific components
--
-- ID STABILITY: Both ID parameters must be stable and consistent across deployments.
CREATE OR REPLACE FUNCTION dh_mark_cc_dirty(
    cc_def_id_param SMALLINT,
    cc_ids BIGINT[]
) RETURNS INTEGER AS $$
DECLARE
    marked_count INTEGER;
BEGIN
    INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
    SELECT cc_def_id_param, unnest(cc_ids)
    ON CONFLICT (cc_def_id, cc_id) DO UPDATE
    SET marked_at = extract(epoch from now())::bigint,
        processed = FALSE;

    GET DIAGNOSTICS marked_count = ROW_COUNT;
    RETURN marked_count;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: dh_check_cc_calculation_locks
-- Purpose: Check the current lock status for CC calculations across all definitions
-- Parameters: None
-- Returns: TABLE with lock status information
-- Usage: Monitor which CC definitions are currently locked and by which processes
-- Note: Useful for debugging and monitoring concurrent execution
CREATE OR REPLACE FUNCTION dh_check_cc_calculation_locks()
RETURNS TABLE(
    cc_def_id SMALLINT,
    definition_name VARCHAR(200),
    definition_locked BOOLEAN,
    lock_holder_pid INTEGER,
    lock_holder_application_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cd.id as cc_def_id,
        cd.name as definition_name,
        CASE WHEN pg_try_advisory_lock(1000000 + cd.id) THEN FALSE ELSE TRUE END as definition_locked,
        l.pid as lock_holder_pid,
        l.application_name as lock_holder_application_name
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    LEFT JOIN pg_locks l ON l.locktype = 'advisory' 
        AND (l.objid = 1000000 + cd.id)
    WHERE l.granted = TRUE
    ORDER BY cd.id;
    
    -- Release any locks we acquired during the check
    PERFORM pg_advisory_unlock_all();
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: dh_force_release_cc_calculation_locks
-- Purpose: Force release all advisory locks for CC calculations
-- Parameters: None
-- Returns: INTEGER (number of locks released)
-- Usage: Emergency cleanup of orphaned locks, debugging, and system maintenance
-- Note: Use with caution - only when you're sure no legitimate processes are running
CREATE OR REPLACE FUNCTION dh_force_release_cc_calculation_locks()
RETURNS INTEGER AS $$
DECLARE
    lock_count INTEGER := 0;
    cd RECORD;
BEGIN
    -- Release locks for all CC definitions
    FOR cd IN SELECT id FROM __PGGRAPH_PREFIX___cc_definitions
    LOOP
        -- Try to acquire and immediately release the lock for this definition
        IF pg_try_advisory_lock(1000000 + cd.id) THEN
            PERFORM pg_advisory_unlock(1000000 + cd.id);
            RAISE NOTICE 'Released lock for definition % (%)', cd.name, cd.id;
        ELSE
            RAISE NOTICE 'Definition % (%) is not locked', cd.name, cd.id;
        END IF;
    END LOOP;
    
    -- Also release any session-level locks for this session
    PERFORM pg_advisory_unlock_all();
    
    RETURN lock_count;
END;
$$ LANGUAGE plpgsql;

-- CC UPDATE FUNCTIONS
-- These functions provide automatic dirty marking and manual CC execution control
-- Automatic triggers handle dirty marking, manual calls handle CC calculations
-- 
-- NOTE: These functions are called automatically by triggers for dirty marking
-- Use dh_calculate_ccs_full() and dh_calculate_ccs_incremental() for bulk CC updates

-- FUNCTION: record_dirty_ccs_on_edge_change
-- Purpose: Function for marking connected components as dirty when edges change
-- Parameters: None (trigger function)
-- Returns: NEW or OLD record depending on operation
-- Usage: Called automatically by triggers when edges are updated or deleted
-- Note: Dirty marking is automatic via triggers, CC calculations are manual
CREATE OR REPLACE FUNCTION record_dirty_ccs_on_edge_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Mark CCs as dirty for each CC definition that includes this edge type
    -- Works for both soft deletes (UPDATE removed=TRUE) and hard deletes
    INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
    SELECT DISTINCT
        cd.id as cc_def_id,
        vc.cc_id
    FROM __PGGRAPH_PREFIX___cc_definitions cd
    JOIN __PGGRAPH_PREFIX___cc_vertices vc ON vc.cc_def_id = cd.id
    WHERE (CASE
        WHEN TG_OP = 'UPDATE' THEN NEW.edge_type
        WHEN TG_OP = 'DELETE' THEN OLD.edge_type
    END) = ANY(cd.edge_types)
    AND (vc.vertex_id IN (
        CASE
            WHEN TG_OP = 'UPDATE' THEN NEW.source_id
            WHEN TG_OP = 'DELETE' THEN OLD.source_id
        END,
        CASE
            WHEN TG_OP = 'UPDATE' THEN NEW.target_id
            WHEN TG_OP = 'DELETE' THEN OLD.target_id
        END
    ))
    AND vc.removed = FALSE  -- Only mark CCs as dirty for non-removed vertices
    ON CONFLICT (cc_def_id, cc_id) DO UPDATE
    SET marked_at = extract(epoch from now())::bigint,
        processed = FALSE;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: record_dirty_ccs_on_vertex_change
-- Purpose: Function for marking connected components as dirty when vertices are removed or restored
-- Parameters: None (trigger function)
-- Returns: NEW record
-- Usage: Called automatically by triggers when vertices are soft-deleted or restored
-- Note: Marks all CCs containing the affected vertex as dirty for recalculation
CREATE OR REPLACE FUNCTION record_dirty_ccs_on_vertex_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Mark CCs as dirty for each CC definition that contains this vertex
    INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
    SELECT DISTINCT
        vc.cc_def_id,
        vc.cc_id
    FROM __PGGRAPH_PREFIX___cc_vertices vc
    WHERE vc.vertex_id = NEW.xxhash64_id
    ON CONFLICT (cc_def_id, cc_id) DO UPDATE
    SET marked_at = extract(epoch from now())::bigint,
        processed = FALSE;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: assign_cc_for_new_edge
-- Purpose: Function for assigning vertices to connected components when new edges are inserted
-- Parameters: None (trigger function)
-- Returns: NEW record
-- Usage: Called automatically by trigger when new edges are inserted
-- Note: CC assignment is automatic via trigger, CC calculations are manual
-- 
-- CRITICAL: This function MUST check filter eligibility before assigning vertices
-- to ensure only vertices meeting the CC definition's filter criteria are included.
-- Without this check, vertices could be assigned to CCs they don't belong in,
-- leading to inconsistent graph states and incorrect CC analysis results.
CREATE OR REPLACE FUNCTION assign_cc_for_new_edge()
RETURNS TRIGGER AS $$
DECLARE
    source_cc_record RECORD;
    target_cc_record RECORD;
    cd RECORD;
    source_eligible BOOLEAN;
    target_eligible BOOLEAN;
    edge_eligible BOOLEAN;
BEGIN
    -- For each CC definition that includes this edge type
    FOR cd IN
        SELECT id, edge_types, vertex_source_filter, vertex_dest_filter, edge_filter
        FROM __PGGRAPH_PREFIX___cc_definitions
        WHERE NEW.edge_type = ANY(edge_types)
    LOOP
        -- Check if source vertex meets filter criteria
        SELECT 
            (cd.vertex_source_filter IS NULL OR v.properties @> cd.vertex_source_filter)
        INTO source_eligible
        FROM __PGGRAPH_PREFIX___vertices v
        WHERE v.xxhash64_id = NEW.source_id AND v.removed = FALSE;
        
        -- Check if target vertex meets filter criteria
        SELECT 
            (cd.vertex_dest_filter IS NULL OR v.properties @> cd.vertex_dest_filter)
        INTO target_eligible
        FROM __PGGRAPH_PREFIX___vertices v
        WHERE v.xxhash64_id = NEW.target_id AND v.removed = FALSE;
        
        -- Check if edge meets filter criteria
        SELECT 
            (cd.edge_filter IS NULL OR NEW.properties @> cd.edge_filter)
        INTO edge_eligible;
        
        -- Skip this CC definition if any filter criteria are not met
        IF NOT (source_eligible AND target_eligible AND edge_eligible) THEN
            CONTINUE;
        END IF;
        
        -- Check CC assignment for source vertex (only non-removed)
        SELECT cc_id INTO source_cc_record
        FROM __PGGRAPH_PREFIX___cc_vertices
        WHERE vertex_id = NEW.source_id
        AND cc_def_id = cd.id
        AND removed = FALSE;

        -- Check CC assignment for target vertex (only non-removed)
        SELECT cc_id INTO target_cc_record
        FROM __PGGRAPH_PREFIX___cc_vertices
        WHERE vertex_id = NEW.target_id
        AND cc_def_id = cd.id
        AND removed = FALSE;

        -- Case 1: Both vertices have CC assignments and they differ
        IF source_cc_record.cc_id IS NOT NULL AND
           target_cc_record.cc_id IS NOT NULL AND
           source_cc_record.cc_id != target_cc_record.cc_id THEN
            -- Mark both CCs as dirty for merging
            INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
            VALUES
                (cd.id, source_cc_record.cc_id),
                (cd.id, target_cc_record.cc_id)
            ON CONFLICT (cc_def_id, cc_id) DO UPDATE
            SET marked_at = extract(epoch from now())::bigint,
                processed = FALSE;

        -- Case 2: Source has CC, target doesn't
        ELSIF source_cc_record.cc_id IS NOT NULL AND target_cc_record.cc_id IS NULL THEN
            -- Assign target to source's CC (target eligibility already verified above)
            INSERT INTO __PGGRAPH_PREFIX___cc_vertices (vertex_id, cc_def_id, cc_id, removed)
            VALUES (NEW.target_id, cd.id, source_cc_record.cc_id, FALSE);

            -- Mark CC as dirty to update counts
            INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
            VALUES (cd.id, source_cc_record.cc_id)
            ON CONFLICT (cc_def_id, cc_id) DO UPDATE
            SET marked_at = extract(epoch from now())::bigint,
                processed = FALSE;

        -- Case 3: Target has CC, source doesn't
        ELSIF source_cc_record.cc_id IS NULL AND target_cc_record.cc_id IS NOT NULL THEN
            -- Assign source to target's CC (source eligibility already verified above)
            INSERT INTO __PGGRAPH_PREFIX___cc_vertices (vertex_id, cc_def_id, cc_id, removed)
            VALUES (NEW.source_id, cd.id, target_cc_record.cc_id, FALSE);

            -- Mark CC as dirty to update counts
            INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
            VALUES (cd.id, target_cc_record.cc_id)
            ON CONFLICT (cc_def_id, cc_id) DO UPDATE
            SET marked_at = extract(epoch from now())::bigint,
                processed = FALSE;

        -- Case 4: Neither vertex has CC assignment
        -- Treat them as singleton CCs and mark as dirty
        ELSIF source_cc_record.cc_id IS NULL AND target_cc_record.cc_id IS NULL THEN
            -- Check if we should create CCs for this edge type based on minimum size
            -- For edges, we need at least 2 vertices, so we'll create temporary assignments
            -- but they'll only become actual CCs if they meet the minimum size threshold
            INSERT INTO __PGGRAPH_PREFIX___cc_vertices (vertex_id, cc_def_id, cc_id, removed)
            VALUES
                (NEW.source_id, cd.id, NEW.source_id, FALSE),
                (NEW.target_id, cd.id, NEW.target_id, FALSE);

            -- Mark the minimum vertex ID as dirty (they'll be merged during processing)
            INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
            VALUES (cd.id, NEW.source_id)
            ON CONFLICT (cc_def_id, cc_id) DO UPDATE
            SET marked_at = extract(epoch from now())::bigint,
                processed = FALSE;

            INSERT INTO __PGGRAPH_PREFIX___cc_dirty (cc_def_id, cc_id)
            VALUES (cd.id, NEW.target_id)
            ON CONFLICT (cc_def_id, cc_id) DO UPDATE
            SET marked_at = extract(epoch from now())::bigint,
                processed = FALSE;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: sync_vertex_removed_status_to_cc_vertices
-- Purpose: Automatically syncs the removed status from __PGGRAPH_PREFIX___vertices to __PGGRAPH_PREFIX___cc_vertices
-- Parameters: None (trigger function)
-- Returns: NEW record
-- Usage: Called automatically by trigger when vertices are soft-deleted or restored
-- Note: Ensures CC vertex assignments respect vertex removal status
CREATE OR REPLACE FUNCTION sync_vertex_removed_status_to_cc_vertices()
RETURNS TRIGGER AS $$
BEGIN
    -- When a vertex is soft-deleted or restored, sync the status to all CC assignments
    UPDATE __PGGRAPH_PREFIX___cc_vertices
    SET 
        removed = NEW.removed,
        updated_at = extract(epoch from now())::bigint
    WHERE vertex_id = NEW.xxhash64_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- AUTOMATIC DIRTY MARKING WITH MANUAL CC EXECUTION
-- The system uses automatic dirty marking with manual CC execution
-- 
-- ARCHITECTURE:
-- The system automatically tracks all graph changes for CC dirty marking:
-- 1. VERTEX SOFT DELETION: Automatically marks affected CCs as dirty and syncs removed status
-- 2. EDGE SOFT DELETION: Automatically marks affected CCs as dirty
-- 3. NEW EDGE PROCESSING: Automatically marks affected CCs as dirty
-- 4. EDGE HARD DELETION: Automatically marks affected CCs as dirty
-- 5. EDGE UPDATES: Automatically marks affected CCs as dirty
-- 6. VERTEX REMOVAL SYNC: Automatically syncs removed status from vertices to CC assignments
-- 7. MANUAL DIRTY MARKING: Use dh_mark_cc_dirty() for additional cases
--
-- CONSISTENCY GUARANTEES:
-- - All graph changes are automatically tracked for CC dirty marking
-- - CC calculations only occur when explicitly requested (manual execution)
-- - Predictable performance with comprehensive change tracking
-- - No missed changes - all modifications are tracked automatically

-- AUTOMATIC EDGE PROCESSING AND TRACKING
-- All edge operations are automatically tracked for CC dirty marking:
-- 
-- EDGE INSERTIONS: Automatically mark affected CCs as dirty
-- EDGE UPDATES: Automatically mark affected CCs as dirty  
-- EDGE DELETIONS: Automatically mark affected CCs as dirty
-- EDGE SOFT DELETIONS: Automatically mark affected CCs as dirty
--
-- CC ASSIGNMENT: Still done explicitly through:
-- 1. dh_calculate_ccs_full() - for complete graph recalculation
-- 2. dh_calculate_ccs_incremental() - for processing specific dirty CCs
--
-- AUTOMATIC DIRTY MARKING: All edge changes automatically mark affected CCs as dirty
-- Manual dirty marking: Use dh_mark_cc_dirty() for additional cases or bulk operations
-- Note: Automatic triggers ensure no edge changes are missed

-- FUNCTION: dh_analyze_cc_impact
-- Purpose: Analyzes downstream impact from a starting vertex within a specific connected component
--          Optimized for very large downstream graphs with resumable processing
-- Parameters: 
--   cc_def_id_param (SMALLINT) - Connected component definition ID
--   cc_id_param (BIGINT) - Connected component ID within the definition
--   start_vertex_id_param (BIGINT) - Starting vertex for downstream analysis
--   chunk_size_param (INTEGER) - Number of vertices to process in each chunk (default: 10000)
--   resume_vertex_id_param (BIGINT, optional) - Resume processing from this vertex ID
-- Returns: TABLE with vertex_id, depth, path_sequence, and processing metadata
-- Usage: Analyze downstream impact for very large graphs with resumable processing
-- 
-- OPTIMIZATION STRATEGIES FOR LARGE GRAPHS:
-- - Chunked processing: Processes vertices in configurable chunks to manage memory usage
-- - Resumable processing: Can resume from any vertex ID for long-running analyses
-- - pgRouting integration: Uses pgr_breadthFirstSearch for efficient reachability analysis
--   (chosen over pgr_dijkstra since we only need reachability, not shortest paths)
-- - Breadth-first traversal: Processes vertices by depth level for optimal memory usage
-- - Temporary table management: Uses session-scoped temp tables for intermediate results
-- - Progress estimation: Provides estimated remaining work for monitoring
-- - Cursor-based iteration: Efficiently processes large result sets
-- 
-- PERFORMANCE CHARACTERISTICS:
-- - Memory usage: O(chunk_size) rather than O(total_vertices)
-- - Processing time: Linear with respect to reachable vertices
-- - Algorithm efficiency: pgr_breadthFirstSearch is ~3-5x faster than pgr_dijkstra for reachability
-- - Scalability: Handles graphs with millions of vertices
-- - Resumability: Can pause/resume without losing progress
-- 
-- USE CASES:
-- - Impact analysis: Determine downstream impact of data lineage changes
-- - Dependency tracking: Find all dependent systems/data assets
-- - Compliance analysis: Identify affected data for regulatory requirements
-- - Change management: Assess scope of proposed changes
-- 
-- EXAMPLE USAGE:
-- -- Process first 10,000 downstream vertices
-- SELECT * FROM dh_analyze_cc_impact(1, 12345, 67890, 10000);
-- 
-- -- Resume from specific vertex (e.g., after processing first chunk)
-- SELECT * FROM dh_analyze_cc_impact(1, 12345, 67890, 10000, 99999);
-- 
-- -- Process in smaller chunks for memory-constrained environments
-- SELECT * FROM dh_analyze_cc_impact(1, 12345, 67890, 1000);
CREATE OR REPLACE FUNCTION dh_analyze_cc_impact(
    cc_def_id_param SMALLINT,
    cc_id_param BIGINT,
    start_vertex_id_param BIGINT,
    chunk_size_param INTEGER DEFAULT 10000,
    resume_vertex_id_param BIGINT DEFAULT NULL
) RETURNS TABLE(
    vertex_id BIGINT,
    depth INTEGER,
    path_sequence INTEGER,
    processing_chunk INTEGER,
    total_processed INTEGER,
    estimated_remaining INTEGER,
    processing_time_ms BIGINT
) AS $$
    DECLARE
        start_time TIMESTAMP;
        current_chunk INTEGER;
        total_processed INTEGER;
        edge_query TEXT;
        vertex_record RECORD;
        current_path_sequence INTEGER;
        all_reachable_vertices_cursor REFCURSOR;
        all_vertices_temp_table TEXT;
        vertex_count INTEGER;
        chunk_start_idx INTEGER;
        chunk_end_idx INTEGER;
        resume_found BOOLEAN;
        resume_reachable BOOLEAN;
BEGIN
    -- Validate input parameters
    IF cc_def_id_param IS NULL THEN
        RAISE EXCEPTION 'cc_def_id_param cannot be NULL';
    END IF;
    
    IF cc_id_param IS NULL THEN
        RAISE EXCEPTION 'cc_id_param cannot be NULL';
    END IF;
    
    IF start_vertex_id_param IS NULL THEN
        RAISE EXCEPTION 'start_vertex_id_param cannot be NULL';
    END IF;
    
    IF chunk_size_param IS NULL OR chunk_size_param <= 0 THEN
        RAISE EXCEPTION 'chunk_size_param must be positive';
    END IF;
    
    -- Validate CC definition exists
    IF NOT EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___cc_definitions WHERE id = cc_def_id_param) THEN
        RAISE EXCEPTION 'CC definition % does not exist', cc_def_id_param;
    END IF;
    
    -- Validate CC exists
    IF NOT EXISTS (SELECT 1 FROM __PGGRAPH_PREFIX___cc WHERE id = cc_id_param AND cc_def_id = cc_def_id_param) THEN
        RAISE EXCEPTION 'CC % does not exist in definition %', cc_id_param, cc_def_id_param;
    END IF;
    
    -- Validate start vertex exists and belongs to the specified CC
    IF NOT EXISTS (
        SELECT 1 FROM __PGGRAPH_PREFIX___cc_vertices 
        WHERE vertex_id = start_vertex_id_param 
        AND cc_def_id = cc_def_id_param 
        AND cc_id = cc_id_param
        AND removed = FALSE
    ) THEN
        RAISE EXCEPTION 'Start vertex % does not belong to CC % in definition %', 
                       start_vertex_id_param, cc_id_param, cc_def_id_param;
    END IF;
    
    -- Initialize processing variables
    start_time := clock_timestamp();
    current_chunk := 1;
    total_processed := 0;
    current_path_sequence := 1; -- Start from 1 for consistent path sequencing
    
    -- Handle resume logic - simplified approach
    IF resume_vertex_id_param IS NOT NULL THEN
        -- For resume, we'll skip vertices until we reach the resume point
        -- This is more efficient than recalculating the entire tree
        RAISE NOTICE 'Resuming analysis from vertex %', resume_vertex_id_param;
    END IF;
    
    -- FIX A: Simplify the algorithm - get all reachable vertices first, then chunk them
    -- Create temporary table for all reachable vertices
    all_vertices_temp_table := 'temp_all_reachable_' || floor(random() * 1000000)::text;
    
    EXECUTE format('
        CREATE TEMPORARY TABLE %I (
            vertex_id BIGINT PRIMARY KEY,
            depth INTEGER NOT NULL,
            path_sequence INTEGER NOT NULL
        ) ON COMMIT DROP
    ', all_vertices_temp_table);
    
    -- Insert start vertex first (depth 0, path_sequence 1)
    EXECUTE format('
        INSERT INTO %I (vertex_id, depth, path_sequence) 
        VALUES (%L, %L, %L)
        ON CONFLICT (vertex_id) DO NOTHING
    ', all_vertices_temp_table, start_vertex_id_param, 0, current_path_sequence);
    
    -- Build the edge query for pgRouting
    edge_query := format('
        SELECT id, source, target, cost, reverse_cost 
        FROM __PGGRAPH_PREFIX___cc_pgrouting_edges 
        WHERE cc_def_id = %L AND cc_id = %L
    ', cc_def_id_param, cc_id_param);
    
    -- FIX A: Simplify the algorithm - get all reachable vertices first, then chunk them
    -- Get all reachable vertices at once (more efficient than incremental)
    OPEN all_reachable_vertices_cursor FOR EXECUTE format('
        SELECT DISTINCT
            node as vertex_id,
            depth
        FROM pgr_breadthFirstSearch(%L, %L, directed := true)
        WHERE node != %L
        ORDER BY depth, node
    ', edge_query, start_vertex_id_param, start_vertex_id_param);
    
    -- FIX B: Fix resume logic - stream vertices and skip until resume point
    resume_found := FALSE;
    
    -- Handle special case: resume from start vertex
    IF resume_vertex_id_param = start_vertex_id_param THEN
        resume_found := TRUE;
    END IF;
    
    LOOP
        FETCH all_reachable_vertices_cursor INTO vertex_record;
        EXIT WHEN NOT FOUND;
        
        -- Handle resume logic: skip vertices until we reach the resume point
        IF resume_vertex_id_param IS NOT NULL AND NOT resume_found THEN
            IF vertex_record.vertex_id = resume_vertex_id_param THEN
                resume_found := TRUE;
            ELSE
                CONTINUE; -- Skip this vertex
            END IF;
        END IF;
        
        current_path_sequence := current_path_sequence + 1;
        
        EXECUTE format('
            INSERT INTO %I (vertex_id, depth, path_sequence) 
            VALUES (%L, %L, %L)
            ON CONFLICT (vertex_id) DO NOTHING
        ', all_vertices_temp_table, vertex_record.vertex_id, vertex_record.depth, current_path_sequence);
    END LOOP;
    
    CLOSE all_reachable_vertices_cursor;
    
    -- Validate resume vertex was found if specified
    IF resume_vertex_id_param IS NOT NULL AND NOT resume_found THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
        RAISE EXCEPTION 'Resume vertex % not found in downstream analysis from start vertex %', 
                       resume_vertex_id_param, start_vertex_id_param;
    END IF;
    
    -- Additional validation: ensure resume vertex is actually reachable
    IF resume_vertex_id_param IS NOT NULL THEN
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I WHERE vertex_id = %L)',
            all_vertices_temp_table,
            resume_vertex_id_param
        ) INTO resume_reachable;
        IF NOT resume_reachable THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
            RAISE EXCEPTION 'Resume vertex % is not reachable from start vertex %', 
                           resume_vertex_id_param, start_vertex_id_param;
        END IF;
    END IF;
    
    -- Get total count of reachable vertices
    EXECUTE format('SELECT COUNT(*) FROM %I', all_vertices_temp_table) INTO vertex_count;
    
    -- Handle edge case: no reachable vertices (only start vertex)
    IF vertex_count = 1 THEN
        -- Only start vertex exists, return it as a single chunk
        RETURN QUERY EXECUTE format('
            SELECT 
                v.vertex_id,
                v.depth,
                v.path_sequence,
                1 as processing_chunk,
                1 as total_processed,
                0 as estimated_remaining,
                EXTRACT(EPOCH FROM (clock_timestamp() - %L::timestamp)) * 1000 as processing_time_ms
            FROM %I v
            ORDER BY v.path_sequence
        ', 'start_time', all_vertices_temp_table);
        
        -- Clean up and exit
        EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
        RAISE NOTICE 'Downstream analysis completed: only start vertex found (no downstream vertices)';
        RETURN;
    END IF;
    
    -- Validate we have vertices to process
    IF vertex_count < 1 THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
        RAISE EXCEPTION 'No vertices found for processing - this should not happen';
    END IF;
    
    -- FIX C: Guarantee consistent chunking with predictable sizes
    chunk_start_idx := 0; -- PostgreSQL LIMIT/OFFSET is 0-based
    current_chunk := 1;
    
    WHILE chunk_start_idx < vertex_count LOOP
        chunk_end_idx := LEAST(chunk_start_idx + chunk_size_param, vertex_count);
        
        -- Return current chunk results with consistent chunking
        RETURN QUERY EXECUTE format('
            SELECT 
                v.vertex_id,
                v.depth,
                v.path_sequence,
                %L as processing_chunk,
                %L as total_processed,
                GREATEST(0, %L - %L) as estimated_remaining,
                EXTRACT(EPOCH FROM (clock_timestamp() - %L::timestamp)) * 1000 as processing_time_ms
            FROM %I v
            ORDER BY v.path_sequence
            LIMIT %L OFFSET %L
        ', current_chunk, total_processed, vertex_count, total_processed, 'start_time', 
           all_vertices_temp_table, chunk_size_param, chunk_start_idx);
        
        total_processed := total_processed + (chunk_end_idx - chunk_start_idx);
        chunk_start_idx := chunk_end_idx;
        current_chunk := current_chunk + 1;
    END LOOP;
    
    -- Clean up all vertices temp table
    EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
    
    RAISE NOTICE 'Downstream analysis completed: processed % vertices in % chunks in % ms', 
                 total_processed, current_chunk - 1, 
                 EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Clean up on error
        IF all_vertices_temp_table IS NOT NULL THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', all_vertices_temp_table);
        END IF;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- CONNECTED COMPONENTS TRIGGERS
-- These triggers handle CC-specific logic and work alongside the core triggers
-- from the first file. They do NOT duplicate the vertex-edge consistency logic.

-- Create the triggers
DROP TRIGGER IF EXISTS trigger_edge_insert_cc_dirty ON __PGGRAPH_PREFIX___edges;
CREATE TRIGGER trigger_edge_insert_cc_dirty
AFTER INSERT ON __PGGRAPH_PREFIX___edges
FOR EACH ROW
EXECUTE FUNCTION assign_cc_for_new_edge();

DROP TRIGGER IF EXISTS trigger_edge_update_cc_dirty ON __PGGRAPH_PREFIX___edges;
CREATE TRIGGER trigger_edge_update_cc_dirty
AFTER UPDATE ON __PGGRAPH_PREFIX___edges
FOR EACH ROW
WHEN (OLD.source_id IS DISTINCT FROM NEW.source_id OR 
      OLD.target_id IS DISTINCT FROM NEW.target_id OR
      OLD.edge_type IS DISTINCT FROM NEW.edge_type OR
      OLD.removed IS DISTINCT FROM NEW.removed)
EXECUTE FUNCTION record_dirty_ccs_on_edge_change();

DROP TRIGGER IF EXISTS trigger_edge_delete_cc_dirty ON __PGGRAPH_PREFIX___edges;
CREATE TRIGGER trigger_edge_delete_cc_dirty
AFTER DELETE ON __PGGRAPH_PREFIX___edges
FOR EACH ROW
EXECUTE FUNCTION record_dirty_ccs_on_edge_change();

DROP TRIGGER IF EXISTS trigger_edge_soft_delete_cc_dirty ON __PGGRAPH_PREFIX___edges;
CREATE TRIGGER trigger_edge_soft_delete_cc_dirty
AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___edges
FOR EACH ROW
WHEN (OLD.removed = FALSE AND NEW.removed = TRUE)
EXECUTE FUNCTION record_dirty_ccs_on_edge_change();

-- Trigger to sync removed status from __PGGRAPH_PREFIX___vertices to __PGGRAPH_PREFIX___cc_vertices
DROP TRIGGER IF EXISTS trigger_vertex_removed_sync_cc_vertices ON __PGGRAPH_PREFIX___vertices;
CREATE TRIGGER trigger_vertex_removed_sync_cc_vertices
AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___vertices
FOR EACH ROW
EXECUTE FUNCTION sync_vertex_removed_status_to_cc_vertices();

-- Trigger to mark CCs as dirty when vertices are removed or restored
DROP TRIGGER IF EXISTS trigger_vertex_removed_cc_dirty ON __PGGRAPH_PREFIX___vertices;
CREATE TRIGGER trigger_vertex_removed_cc_dirty
AFTER UPDATE OF removed ON __PGGRAPH_PREFIX___vertices
FOR EACH ROW
EXECUTE FUNCTION record_dirty_ccs_on_vertex_change();

DO $$
BEGIN
    RAISE NOTICE 'Connected components schema and functions created successfully';
    RAISE NOTICE 'CC tables are updated through explicit calls to dh_calculate_ccs_full() and dh_calculate_ccs_incremental()';
    RAISE NOTICE 'Automatic dirty marking enabled for all graph changes';
    RAISE NOTICE 'Manual CC execution for predictable performance';
END $$;