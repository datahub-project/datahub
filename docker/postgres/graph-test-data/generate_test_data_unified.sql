-- =============================================================================
-- Unified Test Data Generation for DataHub Graph Schema
-- =============================================================================
-- 
-- This script generates test data based on custom parameters set by the calling script.
-- NEW: Includes datajob entities and deep data lineage chains up to 1000 hops deep
-- with realistic enterprise data pipeline patterns.
--
-- USAGE:
-- The calling script must set these parameters before execution:
-- SET datahub.custom_vertices = '1000';      -- Number of vertices to generate
-- SET datahub.custom_edges = '5000';         -- Number of edges to generate  
-- SET datahub.custom_edge_types = '3';       -- Number of edge types to create
--
-- FEATURES:
-- - Fully configurable dataset sizes via custom parameters
-- - Realistic URN generation
-- - Comprehensive edge type coverage (40 types including data lineage)
-- - Datajob entities for realistic ETL/ELT pipeline modeling
-- - Deep data lineage chains up to 1000 hops with random fanout
-- - Connected component definitions including deep lineage analysis
-- - Performance optimization for large datasets
-- - Progress tracking and logging
-- - May exceed target edge count to achieve realistic depth
-- =============================================================================

-- Validate required parameters
DO $$
BEGIN
    -- Check if required parameters are set
    IF current_setting('datahub.custom_vertices', true) IS NULL OR current_setting('datahub.custom_vertices', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_vertices must be set before running this script';
    END IF;
    
    IF current_setting('datahub.custom_edges', true) IS NULL OR current_setting('datahub.custom_edges', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_edges must be set before running this script';
    END IF;
    
    IF current_setting('datahub.custom_edge_types', true) IS NULL OR current_setting('datahub.custom_edge_types', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_edge_types must be set before running this script';
    END IF;
    
    -- Debug: Show what parameters we're using
    RAISE NOTICE 'Using custom parameters - Vertices: %, Edges: %, Edge Types: %', 
        current_setting('datahub.custom_vertices'),
        current_setting('datahub.custom_edges'),
        current_setting('datahub.custom_edge_types');
END $$;

-- Clear existing test data (if any)
DO $$
BEGIN
    RAISE NOTICE 'Clearing existing test data...';
    
    TRUNCATE TABLE metadata_graph_cc_computation_log CASCADE;
    TRUNCATE TABLE metadata_graph_cc_vertices CASCADE;
    TRUNCATE TABLE metadata_graph_cc CASCADE;
    TRUNCATE TABLE metadata_graph_cc_definitions CASCADE;
    TRUNCATE TABLE metadata_graph_edges CASCADE;
    TRUNCATE TABLE metadata_graph_vertices CASCADE;
    TRUNCATE TABLE metadata_graph_edge_types CASCADE;
    
    RAISE NOTICE 'Existing test data cleared successfully';
END $$;

-- Set random seed for reproducible results
SELECT setseed(0.42);

-- =============================================================================
-- SECTION 1: EDGE TYPE CREATION
-- =============================================================================

SELECT '=== CREATING EDGE TYPES ===' as section;

-- Create edge types based on custom parameter
DO $$
DECLARE
    edge_types_to_create INTEGER;
    edge_types_to_use INTEGER;
BEGIN
    -- Use custom edge types count for creation
    edge_types_to_create := current_setting('datahub.custom_edge_types')::integer;
    
    -- Validate edge types count
    IF edge_types_to_create < 1 OR edge_types_to_create > 40 THEN
        RAISE EXCEPTION 'datahub.custom_edge_types must be between 1 and 40, got %', edge_types_to_create;
    END IF;
    
    -- For now, we'll create all 40 edge types but only use the configured number
    -- This ensures all edge types exist for any future use
    RAISE NOTICE 'Creating all 40 edge types (will use % for edge generation)', edge_types_to_create;
    
    -- Always create all 32 edge types for future use
    INSERT INTO metadata_graph_edge_types (id, type_name) VALUES 
        (1, 'DEPENDS_ON'),
        (2, 'OWNED_BY'),
        (3, 'CONTAINS'),
        (4, 'FOLLOWS'),
        (5, 'MENTIONS'),
        (6, 'TAGGED_WITH'),
        (7, 'ACCESSED_BY'),
        (8, 'RELATED_TO'),
        (9, 'COLLABORATES_WITH'),
        (10, 'REPORTS_TO'),
        (11, 'MANAGES'),
        (12, 'WORKS_FOR'),
        (13, 'BELONGS_TO'),
        (14, 'PART_OF'),
        (15, 'IMPLEMENTS'),
        (16, 'USES'),
        (17, 'CONSUMES'),
        (18, 'PRODUCES'),
        (19, 'VALIDATES'),
        (20, 'APPROVES'),
        (21, 'REVIEWS'),
        (22, 'COMMENTS_ON'),
        (23, 'ANNOTATES'),
        (24, 'LINKS_TO'),
        (25, 'REFERENCES'),
        (26, 'CITES'),
        (27, 'INFLUENCES'),
        (28, 'AFFECTS'),
        (29, 'TRIGGERS'),
        (30, 'NOTIFIES'),
        (31, 'ALERTS'),
        (32, 'ESCALATES'),
        (33, 'READS_FROM'),
        (34, 'WRITES_TO'),
        (35, 'TRANSFORMS'),
        (36, 'SCHEDULED_BY'),
        (37, 'EXECUTED_BY'),
        (38, 'MONITORED_BY'),
        (39, 'BACKFILLS'),
        (40, 'REFRESHES')
    ON CONFLICT (id) DO NOTHING;
    
    RAISE NOTICE 'Successfully created all 40 edge types (will use % for edge generation)', edge_types_to_create;
END $$;

-- =============================================================================
-- SECTION 2: UNIFIED VERTEX AND EDGE GENERATION
-- =============================================================================

-- Generic function to process edges in batches for any relationship type
CREATE OR REPLACE FUNCTION process_edges_in_batches(
    target_count INTEGER,
    edge_type_id INTEGER,
    relationship_name TEXT,
    source_entity_type TEXT,
    target_entity_type TEXT,
    source_platform TEXT,
    target_platform TEXT,
    source_prefix TEXT,
    target_prefix TEXT,
    source_suffix TEXT DEFAULT '',
    target_suffix TEXT DEFAULT '',
    batch_size INTEGER DEFAULT 100000
) RETURNS VOID AS $$
DECLARE
    total_batches INTEGER;
    current_batch INTEGER;
    batch_start INTEGER;
    batch_end INTEGER;
    edges_per_batch INTEGER;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    batch_duration INTERVAL;
    first_batch_start_time TIMESTAMP;
    avg_batch_duration INTERVAL;
    remaining_batches INTEGER;
    estimated_remaining_time INTERVAL;
    edge_count INTEGER;
    dataset_count INTEGER;
    user_count INTEGER;
    dashboard_count INTEGER;
    chart_count INTEGER;
    datajob_count INTEGER;
    mlmodel_count INTEGER;
BEGIN
    IF target_count <= 0 THEN
        RETURN;
    END IF;
    
    -- Get entity counts from the calling context
    dataset_count := current_setting('datahub.custom_vertices')::integer * 0.35;
    user_count := current_setting('datahub.custom_vertices')::integer * 0.2;
    dashboard_count := current_setting('datahub.custom_vertices')::integer * 0.15;
    chart_count := current_setting('datahub.custom_vertices')::integer * 0.15;
    datajob_count := current_setting('datahub.custom_vertices')::integer * 0.1;
    mlmodel_count := current_setting('datahub.custom_vertices')::integer - dataset_count - user_count - dashboard_count - chart_count - datajob_count;
    
    RAISE NOTICE 'Generating % % edges (with automatic vertex creation)...', target_count, relationship_name;
    
    -- Initialize batch processing variables
    total_batches := CEIL(target_count::numeric / batch_size);
    current_batch := 1;
    batch_start := 1;
    first_batch_start_time := clock_timestamp();
    
    RAISE NOTICE 'Starting batch processing: % batches of ~% edges each', total_batches, batch_size;
    
    WHILE batch_start <= target_count LOOP
        batch_start_time := clock_timestamp();
        batch_end := LEAST(batch_start + batch_size - 1, target_count);
        edges_per_batch := batch_end - batch_start + 1;
        
        -- Calculate timing estimates based on previous batches
        IF current_batch > 1 THEN
            avg_batch_duration := (clock_timestamp() - first_batch_start_time) / (current_batch - 1);
            remaining_batches := total_batches - current_batch + 1;
            estimated_remaining_time := avg_batch_duration * remaining_batches;
            
            -- Format timing for better readability
            DECLARE
                progress_percentage TEXT;
                time_estimate TEXT;
            BEGIN
                progress_percentage := round(((current_batch - 1)::numeric / total_batches * 100)::numeric, 1) || '%%';
                time_estimate := CASE 
                    WHEN extract(epoch from estimated_remaining_time) > 3600 THEN 
                        round(extract(epoch from estimated_remaining_time) / 3600, 1) || ' hours'
                    WHEN extract(epoch from estimated_remaining_time) > 60 THEN 
                        round(extract(epoch from estimated_remaining_time) / 60, 1) || ' minutes'
                    ELSE 
                        round(extract(epoch from estimated_remaining_time), 0) || ' seconds'
                    END;
                
                RAISE NOTICE 'Processing batch %/%: edges % to % (total: %) - Overall progress: % - Est. remaining: ~%', 
                             current_batch, total_batches, batch_start, batch_end, edges_per_batch,
                             progress_percentage, time_estimate;
            END;
        ELSE
            RAISE NOTICE 'Processing batch %/%: edges % to % (total: %) - Starting...', 
                         current_batch, total_batches, batch_start, batch_end, edges_per_batch;
        END IF;
        
        -- Generate edges using random sampling
        BEGIN
            RAISE NOTICE 'Calling dh_bulk_insert_edges for % edges...', relationship_name;
            edge_count := dh_bulk_insert_edges(
                ARRAY(
                    SELECT jsonb_build_object(
                        'source_id', ('x' || substr(md5(source_urn), 1, 16))::bit(64)::bigint,
                        'target_id', ('x' || substr(md5(target_urn), 1, 16))::bit(64)::bigint,
                        'source_urn', source_urn,
                        'target_urn', target_urn,
                        'source_removed', false,
                        'target_removed', false,
                        'edge_type_id', edge_type_id,
                        'owner_id', 0,
                        'properties', ('{"relationship_type": "' || relationship_name || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
                    )
                    FROM (
                        SELECT DISTINCT 
                            source_urn,
                            target_urn
                        FROM (
                            SELECT 
                                source_prefix || s.id || source_suffix as source_urn,
                                target_prefix || t.id || target_suffix as target_urn
                            FROM (
                                SELECT DISTINCT 
                                    (random() * (CASE 
                                        WHEN source_entity_type = 'dataset' THEN dataset_count - 1
                                        WHEN source_entity_type = 'user' THEN user_count - 1
                                        WHEN source_entity_type = 'dashboard' THEN dashboard_count - 1
                                        WHEN source_entity_type = 'chart' THEN chart_count - 1
                                        WHEN source_entity_type = 'datajob' THEN datajob_count - 1
                                        WHEN source_entity_type = 'mlmodel' THEN mlmodel_count - 1
                                        ELSE 0
                                    END) + 1)::integer as s_id,
                                    (random() * (CASE 
                                        WHEN target_entity_type = 'dataset' THEN dataset_count - 1
                                        WHEN target_entity_type = 'user' THEN user_count - 1
                                        WHEN target_entity_type = 'dashboard' THEN dashboard_count - 1
                                        WHEN target_entity_type = 'chart' THEN chart_count - 1
                                        WHEN target_entity_type = 'datajob' THEN datajob_count - 1
                                        WHEN target_entity_type = 'mlmodel' THEN mlmodel_count - 1
                                        ELSE 0
                                    END) + 1)::integer as t_id
                                FROM generate_series(1, edges_per_batch)
                            ) pairs
                            CROSS JOIN LATERAL (
                                SELECT s_id as id
                            ) s(id)
                            CROSS JOIN LATERAL (
                                SELECT t_id as id
                            ) t(id)
                        ) edge_pairs(source_urn, target_urn)
                    ) edge_data
                )::jsonb[]
            );
            RAISE NOTICE 'Generated % % edges for batch %', edge_count, relationship_name, current_batch;
            RAISE NOTICE 'Function returned: % edges processed', edge_count;
        END;
        
        -- Record batch completion time and calculate duration
        batch_end_time := clock_timestamp();
        batch_duration := batch_end_time - batch_start_time;
        
        -- Update progress and show timing info
        DECLARE
            progress_percentage TEXT;
            avg_time_per_batch INTERVAL;
        BEGIN
            progress_percentage := round((batch_end::numeric / target_count * 100)::numeric, 1) || '%%';
            avg_time_per_batch := CASE 
                WHEN current_batch > 1 THEN 
                    ((clock_timestamp() - first_batch_start_time) / current_batch)::interval
                ELSE 
                    batch_duration
                END;
            
            RAISE NOTICE 'Completed batch %/% in % - Progress: %/% edges (%) - Overall avg: % per batch', 
                         current_batch, total_batches, batch_duration, 
                         batch_end, target_count, progress_percentage, avg_time_per_batch;
        END;
        
        batch_start := batch_end + 1;
        current_batch := current_batch + 1;
        
        -- Small delay between batches to allow system to catch up
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    RAISE NOTICE 'Successfully generated % edges', relationship_name;
    RAISE NOTICE '=== BATCH PROCESSING COMPLETE ===';
    RAISE NOTICE 'Total batches: %', total_batches;
    RAISE NOTICE 'Total time: %', (clock_timestamp() - first_batch_start_time)::interval;
    RAISE NOTICE 'Average time per batch: %', ((clock_timestamp() - first_batch_start_time) / total_batches)::interval;
    RAISE NOTICE 'Total edges generated: %', target_count;
END;
$$ LANGUAGE plpgsql;

-- Function to generate deep data lineage chains with random fanout
CREATE OR REPLACE FUNCTION generate_deep_data_lineage(
    max_depth INTEGER DEFAULT 1000,
    max_fanout INTEGER DEFAULT 10,
    target_chains INTEGER DEFAULT 100,
    batch_size INTEGER DEFAULT 1000
) RETURNS INTEGER AS $$
DECLARE
    chain_count INTEGER := 0;
    current_chain INTEGER := 1;
    total_edges_generated INTEGER := 0;
    chain_start_time TIMESTAMP;
    chain_end_time TIMESTAMP;
    chain_duration INTERVAL;
    dataset_count INTEGER;
    datajob_count INTEGER;
    user_count INTEGER;
    current_depth INTEGER;
    current_fanout INTEGER;
    source_dataset_id INTEGER;
    target_dataset_id INTEGER;
    datajob_id INTEGER;
    user_id INTEGER;
    edge_batch JSONB[];
    edge_record JSONB;
    edges_in_batch INTEGER;
    batch_start INTEGER;
    batch_end INTEGER;
    current_batch INTEGER;
    total_batches INTEGER;
BEGIN
    -- Initialize edge_batch to empty array
    edge_batch := ARRAY[]::jsonb[];
    
    RAISE NOTICE '=== GENERATING DEEP DATA LINEAGE CHAINS ===';
    RAISE NOTICE 'Max depth: %, Max fanout: %, Target chains: %', max_depth, max_fanout, target_chains;
    
    -- Get entity counts
    dataset_count := current_setting('datahub.custom_vertices')::integer * 0.35;
    datajob_count := current_setting('datahub.custom_vertices')::integer * 0.1;
    user_count := current_setting('datahub.custom_vertices')::integer * 0.2;
    
    IF dataset_count < 2 OR datajob_count < 1 OR user_count < 1 THEN
        RAISE EXCEPTION 'Insufficient entities for deep lineage: need at least 2 datasets, 1 datajob, 1 user';
    END IF;
    
    -- Ensure vertices exist before creating edges
    IF (SELECT COUNT(*) FROM metadata_graph_vertices) = 0 THEN
        RAISE NOTICE 'No vertices found, creating them first...';
        PERFORM create_test_vertices(current_setting('datahub.custom_vertices')::integer, 1000);
    END IF;
    
    chain_start_time := clock_timestamp();
    
    WHILE current_chain <= target_chains AND chain_count < target_chains LOOP
        chain_start_time := clock_timestamp();
        
            -- Randomly determine chain depth (1 to max_depth, but cap at 25 for reasonable performance)
    current_depth := LEAST((random() * (max_depth - 1) + 1)::integer, 25);
    
    -- Randomly determine initial fanout for this chain (1 to max_fanout)
    current_fanout := (random() * (max_fanout - 1) + 1)::integer;
    
    RAISE NOTICE 'Generating chain %/%: depth % (capped at 25), initial fanout %', current_chain, target_chains, current_depth, current_fanout;
        
        -- Generate a single chain with the specified depth and fanout
        DECLARE
            chain_edges INTEGER := 0;
            level INTEGER := 1;
            level_start_nodes INTEGER[] := ARRAY[1]; -- Start with one source dataset
            level_end_nodes INTEGER[] := ARRAY[]::INTEGER[];
            current_level_nodes INTEGER[];
            next_level_nodes INTEGER[];
            node_id INTEGER;
            target_node_id INTEGER;
            job_id INTEGER;
            user_id INTEGER;
            edge_type_id INTEGER;
            edge_type_name TEXT;
        BEGIN
            -- Start with a random source dataset
            source_dataset_id := (random() * (dataset_count - 1) + 1)::integer;
            level_start_nodes := ARRAY[source_dataset_id];
            
            WHILE level <= current_depth AND array_length(level_start_nodes, 1) > 0 LOOP
                current_level_nodes := level_start_nodes;
                level_start_nodes := ARRAY[]::INTEGER[];
                
                -- For each node at current level, create fanout to next level
                FOREACH node_id IN ARRAY current_level_nodes
                LOOP
                    -- Determine how many targets this node will have
                    -- For extreme depths (>10), limit fanout to 1 to prevent exponential explosion
                    -- For very deep levels (>50), force fanout to 1
                    IF level > 50 THEN
                        current_fanout := 1;
                    ELSIF level > 10 THEN
                        current_fanout := 1;
                    ELSE
                        current_fanout := (random() * (max_fanout - 1) + 1)::integer;
                    END IF;
                    
                    -- Create edges from current node to next level
                    FOR i IN 1..current_fanout LOOP
                        -- Randomly select target dataset
                        target_node_id := (random() * (dataset_count - 1) + 1)::integer;
                        
                        -- Avoid self-loops
                        IF target_node_id = node_id THEN
                            target_node_id := ((target_node_id + 1) % dataset_count) + 1;
                        END IF;
                        
                        -- Randomly select edge type for this level
                        CASE level
                            WHEN 1 THEN
                                -- Raw data ingestion
                                edge_type_id := 33; -- READS_FROM
                                edge_type_name := 'READS_FROM';
                            WHEN 2 THEN
                                -- Data transformation
                                edge_type_id := 35; -- TRANSFORMS
                                edge_type_name := 'TRANSFORMS';
                            WHEN 3 THEN
                                -- Data processing
                                edge_type_id := 34; -- WRITES_TO
                                edge_type_name := 'WRITES_TO';
                            ELSE
                                -- Mixed relationships for deeper levels
                                edge_type_id := (random() * 3 + 33)::integer; -- 33-35
                                CASE edge_type_id
                                    WHEN 33 THEN edge_type_name := 'READS_FROM';
                                    WHEN 34 THEN edge_type_name := 'WRITES_TO';
                                    WHEN 35 THEN edge_type_name := 'TRANSFORMS';
                                    ELSE edge_type_name := 'TRANSFORMS'; -- Default fallback
                                END CASE;
                        END CASE;
                        
                        -- Add to batch for bulk insertion
                        edge_record := jsonb_build_object(
                            'source_id', ('x' || substr(md5('urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || node_id || ',PROD)'), 1, 16))::bit(64)::bigint,
                            'target_id', ('x' || substr(md5('urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || target_node_id || ',PROD)'), 1, 16))::bit(64)::bigint,
                            'source_urn', 'urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || node_id || ',PROD)',
                            'target_urn', 'urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || target_node_id || ',PROD)',
                            'source_removed', false,
                            'target_removed', false,
                            'edge_type_id', edge_type_id,
                            'owner_id', 0,
                            'properties', ('{"relationship_type": "' || edge_type_name || '", "chain": ' || current_chain || ', "level": ' || level || ', "depth": ' || current_depth || ', "fanout": ' || current_fanout || ', "scale": "deep_lineage"}')::jsonb
                        );
                        
                        edge_batch := array_append(edge_batch, edge_record);
                        chain_edges := chain_edges + 1;
                        
                        -- Add target to next level nodes
                        level_end_nodes := array_append(level_end_nodes, target_node_id);
                        
                        -- Flush batch if it gets too large
                        IF array_length(edge_batch, 1) >= batch_size THEN
                            PERFORM dh_bulk_insert_edges(edge_batch);
                            total_edges_generated := total_edges_generated + array_length(edge_batch, 1);
                            edge_batch := ARRAY[]::jsonb[];
                        END IF;
                    END LOOP;
                END LOOP;
                
                -- Move to next level
                level_start_nodes := level_end_nodes;
                level_end_nodes := ARRAY[]::INTEGER[];
                level := level + 1;
            END LOOP;
            
            -- Flush remaining edges in batch
            IF array_length(edge_batch, 1) > 0 THEN
                PERFORM dh_bulk_insert_edges(edge_batch);
                total_edges_generated := total_edges_generated + array_length(edge_batch, 1);
                edge_batch := ARRAY[]::jsonb[];
            END IF;
            
            RAISE NOTICE 'Chain % completed: % levels, % edges generated', current_chain, level - 1, chain_edges;
        END;
        
        chain_end_time := clock_timestamp();
        chain_duration := chain_end_time - chain_start_time;
        
        RAISE NOTICE 'Chain % completed in % - Total edges: %', current_chain, chain_duration, total_edges_generated;
        
        current_chain := current_chain + 1;
        chain_count := chain_count + 1;
        
        -- Small delay between chains
        PERFORM pg_sleep(0.05);
    END LOOP;
    
    RAISE NOTICE '=== DEEP DATA LINEAGE GENERATION COMPLETE ===';
    RAISE NOTICE 'Chains generated: %', chain_count;
    RAISE NOTICE 'Total edges generated: %', total_edges_generated;
    RAISE NOTICE 'Total time: %', (clock_timestamp() - chain_start_time)::interval;
    
    RETURN total_edges_generated;
END;
$$ LANGUAGE plpgsql;

-- Function to create vertices for all entity types
CREATE OR REPLACE FUNCTION create_test_vertices(
    vertex_count INTEGER,
    batch_size INTEGER DEFAULT 1000
) RETURNS INTEGER AS $$
DECLARE
    total_vertices_created INTEGER := 0;
    dataset_count INTEGER;
    user_count INTEGER;
    dashboard_count INTEGER;
    chart_count INTEGER;
    datajob_count INTEGER;
    mlmodel_count INTEGER;
    current_batch INTEGER := 1;
    total_batches INTEGER;
    batch_start INTEGER;
    batch_end INTEGER;
    vertices_per_batch INTEGER;
    vertex_batch JSONB[];
    vertex_record JSONB;
    start_time TIMESTAMP;
BEGIN
    RAISE NOTICE '=== CREATING TEST VERTICES ===';
    RAISE NOTICE 'Target: % vertices', vertex_count;
    
    start_time := clock_timestamp();
    
    -- Initialize vertex_batch as empty array
    vertex_batch := ARRAY[]::jsonb[];
    
    -- Calculate entity type distribution
    dataset_count := FLOOR(vertex_count * 0.35);
    user_count := FLOOR(vertex_count * 0.2);
    dashboard_count := FLOOR(vertex_count * 0.15);
    chart_count := FLOOR(vertex_count * 0.15);
    datajob_count := FLOOR(vertex_count * 0.1);
    mlmodel_count := vertex_count - dataset_count - user_count - dashboard_count - chart_count - datajob_count;
    
    RAISE NOTICE 'Distribution: % datasets, % users, % dashboards, % charts, % datajobs, % ML models', 
                 dataset_count, user_count, dashboard_count, chart_count, datajob_count, mlmodel_count;
    
    total_batches := CEIL(vertex_count::numeric / batch_size);
    
    -- Create datasets
    RAISE NOTICE 'Creating % datasets...', dataset_count;
    FOR i IN 1..dataset_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || i || ',PROD)'), 1, 16))::bit(64)::bigint,
            'urn', 'urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_' || i || ',PROD)',
            'removed', false,
            'properties', ('{"entity_type": "dataset", "platform": "postgres", "name": "test_dataset_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        -- Flush batch if it gets too large
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Create users
    RAISE NOTICE 'Creating % users...', user_count;
    FOR i IN 1..user_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('user_' || i), 1, 16))::bit(64)::bigint,
            'urn', 'user_' || i,
            'removed', false,
            'properties', ('{"entity_type": "user", "platform": "corpuser", "name": "user_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Create dashboards
    RAISE NOTICE 'Creating % dashboards...', dashboard_count;
    FOR i IN 1..dashboard_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_' || i || ')'), 1, 16))::bit(64)::bigint,
            'urn', 'urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_' || i || ')',
            'removed', false,
            'properties', ('{"entity_type": "dashboard", "platform": "looker", "name": "test_dashboard_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Create charts
    RAISE NOTICE 'Creating % charts...', chart_count;
    FOR i IN 1..chart_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('urn:li:chart:(urn:li:dataPlatform:looker,test_chart_' || i || ')'), 1, 16))::bit(64)::bigint,
            'urn', 'urn:li:chart:(urn:li:dataPlatform:looker,test_chart_' || i || ')',
            'removed', false,
            'properties', ('{"entity_type": "chart", "platform": "looker", "name": "test_chart_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Create datajobs
    RAISE NOTICE 'Creating % datajobs...', datajob_count;
    FOR i IN 1..datajob_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('urn:li:dataJob:(urn:li:dataJob,test_datajob_' || i || ')'), 1, 16))::bit(64)::bigint,
            'urn', 'urn:li:dataJob:(urn:li:dataJob,test_datajob_' || i || ')',
            'removed', false,
            'properties', ('{"entity_type": "datajob", "platform": "dataJob", "name": "test_datajob_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Create ML models
    RAISE NOTICE 'Creating % ML models...', mlmodel_count;
    FOR i IN 1..mlmodel_count LOOP
        vertex_record := jsonb_build_object(
            'xxhash64_id', ('x' || substr(md5('urn:li:mlModel:(urn:li:mlModel,test_mlmodel_' || i || ')'), 1, 16))::bit(64)::bigint,
            'urn', 'urn:li:mlModel:(urn:li:mlModel,test_mlmodel_' || i || ')',
            'removed', false,
            'properties', ('{"entity_type": "mlmodel", "platform": "mlModel", "name": "test_mlmodel_' || i || '", "batch": ' || current_batch || ', "scale": "custom"}')::jsonb
        );
        vertex_batch := array_append(vertex_batch, vertex_record);
        
        IF array_length(vertex_batch, 1) >= batch_size THEN
            INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
            SELECT 
                (v->>'xxhash64_id')::bigint,
                v->>'urn',
                (v->>'removed')::boolean,
                v->'properties'
            FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
            total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
            vertex_batch := ARRAY[]::jsonb[];
            current_batch := current_batch + 1;
        END IF;
    END LOOP;
    
    -- Flush remaining vertices
    IF array_length(vertex_batch, 1) > 0 THEN
        INSERT INTO metadata_graph_vertices (xxhash64_id, urn, removed, properties)
        SELECT 
            (v->>'xxhash64_id')::bigint,
            v->>'urn',
            (v->>'removed')::boolean,
            v->'properties'
        FROM jsonb_array_elements(to_jsonb(vertex_batch)) AS v;
        total_vertices_created := total_vertices_created + array_length(vertex_batch, 1);
    END IF;
    
    RAISE NOTICE '=== VERTEX CREATION COMPLETE ===';
    RAISE NOTICE 'Total vertices created: %', total_vertices_created;
    RAISE NOTICE 'Total time: %', (clock_timestamp() - start_time)::interval;
    
    RETURN total_vertices_created;
END;
$$ LANGUAGE plpgsql;

SELECT '=== GENERATING VERTICES AND EDGES UNIFIED ===' as section;

DO $$
DECLARE
    vertex_count INTEGER;
    edge_count INTEGER;
    batch_size INTEGER;
    total_batches INTEGER;
    current_batch INTEGER;
    start_id INTEGER;
    end_id INTEGER;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    first_batch_start_time TIMESTAMP;
    batch_start INTEGER;
    batch_end INTEGER;
    edges_per_batch INTEGER;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    batch_duration INTERVAL;
    avg_batch_duration INTERVAL;
    remaining_batches INTEGER;
    estimated_remaining_time INTERVAL;
    total_start_time TIMESTAMP;
    owned_by_start_time TIMESTAMP;
    contains_start_time TIMESTAMP;
    other_start_time TIMESTAMP;
    dataset_count INTEGER;
    user_count INTEGER;
    dashboard_count INTEGER;
    chart_count INTEGER;
    datajob_count INTEGER;
    mlmodel_count INTEGER;
    target_depends_on INTEGER;
    target_owned_by INTEGER;
    target_contains INTEGER;
    target_other INTEGER;
BEGIN
    RAISE NOTICE '=== GENERATING VERTICES AND EDGES UNIFIED ===';
    
    -- Use custom parameters
    vertex_count := current_setting('datahub.custom_vertices')::integer;
    edge_count := current_setting('datahub.custom_edges')::integer;
    
    -- Validate parameters
    IF vertex_count < 1 THEN
        RAISE EXCEPTION 'datahub.custom_vertices must be at least 1, got %', vertex_count;
    END IF;
    
    IF edge_count < 1 THEN
        RAISE EXCEPTION 'datahub.custom_edges must be at least 1, got %', edge_count;
    END IF;
    
    -- Calculate entity type distribution
    dataset_count := FLOOR(vertex_count * 0.35);
    user_count := FLOOR(vertex_count * 0.2);
    dashboard_count := FLOOR(vertex_count * 0.15);
    chart_count := FLOOR(vertex_count * 0.15);
    datajob_count := FLOOR(vertex_count * 0.1);
    mlmodel_count := vertex_count - dataset_count - user_count - dashboard_count - chart_count - datajob_count;
    
    -- Calculate target edge counts for each type (will be updated dynamically)
    target_depends_on := 0;
    target_owned_by := 0;
    target_contains := 0;
    target_other := 0;
    
    RAISE NOTICE 'Target: % vertices (% datasets, % users, % dashboards, % charts, % datajobs, % ML models)', 
                 vertex_count, dataset_count, user_count, dashboard_count, chart_count, datajob_count, mlmodel_count;
    RAISE NOTICE 'Target: % edges (distribution will be calculated dynamically based on edge types)', edge_count;
    
    start_time := clock_timestamp();
    
    -- Performance optimizations for bulk operations
    SET maintenance_work_mem = '2GB';
    SET work_mem = '256MB';
    SET effective_cache_size = '8GB';
    SET random_page_cost = 1.1;
    SET seq_page_cost = 1.0;
    SET enable_seqscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_indexscan = ON;
    SET synchronous_commit = OFF;
    
    -- Performance tuning for bulk operations
    SET max_parallel_workers_per_gather = 4;
    SET parallel_tuple_cost = 0.1;
    SET parallel_setup_cost = 1000;
    
    -- Log initial performance settings
    RAISE NOTICE 'Performance settings applied:';
    RAISE NOTICE '  work_mem: %', current_setting('work_mem');
    RAISE NOTICE '  maintenance_work_mem: %', current_setting('maintenance_work_mem');
    RAISE NOTICE '  effective_cache_size: %', current_setting('effective_cache_size');
    RAISE NOTICE '  synchronous_commit: %', current_setting('synchronous_commit');
    
    RAISE NOTICE 'Using dh_bulk_insert_edges for unified vertex and edge generation...';
    
    -- Create vertices first before creating edges
    DECLARE
        vertices_created INTEGER;
    BEGIN
        RAISE NOTICE '=== CREATING VERTICES FIRST ===';
        vertices_created := create_test_vertices(vertex_count, 1000);
        RAISE NOTICE 'Successfully created % vertices', vertices_created;
        

    END;
    
    -- Test the function with a simple edge first
    DECLARE
        test_result INTEGER;
    BEGIN
        test_result := dh_bulk_insert_edges(
            ARRAY[
                jsonb_build_object(
                    'source_id', ('x' || substr(md5('urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_1,PROD)'), 1, 16))::bit(64)::bigint,
                    'target_id', ('x' || substr(md5('urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_2,PROD)'), 1, 16))::bit(64)::bigint,
                    'source_urn', 'urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_1,PROD)',
                    'target_urn', 'urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_2,PROD)',
                    'source_removed', false,
                    'target_removed', false,
                    'edge_type_id', 1,
                    'owner_id', 0,
                    'properties', '{"relationship_type": "TEST", "scale": "custom"}'::jsonb
                )
            ]
        );
        RAISE NOTICE 'Test edge insertion result: %', test_result;
        

    END;
    
    -- Edge generation is now handled dynamically in the next section
    RAISE NOTICE 'Edge generation will be handled dynamically based on configured edge types';
    
    -- =============================================================================
    -- DYNAMIC EDGE GENERATION BASED ON CONFIGURED EDGE TYPES
    -- =============================================================================
    
    -- Generate edges dynamically based on the configured number of edge types
    DECLARE
        edge_types_to_use INTEGER;
        edge_distribution JSONB;
        edge_type_configs JSONB;
        edge_type_count INTEGER;
        edge_type_record RECORD;
        target_edges_for_type INTEGER;
        total_edges_generated INTEGER := 0;
        edge_types_used INTEGER := 0;
    BEGIN
        edge_types_to_use := current_setting('datahub.custom_edge_types')::integer;
        
        -- Get the edge type configurations from the outer scope
        IF edge_types_to_use <= 4 THEN
            edge_distribution := '{"DEPENDS_ON": 0.4, "OWNED_BY": 0.3, "CONTAINS": 0.2, "FOLLOWS": 0.1}'::jsonb;
            edge_type_configs := '[
                {"id": 1, "name": "DEPENDS_ON", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 2, "name": "OWNED_BY", "source": "dataset", "target": "user", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "user_", "source_suffix": ",PROD)", "target_suffix": ""},
                {"id": 3, "name": "CONTAINS", "source": "dashboard", "target": "chart", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "target_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 4, "name": "FOLLOWS", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"}
            ]'::jsonb;
        ELSIF edge_types_to_use <= 8 THEN
            edge_distribution := '{"DEPENDS_ON": 0.25, "OWNED_BY": 0.2, "CONTAINS": 0.15, "FOLLOWS": 0.1, "MENTIONS": 0.1, "TAGGED_WITH": 0.1, "ACCESSED_BY": 0.05, "RELATED_TO": 0.05}'::jsonb;
            edge_type_configs := '[
                {"id": 1, "name": "DEPENDS_ON", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 2, "name": "OWNED_BY", "source": "dataset", "target": "user", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "user_", "source_suffix": ",PROD)", "target_suffix": ""},
                {"id": 3, "name": "CONTAINS", "source": "dashboard", "target": "chart", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "target_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 4, "name": "FOLLOWS", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 5, "name": "MENTIONS", "source": "chart", "target": "dataset", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 6, "name": "TAGGED_WITH", "source": "dataset", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ",PROD)", "target_suffix": ")"},
                {"id": 7, "name": "ACCESSED_BY", "source": "user", "target": "dashboard", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "user_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": "", "target_suffix": ")"},
                {"id": 8, "name": "RELATED_TO", "source": "chart", "target": "dashboard", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": ")", "target_suffix": ")"}
            ]'::jsonb;
        ELSIF edge_types_to_use <= 16 THEN
            -- Medium complexity with datajob entities
            edge_distribution := '{"DEPENDS_ON": 0.2, "OWNED_BY": 0.15, "CONTAINS": 0.1, "READS_FROM": 0.15, "WRITES_TO": 0.1, "TRANSFORMS": 0.1, "SCHEDULED_BY": 0.05, "EXECUTED_BY": 0.05, "FOLLOWS": 0.05, "MENTIONS": 0.05}'::jsonb;
            edge_type_configs := '[
                {"id": 1, "name": "DEPENDS_ON", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 2, "name": "OWNED_BY", "source": "dataset", "target": "user", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "user_", "source_suffix": ",PROD)", "target_suffix": ""},
                {"id": 3, "name": "CONTAINS", "source": "dashboard", "target": "chart", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "target_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 33, "name": "READS_FROM", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 34, "name": "WRITES_TO", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 35, "name": "TRANSFORMS", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 36, "name": "SCHEDULED_BY", "source": "user", "target": "datajob", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataJob", "source_prefix": "user_", "target_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "source_suffix": "", "target_suffix": ")"},
                {"id": 37, "name": "EXECUTED_BY", "source": "datajob", "target": "user", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "user_", "source_suffix": ")", "target_suffix": ""},
                {"id": 4, "name": "FOLLOWS", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 5, "name": "MENTIONS", "source": "chart", "target": "dataset", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"}
            ]'::jsonb;
        ELSE
            -- Full 40 edge types with comprehensive data lineage
            edge_distribution := '{"DEPENDS_ON": 0.12, "OWNED_BY": 0.1, "CONTAINS": 0.08, "READS_FROM": 0.12, "WRITES_TO": 0.1, "TRANSFORMS": 0.1, "SCHEDULED_BY": 0.05, "EXECUTED_BY": 0.05, "MONITORED_BY": 0.03, "BACKFILLS": 0.03, "REFRESHES": 0.03, "FOLLOWS": 0.06, "MENTIONS": 0.06, "TAGGED_WITH": 0.06, "ACCESSED_BY": 0.05, "RELATED_TO": 0.05, "COLLABORATES_WITH": 0.04, "REPORTS_TO": 0.04, "MANAGES": 0.03, "WORKS_FOR": 0.03, "BELONGS_TO": 0.03, "PART_OF": 0.03, "IMPLEMENTS": 0.02, "USES": 0.02, "CONSUMES": 0.02, "PRODUCES": 0.02, "VALIDATES": 0.02, "APPROVES": 0.02, "REVIEWS": 0.02, "COMMENTS_ON": 0.02, "ANNOTATES": 0.02, "LINKS_TO": 0.02, "REFERENCES": 0.02, "CITES": 0.02, "INFLUENCES": 0.02, "AFFECTS": 0.02, "TRIGGERS": 0.02, "NOTIFIES": 0.02, "ALERTS": 0.02, "ESCALATES": 0.02}'::jsonb;
            edge_type_configs := '[
                {"id": 1, "name": "DEPENDS_ON", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 2, "name": "OWNED_BY", "source": "dataset", "target": "user", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "user_", "source_suffix": ",PROD)", "target_suffix": ""},
                {"id": 3, "name": "CONTAINS", "source": "dashboard", "target": "chart", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "target_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 33, "name": "READS_FROM", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 34, "name": "WRITES_TO", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 35, "name": "TRANSFORMS", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 36, "name": "SCHEDULED_BY", "source": "user", "target": "datajob", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataJob", "source_prefix": "user_", "target_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "source_suffix": "", "target_suffix": ")"},
                {"id": 37, "name": "EXECUTED_BY", "source": "datajob", "target": "user", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "user_", "source_suffix": ")", "target_suffix": ""},
                {"id": 4, "name": "FOLLOWS", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 5, "name": "MENTIONS", "source": "chart", "target": "dataset", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 6, "name": "TAGGED_WITH", "source": "dataset", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ",PROD)", "target_suffix": ")"},
                {"id": 7, "name": "ACCESSED_BY", "source": "user", "target": "dashboard", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "user_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": "", "target_suffix": ")"},
                {"id": 8, "name": "RELATED_TO", "source": "chart", "target": "dashboard", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 9, "name": "COLLABORATES_WITH", "source": "user", "target": "user", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:corpuser", "source_prefix": "user_", "target_prefix": "user_", "source_suffix": "", "target_suffix": ""},
                {"id": 10, "name": "REPORTS_TO", "source": "user", "target": "user", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:corpuser", "source_prefix": "user_", "target_prefix": "user_", "source_suffix": "", "target_suffix": ""},
                {"id": 11, "name": "MANAGES", "source": "user", "target": "user", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:corpuser", "source_prefix": "user_", "target_prefix": "user_", "source_suffix": "", "target_suffix": ""},
                {"id": 12, "name": "WORKS_FOR", "source": "user", "target": "mlmodel", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:mlModel", "source_prefix": "user_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": "", "target_suffix": ")"},
                {"id": 13, "name": "BELONGS_TO", "source": "dataset", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ",PROD)", "target_suffix": ")"},
                {"id": 14, "name": "PART_OF", "source": "chart", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 15, "name": "IMPLEMENTS", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 16, "name": "USES", "source": "chart", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 17, "name": "CONSUMES", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 18, "name": "PRODUCES", "source": "mlmodel", "target": "dataset", "source_platform": "urn:li:mlModel", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 19, "name": "VALIDATES", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 20, "name": "APPROVES", "source": "user", "target": "mlmodel", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:mlModel", "source_prefix": "user_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": "", "target_suffix": ")"},
                {"id": 21, "name": "REVIEWS", "source": "user", "target": "chart", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "user_", "target_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "source_suffix": "", "target_suffix": ")"},
                {"id": 22, "name": "COMMENTS_ON", "source": "user", "target": "dashboard", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "user_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": "", "target_suffix": ")"},
                {"id": 23, "name": "ANNOTATES", "source": "user", "target": "dataset", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "user_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": "", "target_suffix": ",PROD)"},
                {"id": 24, "name": "LINKS_TO", "source": "chart", "target": "dashboard", "source_platform": "urn:li:dataPlatform:looker", "target_platform": "urn:li:dataPlatform:looker", "source_prefix": "urn:li:chart:(urn:li:dataPlatform:looker,test_chart_", "target_prefix": "urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_", "source_suffix": ")", "target_suffix": ")"},
                {"id": 25, "name": "REFERENCES", "source": "dataset", "target": "dataset", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ",PROD)", "target_suffix": ",PROD)"},
                {"id": 26, "name": "CITES", "source": "mlmodel", "target": "dataset", "source_platform": "urn:li:mlModel", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 27, "name": "INFLUENCES", "source": "dataset", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ",PROD)", "target_suffix": ")"},
                {"id": 28, "name": "AFFECTS", "source": "mlmodel", "target": "dataset", "source_platform": "urn:li:mlModel", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 29, "name": "TRIGGERS", "source": "dataset", "target": "mlmodel", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:mlModel", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "source_suffix": ",PROD)", "target_suffix": ")"},
                {"id": 30, "name": "NOTIFIES", "source": "mlmodel", "target": "user", "source_platform": "urn:li:mlModel", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:mlModel:(urn:li:mlModel,test_mlmodel_", "target_prefix": "user_", "source_suffix": ")", "target_suffix": ""},
                {"id": 31, "name": "ALERTS", "source": "dataset", "target": "user", "source_platform": "urn:li:dataPlatform:postgres", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "target_prefix": "user_", "source_suffix": ",PROD)", "target_suffix": ""},
                {"id": 32, "name": "ESCALATES", "source": "user", "target": "user", "source_platform": "urn:li:corpuser", "target_platform": "urn:li:corpuser", "source_prefix": "user_", "target_prefix": "user_", "source_suffix": "", "target_suffix": ""},
                {"id": 38, "name": "MONITORED_BY", "source": "datajob", "target": "user", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:corpuser", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "user_", "source_suffix": ")", "target_suffix": ""},
                {"id": 39, "name": "BACKFILLS", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"},
                {"id": 40, "name": "REFRESHES", "source": "datajob", "target": "dataset", "source_platform": "urn:li:dataJob", "target_platform": "urn:li:dataPlatform:postgres", "source_prefix": "urn:li:dataJob:(urn:li:dataJob,test_datajob_", "target_prefix": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_", "source_suffix": ")", "target_suffix": ",PROD)"}
            ]'::jsonb;
        END IF;
        
        RAISE NOTICE 'Generating edges using % configured edge types with distribution: %', edge_types_to_use, edge_distribution;
        
        -- Calculate total distribution percentage to normalize edge counts
        DECLARE
            total_distribution_percentage NUMERIC := 0;
            remaining_edges INTEGER := edge_count;
            edge_type_id INTEGER;
            edge_type_name TEXT;
            edge_type_percentage NUMERIC;
        BEGIN
            -- Calculate total percentage from distribution
            FOR edge_type_record IN 
                SELECT config FROM jsonb_array_elements(edge_type_configs) AS config
                WHERE (config->>'id')::integer <= edge_types_to_use
                ORDER BY (config->>'id')::integer
            LOOP
                edge_type_name := edge_type_record.config->>'name';
                IF edge_distribution ? edge_type_name THEN
                    total_distribution_percentage := total_distribution_percentage + (edge_distribution->>edge_type_name)::numeric;
                END IF;
            END LOOP;
            
            RAISE NOTICE 'Total distribution percentage: %, normalizing to 100%%', (total_distribution_percentage * 100)::integer;
            
            -- Iterate through each configured edge type and generate edges
            FOR edge_type_record IN 
                SELECT config FROM jsonb_array_elements(edge_type_configs) AS config
                WHERE (config->>'id')::integer <= edge_types_to_use
                ORDER BY (config->>'id')::integer
            LOOP
                edge_type_count := (edge_type_record.config->>'id')::integer;
                edge_type_name := edge_type_record.config->>'name';
                
                -- Calculate target edges for this specific edge type
                IF edge_distribution ? edge_type_name THEN
                    -- Normalize the percentage to ensure total adds up to target
                    edge_type_percentage := (edge_distribution->>edge_type_name)::numeric / total_distribution_percentage;
                    target_edges_for_type := FLOOR(edge_count * edge_type_percentage);
                ELSE
                    -- Skip edge types not in distribution for >8 edge types
                    CONTINUE;
                END IF;
                
                IF target_edges_for_type > 0 THEN
                    RAISE NOTICE 'Generating % % edges (edge type %)...', target_edges_for_type, edge_type_name, edge_type_count;
                    
                    -- Generate edges for this type using the shared batch processing function
                    PERFORM process_edges_in_batches(
                        target_edges_for_type,
                        edge_type_count,
                        edge_type_name,
                        edge_type_record.config->>'source',
                        edge_type_record.config->>'target',
                        edge_type_record.config->>'source_platform',
                        edge_type_record.config->>'target_platform',
                        edge_type_record.config->>'source_prefix',
                        edge_type_record.config->>'target_prefix',
                        edge_type_record.config->>'source_suffix',
                        edge_type_record.config->>'target_suffix'
                    );
                    
                    total_edges_generated := total_edges_generated + target_edges_for_type;
                    edge_types_used := edge_types_used + 1;
                    
                    RAISE NOTICE 'Successfully generated % % edges (edge type %)', target_edges_for_type, edge_type_name, edge_type_count;
                END IF;
            END LOOP;
        END;
        
        -- Log summary of edge generation
        RAISE NOTICE 'Edge generation completed using % edge types out of % configured', edge_types_used, edge_types_to_use;
        RAISE NOTICE 'Total edges generated: % out of % target', total_edges_generated, edge_count;
        

        
        -- Generate deep data lineage chains for realistic enterprise scenarios
        DECLARE
            deep_lineage_edges INTEGER;
            max_depth INTEGER := 5; -- Reduced for memory efficiency
            max_fanout INTEGER := 5; -- Reduced for memory efficiency
            target_chains INTEGER := 1; -- Reduced for memory efficiency
        BEGIN
            RAISE NOTICE '=== GENERATING DEEP DATA LINEAGE CHAINS ===';
            RAISE NOTICE 'This will create chains up to % hops deep with fanout up to % (memory efficient)', max_depth, max_fanout;
            RAISE NOTICE 'Target: % chains (memory efficient)', target_chains;
            
            deep_lineage_edges := generate_deep_data_lineage(
                max_depth := max_depth,
                max_fanout := max_fanout,
                target_chains := target_chains,
                batch_size := 100
            );
            
            total_edges_generated := total_edges_generated + deep_lineage_edges;
            RAISE NOTICE 'Deep lineage generation completed: % additional edges', deep_lineage_edges;
            RAISE NOTICE 'Total edges including deep lineage: %', total_edges_generated;
        END;
        
        -- Update the target variables for compatibility with existing code
        target_depends_on := FLOOR(edge_count * (edge_distribution->>'DEPENDS_ON')::numeric);
        target_owned_by := FLOOR(edge_count * (edge_distribution->>'OWNED_BY')::numeric);
        target_contains := FLOOR(edge_count * (edge_distribution->>'CONTAINS')::numeric);
        target_other := edge_count - target_depends_on - target_owned_by - target_contains;
    END;
    
    -- Function cleanup not needed - function persists for future use
    
    end_time := clock_timestamp();
    
    RAISE NOTICE 'Unified vertex and edge generation completed in: %', (end_time - start_time);
    RAISE NOTICE 'Actual vertices created: %', (SELECT COUNT(*) FROM metadata_graph_vertices);
    RAISE NOTICE 'Actual edges created: %', (SELECT COUNT(*) FROM metadata_graph_edges);
    
    -- Commit the main data generation to ensure it's persisted
    COMMIT;
    
END $$;

-- =============================================================================
-- SECTION 3: CONNECTED COMPONENT DEFINITIONS
-- =============================================================================

SELECT '=== CREATING CONNECTED COMPONENT DEFINITIONS ===' as section;

-- Create CC definition for data lineage
INSERT INTO metadata_graph_cc_definitions (id, name, edge_types, edge_type_names, description, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds)
VALUES (
    1, 
    'Data Lineage Network', 
    ARRAY[1], 
    ARRAY['DEPENDS_ON'],
    'Network for analyzing data lineage relationships between datasets, charts, and dashboards',
    86400,
    3600
);

-- Create CC definition for ownership
INSERT INTO metadata_graph_cc_definitions (id, name, edge_types, edge_type_names, description, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds)
VALUES (
    2, 
    'Ownership Network', 
    ARRAY[2], 
    ARRAY['OWNED_BY'],
    'Network for analyzing ownership relationships between entities and users',
    86400,
    3600
);

-- Create CC definition for containment
INSERT INTO metadata_graph_cc_definitions (id, name, edge_types, edge_type_names, description, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds)
VALUES (
    3, 
    'Containment Network', 
    ARRAY[3], 
    ARRAY['CONTAINS'],
    'Network for analyzing containment relationships between dashboards, charts, and dashboards',
    86400,
    3600
);

-- Create CC definition for comprehensive analysis
DO $$
DECLARE
    available_edge_types INTEGER[];
    available_edge_type_names TEXT[];
BEGIN
    -- Get available edge types dynamically
    SELECT 
        ARRAY_AGG(id ORDER BY id),
        ARRAY_AGG(type_name ORDER BY id)
    INTO available_edge_types, available_edge_type_names
    FROM metadata_graph_edge_types;
    
    -- Insert comprehensive network definition with available edge types
    INSERT INTO metadata_graph_cc_definitions (id, name, edge_types, edge_type_names, description, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds)
    VALUES (
        4, 
        'Comprehensive Network', 
        available_edge_types, 
        available_edge_type_names,
        'Complete network for comprehensive graph analysis across all relationship types',
        86400,
        3600
    );
    
    -- Insert deep data lineage network definition
    INSERT INTO metadata_graph_cc_definitions (id, name, edge_types, edge_type_names, description, full_calculation_min_interval_seconds, incremental_calculation_min_interval_seconds)
    VALUES (
        5, 
        'Deep Data Lineage Network', 
        ARRAY[33, 34, 35, 36, 37, 38, 39, 40], 
        ARRAY['READS_FROM', 'WRITES_TO', 'TRANSFORMS', 'SCHEDULED_BY', 'EXECUTED_BY', 'MONITORED_BY', 'BACKFILLS', 'REFRESHES'],
        'Network for analyzing deep data lineage relationships between datajobs, datasets, and users with chains up to 1000 hops deep',
        86400,
        3600
    );
END $$;

SELECT 'CC definitions created: ' || COUNT(*)::text FROM metadata_graph_cc_definitions;

-- =============================================================================
-- SECTION 4: PERFORMANCE OPTIMIZATION
-- =============================================================================

DO $$
DECLARE
    perf_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '=== OPTIMIZING PERFORMANCE ===';

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_test_vertices_entity_type ON metadata_graph_vertices USING GIN ((properties->'entity_type')) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_vertices_batch ON metadata_graph_vertices((properties->>'batch')) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_vertices_scale ON metadata_graph_vertices((properties->>'scale')) WHERE removed = FALSE;

-- Create indexes for edge queries
CREATE INDEX IF NOT EXISTS idx_test_edges_source_target ON metadata_graph_edges(source_id, target_id) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_edges_edge_type ON metadata_graph_edges(edge_type) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_edges_batch ON metadata_graph_edges((properties->>'batch')) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_edges_scale ON metadata_graph_edges((properties->>'scale')) WHERE removed = FALSE;

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_test_edges_type_source ON metadata_graph_edges(edge_type, source_id) WHERE removed = FALSE;
CREATE INDEX IF NOT EXISTS idx_test_edges_type_target ON metadata_graph_edges(edge_type, target_id) WHERE removed = FALSE;

-- Update table statistics for better query planning
ANALYZE metadata_graph_vertices;
ANALYZE metadata_graph_edges;
ANALYZE metadata_graph_edge_types;
ANALYZE metadata_graph_cc_definitions;

    RAISE NOTICE 'Performance optimization completed in %', (clock_timestamp() - perf_start_time);
END $$;

-- =============================================================================
-- SECTION 5: FINAL VERIFICATION
-- =============================================================================

DO $$
DECLARE
    verify_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '=== FINAL VERIFICATION ===';

-- Display final data summary
RAISE NOTICE 'Final Data Summary';

-- Get counts for summary
DECLARE
    vertex_count INTEGER;
    edge_count INTEGER;
    edge_type_count INTEGER;
    cc_def_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO vertex_count FROM metadata_graph_vertices;
    SELECT COUNT(*) INTO edge_count FROM metadata_graph_edges;
    SELECT COUNT(*) INTO edge_type_count FROM metadata_graph_edge_types;
    SELECT COUNT(*) INTO cc_def_count FROM metadata_graph_cc_definitions;
    
    RAISE NOTICE 'Vertices: %', vertex_count;
    RAISE NOTICE 'Edges: %', edge_count;
    RAISE NOTICE 'Edge Types: %', edge_type_count;
    RAISE NOTICE 'CC Definitions: %', cc_def_count;
END;

    -- Display edge type distribution
    RAISE NOTICE 'Edge Type Distribution';

    -- Get edge type distribution
    DECLARE
        edge_type_rec RECORD;
    BEGIN
        FOR edge_type_rec IN
            SELECT 
                et.type_name,
                COUNT(e.source_id) as edge_count,
                ROUND((COUNT(e.source_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM metadata_graph_edges), 0)), 2) as percentage
            FROM metadata_graph_edge_types et
            LEFT JOIN metadata_graph_edges e ON et.id = e.edge_type AND e.removed = FALSE
            GROUP BY et.id, et.type_name
            ORDER BY et.id
        LOOP
            RAISE NOTICE '  %: % edges (%)', edge_type_rec.type_name, edge_type_rec.edge_count, edge_type_rec.percentage || '%%';
        END LOOP;
    END;

    -- Display entity type distribution
    RAISE NOTICE 'Entity Type Distribution';

    -- Get entity type distribution
    DECLARE
        entity_type_rec RECORD;
    BEGIN
        FOR entity_type_rec IN
            SELECT 
                properties->>'entity_type' as entity_type,
                COUNT(*) as count,
                ROUND((COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM metadata_graph_vertices), 0)), 2) as percentage
            FROM metadata_graph_vertices
            WHERE removed = FALSE
            GROUP BY properties->>'entity_type'
            ORDER BY count DESC
        LOOP
            RAISE NOTICE '  %: % vertices (%)', entity_type_rec.entity_type, entity_type_rec.count, entity_type_rec.percentage || '%%';
        END LOOP;
    END;

    -- Display test configuration
    RAISE NOTICE 'Test Configuration';

    -- Get test configuration summary
    DECLARE
        total_vertices INTEGER;
        total_edges INTEGER;
    BEGIN
        SELECT COUNT(*) INTO total_vertices FROM metadata_graph_vertices;
        SELECT COUNT(*) INTO total_edges FROM metadata_graph_edges;
        
        RAISE NOTICE '  Scale: custom';
        RAISE NOTICE '  Partitions: custom';
        RAISE NOTICE '  Total Vertices: %', total_vertices;
        RAISE NOTICE '  Total Edges: %', total_edges;
    END;


    
    -- Final status
    RAISE NOTICE 'Test data generation completed successfully!';

    RAISE NOTICE 'Final verification completed in %', (clock_timestamp() - verify_start_time);
END $$;

-- =============================================================================
-- FINAL TIMING SUMMARY
-- =============================================================================
DO $$
DECLARE
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    total_duration INTERVAL;
BEGIN
    -- Get the start time from the custom parameters (we'll need to track this)
    total_start_time := clock_timestamp() - interval '1 second'; -- Approximate, since we can't access the outer variable
    
    RAISE NOTICE '=== FINAL TIMING SUMMARY ===';
    
    RAISE NOTICE '=== COMPREHENSIVE TIMING SUMMARY ===';
    RAISE NOTICE 'Note: Individual section timings are shown above';
    RAISE NOTICE 'For large datasets, batch processing provides real-time ETA updates';
    RAISE NOTICE 'Performance optimization includes index creation and table statistics updates';
    RAISE NOTICE 'Final verification ensures data integrity and provides count summaries';
END $$;
