-- =============================================================================
-- Unified Test Data Verification for DataHub Graph Schema (Profiles)
-- =============================================================================
-- 
-- This script verifies test data generated with custom parameters.
-- The calling script must set these parameters before execution:
-- SET datahub.custom_vertices = '1000';      -- Expected vertex count
-- SET datahub.custom_edges = '5000';         -- Expected edge count  
-- SET datahub.custom_edge_types = '3';       -- Expected edge type count
--
-- FEATURES:
-- - Validates data counts against custom parameters
-- - Checks data quality and consistency
-- - Reports any discrepancies found
-- =============================================================================

-- Validate required parameters
DO $$
BEGIN
    -- Check if required parameters are set
    IF current_setting('datahub.custom_vertices', true) IS NULL OR current_setting('datahub.custom_vertices', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_vertices must be set before running verification';
    END IF;
    
    IF current_setting('datahub.custom_edges', true) IS NULL OR current_setting('datahub.custom_edges', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_edges must be set before running verification';
    END IF;
    
    IF current_setting('datahub.custom_edge_types', true) IS NULL OR current_setting('datahub.custom_edge_types', true) = '' THEN
        RAISE EXCEPTION 'datahub.custom_edge_types must be set before running verification';
    END IF;
    
    -- Debug: Show what parameters we're verifying against
    RAISE NOTICE 'Verifying against custom parameters - Expected: % vertices, % edges, % edge types', 
        current_setting('datahub.custom_vertices'),
        current_setting('datahub.custom_edges'),
        current_setting('datahub.custom_edge_types');
END $$;

-- =============================================================================
-- SECTION 1: BASIC COUNT VERIFICATION
-- =============================================================================

SELECT '=== BASIC COUNT VERIFICATION ===' as section;

-- Get actual counts
DO $$
DECLARE
    expected_vertices INTEGER;
    expected_edges INTEGER;
    expected_edge_types INTEGER;
    actual_vertices INTEGER;
    actual_edges INTEGER;
    actual_edge_types INTEGER;
    vertex_diff INTEGER;
    edge_diff INTEGER;
    edge_type_diff INTEGER;
BEGIN
    -- Get expected counts from parameters
    expected_vertices := current_setting('datahub.custom_vertices')::integer;
    expected_edges := current_setting('datahub.custom_edges')::integer;
    expected_edge_types := current_setting('datahub.custom_edge_types')::integer;
    
    -- Get actual counts
    SELECT COUNT(*) INTO actual_vertices FROM metadata_graph_vertices;
    SELECT COUNT(*) INTO actual_edges FROM metadata_graph_edges;
    SELECT COUNT(*) INTO actual_edge_types FROM metadata_graph_edge_types;
    
    -- Calculate differences
    vertex_diff := actual_vertices - expected_vertices;
    edge_diff := actual_edges - expected_edges;
    edge_type_diff := actual_edge_types - expected_edge_types;
    
    -- Report results
    RAISE NOTICE '=== COUNT VERIFICATION RESULTS ===';
    RAISE NOTICE 'Vertices: Expected %, Got %, Difference: %', 
        expected_vertices, actual_vertices, 
        CASE WHEN vertex_diff >= 0 THEN '+' || vertex_diff ELSE vertex_diff::text END;
    
    RAISE NOTICE 'Edges: Expected %, Got %, Difference: %', 
        expected_edges, actual_edges, 
        CASE WHEN edge_diff >= 0 THEN '+' || edge_diff ELSE edge_diff::text END;
    
    RAISE NOTICE 'Edge Types: Expected %, Got %, Difference: %', 
        expected_edge_types, actual_edge_types, 
        CASE WHEN edge_type_diff >= 0 THEN '+' || edge_type_diff ELSE edge_type_diff::text END;
    
    -- Check for significant discrepancies
    IF ABS(vertex_diff) > (expected_vertices * 0.01) THEN
        RAISE WARNING 'Vertex count differs by more than 1%% from expected. Expected: %, Got: %', 
            expected_vertices, actual_vertices;
    END IF;
    
    IF ABS(edge_diff) > (expected_edges * 0.01) THEN
        RAISE WARNING 'Edge count differs by more than 1%% from expected. Expected: %, Got: %', 
            expected_edges, actual_edges;
    END IF;
    
    IF edge_type_diff != 0 THEN
        RAISE WARNING 'Edge type count mismatch. Expected: %, Got: %', 
            expected_edge_types, actual_edge_types;
    END IF;
    
    -- Success message if all counts are correct
    IF vertex_diff = 0 AND edge_diff = 0 AND edge_type_diff = 0 THEN
        RAISE NOTICE 'All counts match expected values exactly!';
    ELSIF ABS(vertex_diff) <= (expected_vertices * 0.01) AND ABS(edge_diff) <= (expected_edges * 0.01) AND edge_type_diff = 0 THEN
        RAISE NOTICE 'All counts are within acceptable tolerance!';
    ELSE
        RAISE NOTICE 'Some counts have significant discrepancies. Please review the warnings above.';
    END IF;
END $$;

-- =============================================================================
-- SECTION 2: DATA QUALITY CHECKS
-- =============================================================================

SELECT '=== DATA QUALITY CHECKS ===' as section;

-- Check for orphaned edges (edges pointing to non-existent vertices)
DO $$
DECLARE
    orphaned_edges_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphaned_edges_count
    FROM metadata_graph_edges e
    LEFT JOIN metadata_graph_vertices v1 ON e.source_id = v1.xxhash64_id
    LEFT JOIN metadata_graph_vertices v2 ON e.target_id = v2.xxhash64_id
    WHERE v1.xxhash64_id IS NULL OR v2.xxhash64_id IS NULL;
    
    IF orphaned_edges_count > 0 THEN
        RAISE WARNING 'Found % orphaned edges (edges pointing to non-existent vertices)', orphaned_edges_count;
    ELSE
        RAISE NOTICE 'No orphaned edges found - all edges have valid source and target vertices';
    END IF;
END $$;

-- Check for edges with invalid edge types
DO $$
DECLARE
    invalid_edge_types_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO invalid_edge_types_count
    FROM metadata_graph_edges e
    LEFT JOIN metadata_graph_edge_types et ON e.edge_type = et.id
    WHERE et.id IS NULL;
    
    IF invalid_edge_types_count > 0 THEN
        RAISE WARNING 'Found % edges with invalid edge types', invalid_edge_types_count;
    ELSE
        RAISE NOTICE 'All edges have valid edge types';
    END IF;
END $$;

-- Check for duplicate edges
DO $$
DECLARE
    duplicate_edges_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO duplicate_edges_count
    FROM (
        SELECT source_id, edge_type, target_id, owner_id, COUNT(*)
        FROM metadata_graph_edges
        GROUP BY source_id, edge_type, target_id, owner_id
        HAVING COUNT(*) > 1
    ) duplicates;
    
    IF duplicate_edges_count > 0 THEN
        RAISE WARNING 'Found % duplicate edges (same source, target, edge type, and owner)', duplicate_edges_count;
    ELSE
        RAISE NOTICE 'No duplicate edges found';
    END IF;
END $$;

-- =============================================================================
-- SECTION 3: ENTITY TYPE DISTRIBUTION
-- =============================================================================

SELECT '=== ENTITY TYPE DISTRIBUTION ===' as section;

-- Show vertex distribution by entity type
SELECT 
    properties->>'entity_type' as entity_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM metadata_graph_vertices), 2) as percentage
FROM metadata_graph_vertices
GROUP BY properties->>'entity_type'
ORDER BY count DESC;

-- Show edge distribution by edge type
SELECT 
    et.type_name as edge_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM metadata_graph_edges), 2) as percentage
FROM metadata_graph_edges e
JOIN metadata_graph_edge_types et ON e.edge_type = et.id
GROUP BY et.id, et.type_name
ORDER BY count DESC;

-- =============================================================================
-- SECTION 4: PERFORMANCE METRICS
-- =============================================================================

SELECT '=== PERFORMANCE METRICS ===' as section;

-- Check if indexes exist
DO $$
DECLARE
    index_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes
    WHERE tablename LIKE 'metadata_graph_%' AND indexname LIKE '%test_%';
    
    RAISE NOTICE 'Found % test-related indexes on graph tables', index_count;
    
    IF index_count = 0 THEN
        RAISE WARNING 'No test-related indexes found. Performance may be suboptimal.';
    ELSE
        RAISE NOTICE 'Test-related indexes are in place for optimal performance.';
    END IF;
END $$;

-- =============================================================================
-- SECTION 5: SUMMARY REPORT
-- =============================================================================

SELECT '=== SUMMARY REPORT ===' as section;

-- Final summary
SELECT 
    'custom' as scale,
    'custom' as partitions,
    (SELECT COUNT(*) FROM metadata_graph_vertices) as total_vertices,
    (SELECT COUNT(*) FROM metadata_graph_edges) as total_edges,
    (SELECT COUNT(DISTINCT edge_type) FROM metadata_graph_edges) as total_edge_types,
    current_setting('datahub.custom_vertices') as expected_vertices,
    current_setting('datahub.custom_edges') as expected_edges,
    current_setting('datahub.custom_edge_types') as expected_edge_types;
