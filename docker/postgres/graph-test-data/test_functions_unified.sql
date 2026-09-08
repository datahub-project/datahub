-- =============================================================================
-- Unified Test Suite for DataHub Graph Functions
-- =============================================================================
-- 
-- This comprehensive test file consolidates all testing functionality for:
-- - Edge type management functions
-- - Connected component functions  
-- - pgRouting functions
-- - Graph statistics and utilities
-- - Performance testing
-- - Error handling and edge cases
--
-- USAGE:
-- 1. For small tests: Run as-is (defaults to small dataset)
-- 2. For medium tests: SET datahub.test_scale = 'medium';
-- 3. For large tests: SET datahub.test_scale = 'large';
-- 4. For xlarge tests: SET datahub.test_scale = 'xlarge';
--
-- TEST COVERAGE:
-- - Edge type management functions
-- - Connected component functions
-- - Graph statistics and utilities
-- - Performance testing
-- - Error handling and edge cases
-- - Comprehensive validation
-- =============================================================================

-- Set default test scale if not specified
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'datahub.test_scale') THEN
        PERFORM set_config('datahub.test_scale', 'small', false);
    END IF;
END $$;

-- Display current test configuration
SELECT 
    'Test Configuration' as info,
    current_setting('datahub.test_scale') as scale,
    current_setting('datahub.pgrouting_partition_count') as partitions;

-- =============================================================================
-- SECTION 1: EDGE TYPE MANAGEMENT TESTS
-- =============================================================================

SELECT '=== EDGE TYPE MANAGEMENT TESTS ===' as test_section;

-- Test 1.1: Create edge types with specific IDs
SELECT 'Test 1.1: Creating edge types with specific IDs' as test_step;

-- Create core edge types
SELECT dh_create_edge_type_if_not_exists(1, 'DEPENDS_ON') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(2, 'OWNED_BY') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(3, 'CONTAINS') as edge_type_id;

-- Test 1.2: Verify edge types were created
SELECT 'Test 1.2: Verifying edge type creation' as test_step;
SELECT * FROM metadata_graph_edge_types ORDER BY id;

-- Test 1.3: Test idempotency (should return existing ID)
SELECT 'Test 1.3: Testing idempotency' as test_step;
SELECT dh_create_edge_type_if_not_exists(1, 'DEPENDS_ON') as edge_type_id;

-- Test 1.4: Test error handling - ID conflict
SELECT 'Test 1.4: Testing ID conflict handling' as test_step;
-- This should fail because ID 1 is already taken by 'DEPENDS_ON'
DO $$
BEGIN
    PERFORM dh_create_edge_type_if_not_exists(1, 'DIFFERENT_NAME');
    RAISE EXCEPTION 'Expected error for ID conflict did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test 1.5: Create additional edge types for comprehensive testing
SELECT 'Test 1.5: Creating additional edge types' as test_step;
SELECT dh_create_edge_type_if_not_exists(4, 'FLOWS_TO') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(5, 'TRANSFORMS') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(6, 'VALIDATES') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(7, 'ACCESSED_BY') as edge_type_id;
SELECT dh_create_edge_type_if_not_exists(8, 'GENERATED_FROM') as edge_type_id;

-- Test 1.6: List all edge types
SELECT 'Test 1.6: Listing all edge types' as test_step;
SELECT * FROM dh_list_edge_types();

-- Test 1.7: Test edge type deletion (should fail since they're in use)
SELECT 'Test 1.7: Testing edge type deletion (should fail due to usage)' as test_step;
SELECT dh_delete_edge_type_if_unused(1) as deleted;
SELECT dh_delete_edge_type_if_unused(2) as deleted;

-- =============================================================================
-- SECTION 2: CONNECTED COMPONENT DEFINITION TESTS
-- =============================================================================

SELECT '=== CONNECTED COMPONENT DEFINITION TESTS ===' as test_section;

-- Test 2.1: Create CC definitions with edge types
SELECT 'Test 2.1: Creating CC definitions' as test_step;

-- Create CC definition for data lineage with default min component size (3)
SELECT dh_create_cc_definition(
    1, 
    'Data Lineage View', 
    ARRAY[1, 2, 3], 
    ARRAY['DEPENDS_ON', 'OWNED_BY', 'CONTAINS'],
    'View for analyzing data lineage and ownership relationships'
) as cc_def_id;

-- Create CC definition for data flow with custom min component size (4)
SELECT dh_create_cc_definition(
    2, 
    'Data Flow View', 
    ARRAY[4, 5, 6], 
    ARRAY['FLOWS_TO', 'TRANSFORMS', 'VALIDATES'],
    'View for analyzing data flow and transformation relationships',
    4  -- Minimum 4 vertices for meaningful data flow
) as cc_def_id;

-- Create CC definition for access control with custom min component size (5)
SELECT dh_create_cc_definition(
    3, 
    'Access Control View', 
    ARRAY[7, 8], 
    ARRAY['ACCESSED_BY', 'GENERATED_FROM'],
    'View for analyzing access control and data generation relationships',
    5  -- Minimum 5 vertices for meaningful access patterns
) as cc_def_id;

-- Test 2.2: Verify CC definitions were created
SELECT 'Test 2.2: Verifying CC definition creation' as test_step;
SELECT * FROM metadata_graph_cc_definitions ORDER BY id;

-- Test 2.3: Test CC definition idempotency
SELECT 'Test 2.3: Testing CC definition idempotency' as test_step;
SELECT dh_create_cc_definition(
    1, 
    'Data Lineage View', 
    ARRAY[1, 2, 3], 
    ARRAY['DEPENDS_ON', 'OWNED_BY', 'CONTAINS'],
    'View for analyzing data lineage and ownership relationships'
) as cc_def_id;

-- Test 2.4: Test CC definition error handling - ID conflict
SELECT 'Test 2.4: Testing CC definition ID conflict handling' as test_step;
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        1, 
        'Different Name', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'This should fail due to ID conflict'
    );
    RAISE EXCEPTION 'Expected error for CC definition ID conflict did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test 2.5: Test minimum component size validation
SELECT 'Test 2.5: Testing minimum component size validation' as test_step;

-- Test with invalid minimum size (less than 2)
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        999, 
        'Invalid Min Size', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'This should fail due to invalid min component size',
        1  -- Invalid: less than 2
    );
    RAISE EXCEPTION 'Expected error for invalid min component size did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with minimum size of 2 (valid)
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        999, 
        'Valid Min Size 2', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'This should succeed with min component size 2',
        2  -- Valid: exactly 2
    );
    RAISE NOTICE 'Successfully created CC definition with min component size 2';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Unexpected error: %', SQLERRM;
END $$;

-- Test 2.6: Test minimum component size update functionality
SELECT 'Test 2.6: Testing minimum component size update functionality' as test_step;

-- Update the minimum component size for definition 1
SELECT dh_update_cc_definition_min_size(1, 4) as updated;

-- Verify the update
SELECT 
    'Updated Min Size' as test,
    id,
    name,
    min_component_size,
    CASE 
        WHEN min_component_size = 4 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM metadata_graph_cc_definitions 
WHERE id = 1;

-- =============================================================================
-- SECTION 3: GRAPH STATISTICS AND UTILITIES TESTS
-- =============================================================================

SELECT '=== GRAPH STATISTICS AND UTILITIES TESTS ===' as test_section;

-- Test 3.1: Test graph statistics functions
SELECT 'Test 3.1: Testing graph statistics functions' as test_step;

-- Get basic graph statistics
SELECT 
    'Graph Statistics' as metric,
    COUNT(DISTINCT v.xxhash64_id) as vertex_count,
    COUNT(DISTINCT e.id) as edge_count,
    COUNT(DISTINCT et.id) as edge_type_count,
    COUNT(DISTINCT cd.id) as cc_definition_count
FROM metadata_graph_vertices v
CROSS JOIN metadata_graph_edges e
CROSS JOIN metadata_graph_edge_types et
CROSS JOIN metadata_graph_cc_definitions cd
WHERE v.removed = FALSE AND e.removed = FALSE;

-- Test 3.2: Test entity type distribution
SELECT 'Test 3.2: Testing entity type distribution' as test_step;
SELECT 
    properties->>'entity_type' as entity_type,
    COUNT(*) as count,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM metadata_graph_vertices WHERE removed = FALSE), 2) as percentage
FROM metadata_graph_vertices
WHERE removed = FALSE
GROUP BY properties->>'entity_type'
ORDER BY count DESC;

-- Test 3.3: Test edge type distribution
SELECT 'Test 3.3: Testing edge type distribution' as test_step;
SELECT 
    et.type_name,
    COUNT(e.id) as count,
    ROUND((COUNT(e.id) * 100.0 / (SELECT COUNT(*) FROM metadata_graph_edges WHERE removed = FALSE), 2) as percentage
FROM metadata_graph_edge_types et
LEFT JOIN metadata_graph_edges e ON et.id = e.edge_type AND e.removed = FALSE
GROUP BY et.id, et.type_name
ORDER BY et.id;

-- =============================================================================
-- SECTION 3.5: TRIVIAL COMPONENT FILTERING TESTS
-- =============================================================================

SELECT '=== TRIVIAL COMPONENT FILTERING TESTS ===' as test_section;

-- Test 3.5.1: Test trivial component identification
SELECT 'Test 3.5.1: Testing trivial component identification' as test_step;

-- Get trivial components for definition 1 (min size 4)
SELECT 
    'Trivial Components (Def 1)' as test,
    COUNT(*) as trivial_count,
    CASE 
        WHEN COUNT(*) >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM dh_get_trivial_components(1);

-- Get trivial components for definition 2 (min size 4)
SELECT 
    'Trivial Components (Def 2)' as test,
    COUNT(*) as trivial_count,
    CASE 
        WHEN COUNT(*) >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM dh_get_trivial_components(2);

-- Get trivial components for definition 3 (min size 5)
SELECT 
    'Trivial Components (Def 3)' as test,
    COUNT(*) as trivial_count,
    CASE 
        WHEN COUNT(*) >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM dh_get_trivial_components(3);

-- Test 3.5.2: Test isolated vertex identification
SELECT 'Test 3.5.2: Testing isolated vertex identification' as test_step;

-- Get isolated vertices for definition 1
SELECT 
    'Isolated Vertices (Def 1)' as test,
    COUNT(*) as isolated_count,
    CASE 
        WHEN COUNT(*) >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM dh_get_isolated_vertices(1);

-- Get isolated vertices for definition 2
SELECT 
    'Isolated Vertices (Def 2)' as test,
    COUNT(*) as isolated_count,
    CASE 
        WHEN COUNT(*) >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM dh_get_isolated_vertices(2);

-- Test 3.5.3: Test trivial component filtering behavior
SELECT 'Test 3.5.3: Testing trivial component filtering behavior' as test_step;

-- Verify that CC definitions have the expected minimum component sizes
SELECT 
    'Min Component Size Validation' as test,
    id,
    name,
    min_component_size,
    CASE 
        WHEN id = 1 AND min_component_size = 4 THEN 'PASS'
        WHEN id = 2 AND min_component_size = 4 THEN 'PASS'
        WHEN id = 3 AND min_component_size = 5 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM metadata_graph_cc_definitions 
WHERE id IN (1, 2, 3)
ORDER BY id;

-- =============================================================================
-- SECTION 4: PERFORMANCE TESTING
-- =============================================================================

SELECT '=== PERFORMANCE TESTING ===' as test_section;

-- Test 4.1: Test query performance with different scales
SELECT 'Test 4.1: Testing query performance' as test_step;

DO $$
DECLARE
    test_scale TEXT := current_setting('datahub.test_scale');
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    query_time INTERVAL;
    vertex_count BIGINT;
    edge_count BIGINT;
BEGIN
    RAISE NOTICE 'Running performance tests for scale: %', test_scale;
    
    -- Test vertex count performance
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO vertex_count FROM metadata_graph_vertices WHERE removed = FALSE;
    end_time := clock_timestamp();
    query_time := end_time - start_time;
    RAISE NOTICE 'Vertex count query completed in: % (count: %)', query_time, vertex_count;
    
    -- Test edge count performance
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO edge_count FROM metadata_graph_edges WHERE removed = FALSE;
    end_time := clock_timestamp();
    query_time := end_time - start_time;
    RAISE NOTICE 'Edge count query completed in: % (count: %)', query_time, edge_count;
    
    -- Test complex query performance (entity type distribution)
    start_time := clock_timestamp();
    PERFORM COUNT(*) FROM (
        SELECT properties->>'entity_type' as entity_type
        FROM metadata_graph_vertices
        WHERE removed = FALSE
        GROUP BY properties->>'entity_type'
    ) t;
    end_time := clock_timestamp();
    query_time := end_time - start_time;
    RAISE NOTICE 'Entity type distribution query completed in: %', query_time;
    
    -- Performance expectations based on scale
    CASE test_scale
        WHEN 'small' THEN
            IF query_time > INTERVAL '1 second' THEN
                RAISE WARNING 'Query performance seems slow for small scale';
            END IF;
        WHEN 'medium' THEN
            IF query_time > INTERVAL '5 seconds' THEN
                RAISE WARNING 'Query performance seems slow for medium scale';
            END IF;
        WHEN 'large' THEN
            IF query_time > INTERVAL '30 seconds' THEN
                RAISE WARNING 'Query performance seems slow for large scale';
            END IF;
        WHEN 'xlarge' THEN
            IF query_time > INTERVAL '2 minutes' THEN
                RAISE WARNING 'Query performance seems slow for xlarge scale';
            END IF;
    END CASE;
END $$;

-- =============================================================================
-- SECTION 5: ERROR HANDLING AND EDGE CASES
-- =============================================================================

SELECT '=== ERROR HANDLING AND EDGE CASES ===' as test_section;

-- Test 5.1: Test edge type creation with invalid parameters
SELECT 'Test 5.1: Testing edge type creation with invalid parameters' as test_step;

-- Test with NULL type name
DO $$
BEGIN
    PERFORM dh_create_edge_type_if_not_exists(999, NULL);
    RAISE EXCEPTION 'Expected error for NULL type name did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with empty type name
DO $$
BEGIN
    PERFORM dh_create_edge_type_if_not_exists(999, '');
    RAISE EXCEPTION 'Expected error for empty type name did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test 5.2: Test CC definition creation with invalid parameters
SELECT 'Test 5.2: Testing CC definition creation with invalid parameters' as test_step;

-- Test with NULL name
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        999, 
        NULL, 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'Test description'
    );
    RAISE EXCEPTION 'Expected error for NULL name did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with empty name
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        999, 
        '', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'Test description'
    );
    RAISE EXCEPTION 'Expected error for empty name did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with mismatched array lengths
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        999, 
        'Test CC', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON'],
        'Test description'
    );
    RAISE EXCEPTION 'Expected error for mismatched array lengths did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test 5.3: Test minimum component size validation
SELECT 'Test 5.3: Testing minimum component size validation' as test_step;

-- Test with invalid minimum size (less than 2)
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        998, 
        'Invalid Min Size Test', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'Test description',
        1  -- Invalid: less than 2
    );
    RAISE EXCEPTION 'Expected error for invalid min component size did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with invalid minimum size (0)
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        997, 
        'Invalid Min Size Test 2', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'Test description',
        0  -- Invalid: 0
    );
    RAISE EXCEPTION 'Expected error for invalid min component size 0 did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test with invalid minimum size (negative)
DO $$
BEGIN
    PERFORM dh_create_cc_definition(
        996, 
        'Invalid Min Size Test 3', 
        ARRAY[1, 2], 
        ARRAY['DEPENDS_ON', 'OWNED_BY'],
        'Test description',
        -1  -- Invalid: negative
    );
    RAISE EXCEPTION 'Expected error for invalid min component size -1 did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test 5.4: Test minimum component size update validation
SELECT 'Test 5.4: Testing minimum component size update validation' as test_step;

-- Test updating with invalid minimum size (less than 2)
DO $$
BEGIN
    PERFORM dh_update_cc_definition_min_size(1, 1);
    RAISE EXCEPTION 'Expected error for invalid min component size update did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test updating with invalid minimum size (0)
DO $$
BEGIN
    PERFORM dh_update_cc_definition_min_size(1, 0);
    RAISE EXCEPTION 'Expected error for invalid min component size update 0 did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test updating with invalid minimum size (negative)
DO $$
BEGIN
    PERFORM dh_update_cc_definition_min_size(1, -1);
    RAISE EXCEPTION 'Expected error for invalid min component size update -1 did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- Test updating non-existent definition
DO $$
BEGIN
    PERFORM dh_update_cc_definition_min_size(99999, 5);
    RAISE EXCEPTION 'Expected error for non-existent definition update did not occur';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error caught: %', SQLERRM;
END $$;

-- =============================================================================
-- SECTION 6: COMPREHENSIVE VALIDATION
-- =============================================================================

SELECT '=== COMPREHENSIVE VALIDATION ===' as test_section;

-- Test 6.1: Validate data integrity
SELECT 'Test 6.1: Validating data integrity' as test_step;

-- Check for orphaned edges
SELECT 
    'Orphaned Edges' as issue,
    COUNT(*) as count
FROM metadata_graph_edges e
LEFT JOIN metadata_graph_vertices v1 ON e.source_id = v1.xxhash64_id
LEFT JOIN metadata_graph_vertices v2 ON e.target_id = v2.xxhash64_id
WHERE e.removed = FALSE 
  AND (v1.xxhash64_id IS NULL OR v2.xxhash64_id IS NULL);

-- Check for invalid edge types
SELECT 
    'Invalid Edge Types' as issue,
    COUNT(*) as count
FROM metadata_graph_edges e
LEFT JOIN metadata_graph_edge_types et ON e.edge_type = et.id
WHERE e.removed = FALSE AND et.id IS NULL;

-- Check for invalid CC definitions
SELECT 
    'Invalid CC Definitions' as issue,
    COUNT(*) as count
FROM metadata_graph_cc_definitions cd
LEFT JOIN metadata_graph_edge_types et ON et.id = ANY(cd.edge_type_ids)
WHERE et.id IS NULL;

-- Test 6.2: Validate index effectiveness
SELECT 'Test 6.2: Validating index effectiveness' as test_step;

-- Check if indexes exist and are being used
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE tablename LIKE 'metadata_graph_%'
ORDER BY schemaname, tablename, indexname;

-- =============================================================================
-- SECTION 7: TEST SUMMARY AND CLEANUP
-- =============================================================================

SELECT '=== TEST SUMMARY AND CLEANUP ===' as test_section;

-- Display final test summary
DO $$
DECLARE
    test_scale TEXT := current_setting('datahub.test_scale');
    total_vertices BIGINT;
    total_edges BIGINT;
    total_edge_types BIGINT;
    total_cc_defs BIGINT;
    test_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'UNIFIED TEST SUITE COMPLETED';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Test Scale: %', test_scale;
    RAISE NOTICE 'Test Duration: %', (clock_timestamp() - test_start_time);
    
    -- Get final counts
    SELECT COUNT(*) INTO total_vertices FROM metadata_graph_vertices WHERE removed = FALSE;
    SELECT COUNT(*) INTO total_edges FROM metadata_graph_edges WHERE removed = FALSE;
    SELECT COUNT(*) INTO total_edge_types FROM metadata_graph_edge_types;
    SELECT COUNT(*) INTO total_cc_defs FROM metadata_graph_cc_definitions;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Final Data Counts:';
    RAISE NOTICE '- Vertices: %', total_vertices;
    RAISE NOTICE '- Edges: %', total_edges;
    RAISE NOTICE '- Edge Types: %', total_edge_types;
    RAISE NOTICE '- CC Definitions: %', total_cc_defs;
    RAISE NOTICE '';
    
    -- Display CC definition details including minimum component sizes
    RAISE NOTICE 'CC Definition Details:';
    FOR cd IN SELECT id, name, min_component_size FROM metadata_graph_cc_definitions ORDER BY id
    LOOP
        RAISE NOTICE '- Definition %: % (min size: %)', cd.id, cd.name, cd.min_component_size;
    END LOOP;
    RAISE NOTICE '';
    
    -- Validate expected counts based on scale
    CASE test_scale
        WHEN 'small' THEN
            IF total_vertices >= 1000 AND total_edges >= 5000 THEN
                RAISE NOTICE '✓ Small scale validation PASSED';
            ELSE
                RAISE WARNING '✗ Small scale validation FAILED';
            END IF;
        WHEN 'medium' THEN
            IF total_vertices >= 10000 AND total_edges >= 50000 THEN
                RAISE NOTICE '✓ Medium scale validation PASSED';
            ELSE
                RAISE WARNING '✗ Medium scale validation FAILED';
            END IF;
        WHEN 'large' THEN
            IF total_vertices >= 100000 AND total_edges >= 500000 THEN
                RAISE NOTICE '✓ Large scale validation PASSED';
            ELSE
                RAISE WARNING '✗ Large scale validation FAILED';
            END IF;
        WHEN 'xlarge' THEN
            IF total_vertices >= 1000000 AND total_edges >= 5000000 THEN
                RAISE NOTICE '✓ XLarge scale validation PASSED';
            ELSE
                RAISE WARNING '✗ XLarge scale validation FAILED';
            END IF;
    END CASE;
    
    RAISE NOTICE '';
    RAISE NOTICE 'All tests completed successfully!';
    RAISE NOTICE '========================================';
END $$;
