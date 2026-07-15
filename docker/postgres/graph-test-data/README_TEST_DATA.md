# Test Data Generation for DataHub Graph Schema

This directory contains scripts to generate comprehensive test data for the DataHub graph schema with PostgreSQL and pgRouting.

## Overview

The test data generation creates a realistic graph with:

- **3 Edge Types**: `DEPENDS_ON`, `OWNED_BY`, `CONTAINS`
- **10,000 Vertices**: Datasets, users, dashboards, charts, and ML models
- **100,000 Edges**: Realistic relationships between entities
- **Connected Components**: Automatically calculated for each edge type

## Files

### 1. `generate_test_data_unified.sql`

The main unified SQL script that generates test data at different scales. It:

- Supports multiple scales: small (1K vertices), medium (10K), large (100K), xlarge (1M)
- Creates comprehensive edge types with meaningful names
- Generates vertices with realistic URNs and properties
- Creates edges with realistic relationships
- Defines CC definitions for different analysis purposes
- Calculates connected components using pgRouting
- Includes verification and statistics
- Configurable via `datahub.test_scale` setting

### 2. `verify_test_data_unified.sql`

A comprehensive unified verification script that:

- Automatically detects dataset size and adjusts verification accordingly
- Shows counts and distributions of all data
- Displays sample vertices and edges
- Reports CC statistics and computation logs
- Tests graph functions
- Shows performance metrics
- Provides comprehensive analysis for all scales

### 3. `test_functions_unified.sql`

A comprehensive test suite that consolidates all testing functionality:

- Edge type management function tests
- Connected component function tests
- Graph statistics and utilities tests
- Performance testing with scale-aware expectations
- Error handling and edge case validation
- Comprehensive data integrity validation

### 3. `run_test_data_generation.sh`

An interactive shell script that:

- Checks prerequisites (schema, functions, database connection)
- Runs the test data generation
- Verifies the results
- Shows quick statistics
- Provides cleanup commands

## Prerequisites

Before running the test data generation, ensure:

1. **PostgreSQL Database**: Running with pgRouting extension
2. **Graph Schema**: Tables created via `init_pgrouting_schema.sql`
3. **Graph Functions**: Functions created via `init_pgrouting_functions.sql`
4. **PostgreSQL Client**: `psql` command available

## Quick Start

### Option 1: Interactive Shell Script (Recommended)

```bash
cd docker/postgres-setup
./run_test_data_generation.sh
```

The script will:

- Prompt for database connection details
- Verify prerequisites
- Generate test data
- Verify results
- Show statistics

### Option 2: Manual SQL Execution

```bash
# 1. Connect to your database
psql -h localhost -U your_user -d your_database

# 2. Set test scale (optional, defaults to 'small')
SET datahub.test_scale = 'medium';  -- Options: 'small', 'medium', 'large', 'xlarge'

# 3. Generate test data
\i generate_test_data_unified.sql

# 4. Verify results
\i verify_test_data_unified.sql

# 5. Run comprehensive tests
\i test_functions_unified.sql
```

### Option 3: Environment Variables

```bash
export POSTGRES_HOST=localhost
export POSTGRES_DB=your_database
export POSTGRES_USER=your_user
export POSTGRES_PASSWORD=your_password
export POSTGRES_PORT=5432

cd docker/postgres-setup
./run_test_data_generation.sh
```

## Test Data Scales

The unified test data generation supports multiple scales for different testing needs:

| Scale      | Vertices  | Edges     | Use Case                 | Estimated Time |
| ---------- | --------- | --------- | ------------------------ | -------------- |
| **small**  | 1,000     | 5,000     | Development, quick tests | 1-2 minutes    |
| **medium** | 10,000    | 50,000    | Integration testing      | 5-10 minutes   |
| **large**  | 100,000   | 500,000   | Performance testing      | 15-30 minutes  |
| **xlarge** | 1,000,000 | 5,000,000 | Stress testing           | 30-60 minutes  |

### Setting the Scale

```sql
-- Set scale before running generation
SET datahub.test_scale = 'medium';

-- Generate data at the specified scale
\i generate_test_data_unified.sql
```

## Test Data Details

### Vertex Types and Distribution (Small Scale Example)

| Entity Type | Count | Percentage | Sample URN                                                          |
| ----------- | ----- | ---------- | ------------------------------------------------------------------- |
| Dataset     | 400   | 40%        | `urn:li:dataset:(urn:li:dataPlatform:postgres,test_dataset_1,PROD)` |
| User        | 200   | 20%        | `urn:li:corpuser:user_1`                                            |
| Dashboard   | 150   | 15%        | `urn:li:dashboard:(urn:li:dataPlatform:looker,test_dashboard_1)`    |
| Chart       | 150   | 15%        | `urn:li:chart:(urn:li:dataPlatform:looker,test_chart_1)`            |
| ML Model    | 1,000 | 10%        | `urn:li:mlModel:(urn:li:dataPlatform:sagemaker,test_model_1)`       |

### Edge Types and Distribution

| Edge Type  | Count  | Percentage | Description                                                                    |
| ---------- | ------ | ---------- | ------------------------------------------------------------------------------ |
| DEPENDS_ON | 50,000 | 50%        | Data lineage relationships between datasets, charts, dashboards                |
| OWNED_BY   | 30,000 | 30%        | Ownership relationships from entities to users                                 |
| CONTAINS   | 20,000 | 20%        | Containment relationships (dashboards contain charts, datasets contain models) |

### CC Definitions

1. **Data Lineage Network** (ID: 1)

   - Edge types: `DEPENDS_ON` only
   - Purpose: Lineage analysis
   - Directed: Yes

2. **Ownership Network** (ID: 2)

   - Edge types: `OWNED_BY` only
   - Purpose: Access control analysis
   - Directed: Yes

3. **Composite Network** (ID: 3)
   - Edge types: All three types
   - Purpose: Comprehensive analysis
   - Directed: No

## Performance Characteristics

- **Generation Time**: 2-5 minutes (depending on hardware)
- **Storage**: ~50-100 MB for the complete dataset
- **Indexes**: Optimized for common query patterns
- **Partitioning**: Edges table partitioned by hash for scalability

## Verification Queries

After generation, you can run these queries to verify the data:

```sql
-- Basic counts
SELECT COUNT(*) FROM metadata_graph_vertices;  -- Should be 10,000
SELECT COUNT(*) FROM metadata_graph_edges;     -- Should be 100,000

-- Edge type distribution
SELECT
    et.type_name,
    COUNT(*) as edge_count
FROM metadata_graph_edges e
JOIN metadata_graph_edge_types et ON e.edge_type = et.id
GROUP BY et.id, et.type_name
ORDER BY et.id;

-- Entity type distribution
SELECT
    properties->>'entity_type' as entity_type,
    COUNT(*) as count
FROM metadata_graph_vertices
GROUP BY properties->>'entity_type'
ORDER BY count DESC;
```

## Graph Function Examples

Test the generated functions:

```sql
-- Get overall graph statistics
SELECT * FROM dh_get_graph_statistics();

-- Get vertices in a specific CC
SELECT * FROM dh_get_cc_vertices(1, 1);  -- CC ID 1, Definition 1

-- Get CC for a specific vertex
SELECT * FROM dh_get_vertex_cc(12345, 1);  -- Vertex ID, Definition ID

-- Recalculate CCs for a definition
SELECT * FROM dh_calculate_ccs_full(1);
```

## Cleanup

To remove all test data:

```sql
TRUNCATE TABLE metadata_graph_cc_computation_log CASCADE;
TRUNCATE TABLE metadata_graph_cc_vertices CASCADE;
TRUNCATE TABLE metadata_graph_cc CASCADE;
TRUNCATE TABLE metadata_graph_cc_definitions CASCADE;
TRUNCATE TABLE metadata_graph_edges CASCADE;
TRUNCATE TABLE metadata_graph_vertices CASCADE;
TRUNCATE TABLE metadata_graph_edge_types CASCADE;
```

Or use the cleanup command provided by the shell script.

## Troubleshooting

### Common Issues

1. **"Graph schema not found"**

   - Run `init_pgrouting_schema.sql` first

2. **"Graph functions not found"**

   - Run `init_pgrouting_functions.sql` first

3. **"pgRouting extension not available"**

   - Install pgRouting extension in PostgreSQL

4. **"Permission denied"**

   - Ensure your user has CREATE, INSERT, and SELECT permissions

5. **"Out of memory"**
   - Reduce the number of edges/vertices in the generation script
   - Increase PostgreSQL work_mem setting

### Performance Tuning

For better performance during generation:

```sql
-- Increase work memory temporarily
SET work_mem = '256MB';

-- Disable autocommit for bulk operations
SET autocommit = OFF;

-- Run generation
\i generate_test_data.sql

-- Re-enable autocommit
COMMIT;
SET autocommit = ON;
```

## Next Steps

After generating test data, you can:

1. **Test Graph Algorithms**: Use pgRouting functions for pathfinding
2. **Analyze Connected Components**: Study the CC structure and properties
3. **Performance Testing**: Benchmark queries and functions
4. **Application Development**: Build applications that consume the graph data
5. **Visualization**: Use tools like Gephi or Neo4j Browser for visualization

## Support

For issues or questions:

- Check the DataHub documentation
- Review PostgreSQL and pgRouting documentation
- Check database logs for detailed error messages
