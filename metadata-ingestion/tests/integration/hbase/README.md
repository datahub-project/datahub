# HBase Integration Tests

This directory contains integration tests for the HBase source connector.

**Note**: These tests use **mocked HBase connections** (no real HBase infrastructure required). They test the integration logic and data flow, while unit tests in `tests/unit/test_hbase_*.py` validate individual components.

## Overview

The integration tests use `unittest.mock` to simulate HBase connections via the `happybase` library. This approach:

- Runs quickly (~0.2 seconds for all 9 tests)
- Requires no external infrastructure or Docker
- Tests the integration logic between DataHub SDK and HBase source
- Validates end-to-end data flow with mocked data

For testing against a real HBase cluster, manual verification is required.

## Prerequisites

- Python packages: `pytest`
- No HBase installation or Docker required

## Running Tests

```bash
# All integration tests (runs in ~0.2 seconds)
PYTHONPATH=src pytest tests/integration/hbase/ -v

# Specific test
PYTHONPATH=src pytest tests/integration/hbase/test_hbase_integration.py::TestHBaseIntegration::test_connection_to_hbase -v

# With verbose output
PYTHONPATH=src pytest tests/integration/hbase/ -v -s
```

## Test Coverage

All tests use mocked HBase connections and validate:

1. **test_connection_to_hbase** - Connection initialization with correct parameters
2. **test_get_namespaces** - Namespace extraction from table names
3. **test_get_tables_in_default_namespace** - Table discovery in default namespace
4. **test_get_table_descriptor** - Column family metadata extraction
5. **test_full_ingestion** - Complete ingestion workflow with containers and datasets
6. **test_ingestion_with_namespace_filter** - Namespace pattern filtering
7. **test_ingestion_with_table_filter** - Table pattern filtering
8. **test_schema_extraction** - Schema generation from column families
9. **test_custom_properties** - Custom property population

## Test Data

The mock fixture provides:

- **Tables:** test_table1 (cf1, cf2), test_table2 (info, data)
- **Namespace:** default (extracted from table names)
- **Column Families:** Various configurations with MAX_VERSIONS properties

## Testing Against Real HBase

For testing against a real HBase cluster:

1. Set up HBase with Thrift server enabled on port 9090
2. Manually test connection:

```bash
python -c "import happybase; conn = happybase.Connection('localhost', 9090); print(conn.tables()); conn.close()"
```

3. Run the source with a test config:

```bash
datahub ingest -c your_hbase_config.yml
```

## Architecture

The tests follow DataHub's integration test patterns (similar to Fivetran, etc.):

- Use `@pytest.fixture` with `mock.patch` to mock external dependencies
- Test integration logic without requiring real infrastructure
- Unit tests (50 tests in `tests/unit/`) provide component-level validation
- Integration tests (9 tests here) validate end-to-end data flow
