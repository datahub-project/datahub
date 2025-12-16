# Snowplow Integration Tests

This directory contains integration tests for the Snowplow DataHub connector.

## Directory Structure

```
tests/integration/snowplow/
├── test_snowplow.py                    # Main integration tests with golden files
├── test_snowplow_performance.py        # Performance tests for optimizations
│
├── fixtures/                           # Mock API response fixtures
│   ├── data_structures_response.json
│   ├── data_structures_with_ownership.json
│   ├── enrichments_response.json
│   ├── event_specifications_response.json
│   ├── organization_response.json
│   ├── pipelines_response.json
│   └── tracking_scenarios_response.json
│
├── golden_files/                       # Expected test outputs
│   ├── snowplow_data_products_golden.json
│   ├── snowplow_enrichments_golden.json
│   ├── snowplow_event_specs_golden.json
│   ├── snowplow_iglu_autodiscovery_golden.json
│   ├── snowplow_mces_golden.json
│   └── snowplow_pipelines_golden.json
│
├── recipes/                            # Test ingestion recipes
│   ├── snowplow_with_duckdb.yml
│   ├── test_datahub_ui.yml
│   ├── test_event_specs.yml
│   ├── test_iglu_autodiscovery.yml
│   ├── test_mock_bdp.yml
│   ├── test_ownership_recipe.yml
│   ├── test_real_bdp.yml
│   └── test_real_bdp_to_file.yml
│
├── setup/                              # Setup scripts and configurations
│   ├── docker-compose.iglu.yml         # Docker setup for Iglu Server
│   ├── iglu_config.hocon               # Iglu Server configuration
│   ├── mock_bdp_server.py              # Mock BDP API server for testing
│   ├── setup_duckdb.py                 # DuckDB test database setup
│   ├── setup_iglu.py                   # Iglu Server setup script
│   └── snowplow_test.duckdb            # Pre-configured test database
│
└── docs/                               # Documentation
    ├── BDP_API_VALIDATION.md           # BDP API validation documentation
    ├── LOCAL_TEST_SETUP.md             # Guide for local test environment
    ├── OWNERSHIP_TESTING_GUIDE.md      # Ownership extraction testing guide
    ├── REAL_BDP_TESTING_GUIDE.md       # Testing with real BDP environment
    ├── SETUP_VERIFICATION.md           # Setup verification checklist
    └── SWAGGER_VALIDATION_REPORT.md    # API Swagger validation report
```

## Running Tests

### Run all integration tests:
```bash
cd /path/to/datahub/metadata-ingestion
python -m pytest tests/integration/snowplow/ -v
```

### Run specific test file:
```bash
python -m pytest tests/integration/snowplow/test_snowplow.py -v
python -m pytest tests/integration/snowplow/test_snowplow_performance.py -v
```

### Run performance tests only:
```bash
python -m pytest tests/integration/snowplow/test_snowplow_performance.py -v
```

## Test Categories

### Main Integration Tests (`test_snowplow.py`)
- **test_snowplow_ingest**: Basic data structure ingestion with mocked API
- **test_snowplow_event_specs_and_tracking_scenarios**: Event specifications and tracking scenarios
- **test_snowplow_data_products**: Data products extraction
- **test_snowplow_pipelines**: Pipelines as DataFlow entities
- **test_snowplow_enrichments**: Enrichments as DataJob entities with lineage
- **test_snowplow_iglu_autodiscovery**: Iglu-only mode (requires Docker)
- **test_snowplow_config_validation**: Configuration validation

### Performance Tests (`test_snowplow_performance.py`)
- **test_parallel_fetching_performance**: Parallel vs sequential deployment fetching
- **test_caching_reduces_api_calls**: Instance-level caching effectiveness
- **test_event_schema_urn_caching**: URN extraction caching
- **test_large_dataset_performance**: Performance with 1000 schemas
- **test_api_call_count_without_field_tracking**: API call optimization

## Test Dependencies

### Required for all tests:
- pytest
- freezegun (for time mocking)
- datahub ingestion framework

### Required for Iglu tests only:
- Docker and Docker Compose
- Iglu Server (started via `setup/docker-compose.iglu.yml`)

### Setup Iglu Server for Iglu tests:
```bash
cd tests/integration/snowplow/setup
docker compose -f docker-compose.iglu.yml up -d
python setup_iglu.py
```

### Teardown Iglu Server:
```bash
cd tests/integration/snowplow/setup
docker compose -f docker-compose.iglu.yml down -v
```

## Adding New Tests

1. **Add test fixtures** to `fixtures/` (if mocking API responses)
2. **Add golden files** to `golden_files/` (expected outputs)
3. **Add test recipes** to `recipes/` (if testing via recipe files)
4. **Write test function** in appropriate test file
5. **Update this README** with test description

## Performance Test Expectations

- **Parallel fetching**: 3x+ faster than sequential (with 10 workers)
- **Caching**: Should reduce API calls by 66%+ for repeated calls
- **URN caching**: 10x+ faster on subsequent calls
- **Large datasets**: <5s for 1000 schemas with 20 concurrent workers
