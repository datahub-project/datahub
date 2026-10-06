# ThoughtSpot Integration Tests

## Overview

Integration tests for the ThoughtSpot DataHub connector using mocked REST API v2.0 responses.

Since ThoughtSpot is a cloud service without a Docker image, these tests mock the API endpoints to validate the complete extraction pipeline.

## Test Files

- `test_thoughtspot.py` - Main test file with comprehensive test coverage
- `thoughtspot_mces_golden.json` - Golden file with expected metadata output (24KB, 52 events)
- `thoughtspot_recipe.yml` - Sample ingestion recipe configuration
- `__init__.py` - Package marker

## Running Tests

### Prerequisites

The tests require the DataHub development environment with these dependencies:

- `pytest`
- `requests-mock` (for API mocking)
- `time-machine` (for deterministic timestamps)

### Run All Integration Tests

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion
source venv/bin/activate
PYTHONPATH=/Users/treff7es/shadow/datahub-thoughtspot/metadata-ingestion/src:$PYTHONPATH \
pytest /Users/treff7es/shadow/datahub-thoughtspot/metadata-ingestion/tests/integration/thoughtspot/ -v
```

### Run Specific Tests

```bash
# Golden file validation
pytest tests/integration/thoughtspot/test_thoughtspot.py::test_thoughtspot_ingest -v

# Pattern filtering
pytest tests/integration/thoughtspot/test_thoughtspot.py::test_thoughtspot_ingest_with_filters -v

# Connection tests
pytest tests/integration/thoughtspot/test_thoughtspot.py::test_thoughtspot_connection_success -v
```

### Update Golden File

When connector implementation changes, regenerate the golden file:

```bash
pytest tests/integration/thoughtspot/test_thoughtspot.py::test_thoughtspot_ingest --update-golden-files -v
```

## Test Coverage

### Entity Types Tested

- **Containers (Workspaces)**: 2 workspaces with metadata and ownership
- **Dashboards (Liveboards)**: 2 liveboards linked to workspaces
- **Charts (Answers)**: 2 saved visualizations with metadata
- **Datasets**: 3 datasets (2 worksheets + 1 table) with proper subtypes

### Aspects Verified

- `containerProperties` - Workspace metadata
- `dashboardInfo` - Dashboard titles, descriptions
- `chartInfo` - Chart metadata
- `datasetProperties` - Dataset metadata
- `dataPlatformInstance` - Platform instance linkage (ALWAYS required)
- `status` - Entity status
- `subTypes` - Entity subtypes (Folder, Dashboard, Chart, View, Table)
- `ownership` - Owner extraction from API
- `container` - Parent-child relationships

### Connection Testing

- ✅ Valid credentials (bearer token)
- ✅ Invalid token (401 Unauthorized)
- ✅ Insufficient permissions (403 Forbidden)
- ✅ Wrong URL (connection failure)

## Golden File Metrics

- **Total MCPs**: 52 events
- **File size**: 24KB (exceeds 5KB requirement)
- **Entities**: 2 containers, 2 dashboards, 2 charts, 3 datasets
- **Deterministic**: Uses `@time_machine.travel()` for consistent output

## Test Approach

### Why Mocked API Responses?

ThoughtSpot is a cloud-only service without Docker infrastructure. The test strategy:

1. **Mock REST API v2.0 endpoints** with realistic response structures
2. **Validate full extraction pipeline**: API client → source → MCPs
3. **Test error handling**: Authentication, permissions, connection failures
4. **Ensure determinism**: Fixed responses, frozen timestamps

This approach follows the PowerBI connector pattern for API-based sources.

### When to Use Real API

These tests can be enhanced to run against a real ThoughtSpot test instance when:

- Test credentials are available
- Additional API endpoints are implemented (lineage, usage stats)
- Need to validate against production API response structure changes

## Test Data Structure

### Mocked API Responses

The `mock_thoughtspot_api` fixture provides responses for:

1. **System Config** (`/system/config`) - Connection test endpoint
2. **Workspaces** (`/metadata/search` with `type=TAG`) - List workspaces
3. **Liveboards** (`/metadata/search` with `type=LIVEBOARD`) - List dashboards
4. **Answers** (`/metadata/search` with `type=ANSWER`) - List charts
5. **Logical Tables** (`/metadata/search` with `type=LOGICAL_TABLE`) - List datasets

### Response Structure

Each mocked entity includes:

- `id` - Unique identifier
- `name` - Display name
- `description` - Entity description
- `author` - Owner information (id, name, email)
- `created` / `modified` - Timestamps
- `metadata_type` - Entity type
- `type` - Subtype (for datasets: WORKSHEET or ONE_TO_ONE_LOGICAL)

## Expected Output

The golden file contains MCPs for:

- 2 workspace containers with ownership
- 2 liveboards with full dashboard metadata
- 2 answers with chart metadata
- 3 datasets (2 worksheets, 1 table) with proper subtypes
- All entities linked to parent containers
- All entities have dataPlatformInstance, status, subTypes aspects

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'datahub.metadata'`:

- Ensure you're using the DataHub repo's venv, not the isolated repo's venv
- Set `PYTHONPATH` to include the ThoughtSpot source directory

### Test Failures

If golden file comparison fails:

1. Check if connector implementation changed
2. Regenerate golden file with `--update-golden-files`
3. Review diff to ensure changes are expected

### Mocking Issues

If API mocks aren't working:

- Verify `requests-mock` is installed
- Check that base URL in test matches mock registration
- Ensure request body filters match in `metadata_search_callback`
