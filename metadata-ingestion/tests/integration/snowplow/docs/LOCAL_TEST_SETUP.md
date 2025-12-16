# Snowplow Local Test Setup (Option B)

This directory contains everything needed to run Snowplow integration tests locally without a real Snowplow BDP account.

## Overview

**Option B** provides a lightweight local development environment using:
- **Mock API responses** from JSON fixtures
- **DuckDB database** for warehouse event data
- **Optional mock BDP server** for comprehensive testing

## Quick Start

### 1. Run Integration Tests (Mocked)

The simplest approach - no setup needed:

```bash
# From metadata-ingestion directory
pytest tests/integration/snowplow/test_snowplow.py -v
```

This uses mocked API responses from `fixtures/` - no external dependencies.

### 2. Setup DuckDB for Warehouse Testing

If you want to test warehouse lineage with real data:

```bash
cd tests/integration/snowplow

# Create DuckDB database with sample events
python setup_duckdb.py --event-count 100

# Verify database
duckdb snowplow_test.duckdb "SELECT COUNT(*) FROM snowplow.events"
```

This creates `snowplow_test.duckdb` with:
- `snowplow.events` table (atomic events)
- 100 sample events (checkout_started, product_viewed)
- User context entities
- Realistic timestamp distribution

### 3. Run Mock BDP API Server (Optional)

For end-to-end testing with real HTTP calls:

```bash
# Terminal 1: Start mock server
python mock_bdp_server.py --port 8081

# Terminal 2: Test against mock server
export SNOWPLOW_ORG_ID="test-org-uuid"
export SNOWPLOW_API_KEY_ID="test-key-id"
export SNOWPLOW_API_KEY="test-secret"

datahub ingest -c snowplow_local_test_recipe.yml
```

## Files Overview

```
tests/integration/snowplow/
├── fixtures/
│   ├── data_structures_response.json           # Original simple fixtures
│   └── data_structures_with_ownership.json     # Enhanced with deployments
├── setup_duckdb.py                             # DuckDB database setup
├── mock_bdp_server.py                          # Mock BDP API server
├── test_snowplow.py                            # Integration tests
├── snowplow_to_file.yml                        # Test recipe
└── LOCAL_TEST_SETUP.md                         # This file
```

## Detailed Setup Instructions

### DuckDB Setup

**Purpose**: Create a local warehouse with Snowplow event data for testing lineage.

**Create Database:**
```bash
python setup_duckdb.py --db-path snowplow_test.duckdb --event-count 100
```

**Options:**
- `--db-path`: Database file path (default: `snowplow_test.duckdb`)
- `--event-count`: Number of sample events (default: 100)
- `--recreate`: Delete and recreate database

**Database Schema:**
```sql
snowplow.events (
    -- Base Snowplow columns
    app_id, platform, collector_tstamp, event, event_id,
    user_id, user_ipaddress,

    -- Page context
    page_url, page_title, page_referrer,

    -- Device context
    br_name, br_family, os_name, os_family,

    -- Geo context (enrichments)
    geo_country, geo_region, geo_city, geo_zipcode,

    -- Custom event contexts (JSON)
    contexts_com_acme_checkout_started_1,
    contexts_com_acme_product_viewed_1,
    contexts_com_acme_user_context_1,

    -- Unstruct events (JSON)
    unstruct_event_com_acme_checkout_started_1,
    unstruct_event_com_acme_product_viewed_1,

    -- Timestamps
    derived_tstamp, load_tstamp
)
```

**Query Examples:**
```bash
# Count events by type
duckdb snowplow_test.duckdb "SELECT event, COUNT(*) FROM snowplow.events GROUP BY event"

# View checkout events
duckdb snowplow_test.duckdb "SELECT event_id, user_id, unstruct_event_com_acme_checkout_started_1 FROM snowplow.events WHERE event = 'unstruct' LIMIT 5"

# Check user context
duckdb snowplow_test.duckdb "SELECT user_id, contexts_com_acme_user_context_1 FROM snowplow.events LIMIT 5"
```

### Mock BDP Server Setup

**Purpose**: Run a local HTTP server that mimics Snowplow BDP Console API.

**Start Server:**
```bash
python mock_bdp_server.py --port 8081 --debug
```

**Endpoints:**
- `GET /` - API documentation
- `GET /health` - Health check
- `GET /organizations/{orgId}/credentials/v3/token` - JWT token generation
- `GET /organizations/{orgId}/data-structures/v1` - List data structures
- `GET /organizations/{orgId}/data-structures/v1/{hash}` - Get data structure by hash
- `GET /organizations/{orgId}/data-products/v1` - List data products

**Authentication:**
```bash
# These credentials work with the mock server
X-Api-Key-Id: test-key-id
X-Api-Key: test-secret
```

**Test Server:**
```bash
# Health check
curl http://localhost:8081/health

# Get token
curl -H "X-Api-Key-Id: test-key-id" -H "X-Api-Key: test-secret" \
  http://localhost:8081/organizations/test-org-uuid/credentials/v3/token

# List data structures
curl -H "Authorization: Bearer mock_jwt_token_12345" \
  http://localhost:8081/organizations/test-org-uuid/data-structures/v1
```

## Testing Scenarios

### Scenario 1: Unit Tests Only (Fastest)

```bash
# Run all tests with mocked responses
pytest tests/integration/snowplow/test_snowplow.py -v
```

**Use case**: Quick validation during development

### Scenario 2: DuckDB Warehouse Testing

```bash
# Setup database
python tests/integration/snowplow/setup_duckdb.py --event-count 500

# Run ingestion with warehouse lineage
datahub ingest -c tests/integration/snowplow/snowplow_with_warehouse.yml
```

**Use case**: Test warehouse lineage extraction

### Scenario 3: Full Mock BDP Environment

```bash
# Terminal 1: Start mock server
cd tests/integration/snowplow
python mock_bdp_server.py --port 8081

# Terminal 2: Setup DuckDB
python setup_duckdb.py --event-count 100

# Terminal 3: Run ingestion
export SNOWPLOW_ORG_ID="test-org-uuid"
export SNOWPLOW_API_KEY_ID="test-key-id"
export SNOWPLOW_API_KEY="test-secret"
datahub ingest -c tests/integration/snowplow/snowplow_full_local.yml
```

**Use case**: End-to-end testing with real HTTP calls

## Fixtures Reference

### data_structures_response.json

Simple fixture for basic testing:
- 2 schemas: `page_view` (event), `user_context` (entity)
- No ownership data
- Basic field definitions

### data_structures_with_ownership.json

Enhanced fixture with ownership:
- 3 schemas: `checkout_started`, `product_viewed`, `user_context`
- Includes `deployments` array with `initiator` (for ownership)
- Multiple schema versions (1-0-0, 1-1-0) for evolution testing
- Custom metadata in `meta.customData`

**Ownership Extraction:**
- `createdBy`: Oldest deployment's `initiator`
- `modifiedBy`: Newest deployment's `initiator`
- Field authorship: Derived from version history

## Customizing Test Data

### Add New Schema Fixture

Edit `fixtures/data_structures_with_ownership.json`:

```json
{
  "data": [
    {
      "hash": "new_schema_hash",
      "organizationId": "test-org-uuid",
      "vendor": "com.acme",
      "name": "new_event",
      "format": "jsonschema",
      "description": "New event schema",
      "meta": {
        "hidden": false,
        "schemaType": "event",
        "customData": {"team": "new-team"}
      },
      "deployments": [
        {
          "version": "1-0-0",
          "initiator": "developer@company.com",
          "ts": "2024-03-01T10:00:00Z"
        }
      ],
      "data": {
        "self": {
          "vendor": "com.acme",
          "name": "new_event",
          "version": "1-0-0"
        },
        "properties": {
          "field1": {"type": "string"}
        }
      }
    }
  ]
}
```

### Modify DuckDB Event Data

Edit `setup_duckdb.py`:
- Change `generate_sample_events()` function
- Add new event types
- Modify field distributions
- Change event counts or time ranges

### Add Mock API Endpoints

Edit `mock_bdp_server.py`:
- Add new Flask routes
- Create new fixture files
- Implement new API behaviors

## Troubleshooting

### DuckDB Database Not Found

```bash
# Ensure you're in the right directory
cd tests/integration/snowplow

# Create database
python setup_duckdb.py
```

### Mock Server Connection Refused

```bash
# Check if server is running
curl http://localhost:8081/health

# Check port not in use
lsof -i :8081

# Start with different port
python mock_bdp_server.py --port 8082
```

### Fixture File Not Found

```bash
# Verify fixtures exist
ls -la fixtures/

# Re-create missing fixture (should be committed in git)
git checkout fixtures/data_structures_with_ownership.json
```

### Integration Test Failures

```bash
# Update golden file if output changed intentionally
pytest tests/integration/snowplow/test_snowplow.py --update-golden-files

# View detailed diff
pytest tests/integration/snowplow/test_snowplow.py -vv
```

## Next Steps

1. **Run basic tests**: `pytest tests/integration/snowplow/ -v`
2. **Setup DuckDB**: `python setup_duckdb.py` (for warehouse testing)
3. **Start mock server**: `python mock_bdp_server.py` (for HTTP testing)
4. **Customize fixtures**: Edit JSON files to match your test scenarios
5. **Run full ingestion**: Test against local environment

## Related Documentation

- `SNOWPLOW_TEST_ENVIRONMENT_SETUP.md` - Comprehensive setup guide (Option A + B)
- `SNOWPLOW_PLANNING.md` - Overall implementation plan
- `test_snowplow.py` - Integration test implementation
- DataHub Testing Guide: `../../README.md`

## Comparison: Option A vs Option B

| Feature | Option A (BDP Cloud) | Option B (Local) |
|---------|----------------------|------------------|
| Setup Time | 1-2 hours | 5 minutes |
| External Dependencies | BDP account, Warehouse | None |
| Cost | Requires paid/trial account | Free |
| Realism | Production-like API | Mocked responses |
| Offline Testing | ❌ No | ✅ Yes |
| Best For | Final validation | Development & CI |

## Summary

Option B provides a fast, self-contained testing environment ideal for:
- ✅ Rapid development iterations
- ✅ CI/CD pipelines
- ✅ Offline development
- ✅ Deterministic test results
- ✅ No external account required

Use **Option A** (real BDP) for final validation before production deployment.
