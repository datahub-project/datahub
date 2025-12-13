# Recording/Replay Integration Tests

This directory contains integration tests for the DataHub ingestion recording/replay system.

## Test Suites

### 1. PostgreSQL Recording Test (`test_postgres_recording.py`)

Tests database recording with a real PostgreSQL instance.

**What it validates:**

- SQL query recording via DB-API cursor proxy
- Cursor iteration and result handling
- Datetime serialization/deserialization
- Air-gapped replay without network
- MCP semantic equivalence

**Run:**

```bash
# Docker lifecycle is handled automatically by docker_compose_runner
pytest tests/integration/recording/test_postgres_recording.py -v -s
```

**Test data:**

- 3 schemas (sales, hr, analytics)
- 5 tables with relationships
- 3 views with joins
- ~380 rows of data
- Generates ~20+ SQL queries during ingestion

---

### 2. Manual Recording/Replay Test Script (`test_recording_workflow.sh`)

Reusable bash script for manual testing with any recipe (Snowflake, Databricks, etc.).

**Usage:**

```bash
# Test with any recipe file
./tests/integration/recording/test_recording_workflow.sh <recipe.yaml> [password]

# Example with Snowflake
./tests/integration/recording/test_recording_workflow.sh my_snowflake_recipe.yaml test123
```

**What it does:**

1. Records ingestion run using the provided recipe
2. Replays in air-gapped mode
3. Validates MCPs are semantically identical using metadata-diff
4. Reports results with colored output

**Note:** Requires actual cloud credentials. Best for manual validation or CI with credentials configured.

---

## Running All Tests

```bash
# Run automated docker-based test
pytest tests/integration/recording/test_postgres_recording.py -v -s

# Run manual test with your own recipe
./tests/integration/recording/test_recording_workflow.sh my_recipe.yaml test123
```

**Note:** The `docker_compose_runner` fixture automatically handles:

- Starting containers with `docker-compose up`
- Waiting for healthchecks to pass
- Cleaning up with `docker-compose down -v` after tests

## CI/CD Integration

**Automated tests:**

- PostgreSQL test: ~3-5 seconds total
- Uses docker-compose for isolation
- No external credentials required
- Automatic cleanup on test completion or failure

**Manual tests:**

- Use `test_recording_workflow.sh` for sources requiring credentials
- Can be run in CI with environment variables configured
- Validates complete record → replay → MCP validation workflow
