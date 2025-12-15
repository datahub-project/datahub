# Snowplow DataHub Connector

This directory contains the Snowplow source connector for DataHub.

## Overview

The Snowplow connector extracts metadata from Snowplow's behavioral data platform, including:
- Event schemas (self-describing event definitions)
- Entity schemas (context and entity schemas)
- Event specifications (BDP only)
- Tracking scenarios (BDP only)
- Organizations (as containers)

## Architecture

The connector supports multiple deployment modes:
1. **BDP Mode** - Managed Snowplow with Console API
2. **Iglu Mode** - Open-source Snowplow with Iglu registry

## File Structure

```
snowplow/
├── __init__.py                  # Module exports
├── snowplow.py                  # Main source implementation (~550 lines)
├── snowplow_config.py           # Configuration classes (~270 lines)
├── snowplow_client.py           # BDP Console API client (~320 lines)
├── iglu_client.py               # Iglu Registry API client (~180 lines)
├── snowplow_models.py           # Pydantic models for API responses (~220 lines)
├── schema_parser.py             # JSON Schema → DataHub conversion (~220 lines)
├── snowplow_report.py           # Custom report class (~150 lines)
├── _API_ENDPOINTS.md            # API documentation with links
└── README.md                    # This file
```

## Key Components

### 1. Configuration (`snowplow_config.py`)

Two connection types:
- `SnowplowBDPConnectionConfig` - BDP Console API
- `IgluConnectionConfig` - Iglu Schema Registry

Main config: `SnowplowSourceConfig`

### 2. API Clients

**BDP Client** (`snowplow_client.py`):
- v3 authentication (API Key → JWT)
- Data structures endpoint
- Event specifications endpoint
- Tracking scenarios endpoint
- Automatic retry with exponential backoff

**Iglu Client** (`iglu_client.py`):
- Automatic schema discovery via `/api/schemas` endpoint
- Schema retrieval by vendor/name/version
- Optional authentication for private registries
- Validation service

### 3. Schema Parser (`schema_parser.py`)

Converts JSON Schema to DataHub schema format:
- Type mapping (string, integer, boolean, array, etc.)
- Format handling (date-time, email, uuid, etc.)
- Enum types
- Nullable fields
- SchemaVer parsing (MODEL-REVISION-ADDITION)

### 4. Main Source (`snowplow.py`)

Entry point with extraction logic:
- Organization containers
- Schema extraction (event and entity)
- Event specifications extraction
- Tracking scenarios extraction
- Pattern-based filtering
- Stateful ingestion support

## Testing

### Unit Tests

Location: `tests/unit/snowplow/`

- `test_snowplow_config.py` - Configuration validation (~300 lines)
- `test_schema_parser.py` - Schema parsing logic (~200 lines)

Run with:
```bash
pytest tests/unit/snowplow/
```

### Integration Tests

Location: `tests/integration/snowplow/`

**BDP Mode Tests**:
- `test_snowplow.py` - End-to-end test with golden files
- `fixtures/` - Mocked API responses
- `snowplow_mces_golden.json` - Expected output

**Iglu-Only Mode Tests** (Docker-based):
- `docker-compose.iglu.yml` - Iglu Server + PostgreSQL setup
- `setup_iglu.py` - Script to populate test schemas
- `test_iglu_autodiscovery.yml` - Ingestion recipe for Iglu-only mode
- `snowplow_iglu_autodiscovery_golden.json` - Expected output
- `test_snowplow.py::test_snowplow_iglu_autodiscovery` - Pytest integration test

Run with:
```bash
# BDP mode tests (mocked API)
pytest tests/integration/snowplow/test_snowplow.py::test_snowplow_ingest --update-golden-files
pytest tests/integration/snowplow/test_snowplow.py::test_snowplow_ingest

# Iglu-only mode tests (requires Docker)
cd tests/integration/snowplow
docker compose -f docker-compose.iglu.yml up -d
python setup_iglu.py
pytest tests/integration/snowplow/test_snowplow.py::test_snowplow_iglu_autodiscovery
docker compose -f docker-compose.iglu.yml down -v
```

## Documentation

Location: `docs/sources/snowplow/`

- `snowplow.md` - Complete user guide
- `snowplow_recipe.yml` - Comprehensive configuration reference
- `snowplow_bdp_basic.yml` - Basic BDP example
- `snowplow_iglu.yml` - Open-source Iglu-only mode with automatic discovery
- `snowplow_with_filtering.yml` - Filtering patterns
- `snowplow_with_stateful.yml` - Stateful ingestion

## Installation

From DataHub repository root:

```bash
cd metadata-ingestion
pip install -e ".[snowplow]"
```

## Usage

### Basic BDP Example

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "<ORG_UUID>"
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

Run:
```bash
datahub ingest -c snowplow_recipe.yml
```

### Iglu-Only Mode (Open-Source Snowplow)

For open-source Snowplow deployments without BDP Console API, you can extract schemas directly from Iglu Schema Registry using automatic discovery.

```yaml
source:
  type: snowplow
  config:
    # Iglu Schema Registry connection
    iglu_connection:
      iglu_server_url: "http://localhost:8081"
      # Optional: API key for private registries
      # api_key: "${IGLU_API_KEY}"

    # Schema types to extract
    schema_types_to_extract:
      - "event"
      - "entity"

    env: "PROD"
    platform_instance: "my_snowplow"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

**Important Notes for Iglu-Only Mode**:
- ✅ Extracts event and entity schemas with full JSON Schema definitions
- ✅ **Automatic schema discovery** via `/api/schemas` endpoint (requires Iglu Server 0.6+)
- ✅ Works with any Iglu Server supporting the list schemas endpoint
- ⚠️ No enrichment extraction (requires BDP API)
- ⚠️ No warehouse lineage (requires BDP Destinations API)
- ⚠️ No field tagging/PII detection (requires deployment data from BDP)

Run:
```bash
datahub ingest -c snowplow_iglu_recipe.yml
```

## Features Implemented

✅ **Core Extraction**:
- Event schemas with full JSON Schema definitions
- Entity schemas
- Schema metadata (properties, types, validation)
- Container hierarchy (organizations)

✅ **BDP Features**:
- Event specifications
- Tracking scenarios
- Custom metadata tags
- Pipelines and enrichments as DataFlow/DataJob entities

✅ **Lineage**:
- Event schemas → Enrichments → atomic.events table
- atomic.events → Data Models → derived tables (via Data Models API, disabled by default)
- Field-level lineage for specific enrichments (IP Lookup, UA Parser, etc.)

**Note**: Warehouse lineage (atomic.events → derived tables) is **disabled by default** because warehouse connectors (Snowflake, BigQuery) provide better lineage with column-level detail and SQL transformation logic. Only enable for quick table-level lineage documentation.

✅ **Configuration**:
- Multiple connection types (BDP, Iglu, Hybrid)
- Pattern-based filtering
- Schema type selection
- Hidden schema handling
- Stateful ingestion
- Iglu-only mode with automatic schema discovery

✅ **Error Handling**:
- JWT token auto-refresh
- Retry with exponential backoff
- Comprehensive error reporting
- API permission validation

✅ **Quality**:
- Type-safe Pydantic models
- Unit tests for all major components
- Integration tests with golden files
- Complete documentation
- Registered in setup.py

## Future Enhancements

🚧 **Enhanced Column-level Lineage**:
- Detailed field-level lineage from schemas to warehouse columns

🚧 **dbt Integration**:
- Lineage from warehouse tables to dbt models

## Development

### Running Tests Locally

```bash
# Unit tests
pytest tests/unit/snowplow/ -v

# Integration tests
pytest tests/integration/snowplow/ -v

# With coverage
pytest tests/unit/snowplow/ tests/integration/snowplow/ --cov=datahub.ingestion.source.snowplow
```

### Testing with Local DataHub

1. Start DataHub:
   ```bash
   datahub docker quickstart
   ```

2. Create test recipe (use real credentials):
   ```yaml
   source:
     type: snowplow
     config:
       bdp_connection:
         organization_id: "<YOUR_ORG_UUID>"
         api_key_id: "${SNOWPLOW_API_KEY_ID}"
         api_key: "${SNOWPLOW_API_KEY}"

   sink:
     type: datahub-rest
     config:
       server: "http://localhost:8080"
   ```

3. Run ingestion:
   ```bash
   datahub ingest -c recipe.yml
   ```

4. Verify in UI: http://localhost:9002

### Testing Iglu-Only Mode with Docker

For testing open-source Snowplow / Iglu-only mode locally:

1. **Start Iglu Server with Docker Compose**:
   ```bash
   cd tests/integration/snowplow
   docker compose -f docker-compose.iglu.yml up -d
   ```

   This starts:
   - PostgreSQL database (port 5433)
   - Iglu Server (port 8081)

2. **Populate with test schemas**:
   ```bash
   python setup_iglu.py
   ```

   This uploads 3 test schemas:
   - `com.test.event/page_view/jsonschema/1-0-0`
   - `com.test.event/checkout_started/jsonschema/1-0-0`
   - `com.test.context/user_context/jsonschema/1-0-0`

3. **Run test ingestion**:
   ```bash
   datahub ingest -c test_iglu_only.yml
   ```

4. **Verify output**: Check `snowplow_iglu_only_golden.json` for extracted metadata

5. **Clean up**:
   ```bash
   docker compose -f docker-compose.iglu.yml down -v
   ```

**Docker Compose Configuration Details**:
- Uses `snowplow/iglu-server:0.12.0` image
- Separate database initialization step (`iglu-setup` service)
- Super API key: `12345678-1234-1234-1234-123456789012` (test only)
- Accepts Limited Use License for testing purposes

### Debugging

Enable debug logging:
```bash
export DATAHUB_DEBUG=1
datahub ingest -c recipe.yml
```

Or in recipe:
```yaml
source:
  type: snowplow
  config:
    # ... connection config ...

  # Enable debug logging
  debug: true
```

## References

- [Snowplow Documentation](https://docs.snowplow.io/)
- [BDP Console API](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)
- [DataHub Documentation](https://datahubproject.io/docs/)

## Support

For issues or questions:
- DataHub Slack: [#troubleshoot](https://datahubproject.io/slack)
- GitHub Issues: [datahub-project/datahub](https://github.com/datahub-project/datahub/issues)
