# Snowplow

## Overview

The Snowplow source extracts metadata from Snowplow's behavioral data platform, including:

- **Event schemas** - Self-describing event definitions with properties and validation rules
- **Entity schemas** - Context and entity schemas attached to events
- **Event specifications** - Tracking requirements and specifications (BDP only)
- **Tracking scenarios** - Groupings of related events (BDP only)
- **Organizations** - Top-level containers for all schemas

Snowplow is an open-source behavioral data platform that collects, validates, and models event-level data. This connector supports both:
- **Snowplow BDP** (Behavioral Data Platform) - Managed Snowplow with Console API
- **Open-source Snowplow** - Self-hosted with Iglu schema registry

## Supported Capabilities

| Capability | Status | Notes |
|------------|--------|-------|
| Platform Instance | ✅ Supported | Group schemas by environment |
| Domains | ✅ Supported | Assign domains to schemas |
| Schema Metadata | ✅ Supported | Extract JSON Schema definitions |
| Descriptions | ✅ Supported | From schema descriptions |
| Lineage | ✅ Supported | Event schemas → Enrichments → Warehouse tables (BDP only) |
| Pipelines & Enrichments | ✅ Supported | Extract data pipelines and enrichment jobs (BDP only) |
| Deletion Detection | ✅ Supported | Via stateful ingestion |

## Prerequisites

### For Snowplow BDP (Managed)

1. **Snowplow BDP account** with Console access
2. **Organization ID** - Found in Console URL: `https://console.snowplowanalytics.com/{org-id}/...`
3. **API credentials** - Generated from Console → Settings → API Credentials:
   - API Key ID
   - API Key Secret

### For Open-Source Snowplow

1. **Iglu Schema Registry** - URL of your Iglu server
2. **API Key** (optional) - Required for private Iglu registries

### Python Requirements

- Python 3.8 or newer
- DataHub CLI installed

## Installation

```bash
# Install DataHub with Snowplow support
pip install 'acryl-datahub[snowplow]'
```

## Required Permissions

### Snowplow BDP API Permissions

The connector requires **read-only access** to the following BDP Console API endpoints:

#### Minimum Required Permissions

To extract basic schema metadata:
- **`read:data-structures`** - Read access to data structures (event and entity schemas)
- **`read:organizations`** - Access to organization information

#### Permissions by Capability

| Capability | Required Permissions | Configuration |
|------------|---------------------|---------------|
| **Schema Metadata** | `read:data-structures` | Enabled by default |
| **Event Specifications** | `read:event-specs` | `extract_event_specifications: true` |
| **Tracking Scenarios** | `read:tracking-scenarios` | `extract_tracking_scenarios: true` |
| **Data Products** | `read:data-products` | `extract_data_products: true` (experimental) |

#### Permission Testing

Test your API credentials and permissions:

```bash
# Get JWT token
curl -X POST \
  -H "X-API-Key-ID: <API_KEY_ID>" \
  -H "X-API-Key: <API_KEY>" \
  https://console.snowplowanalytics.com/api/msc/v1/organizations/<ORG_ID>/credentials/v3/token

# List data structures
curl -H "Authorization: Bearer <JWT>" \
  https://console.snowplowanalytics.com/api/msc/v1/organizations/<ORG_ID>/data-structures/v1
```

### Iglu Registry Permissions

For **open-source Snowplow** with Iglu:
- **Public registries**: No authentication required (e.g., Iglu Central)
- **Private registries**: API key with read access to schemas

## Configuration

See the recipe files for complete configuration examples:
- [snowplow_recipe.yml](snowplow_recipe.yml) - Comprehensive configuration with all options
- [snowplow_bdp_basic.yml](snowplow_bdp_basic.yml) - Minimal BDP configuration
- [snowplow_iglu.yml](snowplow_iglu.yml) - Open-source Iglu configuration
- [snowplow_with_filtering.yml](snowplow_with_filtering.yml) - Schema filtering examples

### Connection Options

#### BDP Console Connection

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `organization_id` | string | ✅ | | Organization UUID from Console URL |
| `api_key_id` | string | ✅ | | API Key ID from Console credentials |
| `api_key` | string | ✅ | | API Key secret |
| `console_api_url` | string | | `https://console.snowplowanalytics.com/api/msc/v1` | BDP Console API base URL |
| `timeout_seconds` | int | | 60 | Request timeout in seconds |
| `max_retries` | int | | 3 | Maximum retry attempts |

#### Iglu Connection

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `iglu_server_url` | string | ✅ | | Iglu server base URL |
| `api_key` | string | | | API key for private Iglu registry (UUID format) |
| `timeout_seconds` | int | | 30 | Request timeout in seconds |

**Note**: Iglu-only mode uses automatic schema discovery via the `/api/schemas` endpoint (requires Iglu Server 0.6+). All schemas in the registry will be automatically discovered.

### Feature Options

| Option | Type | Default | Description | Required Permission |
|--------|------|---------|-------------|---------------------|
| `extract_event_specifications` | bool | true | Extract event specifications | `read:event-specs` |
| `extract_tracking_scenarios` | bool | true | Extract tracking scenarios | `read:tracking-scenarios` |
| `extract_data_products` | bool | false | Extract data products (experimental) | `read:data-products` |
| `extract_pipelines` | bool | true | Extract pipelines as DataFlow entities | `read:pipelines` |
| `extract_enrichments` | bool | true | Extract enrichments as DataJob entities with lineage | `read:enrichments` |
| `include_schema_definitions` | bool | true | Include full JSON Schema in properties | N/A |
| `include_hidden_schemas` | bool | false | Include schemas marked as hidden | N/A |

### Warehouse Lineage Options (Advanced)

⚠️ **Note**: Disabled by default. Prefer warehouse connectors (Snowflake, BigQuery) for column-level lineage.

| Option | Type | Default | Description | Required Permission |
|--------|------|---------|-------------|---------------------|
| `warehouse_lineage.enabled` | bool | false | Extract table-level lineage via Data Models API | `read:data-products` |
| `warehouse_lineage.platform_instance` | string | None | Default platform instance for warehouse URNs | N/A |
| `warehouse_lineage.env` | string | PROD | Default environment for warehouse datasets | N/A |
| `warehouse_lineage.validate_urns` | bool | true | Validate warehouse URNs exist in DataHub | DataHub Graph API access |
| `warehouse_lineage.destination_mappings` | list | [] | Per-destination platform instance overrides | N/A |

### Filtering Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema_pattern` | AllowDenyPattern | Allow all | Filter schemas by vendor/name pattern |
| `event_spec_pattern` | AllowDenyPattern | Allow all | Filter event specifications by name |
| `tracking_scenario_pattern` | AllowDenyPattern | Allow all | Filter tracking scenarios by name |
| `schema_types_to_extract` | list | `["event", "entity"]` | Schema types to extract |

### Stateful Ingestion

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `stateful_ingestion.enabled` | bool | false | Enable stateful ingestion for deletion detection |
| `stateful_ingestion.remove_stale_metadata` | bool | true | Remove schemas that no longer exist |

## Quick Start

### 1. BDP Console (Managed Snowplow)

Create a recipe file `snowplow_recipe.yml`:

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

Run ingestion:

```bash
datahub ingest -c snowplow_recipe.yml
```

### 2. Open-Source Snowplow (Iglu-Only Mode)

For self-hosted Snowplow with Iglu registry (without BDP Console API):

```yaml
source:
  type: snowplow
  config:
    iglu_connection:
      iglu_server_url: "https://iglu.example.com"
      api_key: "${IGLU_API_KEY}"  # Optional for private registries

    schema_types_to_extract:
      - "event"
      - "entity"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

**Important notes for Iglu-only mode**:
- ✅ **Supported**: Event and entity schemas with full JSON Schema definitions
- ✅ **Supported**: Automatic schema discovery via `/api/schemas` endpoint (requires Iglu Server 0.6+)
- ⚠️ **Not supported**: Event specifications (requires BDP API)
- ⚠️ **Not supported**: Tracking scenarios (requires BDP API)
- ⚠️ **Not supported**: Field tagging/PII detection (requires BDP deployment data)

For complete configuration options, see [snowplow_iglu.yml](snowplow_iglu.yml).

### 3. With Warehouse Lineage (BDP Only - Advanced)

⚠️ **Note**: This feature is **disabled by default** and should only be enabled in specific scenarios (see below).

Extract table-level lineage from raw events to derived tables via BDP Data Models API:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "<ORG_UUID>"
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

    # Enable warehouse lineage via Data Models API
    warehouse_lineage:
      enabled: true
      platform_instance: "prod_snowflake"  # Optional
      env: "PROD"  # Optional
      validate_urns: true  # Optional

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

**What this creates**:
- **Table-level lineage**: `atomic.events` → `derived.sessions` (or other derived tables)
- No direct warehouse credentials needed (uses BDP API)

**Supported warehouses**: Snowflake, BigQuery, Redshift, Databricks

#### When to Enable This Feature

**✅ Enable warehouse lineage if**:
- You want quick table-level lineage without configuring warehouse connector
- You don't have access to warehouse query logs
- You want to document Data Models API metadata specifically

**❌ Don't enable if you're using warehouse connectors**:
- **Snowflake connector** provides:
  - Column-level lineage by parsing SQL queries
  - Transformation logic from query history
  - Complete dependency graphs
- **BigQuery, Redshift, Databricks connectors** similarly provide richer lineage

**Best practice**: Use warehouse connector for detailed lineage. Only enable this for quick documentation of Data Models metadata.

**Requirements**: Data Models must be configured in your BDP organization.

## Schema Versioning

Snowplow uses **SchemaVer** (semantic versioning for schemas) with the format `MODEL-REVISION-ADDITION`:

- **MODEL** (first digit): Breaking changes - incompatible with previous versions
- **REVISION** (second digit): Non-breaking changes - additions that are backward compatible
- **ADDITION** (third digit): Adding optional fields without breaking changes

Example: `1-0-2`
- Model: 1 (major version)
- Revision: 0 (no revisions)
- Addition: 2 (two optional field additions)

In DataHub, schemas are represented as:
- **Dataset name**: `{vendor}.{name}.{version}` (e.g., `com.example.page_view.1-0-0`)
- **Schema version**: Tracked in dataset properties

## Entity Types Extracted

### Event Schemas

Self-describing event definitions with:
- Property names and types
- Validation rules (required fields, formats)
- Descriptions
- Schema version history

**DataHub Entity**: Dataset with subtype `snowplow_event_schema`

### Entity Schemas

Context and entity schemas that can be attached to events:
- User properties
- Session context
- Custom entities

**DataHub Entity**: Dataset with subtype `snowplow_entity_schema`

### Event Specifications (BDP Only)

Tracking requirements that define:
- Which events should be tracked
- Associated schemas
- Trigger conditions
- Ownership and status

**DataHub Entity**: Dataset with subtype `snowplow_event_spec`

### Tracking Scenarios (BDP Only)

Logical groupings of related events:
- User journey flows
- Feature tracking groups
- Business process events

**DataHub Entity**: Container with related event specifications

### Organizations

Top-level containers for all schemas and specifications within a BDP organization.

**DataHub Entity**: Container

## Container Hierarchy

```
Organization (Container)
├── Event Schema 1 (Dataset)
├── Event Schema 2 (Dataset)
├── Entity Schema 1 (Dataset)
├── Event Specification 1 (Dataset)
└── Tracking Scenario 1 (Container)
    ├── Event Specification 2 (Dataset)
    └── Event Specification 3 (Dataset)
```

## Troubleshooting

### Authentication Errors

**Error**: `Authentication failed: Invalid API credentials`

**Solution**:
1. Verify `api_key_id` and `api_key` are correct
2. Check credentials are for the correct organization
3. Ensure credentials haven't expired
4. Generate new credentials in BDP Console if needed

**Error**: `Authentication failed: Forbidden`

**Solution**:
- Check `organization_id` matches your credentials
- Verify API key has required permissions
- Contact Snowplow support if permissions are unclear

### Permission Errors

**Error**: `Permission denied for /data-structures`

**Solution**:
- API key missing `read:data-structures` permission
- Generate new credentials with correct permissions in BDP Console → Settings → API Credentials

**Error**: `Permission denied for /event-specs`

**Solution**:
- Set `extract_event_specifications: false` in config, or
- Request `read:event-specs` permission for your API key

### Connection Errors

**Error**: `Request timeout: https://console.snowplowanalytics.com`

**Solution**:
- Check network connectivity to Snowplow Console
- Increase `timeout_seconds` in configuration
- Verify Console URL is correct

**Error**: `Iglu connection failed`

**Solution**:
- Verify `iglu_server_url` is correct and accessible
- For private registries, check `api_key` is valid
- Test connectivity: `curl https://iglu.example.com/api/schemas`

### No Schemas Found

**Issue**: Ingestion completes but no schemas extracted

**Solutions**:
1. **Check filtering patterns**:
   ```yaml
   schema_pattern:
     allow: [".*"]  # Allow all schemas
   ```

2. **Check schema types**:
   ```yaml
   schema_types_to_extract: ["event", "entity"]
   ```

3. **Include hidden schemas**:
   ```yaml
   include_hidden_schemas: true
   ```

4. **Verify schemas exist in BDP Console** or Iglu registry

### Rate Limiting

**Error**: `HTTP 429: Rate limit exceeded`

**Solution**:
- Connector implements automatic retry with exponential backoff
- Rate limits should be handled automatically
- If issues persist, contact Snowplow support to increase limits

## Limitations

1. **BDP-specific features**:
   - Event specifications only available via BDP Console API
   - Tracking scenarios only available via BDP Console API
   - Data products only available via BDP Console API
   - Open-source Iglu users won't have these features

2. **Iglu Server requirements**:
   - Automatic schema discovery requires Iglu Server 0.6+ with `/api/schemas` endpoint
   - Older Iglu implementations may not support the list schemas API

3. **Field tagging in Iglu-only mode**:
   - PII/sensitive field detection requires BDP deployment metadata
   - Not available when using Iglu-only mode

## Advanced Configuration

### Custom Platform Instance

Group schemas by environment:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "<ORG_UUID>"
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

    platform_instance: "production"
    env: "PROD"
```

### Schema Filtering

Extract only specific vendor schemas:

```yaml
source:
  type: snowplow
  config:
    # ... connection config ...

    schema_pattern:
      allow:
        - "com\\.example\\..*"        # Allow com.example schemas
        - "com\\.acme\\.events\\..*"  # Allow com.acme.events schemas
      deny:
        - ".*\\.test$"                # Deny test schemas
```

### Stateful Ingestion

Enable deletion detection:

```yaml
source:
  type: snowplow
  config:
    # ... connection config ...

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
```

## Testing the Connection

Use DataHub's built-in test-connection command:

```bash
datahub check source-connection snowplow \
  --config snowplow_recipe.yml
```

This will:
- Test BDP Console API authentication
- Test Iglu registry connectivity (if configured)
- Verify required permissions
- Report capability availability

## References

- [Snowplow Documentation](https://docs.snowplow.io/)
- [Snowplow BDP Console API](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)
- [SchemaVer Specification](https://docs.snowplow.io/docs/api-reference/iglu/common-architecture/schemaver/)
- [Snowplow GitHub](https://github.com/snowplow/snowplow)

## Support

For issues or questions:
- DataHub Slack: [#troubleshoot](https://datahubproject.io/slack)
- GitHub Issues: [datahub-project/datahub](https://github.com/datahub-project/datahub/issues)
- Snowplow Support: [Snowplow Discourse](https://discourse.snowplow.io/)
