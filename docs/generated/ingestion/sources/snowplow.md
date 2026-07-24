


# Snowplow

## Overview

Snowplow is a streaming or integration platform. Learn more in the [official Snowplow documentation](https://snowplow.io/).

The DataHub integration for Snowplow covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `snowplow`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Enabled by default from schema descriptions. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via configuration. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default for event and entity schemas. |
| Table-Level Lineage | ✅ | Optionally enabled via warehouse_lineage.enabled configuration (requires BDP). |

### Overview

The `snowplow` module ingests metadata from Snowplow into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The Snowplow source extracts metadata from Snowplow's behavioral data platform, including:

- **Event schemas** - Self-describing event definitions with properties and validation rules
- **Entity schemas** - Context and entity schemas attached to events
- **Event specifications** - Tracking requirements and specifications (BDP only)
- **Tracking scenarios** - Groupings of related events (BDP only)
- **Organizations** - Top-level containers for all schemas

Snowplow is an open-source behavioral data platform that collects, validates, and models event-level data. This connector supports both:

- **Snowplow BDP** (Behavioral Data Platform) - Managed Snowplow with Console API
- **Open-source Snowplow** - Self-hosted with Iglu schema registry

#### References

- [Snowplow Documentation](https://docs.snowplow.io/)
- [Snowplow BDP Console API](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)
- [SchemaVer Specification](https://docs.snowplow.io/docs/api-reference/iglu/common-architecture/schemaver/)
- [Snowplow GitHub](https://github.com/snowplow/snowplow)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### For Snowplow BDP (Managed)

1. **Snowplow BDP account** with Console access
2. **Organization ID** - Found in Console URL: `https://console.snowplowanalytics.com/{org-id}/...`
3. **API credentials** - Generated from Console → Settings → API Credentials:
   - API Key ID
   - API Key Secret

#### For Open-Source Snowplow

1. **Iglu Schema Registry** - URL of your Iglu server
2. **API Key** (optional) - Required for private Iglu registries

#### Python Requirements

- Python 3.8 or newer
- DataHub CLI installed

#### Snowplow BDP API Permissions

The connector requires **read-only access** to the following BDP Console API endpoints:

##### Minimum Required Permissions

To extract basic schema metadata:

- **`read:data-structures`** - Read access to data structures (event and entity schemas)
- **`read:organizations`** - Access to organization information

##### Permissions by Capability

| Capability               | Required Permissions      | Configuration                        |
| ------------------------ | ------------------------- | ------------------------------------ |
| **Schema Metadata**      | `read:data-structures`    | Enabled by default                   |
| **Event Specifications** | `read:event-specs`        | `extract_event_specifications: true` |
| **Tracking Scenarios**   | `read:tracking-scenarios` | `extract_tracking_scenarios: true`   |
| **Tracking Plans**       | `read:data-products`      | `extract_tracking_plans: true`       |

##### Permission Testing

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

#### Iglu Registry Permissions

For **open-source Snowplow** with Iglu:

- **Public registries**: No authentication required (e.g., Iglu Central)
- **Private registries**: API key with read access to schemas

#### Configuration

See the recipe files for complete configuration examples:

- [snowplow_recipe.yml](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowplow_recipe.yml) - Comprehensive configuration with all options
- [snowplow_bdp_basic.yml](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowplow_bdp_basic.yml) - Minimal BDP configuration
- [snowplow_iglu.yml](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowplow_iglu.yml) - Open-source Iglu configuration
- [snowplow_with_filtering.yml](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowplow_with_filtering.yml) - Schema filtering examples


### Install the Plugin
```shell
pip install 'acryl-datahub[snowplow]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Snowplow Comprehensive Recipe
# This recipe demonstrates ALL available configuration options with detailed comments

source:
  type: snowplow
  config:
    # ============================================
    # Connection Configuration
    # ============================================

    # BDP Console API Connection (for managed Snowplow)
    # Required for: Event specifications, tracking scenarios, data products
    # Optional: Can be omitted if using Iglu-only mode
    bdp_connection:
      # Organization UUID - found in BDP Console URL
      # Example: https://console.snowplowanalytics.com/{org_id}/data-structures
      organization_id: "<YOUR_ORG_UUID>"

      # API credentials from BDP Console → Settings → API Credentials
      # Use environment variables for security
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

      # Optional: BDP Console API base URL (default shown)
      # Only change if using a custom/regional Snowplow deployment
      console_api_url: "https://console.snowplowanalytics.com/api/msc/v1"

      # Optional: Request timeout in seconds (default: 60)
      timeout_seconds: 60

      # Optional: Maximum retry attempts for failed requests (default: 3)
      max_retries: 3

    # Iglu Schema Registry Connection (for open-source Snowplow)
    # Required for: Open-source Snowplow deployments without BDP Console API
    # Note: Either bdp_connection OR iglu_connection is required (not both)
    iglu_connection:
      # Iglu server base URL
      # Examples:
      #   - Public: http://iglucentral.com
      #   - Private: https://iglu.example.com
      iglu_server_url: "https://iglu.example.com"

      # Optional: API key for private Iglu registries (UUID format)
      # Not required for public registries like Iglu Central
      api_key: "${IGLU_API_KEY}"

      # Optional: Request timeout in seconds (default: 30)
      timeout_seconds: 30

    # ============================================
    # Filtering Configuration
    # ============================================

    # Filter schemas by vendor/name pattern
    # Pattern format: "vendor/name" (e.g., "com.example/page_view")
    # Requires permission: read:data-structures
    schema_pattern:
      allow:
        - ".*"  # Allow all schemas (default)
        # Examples of allow patterns:
        # - "com\\.example\\..*"              # Allow all com.example schemas
        # - "com\\.acme\\.events\\..*"        # Allow com.acme.events schemas
        # - "com\\.snowplowanalytics\\..*"    # Allow Snowplow standard schemas
      deny:
        # - ".*\\.test$"      # Deny schemas ending with .test
        # - ".*_sandbox.*"    # Deny sandbox schemas
        # - "com\\.example\\.deprecated\\..*"  # Deny deprecated schemas

    # Filter event specifications by name
    # Only applies when extract_event_specifications is enabled
    # Requires permission: read:event-specs
    event_spec_pattern:
      allow:
        - ".*"  # Allow all event specifications (default)
      deny: []

    # Filter tracking scenarios by name
    # Only applies when extract_tracking_scenarios is enabled
    # Requires permission: read:tracking-scenarios
    tracking_scenario_pattern:
      allow:
        - ".*"  # Allow all tracking scenarios (default)
      deny: []

    # ============================================
    # Feature Flags
    # ============================================

    # Extract event specifications (BDP only)
    # Requires permission: read:event-specs
    # Default: true
    extract_event_specifications: true

    # Extract tracking scenarios (BDP only)
    # Requires permission: read:tracking-scenarios
    # Default: true
    extract_tracking_scenarios: true

    # Include full JSON Schema definition in dataset properties
    # Useful for downstream schema analysis
    # Default: true
    include_schema_definitions: true

    # Include schemas marked as hidden in BDP Console
    # Default: false
    include_hidden_schemas: false

    # ============================================
    # Warehouse Lineage (BDP only - Advanced)
    # ============================================
    # Extract TABLE-LEVEL lineage from atomic.events to derived tables via Data Models API
    # Creates lineage: atomic.events → derived tables (e.g., derived.sessions)
    #
    # ⚠️ IMPORTANT: Disabled by default
    # Warehouse connectors (Snowflake, BigQuery, etc.) provide BETTER lineage:
    # - Column-level lineage (not just table-level)
    # - Transformation logic from actual SQL queries
    # - Complete dependency graphs
    #
    # Only enable this if:
    # - You want quick table-level lineage without setting up warehouse connector
    # - You don't have access to warehouse query logs
    # - You want to document Data Models API metadata specifically
    warehouse_lineage:
      # Enable warehouse lineage extraction (default: false)
      # Disabled by default - prefer using warehouse connector for detailed lineage
      enabled: false

      # Optional: Default platform instance for warehouse URNs
      # Example: "prod_snowflake", "prod_bigquery"
      # Can be overridden per destination using destination_mappings
      platform_instance: "prod_snowflake"

      # Optional: Default environment for warehouse datasets (default: PROD)
      env: "PROD"

      # Optional: Per-destination mappings (overrides defaults for specific destinations)
      destination_mappings:
        # Example: Override platform instance for specific destination
        # - destination_id: "12345678-1234-1234-1234-123456789012"
        #   platform_instance: "staging_snowflake"
        #   env: "DEV"

      # Optional: Validate warehouse URNs exist in DataHub before creating lineage
      # Requires DataHub Graph API access (default: true)
      validate_urns: true

    # ============================================
    # Schema Extraction Options
    # ============================================

    # Schema types to extract
    # Options: "event" and/or "entity"
    # Default: ["event", "entity"]
    schema_types_to_extract:
      - "event"   # Event schemas (self-describing events)
      - "entity"  # Entity schemas (contexts and entities)

    # ============================================
    # Platform Instance (Optional)
    # ============================================

    # Platform instance identifier for multi-environment deployments
    # Groups schemas by environment (e.g., production, staging, dev)
    # Uncomment to enable:
    # platform_instance: "production"

    # ============================================
    # Environment (Optional)
    # ============================================

    # Environment tag (PROD, DEV, QA, etc.)
    # Uncomment to enable:
    # env: "PROD"

    # ============================================
    # Stateful Ingestion (Optional)
    # ============================================

    # Enable stateful ingestion for deletion detection
    # Tracks which schemas have been seen and removes stale ones
    # Requires permission: read:data-structures (to track existence)
    stateful_ingestion:
      enabled: false
      remove_stale_metadata: true  # Remove schemas that no longer exist

# ============================================
# Sink Configuration
# ============================================

sink:
  type: datahub-rest
  config:
    # DataHub GMS server URL
    server: "http://localhost:8080"

    # Optional: Authentication token
    # token: "${DATAHUB_TOKEN}"

    # Optional: Timeout for REST requests (default: 30s)
    # timeout_sec: 30

    # Optional: Extra headers
    # extra_headers:
    #   X-Custom-Header: "value"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">deployed_since</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Only extract schemas deployed/updated since this timestamp (ISO 8601 format: 2025-12-15T00:00:00Z). Enables incremental ingestion by filtering based on deployment timestamps. Leave empty to fetch all schemas. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">deployment_environment</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Filter schemas by Snowplow deployment environment. Matches the `env` field on a data structure deployment (e.g. `PROD`, `DEV`). Only schemas with a matching deployment will be ingested; schemas with no deployment info (e.g. drafts) are included. Case-insensitive. When not set, schemas from all environments are included. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">enrichment_owner</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default owner for enrichments (e.g., 'data-platform@company.com'). Applied as DATAOWNER to all enrichment DataJobs. Leave empty to skip enrichment ownership. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extract_enrichments</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract enrichments as DataJob entities linked to pipelines (requires BDP connection) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_event_specifications</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract event specifications (requires BDP connection) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract pipelines as DataFlow entities (requires BDP connection) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_standard_schemas</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Snowplow standard schemas from Iglu Central that are referenced by event specifications. Standard schemas (vendor: com.snowplowanalytics.*) are not in the Data Structures API but are publicly available. When enabled, creates dataset entities for standard schemas and completes lineage from event specs. Only fetches schemas that are actually referenced, not all standard schemas. Disable if you don't want to fetch from Iglu Central. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_tracking_plans</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract tracking plans (requires BDP connection) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">iglu_central_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Iglu Central base URL for fetching Snowplow standard schemas <div className="default-line default-line-with-docs">Default: <span className="default-value">http://iglucentral.com</span></div> |
| <div className="path-line"><span className="path-main">include_hidden_schemas</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include schemas marked as hidden in BDP Console <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_version_in_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include version in dataset URN (legacy behavior). When False (recommended), version is stored in dataset properties instead. Set to True for backwards compatibility with existing metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">pipeline_label</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Filter Snowplow pipelines by their `label` field in the BDP API. The label is free-form text set by the operator (commonly the deployment env, e.g. `prod` or `dev`, but not enforced by Snowplow). Only pipelines matching this label will be ingested; pipelines with no label are included by default. Case-insensitive. When not set, all pipelines are included. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of schemas to fetch per API page (default: 100). Adjust based on organization size and API performance. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">bdp_connection</span></div> <div className="type-name-line"><span className="type-name">One of SnowplowBDPConnectionConfig, null</span></div> | BDP Console API connection (required for BDP mode) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">api_key</span>&nbsp;<abbr title="Required if bdp_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | API Key secret from BDP Console credentials  |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">api_key_id</span>&nbsp;<abbr title="Required if bdp_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | API Key ID from BDP Console credentials  |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">organization_id</span>&nbsp;<abbr title="Required if bdp_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Organization UUID (found in BDP Console URL)  |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">console_api_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | BDP Console API base URL <div className="default-line default-line-with-docs">Default: <span className="default-value">https://console.snowplowanalytics.com/api/msc/v1</span></div> |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retry attempts for failed requests <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-prefix">bdp_connection.</span><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Request timeout in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">event_spec_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">event_spec_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">event_spec_statuses</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Filter event specifications by status. Only event specs with a status in this list will be ingested. Valid values: draft, published, deprecated, archived. When not set, all statuses are included. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">event_spec_statuses.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">field_tagging</span></div> <div className="type-name-line"><span className="type-name">FieldTaggingConfig</span></div> | Configuration for auto-tagging schema fields.  |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">authorship_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for authorship tags. Use {author} placeholder. <div className="default-line default-line-with-docs">Default: <span className="default-value">added&#95;by&#95;&#123;author&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">emit_tags_and_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit both tags and structured properties for fields. When True, both tags and structured properties are emitted. When False, only the method specified by use_structured_properties is used. Useful during migration from tags to structured properties. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable automatic field tagging <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">event_type_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for event type tags. Use {name} placeholder. <div className="default-line default-line-with-docs">Default: <span className="default-value">snowplow&#95;event&#95;&#123;name&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">pii_tags_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When emit_tags_and_structured_properties is true, only emit tags for PII/sensitive data classification. Version, authorship, and event type will only be in structured properties, not as tags. Useful when you want detailed structured properties but only highlight PII fields with tags. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">schema_version_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for schema version tags. Use {version} placeholder. <div className="default-line default-line-with-docs">Default: <span className="default-value">snowplow&#95;schema&#95;v&#123;version&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">tag_authorship</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag fields with authorship (e.g., added_by_ryan_smith) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">tag_data_class</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag fields with data classification (e.g., PII, Sensitive) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">tag_event_type</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag fields with event type (e.g., snowplow_event_checkout) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">tag_schema_version</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Tag fields with schema version (e.g., snowplow_schema_v1-0-0) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">track_field_versions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Track which version each field was added in. When enabled, compares schema versions to determine when fields were introduced. Tags fields with their introduction version and adds 'Added in version X' to descriptions. Disabled by default as it requires fetching all schema versions (slower ingestion). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">use_pii_enrichment</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract PII fields from PII Pseudonymization enrichment config <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">use_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use structured properties for field metadata instead of (or in addition to) tags. Structured properties provide strongly-typed metadata with better querying capabilities. When enabled, field authorship, version, timestamp, and classification are emitted as structured properties on the schemaField entity. Note: Requires structured property definitions to be registered in DataHub first. See snowplow_field_structured_properties.yaml in the connector directory. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">pii_field_patterns</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Field name patterns to classify as PII (fallback if enrichment not available)  |
| <div className="path-line"><span className="path-prefix">field_tagging.pii_field_patterns.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">field_tagging.</span><span className="path-main">sensitive_field_patterns</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Field name patterns to classify as Sensitive  |
| <div className="path-line"><span className="path-prefix">field_tagging.sensitive_field_patterns.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">iglu_connection</span></div> <div className="type-name-line"><span className="type-name">One of IgluConnectionConfig, null</span></div> | Iglu Schema Registry connection (required for Iglu mode, optional for BDP mode as fallback) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">iglu_connection.</span><span className="path-main">iglu_server_url</span>&nbsp;<abbr title="Required if iglu_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Iglu server base URL (e.g., 'https://iglu.acme.com' or 'http://iglucentral.com')  |
| <div className="path-line"><span className="path-prefix">iglu_connection.</span><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | API key for private Iglu registry (UUID format) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">iglu_connection.</span><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Request timeout in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">performance</span></div> <div className="type-name-line"><span className="type-name">PerformanceConfig</span></div> | Performance and scaling configuration. <br />  <br /> Controls parallel processing, caching, and limits for large-scale deployments.  |
| <div className="path-line"><span className="path-prefix">performance.</span><span className="path-main">enable_parallel_fetching</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable parallel fetching of schema deployments. Significantly speeds up ingestion when field version tracking is enabled. Disable for debugging or if API rate limits are strict. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">performance.</span><span className="path-main">max_concurrent_api_calls</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum concurrent API calls for deployment fetching. Increase for faster ingestion of large organizations with many schemas. Recommended: 5-20 depending on API rate limits. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_types_to_extract</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Schema types to extract: 'event' and/or 'entity'  |
| <div className="path-line"><span className="path-prefix">schema_types_to_extract.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">tracking_plan_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">tracking_plan_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_lineage</span></div> <div className="type-name-line"><span className="type-name">WarehouseLineageConfig</span></div> | Configuration for extracting lineage to warehouse destinations. <br />  <br /> This feature creates table-level lineage from atomic.events to derived tables <br /> by querying the Snowplow BDP Data Models API. <br />  <br /> IMPORTANT: Disabled by default because warehouse connectors (Snowflake, BigQuery, etc.) <br /> provide more detailed lineage by parsing actual SQL queries, including: <br /> - Column-level lineage <br /> - Transformation logic <br /> - Complete dependency graphs <br />  <br /> Only enable this if: <br /> - You want quick table-level lineage without setting up warehouse connector <br /> - You don't have access to warehouse query logs <br /> - You want to document Data Models API metadata specifically  |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable warehouse lineage extraction via data models API. Disabled by default - prefer using warehouse connector (Snowflake, BigQuery) for detailed lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default platform instance prefix for warehouse URNs (e.g., 'prod_snowflake'). Applied globally unless overridden by destination_mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.</span><span className="path-main">validate_urns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Validate that warehouse table URNs exist in DataHub before creating lineage. Requires DataHub Graph API access. Set to False to skip validation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Default environment for warehouse datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.</span><span className="path-main">destination_mappings</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Per-destination platform instance mappings. Overrides global platform_instance for specific destinations.  |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.destination_mappings.</span><span className="path-main">DestinationMapping</span></div> <div className="type-name-line"><span className="type-name">DestinationMapping</span></div> | Mapping configuration for a Snowplow warehouse destination.  |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.destination_mappings.DestinationMapping.</span><span className="path-main">destination_id</span>&nbsp;<abbr title="Required if DestinationMapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowplow destination UUID from data models  |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.destination_mappings.DestinationMapping.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance to prepend to dataset name in URN (e.g., 'prod_snowflake') <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">warehouse_lineage.destination_mappings.DestinationMapping.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment for warehouse datasets (e.g., PROD, DEV) <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for deletion detection <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "DestinationMapping": {
      "additionalProperties": false,
      "description": "Mapping configuration for a Snowplow warehouse destination.",
      "properties": {
        "destination_id": {
          "description": "Snowplow destination UUID from data models",
          "title": "Destination Id",
          "type": "string"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Platform instance to prepend to dataset name in URN (e.g., 'prod_snowflake')",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "Environment for warehouse datasets (e.g., PROD, DEV)",
          "title": "Env",
          "type": "string"
        }
      },
      "required": [
        "destination_id"
      ],
      "title": "DestinationMapping",
      "type": "object"
    },
    "FieldTaggingConfig": {
      "additionalProperties": false,
      "description": "Configuration for auto-tagging schema fields.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Enable automatic field tagging",
          "title": "Enabled",
          "type": "boolean"
        },
        "tag_schema_version": {
          "default": true,
          "description": "Tag fields with schema version (e.g., snowplow_schema_v1-0-0)",
          "title": "Tag Schema Version",
          "type": "boolean"
        },
        "tag_event_type": {
          "default": true,
          "description": "Tag fields with event type (e.g., snowplow_event_checkout)",
          "title": "Tag Event Type",
          "type": "boolean"
        },
        "tag_data_class": {
          "default": true,
          "description": "Tag fields with data classification (e.g., PII, Sensitive)",
          "title": "Tag Data Class",
          "type": "boolean"
        },
        "tag_authorship": {
          "default": true,
          "description": "Tag fields with authorship (e.g., added_by_ryan_smith)",
          "title": "Tag Authorship",
          "type": "boolean"
        },
        "track_field_versions": {
          "default": false,
          "description": "Track which version each field was added in. When enabled, compares schema versions to determine when fields were introduced. Tags fields with their introduction version and adds 'Added in version X' to descriptions. Disabled by default as it requires fetching all schema versions (slower ingestion).",
          "title": "Track Field Versions",
          "type": "boolean"
        },
        "use_structured_properties": {
          "default": true,
          "description": "Use structured properties for field metadata instead of (or in addition to) tags. Structured properties provide strongly-typed metadata with better querying capabilities. When enabled, field authorship, version, timestamp, and classification are emitted as structured properties on the schemaField entity. Note: Requires structured property definitions to be registered in DataHub first. See snowplow_field_structured_properties.yaml in the connector directory.",
          "title": "Use Structured Properties",
          "type": "boolean"
        },
        "emit_tags_and_structured_properties": {
          "default": false,
          "description": "Emit both tags and structured properties for fields. When True, both tags and structured properties are emitted. When False, only the method specified by use_structured_properties is used. Useful during migration from tags to structured properties.",
          "title": "Emit Tags And Structured Properties",
          "type": "boolean"
        },
        "pii_tags_only": {
          "default": false,
          "description": "When emit_tags_and_structured_properties is true, only emit tags for PII/sensitive data classification. Version, authorship, and event type will only be in structured properties, not as tags. Useful when you want detailed structured properties but only highlight PII fields with tags.",
          "title": "Pii Tags Only",
          "type": "boolean"
        },
        "schema_version_pattern": {
          "default": "snowplow_schema_v{version}",
          "description": "Pattern for schema version tags. Use {version} placeholder.",
          "title": "Schema Version Pattern",
          "type": "string"
        },
        "event_type_pattern": {
          "default": "snowplow_event_{name}",
          "description": "Pattern for event type tags. Use {name} placeholder.",
          "title": "Event Type Pattern",
          "type": "string"
        },
        "authorship_pattern": {
          "default": "added_by_{author}",
          "description": "Pattern for authorship tags. Use {author} placeholder.",
          "title": "Authorship Pattern",
          "type": "string"
        },
        "use_pii_enrichment": {
          "default": true,
          "description": "Extract PII fields from PII Pseudonymization enrichment config",
          "title": "Use Pii Enrichment",
          "type": "boolean"
        },
        "pii_field_patterns": {
          "description": "Field name patterns to classify as PII (fallback if enrichment not available)",
          "items": {
            "type": "string"
          },
          "title": "Pii Field Patterns",
          "type": "array"
        },
        "sensitive_field_patterns": {
          "description": "Field name patterns to classify as Sensitive",
          "items": {
            "type": "string"
          },
          "title": "Sensitive Field Patterns",
          "type": "array"
        }
      },
      "title": "FieldTaggingConfig",
      "type": "object"
    },
    "IgluConnectionConfig": {
      "additionalProperties": false,
      "description": "Connection configuration for Iglu Schema Registry.\n\nUse this for open-source Snowplow deployments or as fallback for BDP.",
      "properties": {
        "iglu_server_url": {
          "description": "Iglu server base URL (e.g., 'https://iglu.acme.com' or 'http://iglucentral.com')",
          "title": "Iglu Server Url",
          "type": "string"
        },
        "api_key": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "API key for private Iglu registry (UUID format)",
          "title": "Api Key"
        },
        "timeout_seconds": {
          "default": 30,
          "description": "Request timeout in seconds",
          "title": "Timeout Seconds",
          "type": "integer"
        }
      },
      "required": [
        "iglu_server_url"
      ],
      "title": "IgluConnectionConfig",
      "type": "object"
    },
    "PerformanceConfig": {
      "additionalProperties": false,
      "description": "Performance and scaling configuration.\n\nControls parallel processing, caching, and limits for large-scale deployments.",
      "properties": {
        "max_concurrent_api_calls": {
          "default": 10,
          "description": "Maximum concurrent API calls for deployment fetching. Increase for faster ingestion of large organizations with many schemas. Recommended: 5-20 depending on API rate limits.",
          "title": "Max Concurrent Api Calls",
          "type": "integer"
        },
        "enable_parallel_fetching": {
          "default": true,
          "description": "Enable parallel fetching of schema deployments. Significantly speeds up ingestion when field version tracking is enabled. Disable for debugging or if API rate limits are strict.",
          "title": "Enable Parallel Fetching",
          "type": "boolean"
        }
      },
      "title": "PerformanceConfig",
      "type": "object"
    },
    "SnowplowBDPConnectionConfig": {
      "additionalProperties": false,
      "description": "Connection configuration for Snowplow BDP (Behavioral Data Platform).\n\nUse this for managed Snowplow deployments with Console API access.",
      "properties": {
        "organization_id": {
          "description": "Organization UUID (found in BDP Console URL)",
          "title": "Organization Id",
          "type": "string"
        },
        "api_key_id": {
          "description": "API Key ID from BDP Console credentials",
          "title": "Api Key Id",
          "type": "string"
        },
        "api_key": {
          "description": "API Key secret from BDP Console credentials",
          "format": "password",
          "title": "Api Key",
          "type": "string",
          "writeOnly": true
        },
        "console_api_url": {
          "default": "https://console.snowplowanalytics.com/api/msc/v1",
          "description": "BDP Console API base URL",
          "title": "Console Api Url",
          "type": "string"
        },
        "timeout_seconds": {
          "default": 60,
          "description": "Request timeout in seconds",
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "max_retries": {
          "default": 3,
          "description": "Maximum number of retry attempts for failed requests",
          "title": "Max Retries",
          "type": "integer"
        }
      },
      "required": [
        "organization_id",
        "api_key_id",
        "api_key"
      ],
      "title": "SnowplowBDPConnectionConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    },
    "WarehouseLineageConfig": {
      "additionalProperties": false,
      "description": "Configuration for extracting lineage to warehouse destinations.\n\nThis feature creates table-level lineage from atomic.events to derived tables\nby querying the Snowplow BDP Data Models API.\n\nIMPORTANT: Disabled by default because warehouse connectors (Snowflake, BigQuery, etc.)\nprovide more detailed lineage by parsing actual SQL queries, including:\n- Column-level lineage\n- Transformation logic\n- Complete dependency graphs\n\nOnly enable this if:\n- You want quick table-level lineage without setting up warehouse connector\n- You don't have access to warehouse query logs\n- You want to document Data Models API metadata specifically",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Enable warehouse lineage extraction via data models API. Disabled by default - prefer using warehouse connector (Snowflake, BigQuery) for detailed lineage.",
          "title": "Enabled",
          "type": "boolean"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default platform instance prefix for warehouse URNs (e.g., 'prod_snowflake'). Applied globally unless overridden by destination_mappings.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "Default environment for warehouse datasets",
          "title": "Env",
          "type": "string"
        },
        "destination_mappings": {
          "description": "Per-destination platform instance mappings. Overrides global platform_instance for specific destinations.",
          "items": {
            "$ref": "#/$defs/DestinationMapping"
          },
          "title": "Destination Mappings",
          "type": "array"
        },
        "validate_urns": {
          "default": true,
          "description": "Validate that warehouse table URNs exist in DataHub before creating lineage. Requires DataHub Graph API access. Set to False to skip validation.",
          "title": "Validate Urns",
          "type": "boolean"
        }
      },
      "title": "WarehouseLineageConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for Snowplow source.\n\nSupports two modes:\n1. BDP mode: Uses Snowplow BDP Console API (requires bdp_connection)\n2. Iglu mode: Uses Iglu Schema Registry only (requires iglu_connection)",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration for deletion detection"
    },
    "bdp_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/SnowplowBDPConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "BDP Console API connection (required for BDP mode)"
    },
    "iglu_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/IgluConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Iglu Schema Registry connection (required for Iglu mode, optional for BDP mode as fallback)"
    },
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter (vendor/name format)"
    },
    "event_spec_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for event specifications to filter"
    },
    "tracking_plan_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tracking plans to filter"
    },
    "deployment_environment": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Filter schemas by Snowplow deployment environment. Matches the `env` field on a data structure deployment (e.g. `PROD`, `DEV`). Only schemas with a matching deployment will be ingested; schemas with no deployment info (e.g. drafts) are included. Case-insensitive. When not set, schemas from all environments are included.",
      "title": "Deployment Environment"
    },
    "pipeline_label": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Filter Snowplow pipelines by their `label` field in the BDP API. The label is free-form text set by the operator (commonly the deployment env, e.g. `prod` or `dev`, but not enforced by Snowplow). Only pipelines matching this label will be ingested; pipelines with no label are included by default. Case-insensitive. When not set, all pipelines are included.",
      "title": "Pipeline Label"
    },
    "event_spec_statuses": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Filter event specifications by status. Only event specs with a status in this list will be ingested. Valid values: draft, published, deprecated, archived. When not set, all statuses are included.",
      "title": "Event Spec Statuses"
    },
    "extract_event_specifications": {
      "default": true,
      "description": "Extract event specifications (requires BDP connection)",
      "title": "Extract Event Specifications",
      "type": "boolean"
    },
    "extract_tracking_plans": {
      "default": true,
      "description": "Extract tracking plans (requires BDP connection)",
      "title": "Extract Tracking Plans",
      "type": "boolean"
    },
    "extract_pipelines": {
      "default": true,
      "description": "Extract pipelines as DataFlow entities (requires BDP connection)",
      "title": "Extract Pipelines",
      "type": "boolean"
    },
    "extract_enrichments": {
      "default": true,
      "description": "Extract enrichments as DataJob entities linked to pipelines (requires BDP connection)",
      "title": "Extract Enrichments",
      "type": "boolean"
    },
    "enrichment_owner": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Default owner for enrichments (e.g., 'data-platform@company.com'). Applied as DATAOWNER to all enrichment DataJobs. Leave empty to skip enrichment ownership.",
      "title": "Enrichment Owner"
    },
    "include_hidden_schemas": {
      "default": false,
      "description": "Include schemas marked as hidden in BDP Console",
      "title": "Include Hidden Schemas",
      "type": "boolean"
    },
    "include_version_in_urn": {
      "default": false,
      "description": "Include version in dataset URN (legacy behavior). When False (recommended), version is stored in dataset properties instead. Set to True for backwards compatibility with existing metadata.",
      "title": "Include Version In Urn",
      "type": "boolean"
    },
    "schema_types_to_extract": {
      "description": "Schema types to extract: 'event' and/or 'entity'",
      "items": {
        "type": "string"
      },
      "title": "Schema Types To Extract",
      "type": "array"
    },
    "deployed_since": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Only extract schemas deployed/updated since this timestamp (ISO 8601 format: 2025-12-15T00:00:00Z). Enables incremental ingestion by filtering based on deployment timestamps. Leave empty to fetch all schemas.",
      "title": "Deployed Since"
    },
    "schema_page_size": {
      "default": 100,
      "description": "Number of schemas to fetch per API page (default: 100). Adjust based on organization size and API performance.",
      "title": "Schema Page Size",
      "type": "integer"
    },
    "extract_standard_schemas": {
      "default": true,
      "description": "Extract Snowplow standard schemas from Iglu Central that are referenced by event specifications. Standard schemas (vendor: com.snowplowanalytics.*) are not in the Data Structures API but are publicly available. When enabled, creates dataset entities for standard schemas and completes lineage from event specs. Only fetches schemas that are actually referenced, not all standard schemas. Disable if you don't want to fetch from Iglu Central.",
      "title": "Extract Standard Schemas",
      "type": "boolean"
    },
    "iglu_central_url": {
      "default": "http://iglucentral.com",
      "description": "Iglu Central base URL for fetching Snowplow standard schemas",
      "title": "Iglu Central Url",
      "type": "string"
    },
    "field_tagging": {
      "$ref": "#/$defs/FieldTaggingConfig",
      "description": "Field tagging configuration for auto-tagging schema fields"
    },
    "warehouse_lineage": {
      "$ref": "#/$defs/WarehouseLineageConfig",
      "description": "Warehouse lineage configuration for linking enrichment outputs to warehouse tables"
    },
    "performance": {
      "$ref": "#/$defs/PerformanceConfig",
      "description": "Performance and scaling configuration for large deployments"
    }
  },
  "title": "SnowplowSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connection Options

##### BDP Console Connection

| Option            | Type   | Required | Default                                            | Description                         |
| ----------------- | ------ | -------- | -------------------------------------------------- | ----------------------------------- |
| `organization_id` | string | ✅       |                                                    | Organization UUID from Console URL  |
| `api_key_id`      | string | ✅       |                                                    | API Key ID from Console credentials |
| `api_key`         | string | ✅       |                                                    | API Key secret                      |
| `console_api_url` | string |          | `https://console.snowplowanalytics.com/api/msc/v1` | BDP Console API base URL            |
| `timeout_seconds` | int    |          | 60                                                 | Request timeout in seconds          |
| `max_retries`     | int    |          | 3                                                  | Maximum retry attempts              |

##### Iglu Connection

| Option            | Type   | Required | Default | Description                                     |
| ----------------- | ------ | -------- | ------- | ----------------------------------------------- |
| `iglu_server_url` | string | ✅       |         | Iglu server base URL                            |
| `api_key`         | string |          |         | API key for private Iglu registry (UUID format) |
| `timeout_seconds` | int    |          | 30      | Request timeout in seconds                      |

**Note**: Iglu-only mode uses automatic schema discovery via the `/api/schemas` endpoint (requires Iglu Server 0.6+). All schemas in the registry will be automatically discovered.

#### Feature Options

| Option                         | Type   | Default                | Description                                          | Required Permission       |
| ------------------------------ | ------ | ---------------------- | ---------------------------------------------------- | ------------------------- |
| `extract_event_specifications` | bool   | true                   | Extract event specifications                         | `read:event-specs`        |
| `extract_tracking_scenarios`   | bool   | true                   | Extract tracking scenarios                           | `read:tracking-scenarios` |
| `extract_tracking_plans`       | bool   | true                   | Extract tracking plans                               | `read:data-products`      |
| `extract_pipelines`            | bool   | true                   | Extract pipelines as DataFlow entities               | `read:pipelines`          |
| `extract_enrichments`          | bool   | true                   | Extract enrichments as DataJob entities with lineage | `read:enrichments`        |
| `enrichment_owner`             | string | None                   | Default owner email for enrichment DataJobs          | N/A                       |
| `include_hidden_schemas`       | bool   | false                  | Include schemas marked as hidden                     | N/A                       |
| `include_version_in_urn`       | bool   | false                  | Include version in dataset URN (legacy behavior)     | N/A                       |
| `extract_standard_schemas`     | bool   | true                   | Extract Snowplow standard schemas from Iglu Central  | N/A                       |
| `iglu_central_url`             | string | http://iglucentral.com | URL for fetching standard schemas                    | N/A                       |

#### Schema Extraction Options

| Option                    | Type   | Default               | Description                                                 |
| ------------------------- | ------ | --------------------- | ----------------------------------------------------------- |
| `schema_types_to_extract` | list   | `["event", "entity"]` | Schema types to extract                                     |
| `deployed_since`          | string | None                  | Only extract schemas deployed since this ISO 8601 timestamp |
| `schema_page_size`        | int    | 100                   | Number of schemas per API page                              |

#### Warehouse Lineage Options (Advanced)

⚠️ **Note**: Disabled by default. Prefer warehouse connectors (Snowflake, BigQuery) for column-level lineage.

| Option                                   | Type   | Default | Description                                     | Required Permission      |
| ---------------------------------------- | ------ | ------- | ----------------------------------------------- | ------------------------ |
| `warehouse_lineage.enabled`              | bool   | false   | Extract table-level lineage via Data Models API | `read:data-products`     |
| `warehouse_lineage.platform_instance`    | string | None    | Default platform instance for warehouse URNs    | N/A                      |
| `warehouse_lineage.env`                  | string | PROD    | Default environment for warehouse datasets      | N/A                      |
| `warehouse_lineage.validate_urns`        | bool   | true    | Validate warehouse URNs exist in DataHub        | DataHub Graph API access |
| `warehouse_lineage.destination_mappings` | list   | []      | Per-destination platform instance overrides     | N/A                      |

#### Field Tagging Options

| Option                                              | Type | Default | Description                                             |
| --------------------------------------------------- | ---- | ------- | ------------------------------------------------------- |
| `field_tagging.enabled`                             | bool | true    | Enable automatic field tagging                          |
| `field_tagging.tag_schema_version`                  | bool | true    | Tag fields with schema version                          |
| `field_tagging.tag_event_type`                      | bool | true    | Tag fields with event type                              |
| `field_tagging.tag_data_class`                      | bool | true    | Tag fields with data classification (PII, Sensitive)    |
| `field_tagging.tag_authorship`                      | bool | true    | Tag fields with authorship info                         |
| `field_tagging.track_field_versions`                | bool | false   | Track which version each field was added in             |
| `field_tagging.use_structured_properties`           | bool | true    | Use structured properties instead of tags               |
| `field_tagging.emit_tags_and_structured_properties` | bool | false   | Emit both tags and structured properties                |
| `field_tagging.pii_tags_only`                       | bool | false   | Only emit tags for PII fields when using both           |
| `field_tagging.use_pii_enrichment`                  | bool | true    | Extract PII fields from PII Pseudonymization enrichment |

#### Performance Options

| Option                                 | Type | Default | Description                                          |
| -------------------------------------- | ---- | ------- | ---------------------------------------------------- |
| `performance.max_concurrent_api_calls` | int  | 10      | Maximum concurrent API calls for deployment fetching |
| `performance.enable_parallel_fetching` | bool | true    | Enable parallel fetching of schema deployments       |

#### Filtering Options

| Option                      | Type             | Default   | Description                           |
| --------------------------- | ---------------- | --------- | ------------------------------------- |
| `schema_pattern`            | AllowDenyPattern | Allow all | Filter schemas by vendor/name pattern |
| `event_spec_pattern`        | AllowDenyPattern | Allow all | Filter event specifications by name   |
| `tracking_scenario_pattern` | AllowDenyPattern | Allow all | Filter tracking scenarios by name     |
| `tracking_plan_pattern`     | AllowDenyPattern | Allow all | Filter tracking plans by name         |

#### Stateful Ingestion

| Option                                     | Type | Default | Description                                      |
| ------------------------------------------ | ---- | ------- | ------------------------------------------------ |
| `stateful_ingestion.enabled`               | bool | false   | Enable stateful ingestion for deletion detection |
| `stateful_ingestion.remove_stale_metadata` | bool | true    | Remove schemas that no longer exist              |

#### Quick Start

#### 1. BDP Console (Managed Snowplow)

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

#### 2. Open-Source Snowplow (Iglu-Only Mode)

For self-hosted Snowplow with Iglu registry (without BDP Console API):

```yaml
source:
  type: snowplow
  config:
    iglu_connection:
      iglu_server_url: "https://iglu.example.com"
      api_key: "${IGLU_API_KEY}" # Optional for private registries

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

For complete configuration options, see [snowplow_iglu.yml](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowplow_iglu.yml).

#### 3. With Warehouse Lineage (BDP Only - Advanced)

⚠️ **Note**: This feature is **disabled by default** and should only be enabled in specific scenarios (see below).

Start from the baseline BDP recipe in [1. BDP Console (Managed Snowplow)](#1-bdp-console-managed-snowplow), then add:

```yaml
source:
  type: snowplow
  config:
    # Enable warehouse lineage via Data Models API
    warehouse_lineage:
      enabled: true
      platform_instance: "prod_snowflake" # Optional
      env: "PROD" # Optional
      validate_urns: true # Optional
```

**What this creates**:

- **Table-level lineage**: `atomic.events` → `derived.sessions` (or other derived tables)
- No direct warehouse credentials needed (uses BDP API)

**Supported warehouses**: Snowflake, BigQuery, Redshift, Databricks

##### When to Enable This Feature

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

#### Schema Versioning

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

#### Entity Mapping: Snowplow → DataHub

This section explains how Snowplow concepts are modeled as DataHub entities.

#### Entity Type Mapping

| Snowplow Concept    | DataHub Entity | DataHub Subtype          | Description                                      |
| ------------------- | -------------- | ------------------------ | ------------------------------------------------ |
| Organization        | Container      | `DATABASE`               | Top-level container for all Snowplow metadata    |
| Event Schema        | Dataset        | `snowplow_event_schema`  | Self-describing event definition (JSON Schema)   |
| Entity Schema       | Dataset        | `snowplow_entity_schema` | Context/entity schema attached to events         |
| Event Specification | Dataset        | `snowplow_event_spec`    | Tracking requirement defining what to track      |
| Tracking Scenario   | Container      | (custom)                 | Logical grouping of related event specifications |
| Tracking Plan       | Container      | `tracking_plan`          | Business-level tracking plan grouping            |
| Pipeline            | DataFlow       | -                        | Snowplow data pipeline (Collector → Warehouse)   |
| Enrichment          | DataJob        | -                        | Data transformation job within a pipeline        |
| Collector           | DataJob        | -                        | HTTP endpoint receiving tracking events          |
| Atomic Events       | Dataset        | `atomic_event`           | Raw enriched events table in warehouse           |
| Parsed Events       | Dataset        | `event`                  | Parsed event data combining all schemas          |

#### Pipeline Architecture in DataHub

Snowplow pipelines are modeled as **DataFlow** entities with **DataJob** children representing each processing stage:

```
Tracker SDKs (Web, Mobile, Server)
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Pipeline (DataFlow)                                   │
│  urn:li:dataFlow:(snowplow,pipeline-id,PROD)                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────┐                                                   │
│   │    Collector    │  ◄── Receives HTTP tracking events                │
│   │    (DataJob)    │                                                   │
│   └────────┬────────┘                                                   │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐│
│   │   IP Lookup     │  │   UA Parser     │  │  PII Pseudonymization   ││
│   │   (DataJob)     │  │   (DataJob)     │  │       (DataJob)         ││
│   │                 │  │                 │  │                         ││
│   │ user_ipaddress  │  │ useragent       │  │ user_id, email          ││
│   │  → geo_*, ip_*  │  │  → br_*, os_*   │  │  → (hashed values)      ││
│   └────────┬────────┘  └────────┬────────┘  └────────────┬────────────┘│
│            │                    │                        │              │
│            └────────────────────┼────────────────────────┘              │
│                                 ▼                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │  Atomic Events (Dataset)│
                    │  Enriched event stream  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Warehouse Tables      │
                    │ (Snowflake, BigQuery)   │
                    └─────────────────────────┘
```

#### Lineage Relationships

The connector creates the following lineage relationships:

##### 1. Schema → Event Specification Lineage

Event specifications reference the schemas they require:

```
┌──────────────────────────────┐
│ Event Schema                 │
│ (vendor.event_name.1-0-0)    │────┐
└──────────────────────────────┘    │     ┌─────────────────────────┐
                                    ├────▶│   Event Specification   │
┌──────────────────────────────┐    │     │  (Tracking Requirement) │
│ Entity Schema                │────┘     └─────────────────────────┘
│ (vendor.context.1-0-0)       │
└──────────────────────────────┘
```

##### 2. Enrichment Column-Level Lineage

Enrichments transform specific fields. Example for IP Lookup:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        IP Lookup Enrichment                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Input                          Output                             │
│   ─────                          ──────                             │
│                                  ┌─────────────────┐                │
│                              ┌──▶│ geo_country     │                │
│                              │   ├─────────────────┤                │
│   ┌─────────────────┐        │   │ geo_city        │                │
│   │ user_ipaddress  │────────┼──▶├─────────────────┤                │
│   └─────────────────┘        │   │ geo_region      │                │
│                              │   ├─────────────────┤                │
│                              │   │ geo_latitude    │                │
│                              ├──▶├─────────────────┤                │
│                              │   │ geo_longitude   │                │
│                              │   ├─────────────────┤                │
│                              └──▶│ ip_isp          │                │
│                                  ├─────────────────┤                │
│                                  │ ip_organization │                │
│                                  └─────────────────┘                │
└─────────────────────────────────────────────────────────────────────┘
```

Supported enrichments with column-level lineage:

- **IP Lookup**: `user_ipaddress` → `geo_*`, `ip_*` fields
- **UA Parser**: `useragent` → `br_*`, `os_*` fields
- **YAUAA**: `useragent` → browser, OS, device fields
- **Referer Parser**: `page_referrer` → `refr_*` fields
- **Campaign Attribution**: `page_urlquery` → `mkt_*` fields
- **PII Pseudonymization**: configured fields → same fields (hashed)
- **Currency Conversion**: currency fields → converted fields
- **Event Fingerprint**: event fields → `event_fingerprint`
- **IAB Spiders/Robots**: `useragent` → `iab_*` classification fields

##### 3. Warehouse Lineage (Optional)

When `warehouse_lineage.enabled: true`:

```
┌─────────────────────────┐                    ┌─────────────────────────┐
│     Atomic Events       │   Data Models API  │     Derived Table       │
│ (snowplow.atomic.events)│───────────────────▶│ (warehouse.schema.table)│
└─────────────────────────┘                    └─────────────────────────┘
```

#### Container Hierarchy

```
Organization (Container: DATABASE)
│
├── Event Schema: com.example.page_view.1-0-0 (Dataset)
├── Event Schema: com.example.checkout.1-0-0 (Dataset)
├── Entity Schema: com.example.user_context.1-0-0 (Dataset)
├── Event Specification: "Page View Tracking" (Dataset)
│
├── Tracking Scenario: "Checkout Flow" (Container)
│   ├── Event Specification: "Add to Cart" (Dataset)
│   └── Event Specification: "Purchase Complete" (Dataset)
│
└── Tracking Plan: "Web Analytics" (Container)
    ├── Event Specification (linked)
    └── Schema (linked)
```

#### URN Formats

| Entity Type         | URN Format                                                        |
| ------------------- | ----------------------------------------------------------------- |
| Organization        | `urn:li:container:{guid}`                                         |
| Event/Entity Schema | `urn:li:dataset:(urn:li:dataPlatform:snowplow,vendor.name,ENV)`   |
| Event Specification | `urn:li:dataset:(urn:li:dataPlatform:snowplow,event_spec_id,ENV)` |
| Pipeline            | `urn:li:dataFlow:(snowplow,pipeline-id,ENV)`                      |
| Enrichment/DataJob  | `urn:li:dataJob:(urn:li:dataFlow:(...),job-id)`                   |
| Tracking Scenario   | `urn:li:container:{guid}`                                         |

#### Custom Properties

Each entity type includes relevant custom properties:

**Event/Entity Schemas:**

- `vendor`, `name`, `version` (SchemaVer format)
- `schema_type` (event/entity)
- `json_schema` (full JSON Schema definition)
- `deployed_environments` (PROD, DEV, etc.)

**Event Specifications:**

- `status` (draft, active, deprecated)
- `trigger_conditions`
- `referenced_schemas`

**Enrichments:**

- `enrichment_type`
- `input_fields`, `output_fields`
- `configuration` details

#### Custom Platform Instance

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

#### Schema Filtering

Extract only specific vendor schemas:

```yaml
source:
  type: snowplow
  config:
    # ... connection config ...

    schema_pattern:
      allow:
        - "com\\.example\\..*" # Allow com.example schemas
        - "com\\.acme\\.events\\..*" # Allow com.acme.events schemas
      deny:
        - ".*\\.test$" # Deny test schemas
```

#### Testing the Connection

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

### Limitations

1. **BDP-specific features**:

   - Event specifications only available via BDP Console API
   - Tracking scenarios only available via BDP Console API
   - Tracking plans only available via BDP Console API
   - Open-source Iglu users won't have these features

2. **Iglu Server requirements**:

   - Automatic schema discovery requires Iglu Server 0.6+ with `/api/schemas` endpoint
   - Older Iglu implementations may not support the list schemas API

3. **Field tagging in Iglu-only mode**:
   - PII/sensitive field detection requires BDP deployment metadata
   - Not available when using Iglu-only mode

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Authentication Errors

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

#### Permission Errors

**Error**: `Permission denied for /data-structures`

**Solution**:

- API key missing `read:data-structures` permission
- Generate new credentials with correct permissions in BDP Console → Settings → API Credentials

**Error**: `Permission denied for /event-specs`

**Solution**:

- Set `extract_event_specifications: false` in config, or
- Request `read:event-specs` permission for your API key

#### Connection Errors

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

#### No Schemas Found

**Issue**: Ingestion completes but no schemas extracted

**Solutions**:

1. **Check filtering patterns**:

   ```yaml
   schema_pattern:
     allow: [".*"] # Allow all schemas
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

#### Rate Limiting

**Error**: `HTTP 429: Rate limit exceeded`

**Solution**:

- Connector implements automatic retry with exponential backoff
- Rate limits should be handled automatically
- If issues persist, contact Snowplow support to increase limits


### Code Coordinates
- Class Name: `datahub.ingestion.source.snowplow.snowplow.SnowplowSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snowplow/snowplow.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Snowplow, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
