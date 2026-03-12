# Omni

| Feature | Status |
|---|---|
| Metadata Extraction | ✅ Supported |
| [Column-level Lineage](#column-level-lineage) | ✅ Supported |
| [Dataset Lineage](#lineage-semantics) | ✅ Supported |
| [Schema Metadata](#schema-and-field-metadata) | ✅ Supported |
| [Ownership](#ownership) | ✅ Supported |
| [Platform Instance](#platform-instance) | ✅ Supported |
| [Stateful Ingestion](https://datahubproject.io/docs/metadata-ingestion/docs/dev_guides/stateful_ingestion) | ✅ Supported |
| [Soft Delete on Removal](https://datahubproject.io/docs/metadata-ingestion/docs/dev_guides/stateful_ingestion/#soft-deletes) | ✅ Supported |

## Overview

[Omni](https://omni.co/) is a cloud-native business intelligence platform that lets teams build and share data models, dashboards, and workbooks connected directly to warehouse tables.

This connector ingests Omni metadata into DataHub, including:

- Folders, Dashboards, Charts (workbook tiles)
- Semantic layer: Models, Topics, Views with schema fields
- Upstream lineage all the way to physical warehouse tables
- Column-level (fine-grained) lineage from semantic fields to warehouse columns

## Prerequisites

You will need an **Omni Organization API key** (or Personal Access Token) with read access to the resources you want to ingest. API keys are issued per organization in Omni's admin settings.

## Configuration

### Quickstart recipe

```yaml
source:
  type: omni
  config:
    base_url: "https://your-org.omni.co"
    api_key: "${OMNI_API_KEY}"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Full configuration reference

```yaml
source:
  type: omni
  config:
    # Required
    base_url: "https://your-org.omni.co"        # Base URL of your Omni instance
    api_key: "${OMNI_API_KEY}"                   # Omni API key (use secret ref)

    # Connection → warehouse stitching (optional)
    # Map Omni connection IDs to DataHub platform names / instances so that
    # physical table URNs match what was ingested by your warehouse source.
    connection_to_platform:                      # e.g. { "conn_abc123": "snowflake" }
      "conn_abc123": "snowflake"
    connection_to_platform_instance:             # e.g. { "conn_abc123": "prod_account" }
      "conn_abc123": "my_snowflake_account"
    connection_to_database:                      # override inferred database name
      "conn_abc123": "ANALYTICS"
    normalize_snowflake_names: true              # uppercase database/schema/table for Snowflake

    # Lineage
    include_column_lineage: true                 # emit fine-grained field-level lineage

    # Filtering
    model_pattern:
      allow:
        - ".*"                                   # allow all models (default)
      deny: []
    document_pattern:
      allow:
        - ".*"                                   # allow all documents (default)
      deny: []

    # API client tuning
    max_requests_per_minute: 60                  # rate-limit cap (default 60)
    request_timeout: 30                          # HTTP timeout in seconds

    # Platform / environment
    platform_instance: null                      # DataHub platform instance name
    env: "PROD"                                  # DataHub environment tag

    # Stateful ingestion
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
```

### Config fields

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | `string` | **Required** | Base URL of your Omni instance, e.g. `https://myorg.omni.co` |
| `api_key` | `string` (secret) | **Required** | Omni API key used for authentication |
| `connection_to_platform` | `dict[str, str]` | `{}` | Maps Omni connection IDs to DataHub platform names |
| `connection_to_platform_instance` | `dict[str, str]` | `{}` | Maps Omni connection IDs to DataHub platform instance names |
| `connection_to_database` | `dict[str, str]` | `{}` | Overrides the database name inferred from Omni connections |
| `normalize_snowflake_names` | `bool` | `false` | Uppercases database/schema/table identifiers for Snowflake |
| `include_column_lineage` | `bool` | `true` | Emit fine-grained (column-level) lineage edges |
| `model_pattern` | `AllowDenyPattern` | `allow: [".*"]` | Filter which Omni models to ingest |
| `document_pattern` | `AllowDenyPattern` | `allow: [".*"]` | Filter which Omni documents (dashboards / workbooks) to ingest |
| `max_requests_per_minute` | `int` | `60` | Maximum API requests per minute to respect Omni rate limits |
| `request_timeout` | `int` | `30` | HTTP request timeout in seconds |
| `platform_instance` | `string` | `null` | DataHub platform instance name for emitted Omni entities |
| `env` | `string` | `PROD` | DataHub environment applied to all emitted entities |

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| `DESCRIPTIONS` | ✅ | Extracted from Omni model/topic names and document titles |
| `LINEAGE_COARSE` | ✅ | Dataset-level: Dashboard → Chart → Topic → View → Physical Table |
| `LINEAGE_FINE` | ✅ | Field-level from semantic view columns to warehouse columns |
| `SCHEMA_METADATA` | ✅ | Semantic view fields emitted with inferred types |
| `OWNERSHIP` | ✅ | Document owner propagated to Dashboard and Chart entities |
| `PLATFORM_INSTANCE` | ✅ | Supported via `platform_instance` config |

## Entity mapping

| Omni Object | DataHub Entity Type | URN Pattern |
|---|---|---|
| Folder | Dataset (subType: `Folder`) | `urn:li:dataset:(urn:li:dataPlatform:omni, folder.<id>, ENV)` |
| Dashboard document | Dashboard + Dataset (subType: `Dashboard`) | `urn:li:dashboard:(omni, doc-<id>)` |
| Workbook document | Dataset (subType: `Workbook`) | `urn:li:dataset:(urn:li:dataPlatform:omni, doc-<id>, ENV)` |
| Query / tile | Chart | `urn:li:chart:(omni, <query-id>)` |
| Topic | Dataset (subType: `Topic`) | `urn:li:dataset:(urn:li:dataPlatform:omni, <model_id>.topic.<topic>, ENV)` |
| Semantic View | Dataset (subType: `View`) | `urn:li:dataset:(urn:li:dataPlatform:omni, <model_id>.<view>, ENV)` |
| Warehouse table | Dataset (native platform) | `urn:li:dataset:(urn:li:dataPlatform:<platform>, <db>.<schema>.<table>, ENV)` |

## Lineage semantics

The connector emits a five-hop lineage chain:

```
Folder
  └── Dashboard
        └── Chart (dashboard tile / workbook query)
              └── Topic
                    └── Semantic View
                          └── Physical Table (Snowflake / BigQuery / Redshift / …)
```

All edges are **upstream-pointing**: the entity to the right is upstream of the entity to the left.

### How physical tables are resolved

1. The connector calls `/v1/connections` to list Omni connections and maps each connection to a warehouse platform using the `connection_to_platform` config (or the `dialect` field on the connection).
2. Model YAML is fetched per model to extract `sql_table_name` references on each view.
3. The view's SQL table is parsed into `database.schema.table` and a URN is constructed for the target warehouse platform.
4. If `normalize_snowflake_names: true`, all three components are uppercased.

If `/v1/connections` returns `403 Forbidden`, the connector falls back to config overrides and continues ingestion without hard-failing.

## Schema and field metadata

For each Omni **Semantic View**, the connector emits a `SchemaMetadata` aspect containing one `SchemaField` per dimension and measure found in the model YAML:

- **Dimensions**: emitted with inferred native type (string, date, timestamp, number, boolean)
- **Measures**: emitted with their SQL expression, aggregation type, and native type `NUMBER`
- Field descriptions are extracted from the YAML `description` attribute when present

## Column-level lineage

When `include_column_lineage: true` (default), the connector emits `FineGrainedLineage` entries linking:

```
physical_table.column  →  semantic_view.field  →  dashboard_tile.field
```

Field references are resolved by parsing the `sql` expressions in model YAML and matching identifiers to known view/table columns.

## Ownership

Document owner (the Omni user who owns the dashboard or workbook) is propagated to:
- The DataHub `Dashboard` entity
- Every `Chart` entity linked to that document

Ownership is modeled as `TECHNICAL_OWNER`.

## Platform instance

If `platform_instance` is set, all Omni-native entities (folders, models, topics, views, dashboards, charts) are tagged with that platform instance. Physical warehouse entities use the platform instance mapped via `connection_to_platform_instance`.

## Known limitations

- The connector does not yet ingest Omni **Access Filters**, **User Attributes**, or **Cache schedules**.
- Column lineage is limited to fields that appear in model YAML `sql` expressions; complex/derived expressions may not fully resolve.
- Large organizations with many models may approach Omni API rate limits; tune `max_requests_per_minute` accordingly.
- True E2E integration tests require a live Omni environment; the test suite uses deterministic fixture data.
