


# Omni

## Overview

Omni is a cloud-native business intelligence platform. Learn more in the [official Omni documentation](https://docs.omni.co/).

The DataHub integration for Omni covers BI entities such as dashboards, charts, semantic datasets, and related ownership context. It also captures table- and column-level lineage, ownership, and stateful deletion detection.

## Concept Mapping

| Omni Concept    | DataHub Concept                                    | Notes                                                                                                           |
| --------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `Folder`        | [Container](../../metamodel/entities/container.md) | SubType `"Folder"`                                                                                              |
| `Dashboard`     | [Dashboard](../../metamodel/entities/dashboard.md) | Published document with `hasDashboard=true`                                                                     |
| `Tile`          | [Chart](../../metamodel/entities/chart.md)         | Each query presentation within a dashboard                                                                      |
| `Topic`         | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"Topic"` — the semantic join graph entry point                                                         |
| `View`          | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"View"` — semantic layer table with dimensions and measures as schema fields                           |
| `Workbook`      | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"Workbook"` — unpublished personal exploration document                                                |
| Warehouse table | Lineage reference only                             | Not emitted as entities; referenced via URN in upstream lineage of Omni Views (e.g. Snowflake, BigQuery tables) |
| Document owner  | Ownership relationship                             | URN reference only; propagated as `DATAOWNER` ownership aspect to Dashboard and Chart entities                  |


## Module `omni`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Field-level lineage when include_column_lineage=true. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Document owner extracted from Omni API. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Supported via connection_to_platform_instance config. |
| Schema Metadata | ✅ | Dimensions and measures extracted as schema columns. |
| Table-Level Lineage | ✅ | Dashboard → Tile → Topic → View → DB Table. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `omni` module ingests metadata from the [Omni](https://omni.co/) BI platform into DataHub. It is intended for production ingestion workflows and supports the following:

- Folders (as Containers), Dashboards, and Chart tiles
- Semantic layer: Models, Topics, and Views with schema fields (dimensions and measures)
- Physical warehouse tables with upstream lineage stitched to existing DataHub entities
- Column-level (fine-grained) lineage from semantic view fields back to warehouse columns
- Ownership propagated from the Omni document API

Lineage is emitted as a five-hop chain:

```
Folder → Dashboard → Chart (tile) → Topic → Semantic View → Physical Table
```

### Prerequisites

Before running ingestion, ensure you have the following:

1. **An Omni Organization API key** with read access to models, documents, and connections. Generate API keys in Omni Admin → API Keys.

2. **Connection mapping configuration** if you want physical table lineage to stitch with existing warehouse entities in DataHub. You will need to map each Omni connection ID to the corresponding DataHub platform name, platform instance, and database name:

```yaml
connection_to_platform:
  "conn_abc123": "snowflake"
connection_to_platform_instance:
  "conn_abc123": "my_snowflake_account"
connection_to_database:
  "conn_abc123": "ANALYTICS_PROD"
```

Connection IDs can be found by calling the Omni `/v1/connections` API or from the Omni Admin UI.

:::note
If the Omni API key does not have permission to list connections (`403 Forbidden`), the connector will fall back to the `connection_to_platform` config overrides and continue ingestion without failing.
:::


### Install the Plugin
```shell
pip install 'acryl-datahub[omni]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: omni
  config:
    # Coordinates
    base_url: "https://your-org.omniapp.co/api"

    # Credentials
    api_key: "${OMNI_API_KEY}"

    # Connection → warehouse stitching
    # Map Omni connection IDs to DataHub platform names so that physical table
    # URNs match what was ingested by your warehouse source connector.
    connection_to_platform:
      "conn_abc123": "snowflake"

    # Optional: map connection IDs to platform instances
    # connection_to_platform_instance:
    #   "conn_abc123": "my_snowflake_account"

    # Optional: override the database name inferred from the Omni connection
    # connection_to_database:
    #   "conn_abc123": "ANALYTICS_PROD"

    # Optional: include workbook-only documents (not just published dashboards)
    # include_workbook_only: false

    # Optional: filter which models to ingest
    # model_pattern:
    #   allow:
    #     - ".*"

    # Optional: filter which documents (dashboards/workbooks) to ingest
    # document_pattern:
    #   allow:
    #     - ".*"

    # Optional: disable column-level lineage
    # include_column_lineage: true

    # Optional: stateful ingestion with stale entity removal
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Omni Organization API key (not a Personal Access Token). Generate in Omni Admin → API Keys. The key must have read access to models, documents, and connections.  |
| <div className="path-line"><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Omni instance base URL including the /api suffix, e.g. https://myorg.omniapp.co/api. Found in your Omni organization settings.  |
| <div className="path-line"><span className="path-main">connection_to_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Map Omni connection IDs to canonical database names used in DataHub URNs. Use when the database name in Omni differs from the name registered in DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Map Omni connection IDs to DataHub platform names. Required when the platform cannot be auto-detected from the connection dialect. Example: {'abc-123': 'snowflake', 'def-456': 'bigquery'} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Map Omni connection IDs to DataHub platform instance names. Must exactly match the platform_instance used when ingesting the warehouse. Example: {'abc-123': 'prod_snowflake'} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract column-level (fine-grained) lineage from dashboard query fields back to Omni semantic view fields. Enables precise field-level impact analysis in DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_deleted</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include soft-deleted Omni entities (models, documents) where the API supports it. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_workbook_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include workbook-only documents that have not been published as a dashboard. When False (default), only documents with hasDashboard=true are ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of parallel threads for processing models and documents. Higher values speed up ingestion but increase API load and memory usage. Recommended: 4-8 for most Omni instances. <div className="default-line default-line-with-docs">Default: <span className="default-value">4</span></div> |
| <div className="path-line"><span className="path-main">normalize_snowflake_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Upper-case database, schema, and table name components in URNs when the resolved platform is Snowflake. Set convert_urns_to_lowercase=True when your warehouse connector uses lowercase URNs to ensure lineage stitches correctly. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of records per page for paginated Omni API endpoints. Lower values reduce memory usage; higher values speed up ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">50</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds for Omni API calls. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">document_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">document_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">model_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">model_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for tracking processed entities and removing stale metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    }
  },
  "additionalProperties": false,
  "description": "Configuration for the Omni BI platform DataHub source.",
  "properties": {
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
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
      "description": "Stateful ingestion configuration for tracking processed entities and removing stale metadata."
    },
    "base_url": {
      "description": "Omni instance base URL including the /api suffix, e.g. https://myorg.omniapp.co/api. Found in your Omni organization settings.",
      "title": "Base Url",
      "type": "string"
    },
    "api_key": {
      "description": "Omni Organization API key (not a Personal Access Token). Generate in Omni Admin \u2192 API Keys. The key must have read access to models, documents, and connections.",
      "format": "password",
      "title": "Api Key",
      "type": "string",
      "writeOnly": true
    },
    "page_size": {
      "default": 50,
      "description": "Number of records per page for paginated Omni API endpoints. Lower values reduce memory usage; higher values speed up ingestion.",
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "max_workers": {
      "default": 4,
      "description": "Maximum number of parallel threads for processing models and documents. Higher values speed up ingestion but increase API load and memory usage. Recommended: 4-8 for most Omni instances.",
      "maximum": 20,
      "minimum": 1,
      "title": "Max Workers",
      "type": "integer"
    },
    "timeout_seconds": {
      "default": 30,
      "description": "HTTP request timeout in seconds for Omni API calls.",
      "maximum": 120,
      "minimum": 5,
      "title": "Timeout Seconds",
      "type": "integer"
    },
    "include_deleted": {
      "default": false,
      "description": "Include soft-deleted Omni entities (models, documents) where the API supports it.",
      "title": "Include Deleted",
      "type": "boolean"
    },
    "include_workbook_only": {
      "default": false,
      "description": "Include workbook-only documents that have not been published as a dashboard. When False (default), only documents with hasDashboard=true are ingested.",
      "title": "Include Workbook Only",
      "type": "boolean"
    },
    "model_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny patterns applied to Omni model IDs. Use to restrict ingestion to specific models. Example: allow: ['^prod-.*']"
    },
    "document_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny patterns applied to Omni document identifiers. Use to restrict ingestion to specific dashboards or workbooks."
    },
    "include_column_lineage": {
      "default": true,
      "description": "Extract column-level (fine-grained) lineage from dashboard query fields back to Omni semantic view fields. Enables precise field-level impact analysis in DataHub.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "connection_to_platform": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Map Omni connection IDs to DataHub platform names. Required when the platform cannot be auto-detected from the connection dialect. Example: {'abc-123': 'snowflake', 'def-456': 'bigquery'}",
      "title": "Connection To Platform"
    },
    "connection_to_platform_instance": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Map Omni connection IDs to DataHub platform instance names. Must exactly match the platform_instance used when ingesting the warehouse. Example: {'abc-123': 'prod_snowflake'}",
      "title": "Connection To Platform Instance"
    },
    "connection_to_database": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Map Omni connection IDs to canonical database names used in DataHub URNs. Use when the database name in Omni differs from the name registered in DataHub.",
      "title": "Connection To Database"
    },
    "normalize_snowflake_names": {
      "default": true,
      "description": "Upper-case database, schema, and table name components in URNs when the resolved platform is Snowflake. Set convert_urns_to_lowercase=True when your warehouse connector uses lowercase URNs to ensure lineage stitches correctly.",
      "title": "Normalize Snowflake Names",
      "type": "boolean"
    }
  },
  "required": [
    "base_url",
    "api_key"
  ],
  "title": "OmniSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Physical table lineage

Omni Views reference physical warehouse tables via `sql_table_name` in the topic API response. The connector resolves each reference to a DataHub dataset URN using the `connection_to_platform` mapping. If `normalize_snowflake_names: true` (default), database, schema, and table name components are uppercased to match the casing used by the DataHub Snowflake connector.

#### Column-level lineage

When `include_column_lineage: true` (default), the connector emits two levels of field-level lineage:

- **View → Physical table**: for passthrough fields (dimensions with no SQL expression), the connector maps `physical_table.column → semantic_view.field` using the matching column name. Computed fields (measures with SQL expressions like `SUM(amount)`) are skipped because the view field name does not correspond to a physical column.
- **Dashboard → View**: for each field referenced in a dashboard tile query, the connector maps `semantic_view.field → dashboard.field`.

This enables field-level impact analysis across the chain:

```
physical_table.column → semantic_view.field → dashboard_tile.field
```

#### Schema metadata

For each Omni Semantic View, the connector emits a `SchemaMetadata` aspect containing one `SchemaField` per dimension and measure:

- **Dimensions**: emitted with inferred native type (string, date, timestamp, number, boolean)
- **Measures**: emitted with aggregation type and native type `NUMBER`
- Field descriptions are extracted from the `description` attribute when present

#### Model and document filtering

Use `model_pattern` and `document_pattern` to restrict ingestion to specific models or dashboards:

```yaml
model_pattern:
  allow:
    - "^prod-.*"
  deny:
    - ".*-dev$"

document_pattern:
  allow:
    - ".*"
```

### Limitations

- Access Filters, User Attributes, and Cache schedules are not yet ingested.
- View → physical column lineage is limited to passthrough fields (dimensions without SQL expressions). Computed measures are skipped because the view field name does not map to a physical column.
- Large organizations with many models may approach Omni API rate limits; the connector will automatically retry on 429 responses with exponential backoff.
- True end-to-end integration tests require a live Omni environment; the test suite uses deterministic mock API responses.

### Troubleshooting

If ingestion fails, validate credentials, permissions, and connectivity first. Then review the ingestion report and logs for source-specific errors.

#### Performance and rate limiting

Ingestion performance is primarily limited by the [Omni API rate limits](https://docs.omni.co/api/rate-limits) (default: 60 requests/minute). The connector automatically handles rate limiting via server-side 429 responses with exponential backoff retry. For large Omni instances with thousands of models, expect ingestion to take several hours.

Check the logs for retry warnings (logged by tenacity) to understand if rate limiting or server errors are affecting performance. Frequent 429 retries indicate the connector is saturating the API rate limit and working as efficiently as possible.

Common issues:

| Symptom                                          | Likely Cause                                          | Resolution                                                                    |
| ------------------------------------------------ | ----------------------------------------------------- | ----------------------------------------------------------------------------- |
| `403 Forbidden` on `/v1/connections`             | API key lacks connection read scope                   | Ingestion continues with config fallbacks; physical lineage may be incomplete |
| Physical tables not linked to warehouse entities | `connection_to_platform` not configured               | Add connection mapping for each Omni connection ID                            |
| Snowflake URN mismatch                           | Case mismatch between Omni and DataHub Snowflake URNs | Ensure `normalize_snowflake_names: true` (default)                            |
| Column lineage empty for some fields             | Field is a computed measure (has SQL expression)      | Expected — only passthrough dimensions produce view→physical column edges     |
| Slow ingestion performance                       | Omni API rate limiting (60 req/min default)           | Expected for large instances; check logs for retry warnings                   |


### Code Coordinates
- Class Name: `datahub.ingestion.source.omni.omni.OmniSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/omni/omni.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Omni, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
