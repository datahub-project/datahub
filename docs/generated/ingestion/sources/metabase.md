


# Metabase

## Overview

Metabase is a business intelligence and analytics platform. Learn more in the [official Metabase documentation](https://www.metabase.com/).

The DataHub integration for Metabase covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                                                               | Notes                                                                |
| -------------- | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `"Metabase"`   | [Data Platform](../../metamodel/entities/dataPlatform.md)                                     |                                                                      |
| Dashboard      | [Dashboard](../../metamodel/entities/dashboard.md)                                            |                                                                      |
| Card/Question  | [Chart](../../metamodel/entities/chart.md)                                                    |                                                                      |
| Model          | [Dataset](../../metamodel/entities/dataset.md)                                                | SubTypes `["Metabase Model"]`; transforming models also get `"View"` |
| Collection     | [Tag](../../metamodel/entities/tag.md) and [Container](../../metamodel/entities/container.md) | Tags optional; nested collections keep parent containers             |
| Database Table | [Dataset](../../metamodel/entities/dataset.md)                                                | From connected database                                              |
| User           | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md)                                 | Ownership information                                                |


## Module `metabase`
![GA](https://img.shields.io/badge/support%20status-GA-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Supported by default for charts and dashboards. |

### Overview

The `metabase` module ingests metadata from Metabase into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

To use this connector, you'll need:

- Metabase version v0.41+ (Models require v0.41+)
- Authentication credentials (either username/password or API key — **API key is recommended**)
- Appropriate permissions to access the Metabase API

#### Authentication

DataHub supports two authentication methods:

1. **API Key** (Recommended) — more secure, no password management required. Generate one under Account Settings → API Keys in your Metabase instance.
2. **Username/Password**


### Install the Plugin
```shell
pip install 'acryl-datahub[metabase]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: metabase
  config:
    # Coordinates
    connect_uri: https://metabase.company.com
    
    # Credentials (API key recommended)
    api_key: "${METABASE_API_KEY}"
    
    # Alternative: Username/Password authentication
    # username: "${METABASE_USERNAME}"
    # password: "${METABASE_PASSWORD}"
    
    # Optional: Custom display URI (if connect_uri is only for ingestion)
    # display_uri: https://metabase.company.com
    
    # Feature flags
    extract_collections_as_tags: true
    extract_models: true
    exclude_other_user_collections: false
    
    # Optional: Custom platform mappings
    # engine_platform_map:
    #   athena: glue
    #   sparksql: spark
    
    # Optional: Database name overrides
    # database_alias_map:
    #   postgres: my_postgres_db
    
    # Optional: Platform instance mappings
    # database_id_to_instance_map:
    #   "42": my_platform_instance
    # platform_instance_map:
    #   clickhouse: my_clickhouse_cluster
    
    # Default schema for SQL parsing
    default_schema: public

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Metabase API key. If provided, the username and password will be ignored. Recommended method. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Metabase host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:3000</span></div> |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset (table) names to lowercase when creating lineage URNs. Column names always preserve their original case to match upstream source connectors. Most DataHub connectors (including Postgres, ClickHouse, BigQuery) preserve the original case from the database. Only set to true if your upstream connector explicitly lowercases table names (rare). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">database_alias_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database name map to use when constructing dataset URN. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_id_to_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom mappings between metabase database id and DataHub platform instance <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Default schema name to use when schema is not provided in an SQL query <div className="default-line default-line-with-docs">Default: <span className="default-value">public</span></div> |
| <div className="path-line"><span className="path-main">display_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | optional URL to use in links (if `connect_uri` is only for ingestion) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">engine_platform_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom mappings between metabase database engines and DataHub platforms <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">exclude_other_user_collections</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Flag that if true, exclude other user collections <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_collections_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Metabase collections as tags on dashboards and charts <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_models</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Metabase models (saved questions used as data sources) as datasets. Off by default: enabling it ingests models as `dataset` entities (and dashboards link to them as dataset edges) instead of charts, which changes their URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Metabase password, used when an API key is not provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">request_timeout_sec</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Timeout in seconds for each HTTP request to the Metabase API. Prevents ingestion from hanging indefinitely on an unresponsive server. <div className="default-line default-line-with-docs">Default: <span className="default-value">30.0</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Metabase username, used when an API key is not provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
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
  "properties": {
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
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
      "default": null
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance_map": {
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
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
    },
    "connect_uri": {
      "default": "http://localhost:3000",
      "description": "Metabase host URL.",
      "title": "Connect Uri",
      "type": "string"
    },
    "display_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "optional URL to use in links (if `connect_uri` is only for ingestion)",
      "title": "Display Uri"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Metabase username, used when an API key is not provided.",
      "title": "Username"
    },
    "password": {
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
      "description": "Metabase password, used when an API key is not provided.",
      "title": "Password"
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
      "description": "Metabase API key. If provided, the username and password will be ignored. Recommended method.",
      "title": "Api Key"
    },
    "request_timeout_sec": {
      "default": 30.0,
      "description": "Timeout in seconds for each HTTP request to the Metabase API. Prevents ingestion from hanging indefinitely on an unresponsive server.",
      "exclusiveMinimum": 0,
      "title": "Request Timeout Sec",
      "type": "number"
    },
    "database_alias_map": {
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
      "description": "Database name map to use when constructing dataset URN.",
      "title": "Database Alias Map"
    },
    "engine_platform_map": {
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
      "description": "Custom mappings between metabase database engines and DataHub platforms",
      "title": "Engine Platform Map"
    },
    "database_id_to_instance_map": {
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
      "description": "Custom mappings between metabase database id and DataHub platform instance",
      "title": "Database Id To Instance Map"
    },
    "default_schema": {
      "default": "public",
      "description": "Default schema name to use when schema is not provided in an SQL query",
      "title": "Default Schema",
      "type": "string"
    },
    "exclude_other_user_collections": {
      "default": false,
      "description": "Flag that if true, exclude other user collections",
      "title": "Exclude Other User Collections",
      "type": "boolean"
    },
    "extract_collections_as_tags": {
      "default": true,
      "description": "Extract Metabase collections as tags on dashboards and charts",
      "title": "Extract Collections As Tags",
      "type": "boolean"
    },
    "extract_models": {
      "default": false,
      "description": "Extract Metabase models (saved questions used as data sources) as datasets. Off by default: enabling it ingests models as `dataset` entities (and dashboards link to them as dataset edges) instead of charts, which changes their URNs.",
      "title": "Extract Models",
      "type": "boolean"
    },
    "convert_lineage_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset (table) names to lowercase when creating lineage URNs. Column names always preserve their original case to match upstream source connectors. Most DataHub connectors (including Postgres, ClickHouse, BigQuery) preserve the original case from the database. Only set to true if your upstream connector explicitly lowercases table names (rare).",
      "title": "Convert Lineage Urns To Lowercase",
      "type": "boolean"
    }
  },
  "title": "MetabaseConfig",
  "type": "object"
}
```





### Capabilities

#### Lineage

The connector extracts lineage across all Metabase query types:

##### Native SQL

SQL queries are parsed with DataHub's SQLGlot-based parser to extract table references, including those inside `JOIN` clauses and subqueries. Metabase template variables (`{{variable}}`, `[[WHERE ...]]`) are stripped before parsing.

##### Query Builder (MBQL)

Questions and models built with Metabase's visual query builder store their logic as MBQL — a structured JSON representation. The connector resolves MBQL to upstream database tables and, for **models**, also produces column-level lineage:

- **Table-level**: The `source-table` field and all `joins[].source-table` entries are resolved to DataHub dataset URNs, covering multi-table join scenarios.
- **Column-level** (models only): `result_metadata[].field_ref` records which MBQL expression produced each output column. The connector resolves these refs to upstream field URNs via `/api/field/{id}`:
  - `["field", id, ...]` — direct pass-through column
  - `["expression", name]` — calculated column; traces back through `query.expressions`
  - `["aggregation", index]` — metric column; traces back through `query.aggregation`. `COUNT(*)` with no explicit field fans-in all resolved upstream columns (matching Tableau lineage behaviour).

##### Nested Queries

Charts or models that reference other cards (`source-table: "card__456"`) are recursively resolved to their ultimate source tables (max depth: 5, to guard against circular references).

##### Dashboard Lineage

Table dependencies from all charts in a dashboard are rolled up into direct table-to-dashboard lineage edges, deduplicating tables referenced by multiple charts.

#### Collection Tags

Metabase Collections are mapped to DataHub tags:

- Tag format: `metabase_collection_{sanitized_name}` (e.g. "Sales & Marketing" → `metabase_collection_sales_marketing`)
- Tags are applied to dashboards, charts, and models within that collection
- Disable with `extract_collections_as_tags: false`

#### Database and Platform Mapping

Metabase databases are mapped to a DataHub platform based on the engine field returned by [`/api/database`](https://www.metabase.com/docs/latest/api-documentation.html#database). Override with `engine_platform_map`:

```yaml
engine_platform_map:
  athena: glue
```

DataHub determines the database name from the same API response. Override with `database_alias_map`:

```yaml
database_alias_map:
  postgres: my_custom_db_name
```

#### Platform Instance Mapping

When multiple instances of the same platform exist in DataHub (for example, two ClickHouse clusters), map Metabase database IDs to platform instances with `database_id_to_instance_map`:

```yaml
database_id_to_instance_map:
  "42": platform_instance_in_datahub
```

The key must be a string, not an integer.

If `database_id_to_instance_map` is not set, `platform_instance_map` is used as a fallback. If neither is set, platform instance is omitted from dataset URNs.

#### Filtering Collections

To exclude collections owned by other users:

```yaml
exclude_other_user_collections: true
```

### Limitations

- Column-level lineage is only available for cards saved as a **Model** (type `"model"`). Regular MBQL questions do not expose reliable `field_ref` data.
- Template variables in native SQL queries (`{{variable}}`, `[[optional clause]]`) are stripped before parsing, so lineage based on dynamic table references may be incomplete.
- Circular card references are cut off at depth 5; deeply nested card chains beyond that limit will not have lineage extracted.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

- **Auth failure**: ensure the API key or username/password is correct and the account has access to the relevant collections.
- **Missing lineage**: check that `extract_models: true` is set and that the cards are saved as Models in Metabase.
- **Unknown platform warnings**: add an entry to `engine_platform_map` for the unrecognised engine name.


### Code Coordinates
- Class Name: `datahub.ingestion.source.metabase.source.MetabaseSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/metabase/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Metabase, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
