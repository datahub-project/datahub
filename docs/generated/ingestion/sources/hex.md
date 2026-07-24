


# Hex

## Overview

[Hex](https://hex.tech/) is a collaborative data workspace where teams build interactive notebooks combining SQL, Python, and visualizations.

The DataHub integration emits Hex Projects (Dashboards) and Components (Charts) along with workspace containers, ownership, tags from Collections/Status/Categories, usage statistics, and upstream lineage to the warehouses Hex queries. It also emits per-project run history, and per-project context documents for AI agent retrieval (opt-in via `include_context_documents`). Upstream lineage is produced directly from Hex's own APIs (SQL parsing by default; Hex's `queriedTables` API can be enabled on Hex Enterprise workspaces) — no warehouse ingestion dependency is required.

## Concept Mapping

| Hex Concept | DataHub Concept                                                                           | Notes                                                                                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `"hex"`     | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                                                                                                                                                        |
| Workspace   | [Container](/docs/generated/metamodel/entities/container/)        | Parent container for all projects and components in the workspace.                                                                                     |
| Project     | [Dashboard](/docs/generated/metamodel/entities/dashboard/)        | Subtype `Project`. Carries usage statistics, last refresh time from run history, and upstream lineage edges to warehouse datasets.                     |
| Component   | [Chart](/docs/generated/metamodel/entities/chart/)                | Subtype `Component`. Reusable shared cell group with its own visualization; linked to importing projects via `DashboardInfo.charts`.                   |
| Collection  | [Tag](/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:collection:<name>` when `collections_as_tags` is enabled.                                                                              |
| Status      | [Tag](/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:status:<name>` when `status_as_tag` is enabled.                                                                                        |
| Category    | [Tag](/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:category:<name>` when `categories_as_tags` is enabled.                                                                                 |
| Project Doc | [Document](/docs/generated/metamodel/entities/document/)          | One per Project and per Component when `include_context_documents` is enabled. Hidden from global search; linked to the Dashboard/Chart for AI agents. |

Other Hex concepts are not mapped to DataHub entities yet.


## Module `hex`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Column-level lineage via SQL parsing when datahub-api is configured. The graph-backed SchemaResolver fetches table schemas from DataHub on demand to expand SELECT * and resolve column references. Graceful degradation to dataset-level when datahub-api is absent. |
| Dataset Usage | ✅ | Supported by default. Supported for types - Project. |
| Descriptions | ✅ | Supported by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Supported by default. |
| Extract Tags | ✅ | Status, categories, and collections emitted as tags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default via queriedTables API (Hex Enterprise workspaces) or SQL parsing from cells (all Hex tiers). Applied to both projects and components. Unpublished entities always use SQL parsing. No warehouse ingestion dependency required. |

### Overview

The `hex` module ingests Hex Projects, Components, workspaces, and upstream lineage directly from the Hex REST API.

### Prerequisites

#### Workspace Name

Open the **workspace switcher dropdown** in the top-left corner of the Hex app — the workspace name (and its slug) is shown next to each workspace entry. Use the slug value for `workspace_name`.

#### Authentication

The connector authenticates with a Hex **Workspace token** issued from `Settings → API → Workspace tokens`. Grant the token these **read-only** scopes:

- `Projects → Read access` — list projects/components and read their detail and run history.
- `Cells → Read access` — read SQL cells for lineage and context documents.
- `Read project queried tables` — lineage from Hex's pre-resolved table list. Available on **Hex Enterprise workspaces only**; skip this scope on lower Hex tiers — the connector falls back to SQL parsing.
- `Data connections → Read access` — map each Hex connection to its warehouse platform/database/schema.
- `Users → Read access` — _optional_, only needed to auto-discover the workspace (org) UUID used in external URLs. Skip this scope and set `workspace_id` in the recipe instead.

No write scopes are required — the connector never modifies state in Hex.

Personal Access Tokens (PATs) also work but ingest with the issuing user's permissions, so projects the user cannot see in Hex will be skipped. Workspace tokens are recommended for production ingestion. See the [Hex API overview](https://learn.hex.tech/docs/api-integrations/api/overview) for the full list of token types.

#### Lineage URN Alignment

Upstream URNs are built from Hex's `/v1/data-connections` response — platform, database, and schema all come from there. Configure `connection_platform_map` (keyed by Hex `dataConnectionId`) in two cases:

- the upstream warehouse was ingested under a `platform_instance` — set the matching `platform_instance` so the URNs collide with the warehouse-ingested ones,
- a Hex connection's type is unrecognized (deleted, custom, or the token lacks scope on `/v1/data-connections`) — set `platform` explicitly so its cells aren't skipped.

See **Connection Platform Resolution** in the sections below for the full configuration shape.


### Install the Plugin
```shell
pip install 'acryl-datahub[hex]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: hex
  config:
    # Hex workspace name — find it in the workspace switcher dropdown in the top-left corner of the Hex app
    workspace_name: my-workspace
    workspace_id: id  # optional override for workspace ID (UUID); if not set, the source will call the Hex API to fetch it
    token: "${HEX_TOKEN}"

    # (Optional) platform_instance / env for the Hex side (Dashboards, Charts).
    # platform_instance: prod_hex
    # env: PROD

    # (Optional) Feature toggles — all default to true. Uncomment to opt out.
    # include_components: false
    # include_lineage: false
    # include_run_history: false
    # set_ownership_from_email: false
    # collections_as_tags: false
    # status_as_tag: false
    # categories_as_tags: false

    # (Optional) Emit a DataHub Document per Project and per Component for
    # AI agent retrieval. Off by default — opt in if you use AI agents and
    # want context documents in your catalog.
    # include_context_documents: true

    # (Optional) Hex Enterprise workspaces only — use Hex's queriedTables API
    # as the primary lineage source for published projects/components.
    # Defaults to false (SQL-cell parsing for everything).
    # use_queried_tables_lineage: true

    # (Optional) Match the platform_instance under which the upstream warehouses
    # were ingested. Required so Hex's lineage URNs collide with the
    # warehouse-ingested ones. Keyed by Hex dataConnectionId (UUID).
    # connection_platform_map:
    #   "8f3a1c2d-4b5e-6789-abcd-ef0123456789":
    #     platform: snowflake
    #     platform_instance: prod_snowflake
    #     default_database: ANALYTICS
    #     default_schema: PUBLIC
    #   "1a2b3c4d-5e6f-7890-abcd-1234567890ab":
    #     platform: bigquery
    #     default_database: my-gcp-project
    #     default_schema: analytics

    # (Optional) Filter projects and components by title or category.
    # project_title_pattern:
    #   allow:
    #     - "^Production .*"
    # component_title_pattern:
    #   allow:
    #     - "^Shared .*"
    # category_pattern:
    #   deny:
    #     - "^Sandbox$"

    # (Optional) Cap projects per run — useful for staged rollouts.
    # WARNING: with stateful_ingestion enabled, projects beyond the limit are
    # soft-deleted on the next run.
    # max_projects: 50

    # Enable stale-entity removal (projects deleted in Hex are soft-deleted in DataHub).
    stateful_ingestion:
      enabled: true

# sink configs — see https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Hex Workspace Token with the 'Read projects' scope. Create one at Settings → API → Workspace tokens. The 'Read projects' scope is required to access project cells for lineage; tokens without it can enumerate projects but not read their content. See https://learn.hex.tech/docs/api-integrations/api/overview for token types.  |
| <div className="path-line"><span className="path-main">workspace_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Hex workspace name. Find it in the workspace switcher dropdown in the top-left corner of the Hex app.  |
| <div className="path-line"><span className="path-main">base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. Single-tenant app users should replace this with the URL they use to access Hex. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://app.hex.tech/api/v1</span></div> |
| <div className="path-line"><span className="path-main">categories_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Category as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">collections_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Collections as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_components</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include Hex Components in the ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_context_documents</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit a DataHub Document per Project and per Component containing SQL sources, visualisation metadata, and notebook documentation. Documents are hidden from global search and linked to the Dashboard/Chart for AI agent retrieval. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract upstream lineage. Uses queriedTables API (Hex Enterprise workspaces) or falls back to parsing SQL from cells (all workspaces). No warehouse ingestion dependency required. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_run_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit the most recent COMPLETED run as a DashboardInfo PATCH setting lastRefreshed. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_projects</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of projects to process. Useful for testing or staged rollouts. Components discovered during project processing are not counted. Defaults to None (process all projects). WARNING: with stateful ingestion enabled, projects beyond this limit are soft-deleted on the next run. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per Hex API call. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">patch_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit metadata as patch events <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">set_ownership_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set ownership identity from owner/creator email <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">status_as_tag</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Status as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_queried_tables_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use Hex's queriedTables API (Hex Enterprise workspaces only) as the primary lineage source for published projects and components. Unpublished entities always fall back to SQL-cell parsing since queriedTables is only populated for published runs. Set to False to force SQL-cell parsing for everything. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">workspace_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Hex workspace (org) UUID, used to build external URLs to the Hex app (e.g. https://app.hex.tech/<workspace_id>/hex/<project_id>). If left unset, the connector calls /users/me to auto-discover it — which requires the token to have 'Users → Read access'. Set this explicitly to avoid granting that scope. Find the UUID in any Hex project URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">category_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">category_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">component_title_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">component_title_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">connection_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,HexConnectionDetail)</span></div> | Per-connection override for upstream lineage URN construction.  |
| <div className="path-line"><span className="path-prefix">connection_platform_map.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform name. Required only when Hex's connection type cannot be auto-resolved (deleted connections, permission gaps, custom types). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_platform_map.`key`.</span><span className="path-main">default_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default outer-scope qualifier for unqualified table refs in SQL cells. For BigQuery this is the GCP project ID; for Snowflake/Postgres/Redshift/MSSQL the database; for Trino/Databricks/Presto the catalog. Leave empty for 2-part platforms (MySQL/MariaDB/Clickhouse) — set only `default_schema` there. Overrides the value auto-extracted from Hex's /v1/data-connections response. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_platform_map.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default inner-scope qualifier for unqualified table refs in SQL cells. For BigQuery this is the dataset; for Snowflake/Postgres/Redshift/MSSQL/Trino/Databricks/Presto/Athena the schema; for MySQL/MariaDB/Clickhouse the database name. Overrides the value auto-extracted from Hex's /v1/data-connections response. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform_instance the underlying warehouse was ingested under. Leave unset for warehouses ingested without one (e.g. typical BigQuery). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">project_title_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_title_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Configuration for stateful ingestion and stale metadata removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "HexConnectionDetail": {
      "additionalProperties": false,
      "description": "Per-connection override for upstream lineage URN construction.",
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform name. Required only when Hex's connection type cannot be auto-resolved (deleted connections, permission gaps, custom types).",
          "title": "Platform"
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
          "description": "DataHub platform_instance the underlying warehouse was ingested under. Leave unset for warehouses ingested without one (e.g. typical BigQuery).",
          "title": "Platform Instance"
        },
        "default_database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default outer-scope qualifier for unqualified table refs in SQL cells. For BigQuery this is the GCP project ID; for Snowflake/Postgres/Redshift/MSSQL the database; for Trino/Databricks/Presto the catalog. Leave empty for 2-part platforms (MySQL/MariaDB/Clickhouse) \u2014 set only `default_schema` there. Overrides the value auto-extracted from Hex's /v1/data-connections response.",
          "title": "Default Database"
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default inner-scope qualifier for unqualified table refs in SQL cells. For BigQuery this is the dataset; for Snowflake/Postgres/Redshift/MSSQL/Trino/Databricks/Presto/Athena the schema; for MySQL/MariaDB/Clickhouse the database name. Overrides the value auto-extracted from Hex's /v1/data-connections response.",
          "title": "Default Schema"
        }
      },
      "title": "HexConnectionDetail",
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
      "description": "Configuration for stateful ingestion and stale metadata removal."
    },
    "workspace_name": {
      "description": "Hex workspace name. Find it in the workspace switcher dropdown in the top-left corner of the Hex app.",
      "title": "Workspace Name",
      "type": "string"
    },
    "workspace_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Hex workspace (org) UUID, used to build external URLs to the Hex app (e.g. https://app.hex.tech/<workspace_id>/hex/<project_id>). If left unset, the connector calls /users/me to auto-discover it \u2014 which requires the token to have 'Users \u2192 Read access'. Set this explicitly to avoid granting that scope. Find the UUID in any Hex project URL.",
      "title": "Workspace Id"
    },
    "token": {
      "description": "Hex Workspace Token with the 'Read projects' scope. Create one at Settings \u2192 API \u2192 Workspace tokens. The 'Read projects' scope is required to access project cells for lineage; tokens without it can enumerate projects but not read their content. See https://learn.hex.tech/docs/api-integrations/api/overview for token types.",
      "format": "password",
      "title": "Token",
      "type": "string",
      "writeOnly": true
    },
    "base_url": {
      "default": "https://app.hex.tech/api/v1",
      "description": "Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. Single-tenant app users should replace this with the URL they use to access Hex.",
      "title": "Base Url",
      "type": "string"
    },
    "include_components": {
      "default": true,
      "description": "Include Hex Components in the ingestion",
      "title": "Include Components",
      "type": "boolean"
    },
    "page_size": {
      "default": 100,
      "description": "Number of items to fetch per Hex API call.",
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "patch_metadata": {
      "default": false,
      "description": "Emit metadata as patch events",
      "title": "Patch Metadata",
      "type": "boolean"
    },
    "collections_as_tags": {
      "default": true,
      "description": "Emit Hex Collections as tags",
      "title": "Collections As Tags",
      "type": "boolean"
    },
    "status_as_tag": {
      "default": true,
      "description": "Emit Hex Status as tags",
      "title": "Status As Tag",
      "type": "boolean"
    },
    "categories_as_tags": {
      "default": true,
      "description": "Emit Hex Category as tags",
      "title": "Categories As Tags",
      "type": "boolean"
    },
    "project_title_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern for project titles to filter in ingestion."
    },
    "component_title_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern for component titles to filter in ingestion."
    },
    "set_ownership_from_email": {
      "default": true,
      "description": "Set ownership identity from owner/creator email",
      "title": "Set Ownership From Email",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Extract upstream lineage. Uses queriedTables API (Hex Enterprise workspaces) or falls back to parsing SQL from cells (all workspaces). No warehouse ingestion dependency required.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "use_queried_tables_lineage": {
      "default": false,
      "description": "Use Hex's queriedTables API (Hex Enterprise workspaces only) as the primary lineage source for published projects and components. Unpublished entities always fall back to SQL-cell parsing since queriedTables is only populated for published runs. Set to False to force SQL-cell parsing for everything.",
      "title": "Use Queried Tables Lineage",
      "type": "boolean"
    },
    "connection_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/HexConnectionDetail"
      },
      "description": "Per-connection lineage configuration, keyed by Hex dataConnectionId (UUID). Pins platform and platform_instance so upstream URNs match the warehouse's ingestion. Example: {\"<uuid>\": {\"platform\": \"snowflake\", \"platform_instance\": \"prod_snowflake\"}}",
      "title": "Connection Platform Map",
      "type": "object"
    },
    "include_run_history": {
      "default": true,
      "description": "Emit the most recent COMPLETED run as a DashboardInfo PATCH setting lastRefreshed.",
      "title": "Include Run History",
      "type": "boolean"
    },
    "include_context_documents": {
      "default": false,
      "description": "Emit a DataHub Document per Project and per Component containing SQL sources, visualisation metadata, and notebook documentation. Documents are hidden from global search and linked to the Dashboard/Chart for AI agent retrieval.",
      "title": "Include Context Documents",
      "type": "boolean"
    },
    "category_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern for categories to filter in ingestion. This will exclude any project or component that has any category denied or not explicitly allowed."
    },
    "max_projects": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Maximum number of projects to process. Useful for testing or staged rollouts. Components discovered during project processing are not counted. Defaults to None (process all projects). WARNING: with stateful ingestion enabled, projects beyond this limit are soft-deleted on the next run.",
      "title": "Max Projects"
    }
  },
  "required": [
    "workspace_name",
    "token"
  ],
  "title": "HexSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Upstream Lineage

Lineage is tiered, with both tiers opt-out via `include_lineage: false`:

- **Tier 1 — `queriedTables` (Hex Enterprise workspaces only, opt-in via `use_queried_tables_lineage: true`)**: Hex's own runtime-proven table list for **published** projects and components, served by `/v1/projects/{id}/queriedTables`. Unpublished entities always fall back to Tier 2 since `queriedTables` is only populated for published runs. A `403` (non-Enterprise Hex workspace) falls back to Tier 2 for everything and emits a warning.
- **Tier 2 — SQL parsing via [`sqlglot`](https://github.com/tobymao/sqlglot) (all workspaces, default)**: each cell is parsed with its connection's dialect.

Both tiers resolve warehouse URNs via `/v1/data-connections` (platform + default database/schema), overridable per-connection via `connection_platform_map`. For projects that import components, native project SQL is separated from inlined component SQL via the export API so component lineage isn't attributed twice. Cells whose `dataConnectionId` cannot be resolved are skipped with a structured warning — see [Missing Upstream Lineage](#missing-upstream-lineage) for triage.

##### Connection Platform Resolution

Hex's `/v1/data-connections` endpoint returns a `type` field that the connector maps to a DataHub platform via [`CONNECTION_TYPE_TO_DATAHUB_PLATFORM`](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/hex/constants.py). Default database/schema qualifiers come from the same response.

Configure `connection_platform_map` (keyed by Hex `dataConnectionId` UUID) when:

1. The warehouse was ingested under a `platform_instance` — set the matching value so URNs collide.
2. The connection is deleted, permission-gapped, or a custom type — set `platform` explicitly so its cells aren't skipped.

Example:

```yaml
connection_platform_map:
  "8f3a1c2d-4b5e-6789-abcd-ef0123456789":
    platform: snowflake
    platform_instance: prod_snowflake
    default_database: ANALYTICS
    default_schema: PUBLIC
  "1a2b3c4d-5e6f-7890-abcd-1234567890ab":
    platform: bigquery
    default_database: my-gcp-project
```

#### Migration from `query_fetcher`

Earlier versions of this connector derived lineage by querying DataHub for prior Hex-emitted query metadata (`query_fetcher.py`). That path has been removed: lineage now comes from SQL parsing of cells by default, or from Hex's `queriedTables` API when `use_queried_tables_lineage: true` is set on a Hex Enterprise workspace.

The following config fields fed only the old path and are now removed — drop them from your recipe (the connector will emit a warning if they are still present):

- `lineage_start_time`
- `lineage_end_time`
- `datahub_page_size`

#### Migration: Components are now Charts

Components were previously emitted as **Dashboard** entities (subtype `Component`); they are now **Chart** entities, linked from their Project's `DashboardInfo.charts`. This changes their URN entity type, so any saved views, glossary/tag/ownership assignments, and policies that targeted the old Dashboard-typed Component URNs are lost and must be manually reapplied to the new Chart URNs.

Legacy Dashboard-typed Components left over from the old version are soft-deleted by stale-entity removal when `stateful_ingestion` was enabled on the old run. Because every Component changes URN type, a component-heavy workspace can exceed the stale-removal fail-safe (`fail_safe_threshold`, default 75%); if that happens, raise the threshold or perform a one-time bulk cleanup via the DataHub UI or CLI.

#### Stale Entity Removal

Enable by configuring `stateful_ingestion`. Projects deleted in Hex are soft-deleted in DataHub on the next run.

`max_projects` caps projects per run. With `stateful_ingestion` enabled, projects beyond the limit are treated as stale and soft-deleted — only set it if that is the intended behavior.

#### Context Documents

Opt-in via `include_context_documents: true`. When enabled, the connector emits a DataHub Document per Project and per Component containing SQL sources, visualization metadata, and notebook documentation.

#### Run History

When `include_run_history` is enabled (default), the most recent scheduled run is emitted as an Operation aspect, and `last_run_status` / `last_run_elapsed_seconds` are written to the project's custom properties — `ERRORED` runs surface there so operators can see failures. Only `COMPLETED` runs additionally update `DashboardInfo.lastRefreshed` via a targeted PATCH, so projects with sustained failures keep their last known-good refresh time as a freshness signal.

#### Usage Statistics

Each Project and Component emits an all-time `viewsCount` and a rolling 7-day window with `lastViewedAt`. Hex counts app views only when the published app is accessed — unpublished drafts have no view counts, so usage statistics are only emitted for **published** Projects and Components.

### Limitations

1. **`queriedTables` requires a Hex Enterprise workspace and opt-in.** Defaults to SQL parsing; enable `use_queried_tables_lineage` on Hex Enterprise workspaces to use Hex's API as the primary source.
2. **Non-SQL query paths produce no lineage.** SQL parsing cannot recover table references from `hextoolkit` Python cells, dynamic SQL built from variables, or parameterized table names — the resulting projects will be missing those upstreams.
3. **Context documents are not a complete mirror of the Hex notebook.** Only a subset of cell types is captured, so the rendered document will not match the source notebook exactly.
4. **Upstream lineage may be missing or mismatched when Hex's `/v1/data-connections` metadata is incomplete or uses an unrecognized `connectionDetails` shape.** Without `default_database` / `default_schema`, neither SQL parsing nor `queriedTables` can assemble fully-qualified URNs; without the right `platform_instance`, URNs won't align with the warehouse ingestion. Set the affected `dataConnectionId` under `connection_platform_map` with the correct `platform_instance` / `default_database` / `default_schema`, or report the new connection shape to the DataHub team so the parser can be updated.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first, then review ingestion logs for source-specific errors.

#### Missing Upstream Lineage

The source report lists every skipped cell with its `dataConnectionId` and a reason (`missing_connection_id` or `unresolved_platform`). For each unresolved connection, add an entry under `connection_platform_map` and re-run. Cells with no `dataConnectionId` are non-SQL cells or cells without a Hex connection assigned — these cannot be recovered.

#### Column Lineage Looks Sparse

When `use_queried_tables_lineage` is enabled on a Hex Enterprise workspace, the report exposes `enterprise_cells_with_mismatch` and `enterprise_sample_mismatched_cells` — SQL cells whose parsed table URN did not match the `queriedTables` result. Adjusting `default_database` / `default_schema` in `connection_platform_map` resolves most cases.


### Code Coordinates
- Class Name: `datahub.ingestion.source.hex.hex.HexSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/hex/hex.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Hex, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
