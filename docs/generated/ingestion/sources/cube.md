


# Cube

## Overview

[Cube](https://cube.dev/) is a headless semantic layer that defines metrics, dimensions, and joins once and exposes them to BI tools, data apps, and AI agents through SQL, REST, and GraphQL APIs. Its data model is organised into **cubes** (business entities such as orders or customers) and **views** (curated, query-ready datasets built on top of cubes).

This source ingests the Cube data model into DataHub as datasets: each cube and view becomes a dataset whose measures and dimensions are modelled as schema fields. It supports both **Cube Core** (self-hosted, via the `/v1/meta` REST endpoint) and **Cube Cloud**, where it merges `/v1/meta` (structural and presentation metadata) with the richer **Metadata API** (warehouse and column-level lineage). On Cube Cloud the connector can mint the metadata-scoped token automatically via the Control Plane API. DataHub captures descriptions, measure/dimension classification, view-to-cube lineage, and — where the deployment exposes it — column-level lineage down to the underlying warehouse tables. On Cube Cloud it also ingests saved **reports** as charts and **workbooks** as dashboards via the Platform API, extending lineage to the BI consumption layer. Stateful ingestion removes cubes and views that have been deleted from the model.

## Concept Mapping

| Source Concept                                          | DataHub Concept                                                                   | Notes                                                |
| ------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Deployment / data model                                 | [Container](/docs/generated/metamodel/entities/container) | Subtype `Cube Deployment`                            |
| Cube                                                    | [Dataset](/docs/generated/metamodel/entities/dataset)     | Subtype `Cube`                                       |
| View                                                    | [Dataset](/docs/generated/metamodel/entities/dataset)     | Subtype `View`                                       |
| Measure                                                 | Schema Field                                                                      | Tagged `Measure`; aggregation in native type         |
| Dimension                                               | Schema Field                                                                      | Tagged `Dimension`; primary keys marked as key       |
| `format` / `drillMembers` / `cumulative`                | Schema Field `jsonProps`                                                          | Measure presentation hints                           |
| `joins` / `hierarchies` / `folders` / `preAggregations` | Dataset custom properties                                                         | Structural model metadata                            |
| `public` / `isVisible`                                  | Ingestion filter                                                                  | Hidden cubes/members skipped unless `include_hidden` |
| `table_references` / cube `sql`                         | [Lineage](/docs/features/feature-guides/lineage)          | Lineage to upstream warehouse tables                 |
| View member `aliasMember`                               | Fine-Grained Lineage                                                              | Column-level view-to-cube lineage                    |
| `meta`                                                  | Tags / Terms / Owners / Domains                                                   | Mapped via `meta_mapping` / `column_meta_mapping`    |
| Report (Cube Cloud)                                     | [Chart](/docs/generated/metamodel/entities/chart)         | Input lineage to queried cubes/views                 |
| Workbook (Cube Cloud)                                   | [Dashboard](/docs/generated/metamodel/entities/dashboard) | Contains its reports' charts                         |


## Module `cube`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via `include_column_lineage`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Enabled via the `domain` config and `meta_mapping`. |
| Extract Ownership | ✅ | Enabled via `meta_mapping` against Cube `meta`. |
| Extract Tags | ✅ | Enabled via `meta_mapping`/`column_meta_mapping`, plus Measure/Dimension/Temporal field tags. |
| Glossary Terms | ✅ | Enabled via `meta_mapping`/`column_meta_mapping`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. Includes view->cube lineage and, where available, lineage to upstream warehouse tables. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `cube` module ingests the [Cube](https://cube.dev/) semantic layer data model into DataHub. Every cube and view is emitted as a dataset, with its measures and dimensions modelled as schema fields, organised under a container that represents the Cube deployment. The module works against both Cube Core and Cube Cloud.

### Prerequisites

#### Choose a deployment type

Set `deployment_type` to match your Cube installation:

- `CORE` — a self-hosted [Cube Core](https://cube.dev/docs/product/deployment/core) instance. Metadata is read from the `/v1/meta` REST endpoint.
- `CLOUD` — a [Cube Cloud](https://cube.dev/docs/product/deployment/cloud) deployment. When `use_metadata_api` is enabled, the connector reads from the [Metadata API](https://cube.dev/docs/product/apis-integrations/core-data-apis/rest-api/reference#metadata-api), which additionally exposes lineage to upstream warehouse tables. If the supplied token lacks the required scope, the connector automatically falls back to `/v1/meta`.

#### Obtain an API token

The connector authenticates with a token sent in the `Authorization` header.

- **Cube Core**: generate a JWT signed with your deployment's `CUBEJS_API_SECRET`. See [Security context](https://cube.dev/docs/product/auth).
- **Cube Cloud** (`/v1/meta`): copy a token from the deployment's **Playground → API** tab, or sign one with the deployment's API secret.
- **Cube Cloud Metadata API**: obtain a token via the [Control Plane API](https://cube.dev/docs/product/apis-integrations/control-plane-api). This token is required for warehouse lineage.

#### Configure the API URL

`api_url` is the base URL of the REST API, including the base path (defaults to `/cubejs-api`):

- Cube Core: `http://localhost:4000/cubejs-api`
- Cube Cloud: `https://<deployment>.cubecloud.dev/cubejs-api`

#### Warehouse lineage (optional)

To connect cubes to the warehouse tables they read from, set `warehouse_platform` (e.g. `snowflake`, `bigquery`, `postgres`) and, if your existing datasets use them, `warehouse_platform_instance` and `warehouse_env`. On Cube Cloud with the Metadata API enabled, the warehouse platform and database are auto-detected from the deployment's data sources. On Cube Core, set `parse_sql_for_lineage` to derive table lineage from each cube's SQL definition (requires `warehouse_platform`).

Note that cubes marked `public: false` are not returned by the `/v1/meta` endpoint, so views that reference them will still produce lineage edges to those cubes even though the cubes themselves are not ingested.


### Install the Plugin
```shell
pip install 'acryl-datahub[cube]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: cube
  config:
    # Base URL of the Cube REST API, including the base path.
    api_url: "https://your-deployment.cubecloud.dev/cubejs-api"
    api_token: "${CUBE_API_TOKEN}"

    # CORE (self-hosted) or CLOUD.
    deployment_type: "CLOUD"

    # Connect cubes to their upstream warehouse tables. Auto-detected on Cube
    # Cloud via the Metadata API; set explicitly for Cube Core.
    # warehouse_platform: "snowflake"
    # warehouse_database: "ANALYTICS"

    # Cube Cloud only: ingest reports as charts and workbooks as dashboards, and
    # auto-mint a Metadata API token. cloud_api_key + deployment_id are required;
    # environment_id is needed only for the Metadata API token.
    # cloud_api_key: "${CUBE_CLOUD_API_KEY}"
    # deployment_id: "12345"
    # environment_id: "production"

    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | API token used to authenticate against Cube. For Cube Core this is a JWT signed with `CUBEJS_API_SECRET`; for the Cube Cloud Metadata API use a token obtained from the Control Plane API.  |
| <div className="path-line"><span className="path-main">api_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL of the Cube REST API, including the base path. For Cube Core this is typically `http://localhost:4000/cubejs-api`; for Cube Cloud it looks like `https://<name>.cubecloud.dev/cubejs-api`.  |
| <div className="path-line"><span className="path-main">cloud_api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Cube Cloud Control Plane API key (Account → API keys). When set together with `deployment_id` and `environment_id`, the connector automatically mints a metadata-scoped JWT via the Control Plane `tokens-for-meta-sync` endpoint to access the Metadata API, instead of requiring a pre-generated token in `api_token`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">cloud_api_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Base URL of the Cube Cloud Control Plane API (e.g. `https://<tenant>.cubecloud.dev`). If unset, it is derived from the scheme and host of `api_url`. Only used when `cloud_api_key` is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">column_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,object)</span></div> |   |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lowercase upstream warehouse table and column names when building lineage URNs. Must match the `convert_urns_to_lowercase` setting of the warehouse connector (e.g. Snowflake ingests lowercased URNs by default) so that the lineage edges resolve. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">deployment_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Cube Cloud deployment id, used to mint a Metadata API token via the Control Plane API. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">deployment_type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "CORE", "CLOUD"  |
| <div className="path-line"><span className="path-main">deployment_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Base URL of the Cube deployment UI, used to build an external link on the deployment container. If unset, it is derived from `api_url` by stripping the API base path. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_member_details</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to capture Cube member presentation hints (format, drill-down members, cumulative flag) as schema-field `jsonProps`, and structural metadata (joins, hierarchies, folders, pre-aggregations) as dataset custom properties. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to process `meta_mapping` and `column_meta_mapping` rules. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">environment_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Cube Cloud environment id, used to mint a Metadata API token via the Control Plane API. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit column-level (fine-grained) lineage. Requires `include_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_cubes</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest base cubes as datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_hidden</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest cubes, views, and members that Cube marks as hidden (`public: false` / `isVisible: false`). Hidden cubes are typically excluded from Cube's own API consumers; enable this to surface them in DataHub anyway. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit lineage. This includes view->cube lineage and, where available, lineage from cubes to their upstream warehouse tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Cube Cloud only. Whether to ingest saved reports as DataHub charts, with lineage to the cubes/views they query. Requires Platform API access (`cloud_api_key` + `deployment_id`). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest views as datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_workbooks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Cube Cloud only. Whether to ingest workbooks as DataHub dashboards containing their reports' charts. Requires Platform API access (`cloud_api_key` + `deployment_id`). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">meta_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,object)</span></div> |   |
| <div className="path-line"><span className="path-main">meta_sync_token_expires_in</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Expiry (in seconds) of the minted Metadata API token. Defaults to 24 hours. <div className="default-line default-line-with-docs">Default: <span className="default-value">86400</span></div> |
| <div className="path-line"><span className="path-main">parse_sql_for_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Cube Core only. When the `/v1/meta?extended` response includes a cube's SQL definition, parse it to derive upstream warehouse lineage. Requires `warehouse_platform` to be set. The Cloud Metadata API provides lineage directly, so this is ignored for Cube Cloud. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">request_timeout_sec</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Per-request timeout, in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">security_context</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Security context embedded in the minted Metadata API token. Controls which parts of the data model are visible, following Cube's multi-tenancy rules.  |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to strip the email domain from owners derived via `meta_mapping`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to tag schema fields with `Measure`/`Dimension` (and `Temporal` for time dimensions) so the kinds of Cube members can be distinguished and filtered in DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Prefix added to tags created via `meta_mapping`. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">use_metadata_api</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Cube Cloud only. When enabled, the richer Metadata API (`/v1/entities`) is used to extract warehouse and column-level lineage, which is merged with the structural metadata from `/v1/meta`. When disabled, only the `/v1/meta` endpoint is used. Has no effect for Cube Core deployments. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database name to prepend to upstream warehouse table references that do not already include one. If unset, it is taken from the Cube data source definition when available. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">warehouse_env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment of the upstream warehouse datasets referenced by lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">warehouse_platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform name of the warehouse that backs the Cube data model (e.g. `snowflake`, `bigquery`, `postgres`). Used to build upstream lineage URNs. If unset, it is auto-detected from the Cube data source type when the Metadata API is available. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">warehouse_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance of the upstream warehouse, used when building lineage URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">cube_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">cube_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">report_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workbook_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workbook_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "CubeDeploymentType": {
      "enum": [
        "CORE",
        "CLOUD"
      ],
      "title": "CubeDeploymentType",
      "type": "string"
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
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
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
      "description": "Stateful ingestion configuration."
    },
    "api_url": {
      "description": "Base URL of the Cube REST API, including the base path. For Cube Core this is typically `http://localhost:4000/cubejs-api`; for Cube Cloud it looks like `https://<name>.cubecloud.dev/cubejs-api`.",
      "title": "Api Url",
      "type": "string"
    },
    "api_token": {
      "description": "API token used to authenticate against Cube. For Cube Core this is a JWT signed with `CUBEJS_API_SECRET`; for the Cube Cloud Metadata API use a token obtained from the Control Plane API.",
      "format": "password",
      "title": "Api Token",
      "type": "string",
      "writeOnly": true
    },
    "deployment_type": {
      "$ref": "#/$defs/CubeDeploymentType",
      "default": "CORE",
      "description": "Whether the target is Cube Core (`CORE`) or Cube Cloud (`CLOUD`)."
    },
    "use_metadata_api": {
      "default": true,
      "description": "Cube Cloud only. When enabled, the richer Metadata API (`/v1/entities`) is used to extract warehouse and column-level lineage, which is merged with the structural metadata from `/v1/meta`. When disabled, only the `/v1/meta` endpoint is used. Has no effect for Cube Core deployments.",
      "title": "Use Metadata Api",
      "type": "boolean"
    },
    "cloud_api_key": {
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
      "description": "Cube Cloud Control Plane API key (Account \u2192 API keys). When set together with `deployment_id` and `environment_id`, the connector automatically mints a metadata-scoped JWT via the Control Plane `tokens-for-meta-sync` endpoint to access the Metadata API, instead of requiring a pre-generated token in `api_token`.",
      "title": "Cloud Api Key"
    },
    "cloud_api_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Base URL of the Cube Cloud Control Plane API (e.g. `https://<tenant>.cubecloud.dev`). If unset, it is derived from the scheme and host of `api_url`. Only used when `cloud_api_key` is set.",
      "title": "Cloud Api Url"
    },
    "deployment_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Cube Cloud deployment id, used to mint a Metadata API token via the Control Plane API.",
      "title": "Deployment Id"
    },
    "environment_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Cube Cloud environment id, used to mint a Metadata API token via the Control Plane API.",
      "title": "Environment Id"
    },
    "security_context": {
      "additionalProperties": true,
      "description": "Security context embedded in the minted Metadata API token. Controls which parts of the data model are visible, following Cube's multi-tenancy rules.",
      "title": "Security Context",
      "type": "object"
    },
    "meta_sync_token_expires_in": {
      "default": 86400,
      "description": "Expiry (in seconds) of the minted Metadata API token. Defaults to 24 hours.",
      "title": "Meta Sync Token Expires In",
      "type": "integer"
    },
    "request_timeout_sec": {
      "default": 30,
      "description": "Per-request timeout, in seconds.",
      "title": "Request Timeout Sec",
      "type": "integer"
    },
    "include_cubes": {
      "default": true,
      "description": "Whether to ingest base cubes as datasets.",
      "title": "Include Cubes",
      "type": "boolean"
    },
    "include_views": {
      "default": true,
      "description": "Whether to ingest views as datasets.",
      "title": "Include Views",
      "type": "boolean"
    },
    "include_reports": {
      "default": true,
      "description": "Cube Cloud only. Whether to ingest saved reports as DataHub charts, with lineage to the cubes/views they query. Requires Platform API access (`cloud_api_key` + `deployment_id`).",
      "title": "Include Reports",
      "type": "boolean"
    },
    "include_workbooks": {
      "default": true,
      "description": "Cube Cloud only. Whether to ingest workbooks as DataHub dashboards containing their reports' charts. Requires Platform API access (`cloud_api_key` + `deployment_id`).",
      "title": "Include Workbooks",
      "type": "boolean"
    },
    "cube_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering cubes to ingest (matched on the cube name)."
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering views to ingest (matched on the view name)."
    },
    "report_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering reports to ingest (matched on the report name)."
    },
    "workbook_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering workbooks to ingest (matched on the workbook name)."
    },
    "include_lineage": {
      "default": true,
      "description": "Whether to emit lineage. This includes view->cube lineage and, where available, lineage from cubes to their upstream warehouse tables.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Whether to emit column-level (fine-grained) lineage. Requires `include_lineage` to be enabled.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "parse_sql_for_lineage": {
      "default": true,
      "description": "Cube Core only. When the `/v1/meta?extended` response includes a cube's SQL definition, parse it to derive upstream warehouse lineage. Requires `warehouse_platform` to be set. The Cloud Metadata API provides lineage directly, so this is ignored for Cube Cloud.",
      "title": "Parse Sql For Lineage",
      "type": "boolean"
    },
    "warehouse_platform": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "DataHub platform name of the warehouse that backs the Cube data model (e.g. `snowflake`, `bigquery`, `postgres`). Used to build upstream lineage URNs. If unset, it is auto-detected from the Cube data source type when the Metadata API is available.",
      "title": "Warehouse Platform"
    },
    "warehouse_platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Platform instance of the upstream warehouse, used when building lineage URNs.",
      "title": "Warehouse Platform Instance"
    },
    "warehouse_env": {
      "default": "PROD",
      "description": "Environment of the upstream warehouse datasets referenced by lineage.",
      "title": "Warehouse Env",
      "type": "string"
    },
    "warehouse_database": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Database name to prepend to upstream warehouse table references that do not already include one. If unset, it is taken from the Cube data source definition when available.",
      "title": "Warehouse Database"
    },
    "convert_lineage_urns_to_lowercase": {
      "default": true,
      "description": "Whether to lowercase upstream warehouse table and column names when building lineage URNs. Must match the `convert_urns_to_lowercase` setting of the warehouse connector (e.g. Snowflake ingests lowercased URNs by default) so that the lineage edges resolve.",
      "title": "Convert Lineage Urns To Lowercase",
      "type": "boolean"
    },
    "deployment_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Base URL of the Cube deployment UI, used to build an external link on the deployment container. If unset, it is derived from `api_url` by stripping the API base path.",
      "title": "Deployment Url"
    },
    "tag_measures_and_dimensions": {
      "default": true,
      "description": "Whether to tag schema fields with `Measure`/`Dimension` (and `Temporal` for time dimensions) so the kinds of Cube members can be distinguished and filtered in DataHub.",
      "title": "Tag Measures And Dimensions",
      "type": "boolean"
    },
    "include_hidden": {
      "default": false,
      "description": "Whether to ingest cubes, views, and members that Cube marks as hidden (`public: false` / `isVisible: false`). Hidden cubes are typically excluded from Cube's own API consumers; enable this to surface them in DataHub anyway.",
      "title": "Include Hidden",
      "type": "boolean"
    },
    "emit_member_details": {
      "default": true,
      "description": "Whether to capture Cube member presentation hints (format, drill-down members, cumulative flag) as schema-field `jsonProps`, and structural metadata (joins, hierarchies, folders, pre-aggregations) as dataset custom properties.",
      "title": "Emit Member Details",
      "type": "boolean"
    },
    "enable_meta_mapping": {
      "default": true,
      "description": "Whether to process `meta_mapping` and `column_meta_mapping` rules.",
      "title": "Enable Meta Mapping",
      "type": "boolean"
    },
    "meta_mapping": {
      "additionalProperties": {
        "additionalProperties": true,
        "type": "object"
      },
      "description": "Mapping rules applied to the `meta` of each cube/view to derive tags, glossary terms, owners, domains, and documentation links. Uses the same syntax as the dbt connector's `meta_mapping`.",
      "title": "Meta Mapping",
      "type": "object"
    },
    "column_meta_mapping": {
      "additionalProperties": {
        "additionalProperties": true,
        "type": "object"
      },
      "description": "Mapping rules applied to the `meta` of each measure/dimension to derive schema-field tags and glossary terms.",
      "title": "Column Meta Mapping",
      "type": "object"
    },
    "tag_prefix": {
      "default": "",
      "description": "Prefix added to tags created via `meta_mapping`.",
      "title": "Tag Prefix",
      "type": "string"
    },
    "strip_user_ids_from_email": {
      "default": false,
      "description": "Whether to strip the email domain from owners derived via `meta_mapping`.",
      "title": "Strip User Ids From Email",
      "type": "boolean"
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "description": "Regex patterns matched against a cube/view name to assign it to a DataHub domain (keyed by domain id or urn).",
      "title": "Domain",
      "type": "object"
    }
  },
  "required": [
    "api_url",
    "api_token"
  ],
  "title": "CubeSourceConfig",
  "type": "object"
}
```





### Capabilities

The connector extracts the following metadata:

- **Cubes and views** as datasets, grouped under a container representing the deployment. The container links back to the deployment UI (derived from `api_url`, or set `deployment_url`).
- **Schema** — each measure and dimension becomes a schema field. Measures carry their aggregation type (e.g. `count`, `sum`) in the native data type; primary-key dimensions are flagged as part of the key. Fields are tagged `Measure` or `Dimension` — and `Temporal` for time dimensions (disable with `tag_measures_and_dimensions: false`).
- **Descriptions and properties** — titles, descriptions, segment names, source file name, and any custom `meta` defined in the model.
- **Structural metadata** — joins (with relationship), hierarchies (with levels), folders/nested folders (with members), and pre-aggregation names are captured as dataset custom properties (disable with `emit_member_details: false`).
- **Measure presentation hints** — each measure's `format`, drill-down members, and cumulative flag are stored on the schema field as `jsonProps`.
- **Hidden members** — cubes, views, and members marked `public: false` / `isVisible: false` are skipped by default; set `include_hidden: true` to ingest them.
- **Tags, glossary terms, owners, domains, and documentation links** — derived from the `meta` defined on cubes/views via `meta_mapping`, and from member `meta` via `column_meta_mapping` (same syntax as the dbt connector). Domains can also be assigned by name pattern via the `domain` config.
- **Reports and workbooks** (Cube Cloud only) — saved [reports](https://docs.cube.dev/api-reference/reports/list-reports) become DataHub **charts** with input lineage to the cubes/views they query, and [workbooks](https://docs.cube.dev/api-reference/workbooks/get-workbooks) become DataHub **dashboards** containing those charts. Owners and titles are carried across. Disable with `include_reports: false` / `include_workbooks: false`, and filter with `report_pattern` / `workbook_pattern`.

#### Lineage

Lineage is emitted when `include_lineage` is enabled (the default):

- **View to cube** — views are linked to the cubes they are built on, including column-level lineage derived from each member's `aliasMember`.
- **Cube to warehouse** — on Cube Cloud with the Metadata API, table and column references are read directly. On Cube Core, table-level lineage is parsed from each cube's SQL definition when `parse_sql_for_lineage` and `warehouse_platform` are set. Column-level lineage on Cube Core is best-effort: since `/v1/meta` does not expose per-member SQL, members are matched by name against the upstream table's columns as found in DataHub (so the warehouse must be ingested first, and members whose name differs from the underlying column — e.g. aggregate measures — are not linked).
- **Report and workbook to view** — on Cube Cloud, charts (reports) carry input lineage to the cubes/views in their query, and dashboards (workbooks) contain those charts, extending the chain to `warehouse → cube → view → chart → dashboard`.

Disable column-level lineage with `include_column_lineage: false`.

#### Cube Cloud authentication and metadata merging

On Cube Cloud the connector reads both endpoints and merges them: `/v1/meta` supplies the structural and presentation metadata (joins, hierarchies, folders, formats, visibility), while the [Metadata API](https://docs.cube.dev/reference/control-plane-api) (`/v1/entities`, `/v1/data-sources`) supplies warehouse and column-level lineage. This gives a Cloud ingestion the union of both.

The Metadata API requires a metadata-scoped JWT. You can either:

- Provide a pre-generated token in `api_token`, or
- Let the connector mint one automatically: set `cloud_api_key` (a Cube Cloud API key from Account → API keys) together with `deployment_id` and `environment_id`. The connector calls the Control Plane `tokens-for-meta-sync` endpoint to obtain a short-lived, metadata-only token. Override the Control Plane host with `cloud_api_url` if it differs from the `api_url` host, and embed a `security_context` to scope multi-tenant visibility.

If the Metadata API cannot be reached, the connector logs a warning and continues with `/v1/meta` only (structural metadata and view-to-cube lineage, but no warehouse lineage).

#### Reports and workbooks (Cube Cloud Platform API)

Reports and workbooks are read from the Cube Cloud [Platform API](https://docs.cube.dev/api-reference/introduction), which is authenticated with a Cube Cloud API key as a `Bearer` token. Set `cloud_api_key` and `deployment_id` to enable this (`environment_id` is _not_ required for reports/workbooks — it is only needed when minting a Metadata API token). When these are absent, or for Cube Core, report/workbook ingestion is skipped silently. A failed Platform API call logs a warning and does not abort the run.

#### Multi-tenancy and context variables

Cube [context variables](https://cube.dev/docs/reference/data-model/context-variables) (`COMPILE_CONTEXT`, `SECURITY_CONTEXT`, `FILTER_PARAMS`, `FILTER_GROUP`, `SQL_UTILS`) are data-model authoring constructs, not metadata the APIs expose as structured fields — there is nothing separate to ingest. They affect the connector only indirectly:

- **`COMPILE_CONTEXT` (multi-tenancy).** Cube compiles a different data model per security context. The connector ingests the single compiled model that matches the security context carried by its token: set `security_context` when minting a token via the Control Plane API, or rely on the claims baked into a directly-supplied `api_token`. To catalog multiple tenants, run one ingestion per tenant — but their cubes and views share names, so distinguish them with `platform_instance` / `env` (or `cube_pattern` / `view_pattern`) to avoid URN collisions.
- **`FILTER_PARAMS` / `SQL_UTILS` in cube SQL.** The SQL returned by `/v1/meta` is already compiled (`FILTER_PARAMS` render to their defaults and `COMPILE_CONTEXT` is resolved), so Cube Core SQL lineage parsing operates on the resolved SQL and is wrapped defensively if a template still cannot be parsed. On Cube Cloud the Metadata API returns resolved `table_references` / `column_references`, so templating is irrelevant there.

### Limitations

- The `/v1/meta` endpoint does not return cubes or views marked `public: false`. On Cube Cloud the Metadata API may still return them (and the connector merges them in); on Cube Core such cubes are not ingested as datasets, though lineage edges to them are still emitted.
- Warehouse lineage on Cube Cloud requires a metadata-scoped token for the Metadata API (supplied via `api_token`, or minted automatically with `cloud_api_key` + `deployment_id` + `environment_id`). Without it, the connector falls back to `/v1/meta` and only view-to-cube lineage is available.
- The Control Plane **audit-logs export** and **Orchestration API** (pre-aggregation build jobs) are intentionally not used — they are operational/governance surfaces rather than data-catalog metadata, and the audit-logs export is an Enterprise-only CSV stream.
- Column-level lineage on Cube Core relies on member names matching the warehouse column names (Cube's default convention) and on the upstream table's schema already being present in DataHub. Members backed by a renamed or computed expression (e.g. `total_amount` over `amount`, or any aggregate measure) are not column-linked, since Cube Core's `/v1/meta` does not expose the underlying member SQL. Cube Cloud's Metadata API provides exact references and has no such limitation.
- **Usage statistics and query profiling are not ingested.** Cube does not expose query history through a pull API — it is only available via [Query History export](https://cube.dev/docs/product/administration/monitoring/query-history-export), which pushes logs to an external sink (e.g. S3). Ingesting that exported data would be a separate pipeline rather than a Metadata API feature.
- Pre-aggregation definitions are not exposed by Cube Core's `/v1/meta` (it returns only `measures`, `dimensions`, `segments`, `hierarchies`, and `folders`); they are an internal caching concern. Where a payload does include them, their names are captured as custom properties.

### Troubleshooting

#### "Required scope is missing" / Metadata API falls back to `/v1/meta`

The configured `api_token` is a regular REST/data token rather than a metadata-scoped token. Either set `cloud_api_key` + `deployment_id` + `environment_id` so the connector mints a metadata token via the Control Plane API, supply a pre-generated metadata token in `api_token`, or set `use_metadata_api: false` to silence the fallback warning.

#### No warehouse lineage appears

Confirm `warehouse_platform` is set (or auto-detected), and that the upstream datasets were ingested with the same `warehouse_platform_instance` and `warehouse_env` you configured here.

#### Warehouse lineage edges do not connect to existing datasets

When run against a DataHub instance (the usual case), the connector reconciles the casing of upstream warehouse table URNs and column names against what the warehouse connector actually ingested — it looks up the real schema in DataHub and snaps Cube's reported identifiers to it. This handles platforms that fold identifiers differently (Postgres/Redshift lower-case, Snowflake upper-case, BigQuery case-sensitive) without per-platform configuration.

When the upstream schema is not yet in DataHub (e.g. the warehouse has not been ingested, or a dry run with no server), there is nothing to reconcile against, so the connector falls back to its configured behaviour: it lowercases upstream warehouse table and column names by default. If the warehouse connector was configured with `convert_urns_to_lowercase: false`, set `convert_lineage_urns_to_lowercase: false` here so the fallback URNs match. Ingesting the warehouse first is the most reliable fix.


### Code Coordinates
- Class Name: `datahub.ingestion.source.cube.cube.CubeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/cube/cube.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Cube, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
