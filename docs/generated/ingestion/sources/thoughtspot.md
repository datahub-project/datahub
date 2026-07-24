


# ThoughtSpot

## Overview

[ThoughtSpot](https://www.thoughtspot.com/) is a search- and AI-driven analytics platform used to author Liveboards (interactive dashboards), Answers (ad-hoc queries), and Worksheets (semantic models) over an organization's data warehouse.

The DataHub integration for ThoughtSpot ingests Liveboards as dashboards, Answers and Visualizations as charts, and Worksheets and Tables as datasets. When the ingestion principal can enumerate Orgs, those entities sit under Workspace containers; otherwise the catalog is flat.

Lineage is captured at both the table and column level: from Visualizations and Answers back to their Worksheets, and from federated Worksheets out to the underlying warehouse (Databricks, Snowflake, BigQuery, and similar). The connector also extracts ownership, ThoughtSpot tags, optional view counts, and uses stateful ingestion to soft-delete entities that disappear between runs.

## Concept Mapping

| ThoughtSpot Concept     | DataHub Concept                                 | Notes                                                                                                                                                             |
| ----------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace (Org)         | Container (`Workspace` subtype)                 | Parent of Liveboards, Answers, and Worksheets. Requires system-org admin to enumerate via `/orgs/search`.                                                         |
| Liveboard               | Dashboard (`Liveboard` subtype)                 | Formerly called "Pinboard"; the REST API still uses `pinboard` in some legacy field names.                                                                        |
| Answer                  | Chart (`Answer` subtype)                        | Standalone saved searches.                                                                                                                                        |
| Visualization           | Chart (`Visualization` subtype)                 | Charts/tiles embedded inside a Liveboard.                                                                                                                         |
| Worksheet               | Dataset (`Worksheet` subtype)                   | TS's semantic layer — joined logical model over connection tables.                                                                                                |
| Logical View / SQL View | Dataset (`View` subtype)                        | TS-side SQL definitions on top of a connection's schema.                                                                                                          |
| Logical Table           | Dataset (`Table` subtype)                       | Direct one-to-one mapping of a physical source table.                                                                                                             |
| Tag                     | GlobalTag                                       | Resolved to `urn:li:tag:<name>` with URL-encoded names.                                                                                                           |
| Owner / Author          | CorpUser                                        | DATAOWNER ownership; UUIDs in the `author.name` field are skipped (not real usernames).                                                                           |
| Column source reference | UpstreamLineage + FineGrainedLineage            | Worksheet→Table column-level lineage from TS's pre-resolved `columns[*].sources` field on `metadata/search`.                                                      |
| Federated connection    | UpstreamLineage (cross-platform)                | Worksheet → external `databricks` / `snowflake` / `bigquery` / etc. URN with table- and column-level edges; configured per-connection via `external_connections`. |
| View count              | DashboardUsageStatistics / ChartUsageStatistics | Cumulative lifetime counter from `metadata_search` `include_stats`. Emitted by default; set `include_usage_stats: false` to skip.                                 |


## Module `thoughtspot`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Workspaces (Orgs) emit as containers. |
| Column-level Lineage | ✅ | Column-level lineage from TS's pre-resolved `columns[*].sources` field on `metadata/search` (TS-internal) and from `physicalColumnName` (cross-platform external). TML edocs are consulted separately for chart-layer details (search query, source-table FQNs, chart type), not for column-level lineage. |
| Dataset Usage | ✅ | Per-entity cumulative view counts via `metadata_search` `include_stats`; enabled by `include_usage_stats: true`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default, configured via `include_ownership`. |
| Extract Tags | ✅ | Resolved from `/tags/search` and emitted as GlobalTags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Worksheet→Table, Chart→Worksheet, Dashboard→Worksheet, plus cross-platform Worksheet→external warehouse (Databricks / Snowflake / BigQuery / ...) configured via `external_connections`. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `thoughtspot` module ingests ThoughtSpot Liveboards, Answers, Worksheets, and Tables into DataHub via the REST API v2.0. It also extracts ownership, tags, per-entity view counts, and table- and column-level lineage from charts back to worksheets and out to your warehouse.

### Prerequisites

To get started you need:

1. A reachable ThoughtSpot instance (`https://<your-cluster>.thoughtspot.cloud`).
2. A ThoughtSpot user (or service account) with the permissions described below.
3. Trusted-auth credentials (recommended) or username + password.

#### Permissions

The ingestion user needs:

- **`Has data download permissions`** (`DATADOWNLOADING`) — required to read metadata.
- **`Has administration privileges`** (`ADMINISTRATION`) — recommended; covers Org enumeration and other admin-gated APIs.
- **Read access** on each Liveboard, Answer, Worksheet, and Table you want to ingest. In the ThoughtSpot UI, open the object → **Share** → grant the user (or a group it belongs to) at least **Can view** access. Or set the ingestion user as the owner.

This is enough for a working catalog: worksheet schemas, column-level lineage from worksheets to tables, cross-platform lineage to your warehouse, dashboard-to-dataset lineage, ownership, and tags all emit from `metadata/search`, which is gated only by ordinary read access on the object itself.

:::tip For full chart-level lineage, also share dependencies
The chart-tile layer under a Liveboard and the per-Answer source-table lineage are enriched from ThoughtSpot's `metadata/tml/export` endpoint, which walks the full dependency graph of each Worksheet (connection → joined tables → referenced answers) and returns `FORBIDDEN` if any one is invisible to the principal. Granting `ADMINISTRATION` does **not** override these per-object ACLs.

If TML access is missing for a given object, the connector still emits the entity itself and falls back to dashboard-level lineage — only the chart tiles and per-Answer edges are skipped, and a structured warning names every affected object so you can tell exactly what's missing. See [Lineage Coverage](#lineage-coverage) and the [TML troubleshooting](#charts-disappear-with-a-liveboard-visualization-fetch-failed-warning) section.
:::

#### Authentication

Trusted authentication is recommended for production. Generate the secret key under **Develop > Customizations > Security Settings > Enable Trusted Authentication**:

```yaml
source:
  type: thoughtspot
  config:
    connection:
      base_url: "https://your-cluster.thoughtspot.cloud"
      auth:
        type: trusted
        username: "${THOUGHTSPOT_USERNAME}"
        secret_key: "${THOUGHTSPOT_SECRET_KEY}"
```

Password authentication is also supported — set `type: password` and provide `password:` instead of `secret_key:`. Both methods mint a short-lived bearer token per run via `auth_token_full`, so the credential on disk stays stable across token expiry. Pre-generated bearer tokens are not accepted (see [Limitations](#bearer-token-auth-is-not-supported)).


### Install the Plugin
```shell
pip install 'acryl-datahub[thoughtspot]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: thoughtspot
  config:
    connection:
      base_url: "https://your-cluster.thoughtspot.cloud"

      # Trusted authentication (recommended for production).
      # Generate the secret key under:
      # Develop > Customizations > Security Settings > Enable Trusted Authentication
      auth:
        type: trusted
        username: "${THOUGHTSPOT_USERNAME}"
        secret_key: "${THOUGHTSPOT_SECRET_KEY}"

      # (Optional) Scope ingestion to a specific org in multi-org tenants.
      # Use the numeric org id or the org name. Leave unset for single-org
      # deployments or to use the principal's primary org.
      # org_identifier: "production"

      # (Optional) HTTP request tuning.
      # timeout_seconds: 30        # default 30
      # max_retries: 3             # default 3; 429 / 5xx retried with exponential backoff

    # (Optional) Filter entities by name. Defaults allow everything.
    # liveboard_pattern:
    #   allow:
    #     - "^Customer.*"
    #   deny:
    #     - "^Draft.*"
    # answer_pattern:
    #   allow:
    #     - ".*"
    # worksheet_pattern:
    #   allow:
    #     - ".*"

    # (Optional) Server-side tag filters. Only ingest Liveboards / Answers
    # tagged with at least one of the listed tags. Cheaper than client-side
    # name filtering at scale; tag names are case-sensitive.
    # liveboard_tag_filter:
    #   - "Production"
    # answer_tag_filter:
    #   - "Curated"

    include_ownership: true

    # Per-entity view counts via metadata_search include_stats (same counter as
    # the TS UI "Views" column). Enabled by default; set to ``false`` to skip
    # emitting the usage aspects entirely.
    include_usage_stats: true

    # Cross-platform upstream lineage from TS Worksheets to the underlying
    # Databricks / Snowflake / BigQuery / etc. tables. On by default.
    include_external_lineage: true

    # (Optional) Per-TS-connection settings for cross-platform lineage. Keys
    # are the TS connection GUID (stable across renames) or display name.
    # external_connections:
    #   conn-guid-prod-dbx:
    #     # Must match the platform_instance your DataHub Databricks
    #     # ingestion uses, otherwise the upstream URNs won't resolve.
    #     platform_instance: prod-databricks
    #     env: PROD
    #   "Prod BigQuery":
    #     platform_instance: prod-bq
    #     # BigQuery preserves source case in column URNs — opt in here.
    #     # Default is to lowercase external column names (Databricks /
    #     # Snowflake / Postgres / MySQL / Redshift convention).
    #     preserve_column_case: true

    # Stateful ingestion soft-deletes entities that disappear between runs.
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

# Default sink is datahub-rest and doesn't need to be configured.
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options.

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">connection</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">ThoughtSpotConnectionConfig</span></div> | Connection configuration for ThoughtSpot REST API v2.0.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">base_url</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ThoughtSpot instance URL (e.g., 'https://my-company.thoughtspot.cloud'). Do not include '/api/rest/2.0' - it will be added automatically.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">auth</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">One of TrustedAuth, PasswordAuth</span></div> | Authentication block. Use ``type: trusted`` with ``username`` + ``secret_key`` (recommended for production) or ``type: password`` with ``username`` + ``password``. Both modes mint short-lived bearer tokens per ingestion run.  |
| <div className="path-line"><span className="path-prefix">connection.auth.</span><span className="path-main">password</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Password for the ThoughtSpot user.  |
| <div className="path-line"><span className="path-prefix">connection.auth.</span><span className="path-main">secret_key</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Secret key generated in ThoughtSpot under Develop > Customizations > Security Settings > Enable Trusted Authentication. Supports ``${THOUGHTSPOT_SECRET_KEY}``.  |
| <div className="path-line"><span className="path-prefix">connection.auth.</span><span className="path-main">username</span>&nbsp;<abbr title="Required if auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ThoughtSpot user to impersonate.  |
| <div className="path-line"><span className="path-prefix">connection.auth.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Const value: trusted <div className="default-line default-line-with-docs">Default: <span className="default-value">trusted</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retry attempts for failed API requests. Uses exponential backoff between retries. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">metadata_fetch_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of object IDs sent in a single ``metadata/search`` detail fetch (used to backfill column-level schema for upstream tables). Chunked for the same reason as ``tml_export_batch_size`` — avoiding per-request body-size limits on large tenants. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">org_identifier</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional ThoughtSpot org identifier — numeric org id (e.g. ``"615000845"``) or org name (e.g. ``"datahub"``). Forwarded to every metadata API call so the ingestion is scoped to a single org. Leave unset to use the principal's primary org (the default). Single-tenant deployments can ignore this field. If the principal is not a member of the specified org, API calls will return 403 and the run will fail with a permission error — this is intentional, since silently falling back to the primary org would emit data into the wrong namespace. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Request timeout in seconds. Increase if you experience timeout errors with large metadata responses. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">tml_export_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of object IDs sent in a single TML export request. ThoughtSpot's TML export bundles full YAML edocs per object; at ~1000+ liveboards in one POST the response can exceed server limits and time out. The connector chunks IDs into batches of this size; per-batch failures emit a structured warning, never a silent drop. Default 100 halves the TML round-trips vs the previous 50 at 10K+ scale (saves several minutes of wall-clock) and still leaves a 10x safety margin below the documented limit. Lower to 50 (or less) if your TS deployment is memory-constrained or you've observed batch timeouts. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">include_external_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit upstream lineage from TS Logical Tables to external sources (Databricks, Snowflake, BigQuery, etc.). Disable to keep dataset lineage scoped to ThoughtSpot only. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract ownership information from ThoughtSpot. Owners are mapped to DataHub CorpUser entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit per-entity view counts as ``DashboardUsageStatistics`` / ``ChartUsageStatistics`` aspects. View counts are the global cumulative counter returned by ``metadata_search`` when ``include_stats=True`` is sent — the same number shown in the TS UI's 'Views' column. No additional API round-trip is performed: the data is piggybacked on the same paginated ``metadata_search`` calls that build the entities. Enabled by default since the cost is ~50 bytes of wire bytes per entity. Set to ``false`` to skip emitting the usage aspects entirely. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">answer_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">answer_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">answer_tag_filter</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Optional list of TS tag names to filter Answers on the server side. Same shape as ``liveboard_tag_filter``. Leave unset to ingest all Answers (subject to ``answer_pattern``). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">answer_tag_filter.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">external_connections</span></div> <div className="type-name-line"><span className="type-name">map(str,ExternalConnectionConfig)</span></div> | Per-TS-connection cross-platform lineage settings. <br />  <br /> All fields are optional — a connection only needs an entry under <br /> ``external_connections`` if at least one override is needed. <br /> Connections absent from the map use the defaults defined here <br /> (no platform_instance, connector-level ``env``, column names <br /> lowercased).  |
| <div className="path-line"><span className="path-prefix">external_connections.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub ``platform_instance`` to apply to the upstream URN for this TS connection's federated tables. Default: no platform_instance prefix on emitted URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">external_connections.`key`.</span><span className="path-main">preserve_column_case</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Preserve column-name case in emitted cross-platform fine-grained lineage URNs instead of lowercasing. Default ``false`` because most DataHub source connectors lowercase column URNs (Databricks, Snowflake, Postgres, MySQL, Redshift, Hive). Set to ``true`` for BigQuery, SQL Server, or Databricks tenants using case-quoted identifiers — anywhere the upstream connector preserves case. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">external_connections.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub ``env`` (PROD / STAGING / DEV / ...) override for this TS connection's federated tables. Default: inherits the connector-level ``env``. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">liveboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">liveboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">liveboard_tag_filter</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Optional list of TS tag names to filter Liveboards on the server side. When set, only Liveboards tagged with at least one of these tags are returned by ``metadata_search``. More efficient than client-side name filtering for tenants where the ingestion principal can see far more Liveboards than the desired subset. Example: ``["Production", "Curated"]``. Leave unset to ingest all Liveboards (subject to ``liveboard_pattern``). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">liveboard_tag_filter.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">worksheet_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">worksheet_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion config with stale-entity removal support. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "ExternalConnectionConfig": {
      "additionalProperties": false,
      "description": "Per-TS-connection cross-platform lineage settings.\n\nAll fields are optional \u2014 a connection only needs an entry under\n``external_connections`` if at least one override is needed.\nConnections absent from the map use the defaults defined here\n(no platform_instance, connector-level ``env``, column names\nlowercased).",
      "properties": {
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
          "description": "DataHub ``platform_instance`` to apply to the upstream URN for this TS connection's federated tables. Default: no platform_instance prefix on emitted URNs.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub ``env`` (PROD / STAGING / DEV / ...) override for this TS connection's federated tables. Default: inherits the connector-level ``env``.",
          "title": "Env"
        },
        "preserve_column_case": {
          "default": false,
          "description": "Preserve column-name case in emitted cross-platform fine-grained lineage URNs instead of lowercasing. Default ``false`` because most DataHub source connectors lowercase column URNs (Databricks, Snowflake, Postgres, MySQL, Redshift, Hive). Set to ``true`` for BigQuery, SQL Server, or Databricks tenants using case-quoted identifiers \u2014 anywhere the upstream connector preserves case.",
          "title": "Preserve Column Case",
          "type": "boolean"
        }
      },
      "title": "ExternalConnectionConfig",
      "type": "object"
    },
    "PasswordAuth": {
      "additionalProperties": false,
      "description": "Basic-authentication mode.\n\nThe connector mints a short-lived bearer token per ingestion run via\n``auth_token_full`` using the user's password. Prefer ``TrustedAuth``\nfor production because password rotation policies tend to break\nscheduled ingestion.",
      "properties": {
        "type": {
          "const": "password",
          "default": "password",
          "title": "Type",
          "type": "string"
        },
        "username": {
          "description": "ThoughtSpot username.",
          "title": "Username",
          "type": "string"
        },
        "password": {
          "description": "Password for the ThoughtSpot user.",
          "format": "password",
          "title": "Password",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "username",
        "password"
      ],
      "title": "PasswordAuth",
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
    "ThoughtSpotConnectionConfig": {
      "additionalProperties": false,
      "description": "Connection configuration for ThoughtSpot REST API v2.0.",
      "properties": {
        "base_url": {
          "description": "ThoughtSpot instance URL (e.g., 'https://my-company.thoughtspot.cloud'). Do not include '/api/rest/2.0' - it will be added automatically.",
          "title": "Base Url",
          "type": "string"
        },
        "auth": {
          "description": "Authentication block. Use ``type: trusted`` with ``username`` + ``secret_key`` (recommended for production) or ``type: password`` with ``username`` + ``password``. Both modes mint short-lived bearer tokens per ingestion run.",
          "discriminator": {
            "mapping": {
              "password": "#/$defs/PasswordAuth",
              "trusted": "#/$defs/TrustedAuth"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/TrustedAuth"
            },
            {
              "$ref": "#/$defs/PasswordAuth"
            }
          ],
          "title": "Auth"
        },
        "timeout_seconds": {
          "default": 30,
          "description": "Request timeout in seconds. Increase if you experience timeout errors with large metadata responses.",
          "exclusiveMinimum": 0,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "max_retries": {
          "default": 3,
          "description": "Maximum number of retry attempts for failed API requests. Uses exponential backoff between retries.",
          "minimum": 0,
          "title": "Max Retries",
          "type": "integer"
        },
        "tml_export_batch_size": {
          "default": 100,
          "description": "Maximum number of object IDs sent in a single TML export request. ThoughtSpot's TML export bundles full YAML edocs per object; at ~1000+ liveboards in one POST the response can exceed server limits and time out. The connector chunks IDs into batches of this size; per-batch failures emit a structured warning, never a silent drop. Default 100 halves the TML round-trips vs the previous 50 at 10K+ scale (saves several minutes of wall-clock) and still leaves a 10x safety margin below the documented limit. Lower to 50 (or less) if your TS deployment is memory-constrained or you've observed batch timeouts.",
          "exclusiveMinimum": 0,
          "title": "Tml Export Batch Size",
          "type": "integer"
        },
        "metadata_fetch_batch_size": {
          "default": 100,
          "description": "Maximum number of object IDs sent in a single ``metadata/search`` detail fetch (used to backfill column-level schema for upstream tables). Chunked for the same reason as ``tml_export_batch_size`` \u2014 avoiding per-request body-size limits on large tenants.",
          "exclusiveMinimum": 0,
          "title": "Metadata Fetch Batch Size",
          "type": "integer"
        },
        "org_identifier": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Optional ThoughtSpot org identifier \u2014 numeric org id (e.g. ``\"615000845\"``) or org name (e.g. ``\"datahub\"``). Forwarded to every metadata API call so the ingestion is scoped to a single org. Leave unset to use the principal's primary org (the default). Single-tenant deployments can ignore this field. If the principal is not a member of the specified org, API calls will return 403 and the run will fail with a permission error \u2014 this is intentional, since silently falling back to the primary org would emit data into the wrong namespace.",
          "title": "Org Identifier"
        }
      },
      "required": [
        "base_url",
        "auth"
      ],
      "title": "ThoughtSpotConnectionConfig",
      "type": "object"
    },
    "TrustedAuth": {
      "additionalProperties": false,
      "description": "Trusted-authentication mode (recommended for production).\n\nThe connector mints a short-lived bearer token per ingestion run via\n``auth_token_full`` using the ``secret_key`` as the long-lived credential.\nThe ``username`` identifies the user to impersonate.",
      "properties": {
        "type": {
          "const": "trusted",
          "default": "trusted",
          "title": "Type",
          "type": "string"
        },
        "username": {
          "description": "ThoughtSpot user to impersonate.",
          "title": "Username",
          "type": "string"
        },
        "secret_key": {
          "description": "Secret key generated in ThoughtSpot under Develop > Customizations > Security Settings > Enable Trusted Authentication. Supports ``${THOUGHTSPOT_SECRET_KEY}``.",
          "format": "password",
          "title": "Secret Key",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "username",
        "secret_key"
      ],
      "title": "TrustedAuth",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for ThoughtSpot metadata extraction.",
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
      "description": "Stateful ingestion config with stale-entity removal support."
    },
    "connection": {
      "$ref": "#/$defs/ThoughtSpotConnectionConfig",
      "description": "ThoughtSpot connection settings"
    },
    "workspace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter workspaces by name. Example: {'allow': ['^Production.*'], 'deny': ['^Test.*']}"
    },
    "liveboard_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter Liveboards (dashboards) by name. Example: {'allow': ['.*'], 'deny': ['^Draft.*']}"
    },
    "answer_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter Answers (saved queries) by name. Example: {'allow': ['.*'], 'deny': ['^Temp.*']}"
    },
    "worksheet_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter Worksheets (logical datasets) by name. Example: {'allow': ['.*'], 'deny': ['^Legacy.*']}"
    },
    "liveboard_tag_filter": {
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
      "description": "Optional list of TS tag names to filter Liveboards on the server side. When set, only Liveboards tagged with at least one of these tags are returned by ``metadata_search``. More efficient than client-side name filtering for tenants where the ingestion principal can see far more Liveboards than the desired subset. Example: ``[\"Production\", \"Curated\"]``. Leave unset to ingest all Liveboards (subject to ``liveboard_pattern``).",
      "title": "Liveboard Tag Filter"
    },
    "answer_tag_filter": {
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
      "description": "Optional list of TS tag names to filter Answers on the server side. Same shape as ``liveboard_tag_filter``. Leave unset to ingest all Answers (subject to ``answer_pattern``).",
      "title": "Answer Tag Filter"
    },
    "include_ownership": {
      "default": true,
      "description": "Extract ownership information from ThoughtSpot. Owners are mapped to DataHub CorpUser entities.",
      "title": "Include Ownership",
      "type": "boolean"
    },
    "include_usage_stats": {
      "default": true,
      "description": "Emit per-entity view counts as ``DashboardUsageStatistics`` / ``ChartUsageStatistics`` aspects. View counts are the global cumulative counter returned by ``metadata_search`` when ``include_stats=True`` is sent \u2014 the same number shown in the TS UI's 'Views' column. No additional API round-trip is performed: the data is piggybacked on the same paginated ``metadata_search`` calls that build the entities. Enabled by default since the cost is ~50 bytes of wire bytes per entity. Set to ``false`` to skip emitting the usage aspects entirely.",
      "title": "Include Usage Stats",
      "type": "boolean"
    },
    "external_connections": {
      "additionalProperties": {
        "$ref": "#/$defs/ExternalConnectionConfig"
      },
      "description": "Per-TS-connection cross-platform lineage settings. Keyed by TS connection GUID (stable across renames, preferred) or display name (operator-friendly). GUID matches take precedence. Each value is an ``ExternalConnectionConfig`` block holding the overrides for that connection \u2014 ``platform_instance``, ``env``, and ``preserve_column_case``. Connections absent from the map use the connector defaults.",
      "examples": [
        {
          "Prod BigQuery": {
            "platform_instance": "prod-bq",
            "preserve_column_case": true
          },
          "conn-guid-abc": {
            "env": "PROD",
            "platform_instance": "prod-dbx"
          }
        }
      ],
      "title": "External Connections",
      "type": "object"
    },
    "include_external_lineage": {
      "default": true,
      "description": "Emit upstream lineage from TS Logical Tables to external sources (Databricks, Snowflake, BigQuery, etc.). Disable to keep dataset lineage scoped to ThoughtSpot only.",
      "title": "Include External Lineage",
      "type": "boolean"
    }
  },
  "required": [
    "connection"
  ],
  "title": "ThoughtSpotConfig",
  "type": "object"
}
```





### Capabilities

The connector emits the following aspects on every supported entity:

- **`subTypes`** — `Liveboard`, `Visualization`, `Answer`, `Worksheet`, `View`, or `Table`, so the DataHub UI shows the ThoughtSpot-native label rather than a generic Dashboard/Chart/Dataset badge.
- **`browsePathsV2`** — entities are browsable under their parent Workspace when workspace enumeration is available.
- **`status`** — emitted by the SDK V2 entity classes so stateful stale-removal can soft-delete entities that disappear between runs.
- **`dataPlatformInstance`** — set when `platform_instance` is configured.
- **`ownership`** — populated from each entity's `author.name`; UUIDs (which TS sometimes substitutes for the username) are skipped.
- **`globalTags`** — every tag attached to a Liveboard/Answer/Worksheet is resolved to `urn:li:tag:<name>`. Tag colors and other tag-side metadata are not propagated.

#### Container Hierarchy

ThoughtSpot Workspaces (Orgs) emit as Container entities and parent the Liveboards, Answers, and Worksheets that live in them. Enumerating Orgs requires **Has administration privileges** at the system org (orgID 0); without it, the connector logs an INFO and emits a flat catalog (entities directly under `platform: thoughtspot` with no Workspace container above them).

#### Lineage Coverage

- **Worksheet → Connection tables** — table-level and column-level lineage from TS's pre-resolved `columns[*].sources` field on `metadata/search`. Each source reference becomes a `FineGrainedLineage` edge linking the Worksheet column to the underlying physical table column.
- **Worksheet → External warehouse (cross-platform)** — federated Worksheets emit upstream lineage past ThoughtSpot to the underlying Databricks / Snowflake / BigQuery / Redshift / etc. table at both table and column level. The URN is built from TS's `physicalDatabaseName` / `physicalSchemaName` / `physicalTableName` and routed via `external_connections` (see below).
- **Visualization → Worksheet** — table-level lineage from TML's `liveboard.visualizations[].answer.tables[].fqn`, plus column-level lineage from the bracketed `[col]` tokens in `answer.search_query` (rendered via the chart `inputFields` aspect).
- **Answer → Worksheet** — table-level lineage from TML's `answer.tables[].fqn`, plus column-level lineage from the bracketed `[col]` tokens in `answer.search_query` (rendered via the chart `inputFields` aspect). Same shape as the Visualization path, sourced from each Answer's TML export.
- **Liveboard → Worksheet (fallback)** — used when per-viz TML export is forbidden. The connector falls back to `reportContent...pinboardFilterDetails` for dashboard-level dataset lineage; the chart layer is lost but dashboard-to-dataset edges remain intact.

#### Cross-Platform Lineage

Cross-platform lineage is on by default (`include_external_lineage: true`). For the edges to land on the matching DataHub upstream dataset, configure `external_connections` with the `platform_instance` (and optionally `env`) that your existing Databricks / Snowflake / BigQuery / etc. ingestion uses:

```yaml
external_connections:
  # Keys are the TS connection GUID (stable across renames) or display name.
  conn-guid-prod-dbx:
    platform_instance: prod-databricks # match your DataHub Databricks ingestion
    env: PROD
  "Prod BigQuery":
    platform_instance: prod-bq
    # BigQuery preserves source case in column URNs — opt in here.
    preserve_column_case: true
```

Each entry accepts three optional fields:

- **`platform_instance`** — DataHub `platform_instance` to attach to the upstream URN. Match what your existing upstream ingestion uses, or omit to emit URNs without an instance prefix.
- **`env`** — Per-connection `env` override (e.g. `STAGING`). Defaults to the connector-level `env`.
- **`preserve_column_case`** — Default `false`, which lowercases external column names to match the canonical schemaField URN convention used by Databricks, Snowflake, Postgres, MySQL, Redshift, and Hive. Set to `true` for BigQuery, SQL Server, or any Databricks tenant using case-quoted identifiers.

Unlisted connections still emit cross-platform edges using connector-level defaults. Sub-field typos (e.g. `platfom_instance`) fail loud at config-load time. Connection-key typos surface as an `External Lineage Config Keys Unmatched` warning at ingestion time. Disable cross-platform lineage entirely with `include_external_lineage: false`.

#### Multi-Org Tenants

ThoughtSpot supports multi-org tenancy where a single cluster hosts multiple isolated orgs (each with their own liveboards, worksheets, and connections). By default the connector ingests from whichever org the principal is logged into. To scope a run to a single org, set `connection.org_identifier` to the numeric org id (e.g. `"615000845"`) or the org name (e.g. `"production"`). Multi-org coverage requires one recipe per org — there is no single-recipe multi-org mode, since that would conflict with the per-object permission gates ThoughtSpot enforces at the org boundary.

#### Usage Statistics

Per-Liveboard / per-Chart view counts emit by default as `DashboardUsageStatistics` / `ChartUsageStatistics` aspects. The count comes from `metadata_search`'s `include_stats` response — the same global counter the TS UI's "Views" column displays. No extra permission or API round-trip is required; the data piggybacks on the `metadata_search` calls that build each entity. Set `include_usage_stats: false` to skip emitting the aspects.

The connector also emits two custom properties when available:

- `thoughtspot_favorites` — number of users who have favourited the entity.
- `thoughtspot_last_accessed` — most recent access time (ISO 8601 UTC).

Counts are lifetime-cumulative. ThoughtSpot doesn't expose a time-bounded counter on this endpoint, so the `start_time`/`end_time` knobs from other connectors don't apply. For windowed usage, diff DataHub's own `dashboardUsageStatistics` timeseries across runs.

#### Visualizations vs. Answers

Each Liveboard contains one or more Visualizations (chart tiles), emitted as Chart entities with the `Visualization` subtype. Standalone Answers (independent saved searches) emit the `Answer` subtype. Both share the `Chart` entity type so the DataHub UI handles them uniformly.

#### Tags

The connector pulls the full tag list once per run via `/api/rest/2.0/tags/search` and emits a `GlobalTags` aspect on each entity whose `metadata_header.tags` references a known tag id. If the principal can't enumerate tags (the endpoint returns 403 or 404), the connector logs an INFO and continues — entities are emitted without tag aspects rather than failing the pipeline.

#### Tag-Based Filtering

To restrict ingestion to a subset of tagged entities, set `liveboard_tag_filter` and/or `answer_tag_filter` to a list of TS tag names. The filter is applied server-side on `metadata_search` — cheaper than fetching everything and discarding client-side, and useful for tenants where the ingestion principal can see far more entities than the desired subset.

```yaml
liveboard_tag_filter:
  - "Production"
  - "Curated"
answer_tag_filter:
  - "KPI"
```

Both filters are optional and independent — set one, the other, or both. Tag names are **case-sensitive** (`"Production"` and `"production"` are different tags); a typo'd filter that matches zero entities surfaces a `Tag filter matched 0 ...` warning in the run report so it doesn't silently produce an empty catalog. Tag-based filtering only narrows Liveboards / Answers; Worksheets and Tables are unaffected and stay subject to `worksheet_pattern`.

#### Stateful Ingestion

Set `stateful_ingestion.remove_stale_metadata: true` (with `enabled: true`) in your recipe to soft-delete entities that disappear between runs. The handler diffs each run's emitted URN set against the prior checkpoint and emits `status(removed=true)` MCPs for missing entities.

### Limitations

#### Container hierarchy requires system-org admin

Enumerating Workspaces uses ThoughtSpot's `/orgs/search` endpoint, which is gated by **Has administration privileges** at the system org (orgID 0). Regular org admins get a 403; the connector logs an INFO and continues with a flat catalog. This is intentional — peer connectors (Atlan, OpenMetadata) follow the same "use the native container API or fall back to flat" pattern rather than inventing synthetic containers.

#### Pinboard ⇄ Liveboard naming

ThoughtSpot renamed Pinboards to Liveboards around v8.4, but the REST API rename is incomplete. The connector handles the inconsistency in three places: the top-level entity (`metadata_type: LIVEBOARD`), the embedded `reportContent.sheets[].sheetContent.pinboardFilterDetails` JSON path used for fallback liveboard-level lineage, and UI deep-link URLs (still served under `/#/pinboard/{id}`). Older tutorials that reference Pinboards map cleanly to DataHub Dashboards with the `Liveboard` subtype.

#### Folders / Collections are not modelled

ThoughtSpot Collections (formerly Folders) are not exposed via the public v2 `metadata/search` API and are not currently emitted as containers.

#### Bearer-token auth is not supported

Recipes must use trusted auth (`username + secret_key`) or password auth (`username + password`). Pre-generated bearer tokens are deliberately rejected because their minute-to-hour TTL fails scheduled ingestion at the first expiry. Both supported modes mint fresh bearer tokens per run via `auth_token_full`.

### Troubleshooting

#### Charts disappear with a `Liveboard Visualization Fetch Failed` warning

This is the most common failure mode. ThoughtSpot's TML export endpoint enforces per-object ACLs that override admin-level privileges — the principal must have **direct sharing** on each Liveboard, its underlying Worksheets, the Worksheets' Connections, and every joined Table. A single missing share anywhere in the dependency tree triggers `FORBIDDEN: Cannot download TML due to lack of access to objects`.

Fix: in the ThoughtSpot UI, open each affected object → **Share** → grant the ingestion user (or its group) at least **Can view** access. `Has administration privileges` does **not** bypass per-object ACLs for TML export.

#### `0 workspaces scanned` in the ingestion report

Expected when the principal isn't a system-org admin. See [Container hierarchy requires system-org admin](#container-hierarchy-requires-system-org-admin) above — the catalog is flat in this case.

#### Multi-Org tenants ingest from the wrong org

Set `connection.org_identifier` to the numeric org id (e.g. `"615000845"`) or the org name (e.g. `"production"`). Without it, ingestion runs against the principal's primary org.

#### Usage statistics are missing

Usage statistics are emitted by default. If view counts are absent on emitted Liveboards/Charts:

- The recipe may have `include_usage_stats: false`. Remove that line or set it to `true`.
- The entity may have zero views. The connector deliberately skips emitting `viewsCount: 0` to avoid clobbering a non-zero count from a prior run.
- The principal may lack read access to the entity. Without read access the entity itself wouldn't be ingested — check the run report for parse failures or skipped entities.

If counts look lower than the TS UI on a recent TS version, file an issue with the version and a sample entity GUID.

#### Cross-platform upstream edges dangle in the lineage UI

The Worksheet shows the table-level edge to the warehouse but column-level edges resolve to `dangling` schemaField URNs. Two common causes:

1. **`platform_instance` mismatch.** The Worksheet's external URN must match the dataset URN your existing warehouse ingestion emits. Find the upstream's `platform_instance` in any of its DataHub URNs (the segment between the platform name and the table name) and set the matching value in `external_connections.<conn>.platform_instance`.
2. **Column-case mismatch.** TS often emits column names in upper case (`PET_ID`) but most upstream connectors lowercase column URNs (`pet_id`). The default behaviour lowercases external column names to match. If your upstream connector preserves case — BigQuery, SQL Server, or a Databricks tenant using case-quoted identifiers — set `preserve_column_case: true` on that connection's `external_connections` block.

#### `External Lineage Resolution Failed` warning

The connector found Worksheets that reference TS connections it couldn't read. Either grant `Can view` on the connection page in the TS UI, or remove the dead reference upstream.

#### `External Lineage Config Keys Unmatched` warning

A key in `external_connections` doesn't match any real TS connection — usually a typo. Verify the GUID or display name in the ThoughtSpot UI matches exactly. The warning lists the offending keys.


### Code Coordinates
- Class Name: `datahub.ingestion.source.thoughtspot.source.ThoughtSpotSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/thoughtspot/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for ThoughtSpot, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
