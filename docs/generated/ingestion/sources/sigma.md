


# Sigma

## Overview

Sigma is a business intelligence and analytics platform. Learn more in the [official Sigma documentation](https://www.sigmacomputing.com/).

The DataHub integration for Sigma covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table-level lineage, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Sigma       | Datahub                                                       | Notes                       |
| ----------- | ------------------------------------------------------------- | --------------------------- |
| `Workspace` | [Container](../../metamodel/entities/container.md)            | SubType `"Sigma Workspace"` |
| `Workbook`  | [Dashboard](../../metamodel/entities/dashboard.md)            | SubType `"Sigma Workbook"`  |
| `Page`      | [Dashboard](../../metamodel/entities/dashboard.md)            |                             |
| `Element`   | [Chart](../../metamodel/entities/chart.md)                    |                             |
| `Dataset`   | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Sigma Dataset"`   |
| `User`      | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted        |


## Module `sigma`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Sigma Workspace, Sigma Data Model. |
| Column-level Lineage | ✅ | Enabled by default. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default, configured using `ingest_owner`. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Containers, with each element as a Dataset
  inside the Container (`ingest_data_models` defaults to `true`; set it
  to `false` to skip Data Model ingestion). `UpstreamLineage` is emitted on each DM element Dataset for
  intra-DM, cross-DM, and external (warehouse / Sigma Dataset) upstream
  references. Workbook-to-DM connections are emitted as `ChartInfo.inputs`
  on the workbook chart entity (not as `UpstreamLineage` on the DM element
  Dataset). Personal-space DMs referenced from ingested DMs are
  discovered on demand and gated by `ingest_shared_entities` and
  `data_model_pattern`. Report counters under
  `data_model_*` / `element_dm_*` surface per-shape resolution outcomes.

  Notes:

  - Element Dataset URNs are keyed by the immutable Data Model UUID
    (`urn:li:dataset:(sigma,<dataModelId>.<elementId>,env)`) so
    attachments survive Sigma slug rotation; the slug is captured on
    `customProperties.dataModelUrlId`.
  - Column types are emitted as `NullType` with `nativeDataType: "unknown"` —
    Sigma's `/columns` API does not return per-column types today. Earlier
    pre-releases hardcoded `String`; that was a lie (no Sigma side confirms
    it) and has been softened so downstream type-aware features can tell
    "unknown" from "actually a string."
  - Column-level lineage is not emitted yet; `SchemaMetadata` is
    present, so CLL can be added later without re-ingestion.
  - Unresolved upstream references (Sigma Dataset not ingested, DM not
    reachable, element name not matched) are suppressed rather than
    fabricated as dangling URNs. External upstreams to Sigma Datasets
    only resolve when the referenced dataset is ingested in the same
    recipe run. Splitting Sigma Datasets and Data Models into separate
    recipes will leave those upstreams unresolved. The report splits
    these between `data_model_element_upstreams_unresolved_external`
    (target Sigma Dataset exists but wasn't ingested this run) and
    `data_model_element_upstreams_unknown_shape` (source*id shapes this
    release does not parse — cross-DM refs have their own dedicated
    `data_model_element_cross_dm_upstreams*\*`counters); the aggregate`data_model_element_upstreams_unresolved`
    is retained for dashboards that already read it. Re-run with
    broader patterns to materialize them.
  - When `ingest_data_models` is enabled, `/dataModels/{id}/elements`
    and `/columns` calls are issued per DM unconditionally, but the per-DM
    `/lineage` call is gated on `extract_lineage: true`. If you opt out
    of lineage at the workbook surface, the DM connector also stops
    hitting any `/lineage` endpoint — DM Containers, element Datasets,
    and `SchemaMetadata` are still emitted, but without `UpstreamLineage`.
  - The DM Container URN is keyed on `platform` and
    `platform_instance` but not `env`. Multi-environment deployments
    against the same tenant should set a distinct `platform_instance`
    per env so DM Containers do not collide on a single URN. Element
    Datasets are already env-scoped.
  - **Cross-DM lineage is name-based.** Sigma's `/lineage` endpoint
    identifies cross-DM producer elements by free-text `name` only
    (there is no stable element ID on the upstream node), so the
    resolver matches producer and consumer elements by
    case-insensitive `name`. Renaming an element in the producing DM
    between runs silently moves the edge: the old target loses the
    upstream and the new target may never match. The counter that
    ticks up is `data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known`
    — a non-zero value usually means an upstream rename, not a
    connector bug. For single-element producer DMs, the fallback
    above recovers the edge automatically; for multi-element DMs
    operators should treat a jump in that counter as a re-ingest
    prompt.
  - **Single-element cross-DM fallback.** When a DM element references
    another DM's element (`sourceIds = ["<otherDmUrlId>/<suffix>"]`) and
    the name bridge cannot match the consuming element's name against
    any producer-DM element, the resolver falls back to binding the
    edge to the producer's sole element _iff_ the producer DM contains
    exactly one element. This covers the CSV-upload / single-asset
    personal-DM case where either side has been renamed (by
    construction, there is only one possible target). The fallback bumps both
    `data_model_element_cross_dm_upstreams_resolved` (aggregate success
    count) and `data_model_element_cross_dm_upstreams_single_element_fallback`
    (sub-shape count), so it is distinguishable in the report.
    Semantics: if the producer DM is later edited to add a second
    element and the user renames the consumer to point at the new one,
    the fallback no longer fires and the previously-emitted edge will
    reflect whatever name matches at the next ingest — i.e. the edge
    tracks "current state of Sigma," not "state at edge-creation time."
    This matches how Sigma's own UI resolves the reference.
  - **Ambiguous cross-DM name matches.** When two or more elements in a
    producer DM share the same case-insensitive name, the resolver
    cannot determine which element is the intended upstream. It picks
    the lexicographically smallest Dataset URN among the candidates
    (stable and deterministic across runs) and increments
    `data_model_element_cross_dm_upstreams_ambiguous`. The emitted
    lineage edge is technically valid but may point at the wrong
    element — a `logger.warning` is also emitted at ingestion time so
    the ambiguous pick is visible in the connector log. The root cause
    is a naming collision on the Sigma side: renaming one of the
    duplicates in Sigma will let the resolver match precisely and
    clear the counter. This is distinct from the rename case above:
    here the name _is_ found but is not unique, whereas the rename case
    produces a name-miss (`data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known`).

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.
3. The API client must have read access to `/v2/connections` so the connector can
   build its connection registry at startup. Existing credentials that pre-date
   this requirement may need to be regenerated or have their scope expanded.
   If the call fails (e.g. the token lacks the required scope), the ingest
   continues with an empty registry and a warning is recorded in the run
   report — typically `Sigma paginated endpoint aborted` for transport or
   permission failures, or `Sigma Connection registry build failed` for
   unexpected errors during registry construction.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.


### Install the Plugin
```shell
pip install 'acryl-datahub[sigma]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: sigma
  config:
    # Coordinates
    api_url: "https://aws-api.sigmacomputing.com/v2"
    # Credentials
    client_id: "CLIENTID"
    client_secret: "CLIENT_SECRET"

    # Optional - filter for certain workspace names instead of ingesting everything.
    # workspace_pattern:
    #   allow:
    #     - workspace_name

    # Optional - filter for certain workbook names instead of ingesting everything.
    # workbook_pattern:
    #   allow:
    #     - workbook_name

    ingest_owner: true

    # Optional - per-connection overrides for env, platform_instance, and URN casing.
    # Required for warehouses (e.g. Redshift) whose Sigma connection record omits
    # database/schema; set default_database/default_schema so customSQL SQL is fully qualified.
    # connection_to_platform_map:
    #   "<sigma-connection-id>":
    #     env: PROD
    #     platform_instance: prod-snowflake
    #     convert_urns_to_lowercase: true
    #     default_database: my_redshift_db   # omit for Snowflake/BigQuery (returned by Sigma API)
    #     default_schema: public             # omit unless Sigma API omits the schema field

    # Optional - legacy fallback for the workbook-chart SQL parser path. Fires when a workbook
    # element exposes an element.query but no connectionId is available for auto-resolution.
    # Prefer connection_to_platform_map for warehouse connections (Snowflake, Redshift, etc.).
    # chart_sources_platform_mapping:
    #   folder_path:
    #     data_source_platform: postgres
    #     platform_instance: cloud_instance
    #     env: DEV
    #     default_db: my_db       # optional: default database for SQL parsing
    #     default_schema: public  # optional: default schema for SQL parsing

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Sigma Client ID  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Sigma Client Secret  |
| <div className="path-line"><span className="path-main">api_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Sigma API hosted URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://aws-api.sigmacomputing.com/v2</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to extract lineage of workbook's elements and datasets or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_data_models</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest Sigma Data Models. Each Data Model is emitted as a Container with one Dataset per element inside it (plus per-element ``SchemaMetadata`` and, when ``extract_lineage`` is also enabled, ``UpstreamLineage``). Enabling this issues ``/dataModels/{id}/elements`` and ``/columns`` calls per Data Model unconditionally; the ``/lineage`` call is only issued when ``extract_lineage`` is also ``True`` (so users who opt out of lineage at the workbook surface don't get a lineage endpoint hit under a different flag). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest Owner from source. This will override Owner info entered from UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_shared_entities</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ingest the shared entities or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_personal_dm_discovery_rounds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Belt-and-braces safety cap on the number of passes the personal-space Data Model discovery loop is allowed to make. Each pass fetches ``/v2/dataModels/{urlId}`` for every newly-seen cross-DM ``<urlId>`` prefix; the loop terminates naturally when ``unresolved_seen`` plateaus (monotonically growing set), so under a well-behaved API this cap is never hit. Must be ``>= 1`` -- set ``ingest_shared_entities: False`` (or leave it at the default) if the goal is to disable personal-space discovery entirely; ``0`` / negative values are rejected because the first pass is required to prepopulate the bridge maps for listed DMs. Exists to protect against pathological Sigma payloads (e.g. a chain of personal-space DMs that keep referencing newly-discovered personal-space DMs) by breaking with a ``SourceReport.warning`` instead of looping unbounded. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">chart_sources_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">data_source_platform</span>&nbsp;<abbr title="Required if chart_sources_platform_mapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | A chart's data sources platform name.  |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">default_db</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default database name to use when parsing SQL queries. Used to generate fully qualified table URNs (e.g., 'prod' for 'prod.public.table'). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default schema name to use when parsing SQL queries. Used to generate fully qualified table URNs (e.g., 'public' for 'prod.public.table'). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,WarehouseConnectionConfig)</span></div> | Per-connection env / platform_instance overrides for warehouse URN construction. <br />  <br /> Maps a Sigma connectionId to the env and platform_instance of the <br /> corresponding DataHub warehouse connector run.  When a connection is not <br /> listed, the Sigma source's own env is used and platform_instance defaults <br /> to None — correct for single-env, single-instance deployments but <br /> produces dangling lineage for multi-env or multi-instance setups.  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lower-case warehouse identifiers when constructing Dataset URNs. Must match the convert_urns_to_lowercase setting used by the corresponding warehouse connector recipe. Defaults to True (matching the Snowflake connector default). Set to False if the warehouse source was ingested with convert_urns_to_lowercase: false. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default database name for this connection. Used (a) to fill the database layer when Sigma's /files path omits it (e.g. Redshift: 'Connection Root/SCHEMA') and (b) as the SQL parser fallback when customSQL definitions reference unqualified tables. Set this to the database name the warehouse connector uses so the emitted URNs match (e.g. 'dev' to produce 'dev.public.table'). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default schema name for this connection. Used as the SQL parser fallback when customSQL definitions reference unqualified tables. Required for warehouses where Sigma's connection record does not include a schema field. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">data_model_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">data_model_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workbook_lineage_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workbook_lineage_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workbook_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workbook_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Sigma Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "PlatformDetail": {
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
        "data_source_platform": {
          "description": "A chart's data sources platform name.",
          "title": "Data Source Platform",
          "type": "string"
        },
        "default_db": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default database name to use when parsing SQL queries. Used to generate fully qualified table URNs (e.g., 'prod' for 'prod.public.table').",
          "title": "Default Db"
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
          "description": "Default schema name to use when parsing SQL queries. Used to generate fully qualified table URNs (e.g., 'public' for 'prod.public.table').",
          "title": "Default Schema"
        }
      },
      "required": [
        "data_source_platform"
      ],
      "title": "PlatformDetail",
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
    "WarehouseConnectionConfig": {
      "additionalProperties": false,
      "description": "Per-connection env / platform_instance overrides for warehouse URN construction.\n\nMaps a Sigma connectionId to the env and platform_instance of the\ncorresponding DataHub warehouse connector run.  When a connection is not\nlisted, the Sigma source's own env is used and platform_instance defaults\nto None \u2014 correct for single-env, single-instance deployments but\nproduces dangling lineage for multi-env or multi-instance setups.",
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
          "description": "Default database name for this connection. Used (a) to fill the database layer when Sigma's /files path omits it (e.g. Redshift: 'Connection Root/SCHEMA') and (b) as the SQL parser fallback when customSQL definitions reference unqualified tables. Set this to the database name the warehouse connector uses so the emitted URNs match (e.g. 'dev' to produce 'dev.public.table').",
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
          "description": "Default schema name for this connection. Used as the SQL parser fallback when customSQL definitions reference unqualified tables. Required for warehouses where Sigma's connection record does not include a schema field.",
          "title": "Default Schema"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Whether to lower-case warehouse identifiers when constructing Dataset URNs. Must match the convert_urns_to_lowercase setting used by the corresponding warehouse connector recipe. Defaults to True (matching the Snowflake connector default). Set to False if the warehouse source was ingested with convert_urns_to_lowercase: false.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "WarehouseConnectionConfig",
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
      "description": "Sigma Stateful Ingestion Config."
    },
    "api_url": {
      "default": "https://aws-api.sigmacomputing.com/v2",
      "description": "Sigma API hosted URL.",
      "title": "Api Url",
      "type": "string"
    },
    "client_id": {
      "description": "Sigma Client ID",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Sigma Client Secret",
      "format": "password",
      "title": "Client Secret",
      "type": "string",
      "writeOnly": true
    },
    "workspace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Sigma workspaces in ingestion.Mention 'My documents' if personal entities also need to ingest."
    },
    "ingest_owner": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Ingest Owner from source. This will override Owner info entered from UI.",
      "title": "Ingest Owner"
    },
    "ingest_shared_entities": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Whether to ingest the shared entities or not.",
      "title": "Ingest Shared Entities"
    },
    "extract_lineage": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract lineage of workbook's elements and datasets or not.",
      "title": "Extract Lineage"
    },
    "workbook_lineage_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter workbook's elements and datasets lineage in ingestion.Requires extract_lineage to be enabled."
    },
    "chart_sources_platform_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "A mapping of the sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.",
      "title": "Chart Sources Platform Mapping",
      "type": "object"
    },
    "connection_to_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/WarehouseConnectionConfig"
      },
      "description": "Per-connection env / platform_instance overrides for warehouse URN construction from DM element lineage. Keys are Sigma connectionIds (visible in the Sigma admin UI or /v2/connections response). When a connection is not listed, the Sigma source's own env is used and platform_instance defaults to None, which is correct for single-env, single-instance deployments. For multi-env or multi-instance warehouse setups, add an entry here so the emitted UpstreamLineage edge points at the URN the warehouse connector actually produced.",
      "title": "Connection To Platform Map",
      "type": "object"
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
      "description": "Regex patterns to filter Sigma workbook names in ingestion."
    },
    "ingest_data_models": {
      "default": true,
      "description": "Whether to ingest Sigma Data Models. Each Data Model is emitted as a Container with one Dataset per element inside it (plus per-element ``SchemaMetadata`` and, when ``extract_lineage`` is also enabled, ``UpstreamLineage``). Enabling this issues ``/dataModels/{id}/elements`` and ``/columns`` calls per Data Model unconditionally; the ``/lineage`` call is only issued when ``extract_lineage`` is also ``True`` (so users who opt out of lineage at the workbook surface don't get a lineage endpoint hit under a different flag).",
      "title": "Ingest Data Models",
      "type": "boolean"
    },
    "data_model_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Sigma Data Model names in ingestion. Requires ingest_data_models to be enabled."
    },
    "max_personal_dm_discovery_rounds": {
      "default": 20,
      "description": "Belt-and-braces safety cap on the number of passes the personal-space Data Model discovery loop is allowed to make. Each pass fetches ``/v2/dataModels/{urlId}`` for every newly-seen cross-DM ``<urlId>`` prefix; the loop terminates naturally when ``unresolved_seen`` plateaus (monotonically growing set), so under a well-behaved API this cap is never hit. Must be ``>= 1`` -- set ``ingest_shared_entities: False`` (or leave it at the default) if the goal is to disable personal-space discovery entirely; ``0`` / negative values are rejected because the first pass is required to prepopulate the bridge maps for listed DMs. Exists to protect against pathological Sigma payloads (e.g. a chain of personal-space DMs that keep referencing newly-discovered personal-space DMs) by breaking with a ``SourceReport.warning`` instead of looping unbounded.",
      "minimum": 1,
      "title": "Max Personal Dm Discovery Rounds",
      "type": "integer"
    }
  },
  "required": [
    "client_id",
    "client_secret"
  ],
  "title": "SigmaSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connection record overrides (`connection_to_platform_map`)

All four warehouse lineage paths — DM element → warehouse table, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS — resolve the target DataHub platform and URN
coordinates from the Sigma connection record. `connection_to_platform_map` lets you override
those coordinates per connection.

**env / platform_instance / convert_urns_to_lowercase**

For multi-environment or multi-instance setups, specify the exact env and platform_instance per
Sigma connectionId so emitted lineage edges point to the correct warehouse connector run:

```yml
connection_to_platform_map:
  # Key is the Sigma connectionId (UUID from /v2/connections).
  "4b39cdcd-5a58-4ff6-af0d-8409ff880a23":
    env: PROD
    platform_instance: prod-snowflake
    # Set to false only if the Snowflake connector was run with
    # convert_urns_to_lowercase: false (non-default).
    convert_urns_to_lowercase: true
```

**Warehouses that omit database or schema from the Sigma connection record** (e.g. Redshift):
Sigma's `/v2/connections` API does not return `database` or `schema` fields for all warehouse
types. When those fields are absent, lineage URNs may be under-qualified and will not match
what your warehouse connector emitted. Use `default_database` and `default_schema` to supply
the missing values:

```yml
connection_to_platform_map:
  "a1b2c3d4-0000-0000-0000-000000000001":
    env: PROD
    default_database: my_redshift_db # expands `schema.table` → `my_redshift_db.schema.table`
    default_schema: public # expands bare `table` → `public.table` (SQL parser only)
```

`default_database` applies to all four lineage paths (DM element, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS). `default_schema` is consumed only by the
SQL parser (DM customSQL and workbook customSQL paths) — the entity-level BFS and DM element
paths derive the schema from the `/files` path and do not use this field. The `env` and
`platform_instance` fields are also consumed by the customSQL parsers, so URNs minted from
customSQL definitions match the warehouse connector's env/instance for that connection.

#### Data Model customSQL element lineage

Data Model elements backed by a customSQL source emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` automatically — no additional configuration is required beyond a valid connection in the Sigma connection registry for most platforms. **Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`, set `default_database` and `default_schema` in `connection_to_platform_map` — see [Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

For elements with explicit column lists in their SQL (`SELECT col_a, col_b FROM ...`), column lineage is derived directly from the SQL by the parser (confidence score 0.2). For elements using `SELECT *`, column lineage is inferred from Sigma's formula metadata (`[Custom SQL/COL]` refs on each element column); these entries carry a confidence score of 0.1 — lower than SQL-parsed lineage — because they rely on formula-derived inference rather than direct SQL analysis.

The following report counters are available for operational visibility:

| Counter                                     | Meaning                                                                                        |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `dm_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `dm_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `dm_customsql_skipped`                      | Elements skipped before parsing (missing definition, unknown connection, unsupported platform) |
| `dm_customsql_parse_failed`                 | Definitions the SQL parser could not interpret (syntax errors, unsupported features, etc.)     |
| `dm_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `dm_customsql_column_lineage_emitted`       | Elements with at least one column lineage entry emitted                                        |
| `dm_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook customSQL chart lineage

When `extract_lineage: true` (default), workbook chart elements whose data source is a customSQL definition
emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` via the SQL parser — no additional
configuration is required beyond a valid connection in the Sigma connection registry for most platforms.
**Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`,
set `default_database` and `default_schema` in `connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

The connector reads the workbook-level lineage graph (`/v2/workbooks/{id}/lineage`) to find `type=customSQL`
entries, parses each SQL definition, and registers the results with the `SqlParsingAggregator`.
Column-level lineage is emitted for chart columns whose formula resolves to a named SQL column
(`[CustomSQLName/col]` pattern). Columns using `SELECT *` sources carry lower-confidence (0.1) inferred lineage.

The following report counters are available for operational visibility:

| Counter                                           | Meaning                                                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `workbook_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `workbook_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `workbook_customsql_skipped`                      | Entries skipped before parsing (missing definition, unknown connection, unsupported platform)  |
| `workbook_customsql_parse_failed`                 | Definitions the SQL parser could not interpret                                                 |
| `workbook_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `workbook_customsql_column_lineage_emitted`       | Charts with at least one column lineage entry emitted                                          |
| `workbook_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook chart entity-level warehouse upstream

When `extract_lineage: true` (default), workbook chart elements that pull **directly** from a
warehouse table (no Data Model, no customSQL, no Sigma Dataset in between) emit an entity-level
`chartInfo.inputs` edge to the warehouse Dataset.

Column-level lineage for these charts is handled separately by the
[chart inputFields warehouse qualification](#workbook-chart-inputfields-warehouse-column-level-qualification)
path; this feature adds the missing entity-level edge.

No additional configuration is required for most platforms. For Redshift connections where the
Sigma connection record omits `database`/`schema`, set `default_database` in
`connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

| Counter                                | Meaning                                                                                                           |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `chart_warehouse_upstream_emitted`     | Entity-level chart→warehouse edges emitted (post-dedup)                                                           |
| `chart_warehouse_table_name_unmatched` | Table not found in workbook warehouse index; edge not emitted                                                     |
| `chart_warehouse_table_node_skipped`   | Lineage node missing `name` field or has unexpected ID format; skipped                                            |
| `chart_warehouse_table_name_ambiguous` | Table name matched multiple warehouse URNs; edge skipped — set `default_database` in `connection_to_platform_map` |

#### Workbook chart inputFields warehouse column-level qualification

When `extract_lineage: true` (default), the connector qualifies chart column `InputFields` to
warehouse Dataset URNs. For each chart column whose formula references a warehouse table (e.g.,
`[TABLE/col]`), the connector resolves the short table name to a fully-qualified warehouse Dataset
URN via a two-level index: first the per-element SQL-parser index, then the workbook-level index
from `/v2/workbooks/{id}/lineage`. The resolved URN is written into `schemaFieldUrn` on each
`InputField` entry.

| Counter                                                     | Meaning                                                              |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `chart_input_fields_warehouse_qualified`                    | Individual column fields successfully qualified to a warehouse URN   |
| `chart_input_fields_warehouse_qualified_via_workbook_index` | Subset qualified via the workbook-level index (not per-element SQL)  |
| `chart_input_fields_warehouse_index_lookup_failed`          | Workbook-level lineage fetch failed; column qualification incomplete |
| `chart_input_fields_warehouse_table_lookup_failed`          | `/files/{inodeId}` call failed for a workbook-level table entry      |
| `chart_input_fields_warehouse_path_unparseable`             | `/files` path did not match expected format                          |
| `chart_input_fields_warehouse_unknown_connection`           | ConnectionId not in registry or platform unmappable                  |

#### Data Model element -> warehouse table lineage

When `ingest_data_models: true` and `extract_lineage: true` (both default), the connector also emits entity-level `UpstreamLineage` from each Sigma Data Model element to the warehouse table it is sourced from.
Resolution uses Sigma's `/v2/dataModels/{id}/lineage` (`type=table` entries) and `/v2/files/{inodeId}` to construct the fully-qualified `<DB>/<SCHEMA>/<TABLE>` identifier from the path and table name fields (path = `Connection Root/<DB>/<SCHEMA>` for most platforms; `Connection Root/<SCHEMA>` for Redshift), then maps the Sigma connection to a DataHub platform via the connection registry.

**Supported platforms**: All Sigma connection types in `SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP` (Snowflake, BigQuery, Redshift, Databricks, Postgres, MySQL, Athena, Spark, Trino, Presto, Synapse/MSSQL).
Identifier casing is preserved as Sigma reports it, which matches the warehouse catalog for most platforms.
Snowflake is the only platform that requires a case bridge (Snowflake's catalog uses uppercase identifiers, but the DataHub Snowflake connector lowercases them by default).

**Matching URNs to your warehouse connector**: The emitted URNs use the Sigma recipe's `env` and
`platform_instance=None` by default. For multi-environment or multi-instance setups, or for
Redshift connections where the Sigma connection record omits `database`/`schema`, see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

**Counters to monitor** (visible in the ingestion report):

| Counter                                       | Meaning                                             |
| --------------------------------------------- | --------------------------------------------------- |
| `dm_element_warehouse_upstream_emitted`       | Warehouse lineage edges successfully emitted        |
| `dm_element_warehouse_unknown_connection`     | ConnectionId not in registry or platform unmappable |
| `dm_element_warehouse_table_lookup_failed`    | `/files/{inodeId}` call failed                      |
| `dm_element_warehouse_path_unparseable`       | `/files` path did not match expected format         |
| `dm_element_warehouse_table_entry_incomplete` | Lineage entry missing inodeId or connectionId       |

##### Chart source platform mapping

`chart_sources_platform_mapping` is the legacy fallback for the workbook-chart SQL parser path.
It fires whenever a workbook element exposes an `element.query` (regardless of whether the element
is backed by a Sigma Dataset, a DM element, customSQL, or inline SQL). The SQL-bearing endpoint
does not return a `connectionId`, so the platform, env, and default database/schema cannot be
auto-resolved. You declare them explicitly, scoped to a workbook path prefix or the `"*"` wildcard.

**Prefer `connection_to_platform_map`** for warehouse connections (Snowflake, Redshift, BigQuery,
etc.) — it auto-resolves the platform from the connection record and covers all lineage paths that
have a `connectionId` on hand (DM warehouse table lineage, DM and workbook customSQL parsing,
chart inputFields qualification). Use `chart_sources_platform_mapping` only when the chart's SQL
parser path fires and you cannot reach the connection via `connection_to_platform_map`.

##### Example - For just one specific chart's external upstream data sources

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name/chart_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name/chart_name_2":
    data_source_platform: postgres
    platform_instance: cloud_instance
    env: DEV
```

##### Example - For all charts within one specific workbook

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name_2":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - For all workbooks charts within one specific workspace

```yml
chart_sources_platform_mapping:
  "workspace_name":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - All workbooks use the same connection

```yml
chart_sources_platform_mapping:
  "*":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.sigma.sigma.SigmaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sigma/sigma.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Sigma, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
