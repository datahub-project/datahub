


# SAP Datasphere

## Overview

The SAP Datasphere connector extracts metadata from [SAP Datasphere](https://www.sap.com/products/technology-platform/datasphere.html) — SAP's cloud-native data warehouse platform (the successor to SAP Data Warehouse Cloud). It reads the SAP-supported REST/OData v4 Catalog and Consumption APIs (authenticated via XSUAA OAuth), so no JDBC driver or SQL dialect is required.

DataHub ingests Spaces as containers and Views, Analytic Models, and Local Tables as datasets, with column schema read from the per-asset OData EDMX (`$metadata`) surface. Optional features include table- and column-level lineage (`include_lineage`), SAP CDS semantic annotations emitted as tags, federated Remote Tables routed to their native storage platform (so they merge with native warehouse connectors), and stateful stale-entity removal.

## Concept Mapping

| SAP Datasphere concept                       | DataHub entity                      | Notes                                                                         |
| -------------------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------- |
| Space                                        | Container                           | 2-tier Space → object model (no folder layer)                                 |
| View                                         | Dataset (subtype `View`)            | Emits `viewProperties`; SQL-editor views use `viewLanguage="SQL"`, else `CSN` |
| Analytic Model                               | Dataset (subtype `Analytic Model`)  | Assets with `supportsAnalyticalQueries: true`; star-schema lineage            |
| Local Table (base table)                     | Dataset (subtype `Local Table`)     | Discovered when `include_local_tables: true`; schema from per-table CSN       |
| Remote Table (federated)                     | Dataset on its **storage** platform | Routed via `connection_to_platform_map` / `platform_type_defaults`            |
| Column                                       | Schema field                        | Types from OData EDMX                                                         |
| CDS semantic annotation (Dimension, Measure) | Tag                                 | Emitted when `emit_sap_semantics_as_tags: true`                               |


## Module `sap-datasphere`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Spaces emitted as containers. |
| Column-level Lineage | ✅ | Column-level lineage from CSN columns[] expressions. Enable via `include_lineage: true`. |
| Descriptions | ✅ | Field descriptions from EDMX Common.Label annotations. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Soft-delete via stateful ingestion stale-entity removal. |
| Extract Tags | ✅ | CDS semantic annotations (Dimension/Measure/Calendar/Currency/Unit/DimensionType) emitted as DataHub tags on the relevant schema fields and datasets. Toggle via `emit_sap_semantics_as_tags` (default True). |
| [Platform Instance](../../../platform-instances.md) | ✅ | Per-connection platform_instance via connection_to_platform_map. |
| Schema Metadata | ✅ | Columns from OData EDMX. |
| Table-Level Lineage | ✅ | Table-level lineage from CSN query refs and @remote.source annotations. Enable via `include_lineage: true`. |
| Test Connection | ✅ | Validates OAuth credentials and tenant URL. |

### Overview

The connector emits assets on **two platforms**, depending on whether an asset is
managed (lives in Datasphere) or federated (lives in an external system that
Datasphere accesses via a Remote Table).

#### Managed assets → `sap-datasphere` platform

Views, Analytical Models, and Local Tables — the objects you create _inside_
Datasphere — emit on the `sap-datasphere` platform. Their URN shape:

```text
urn:li:dataset:(urn:li:dataPlatform:sap-datasphere, <space>.<asset>, ENV)
```

(prefixed with `<platform_instance>.` if you set the connector's top-level
`platform_instance`).

Datasphere-specific subtypes (`Local Table`, `View`, `Analytic Model`),
CDS-annotation tags, and Space hierarchy are all emitted on top of these URNs.

#### Federated Remote Tables → storage platform

A Datasphere Remote Table that federates from e.g. Snowflake emits on the
_Snowflake_ platform — its URN matches what DataHub's native Snowflake
connector emits for the same physical table. Lineage joins automatically:
a downstream Datasphere View's `UpstreamLineage` points at the same
`snowflake:` URN that the Snowflake connector ingests, with no Siblings
configuration needed.

Federated routing is driven by `connection_to_platform_map` (per connection
name) and `platform_type_defaults` (per typeId fallback). See the recipe for
examples.

Two per-connection knobs matter for URN stitching with the native connector:

- **`convert_urns_to_lowercase`** (per connection; defaults to `true`) — set it
  to `false` when the sibling native connector preserves source case, so the
  URNs match. Needed for BigQuery (`project.dataset.MyTable`) and for a HANA
  connector left at its uppercase default. This is independent of the
  connector's top-level `convert_urns_to_lowercase`, which governs managed
  `sap-datasphere` assets.
- **`database`** (per connection) — a leading name segment prepended ahead of
  the schema/dataset the flow reports. Chiefly the **BigQuery GCP project**,
  which the Datasphere API never exposes: with `database: my-gcp-project`, a
  replication-flow target in dataset `staging` becomes
  `my-gcp-project.staging.<table>`. Set it on the per-connection entry (keyed by
  connection name), not on `platform_type_defaults`, since different connections
  of the same type can point at different projects.

typeId matching in `platform_type_defaults` is **case-insensitive** (`BIGQUERY`,
`BigQuery`, and `bigquery` all match the same entry).

#### Built-in typeId routing

The following SAP Datasphere connection typeIds ship with built-in platform
defaults (verified against a live tenant). You can override any of them in
your recipe under `platform_type_defaults`.

| Datasphere typeId    | DataHub platform | Common usage                                                          |
| -------------------- | ---------------- | --------------------------------------------------------------------- |
| `HANA`               | `hana`           | HANA on-prem / external HANA Cloud federated as a Remote Table source |
| `MSSQL`              | `mssql`          | SQL Server federation                                                 |
| `S3`                 | `s3`             | S3 buckets federated as remote tables                                 |
| `GCS`                | `gcs`            | Google Cloud Storage federation                                       |
| `ABAP`               | `abap`           | SAP ABAP system extraction                                            |
| `SAPS4HANACLOUD`     | `s4hana`         | SAP S/4HANA Cloud federation                                          |
| `SAPBWMODELTRANSFER` | `bw`             | SAP BW analytical model transfer (matches the SAC connector's `bw`)   |
| `BIGQUERY`           | `bigquery`       | Google BigQuery replication-flow target / federated remote tables     |

Other typeIds (Snowflake, Kafka, Salesforce, ...) default to
`enabled: false` with a warning — opt in by adding them to
`platform_type_defaults` in your recipe. The connector reports each
unmapped-typeId asset once via `report.assets_skipped_unknown_typeid`.

#### Local Tables (base tables)

By default the connector emits only Datasphere assets exposed for OData
consumption (views and analytical models). Base tables — which lineage edges
typically point at — are not in that surface. Set `include_local_tables: true`
to ALSO discover them via the supported
`/dwaas-core/api/v1/spaces/X/localtables` endpoint (the same endpoint the
official `datasphere` CLI uses; SAP-blessed, no policy caveat).

Local Tables emit on the `sap-datasphere` platform (same as Views and
Analytical Models) so phantom-lineage edges from consuming views resolve to
real entities. Their column schema is read from the per-table CSN
(`/dwaas-core/api/v1/spaces/X/localtables/Y`) when available, enabling
column-level lineage edges between a View and its base table; if the CSN is
unavailable the table is still emitted as a schema-less stub. Each Local Table
has subtype `Local Table` and parents directly to its Space container — the
connector uses a 2-tier Space → object model, with no separate folder layer.

### Prerequisites

#### Connection

Set `base_url` to your SAP Datasphere tenant URL (e.g.
`https://yourtenant.eu10.hcs.cloud.sap`). The previous field name `tenant_url`
remains accepted as a deprecated alias; new recipes should use `base_url`.

#### Authentication

The connector supports three authentication methods (in priority order):

1. **Raw bearer token** (`token`) — For local development. Obtain from your browser's DevTools after logging into Datasphere.
2. **OAuth refresh token** (`refresh_token`) — Authorization code flow. Compatible with credentials created for Atlan. Requires `client_id` too.
3. **XSUAA client credentials** (`client_id` + `client_secret`) — Recommended for production. Requires a Technical User OAuth Client created under **System → Administration → App Integration** by a DW Administrator.

#### Creating an OAuth Client (Client Credentials)

1. Log into your SAP Datasphere tenant as DW Administrator.
2. Navigate to **System → Administration → App Integration**.
3. Click **Add a New OAuth Client** → Purpose: **API Access**.
4. Note the generated `client_id`, `client_secret`, and the OAuth Token URL (this is your `xsuaa_url`).

#### Space membership

> **The ingestion principal must be a member of every Datasphere space you want to ingest.**

Both the consumption catalog and the dwaas-core APIs only return spaces that the
ingestion OAuth **principal** (the user behind a refresh token, or the technical
user behind a client-credentials OAuth client) **is a member of**. A space the
principal is not a member of returns **HTTP 403** and is silently skipped — the
connector logs a `report` warning _"Not a member of SAP Datasphere space"_ and
moves on.

Add the principal as a member of each target space:

1. Open **Space Management**.
2. Select the space.
3. Under **Members**, add the ingestion user / OAuth client.


### Install the Plugin
```shell
pip install 'acryl-datahub[sap-datasphere]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Example recipe for SAP Datasphere ingestion into DataHub.
#
# Managed Datasphere assets (Views, Analytical Models, Local Tables) emit on the
# `sap-datasphere` platform. Federated Remote Tables emit on the storage platform
# they federate from (snowflake / s3 / bigquery / hana-on-prem / ...) so they
# merge automatically with native warehouse-connector URNs for downstream
# lineage join purposes.
#
# THREE AUTHENTICATION OPTIONS are supported; pick exactly one:
#   1) Raw bearer token (simplest, but expires; good for ad-hoc runs)
#   2) Refresh-token grant: refresh_token + client_id + xsuaa_url
#   3) Client-credentials grant: client_id + client_secret + xsuaa_url
#
# PREREQUISITE — SPACE MEMBERSHIP: the ingestion principal (the user / OAuth
# client behind the credentials above) must be a MEMBER of every Datasphere
# space you want to ingest (Space Management -> space -> Members). Spaces the
# principal is not a member of return HTTP 403 and are silently skipped (logged
# as a `report` warning). This is the #1 reason DataHub captures fewer objects
# than a human sees in the Data Builder.
#
source:
  type: sap-datasphere
  config:
    # The SAP Datasphere tenant URL. The legacy field name `tenant_url` is still
    # accepted (deprecated alias) for backwards compatibility.
    base_url: "https://yourtenant.eu10.hcs.cloud.sap"

    # Option 1: raw bearer token from the UI.
    token: "${SAP_DATASPHERE_TOKEN}"

    # Option 2: refresh-token flow.
    # refresh_token: "${SAP_DATASPHERE_REFRESH_TOKEN}"
    # client_id: "${SAP_DATASPHERE_CLIENT_ID}"
    # xsuaa_url: "https://yourtenant.authentication.eu10.hana.ondemand.com"

    # Option 3: client-credentials flow.
    # client_id: "${SAP_DATASPHERE_CLIENT_ID}"
    # client_secret: "${SAP_DATASPHERE_CLIENT_SECRET}"
    # xsuaa_url: "https://yourtenant.authentication.eu10.hana.ondemand.com"

    env: "PROD"

    # ─────────────────────────────────────────────────────────────────────────
    # Platform mapping — federated Remote Tables only.
    #
    # Managed Datasphere assets (Views, Analytical Models, Local Tables) always
    # emit on the `sap-datasphere` platform and do NOT use these maps.
    #
    # `connection_to_platform_map` is keyed by the Datasphere CONNECTION NAME
    # (visible in the Datasphere UI's Connections page) for federated Remote
    # Tables.
    #
    # `platform_type_defaults` is a fallback keyed by Datasphere connection
    # TYPE-ID (e.g., HANA, SNOWFLAKE, S3) — applied when a federated asset's
    # connection isn't explicitly listed in `connection_to_platform_map`. The
    # connector ships with built-in defaults for HANA, MSSQL, S3, GCS, ABAP,
    # SAPS4HANACLOUD, and SAPBWMODELTRANSFER; others default to
    # `enabled: false` until you opt them in.
    # ─────────────────────────────────────────────────────────────────────────
    # Managed assets (Views, Analytical Models, Local Tables in Datasphere's own
    # HANA Cloud) always emit on the `sap-datasphere` platform — no entry needed.
    #
    # Populate connection_to_platform_map ONLY for federated Remote Tables so
    # their URNs match what your native warehouse connectors emit.
    #
    # To skip managed-asset ingestion entirely, set `_managed: {enabled: false}`
    # — that's the only field honored on the synthetic `_managed` key.
    connection_to_platform_map:
      # Example: federated Snowflake source
      # SNOWFLAKE_PROD:
      #   platform: snowflake
      #   platform_instance: "acct_xyz_warehouse_prod"
      #   env: "PROD"
      #   enabled: true

      # Example: federated S3 — disable to skip these assets
      # CLOUD_STORAGE:
      #   platform: s3
      #   enabled: false

      # Example: federated BigQuery. The GCP project never appears in the
      # Datasphere connection/flow payloads, so supply it via `database`, and
      # keep source case so the URN stitches to the BigQuery connector
      # (project.dataset.MyTable). `convert_urns_to_lowercase` here is
      # per-connection and independent of the top-level flag below.
      # BIGQUERY_PROD:
      #   platform: bigquery
      #   database: my-gcp-project
      #   convert_urns_to_lowercase: false
      #   enabled: true

    # Override platform defaults for typeIds not in the built-in table:
    # platform_type_defaults:
    #   SNOWFLAKE:
    #     platform: snowflake
    #   BIGQUERY:
    #     platform: bigquery

    # ─────────────────────────────────────────────────────────────────────────
    # Lineage extraction (table-level + column-level).
    #
    # Uses the SAP-supported per-object-type endpoint under
    # `/dwaas-core/api/v1/spaces/X/{views,analyticmodels}/Y` (same surface the
    # official `datasphere` CLI uses). All other connector functionality
    # (catalog discovery, EDMX schema, connection routing) is also on
    # SAP-supported APIs. Enabling include_lineage:
    #   - Gives you full table-level + column-level lineage from each
    #     view's SELECT FROM and SELECT columns clauses
    #   - Adds one extra HTTP call per asset (assets where the CSN fetch
    #     fails are surfaced via `report.assets_csn_fetch_failed` and
    #     emitted without lineage rather than aborting ingestion)
    #
    # The legacy field name `include_table_lineage` is still accepted (deprecated
    # alias) for backwards compatibility with older recipes.
    # ─────────────────────────────────────────────────────────────────────────
    include_lineage: false

    # Filters
    space_pattern:
      allow:
        - "^PROD_.*"
      deny:
        - "^TEMP_.*"

    asset_pattern:
      deny:
        - "^STAGING_.*"

    expose_for_consumption_only: false

    # HTTP behavior
    request_timeout_sec: 30
    max_retries: 3

    # Stateful ingestion — enables soft-deletion of removed assets/spaces.
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "${DATAHUB_GMS_URL}"
    token: "${DATAHUB_GMS_TOKEN}"

# ─────────────────────────────────────────────────────────────────────────────
# SAP semantic annotations surfaced by this connector
# ─────────────────────────────────────────────────────────────────────────────
# These are emitted as customProperties / description suffixes on the
# dataset URN the connector emits for each asset (sap-datasphere for managed
# assets; the federated storage platform for Remote Tables).
#
# ENTITY-LEVEL (on dataset customProperties):
#   sap_is_dimension   = "true"   if EDMX has Analytics.Dimension
#   sap_is_measure     = "true"   if Analytics.Measure
#   sap_dimension_type = "Time"   from Analytics.dimensionType (enum prefix stripped)
#   sap_datasphere_space = "<space-id>"
#   sap_datasphere_asset = "<asset-name>"
#
# FIELD-LEVEL (in field description brackets):
#   sap_calendar_type = "date" | "year" | "quarter" | "month" | "week" | "yearmonth"
#   sap_semantic      = "currency" | "unit"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | SAP Datasphere tenant URL, e.g. https://foo.eu10.hcs.cloud.sap  |
| <div className="path-line"><span className="path-main">asset_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many assets are submitted to the parallel executor at a time. The connector processes each space's assets in batches of this size so memory stays bounded regardless of how many assets a single space holds (the executor otherwise submits every task up front). Larger = slightly better parallelism overlap at higher memory; the default keeps peak memory low while preserving effectively full parallelism (batch size >> max_workers_assets). Only relevant when max_workers_assets > 1. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000</span></div> |
| <div className="path-line"><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | XSUAA OAuth client ID for client credentials flow. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | XSUAA OAuth client secret for client credentials flow. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lowercase emitted dataset and container URNs. The original case is preserved in display names and custom properties. Defaults to True to match the convention used by Snowflake and other DataHub sources so lineage merges case-insensitively across connectors. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">emit_sap_semantics_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True (default), emit SAP CDS semantic annotations (@Analytics.Dimension/Measure, @Common.IsCalendar*, @Common.IsCurrency/IsUnit, @Analytics.DimensionType) as DataHub tags on the relevant schema fields or datasets. Tags are searchable/filterable in DataHub Search and render as colored badges in the UI. The existing `sap_*` custom properties are emitted in parallel (additive) for backward compat — no breaking change. Set to False to suppress tag emission entirely. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">expose_for_consumption_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, only emit datasets that have an `assetRelationalMetadataUrl` set (i.e., assets exposed for consumption via the SAP Datasphere consumption API). Assets without an exposure URL are counted in `assets_filtered`. Useful when you want DataHub to only show consumable views. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_data_flows</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, discover SAP Datasphere Data Flows and emit each as its own DataFlow + DataJob (parented under the space container) with its source/target tables as input/output datasets and column-level lineage from the producer's attributeMappings. Data Flows are the primary lineage source for Local Tables populated by an ETL flow. Uses the supported `/dwaas-core/api/v1/spaces/X/dataflows` endpoint (one list call per space + one read call per flow). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract lineage from each asset's CSN. When enabled, the connector emits table-level upstream references (from SELECT FROM clauses and @remote.source annotations) AND column-level FineGrainedLineage (walking SELECT columns[] expressions). Uses the SAP-supported per-object-type endpoint under `/dwaas-core/api/v1/spaces/X/{views,analyticmodels}/Y` (same surface the official `datasphere` CLI uses); assets where CSN fetch fails are logged to `report.assets_csn_fetch_failed` and emitted without lineage. Cost: one extra HTTP call per asset. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_local_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, ALSO discover Datasphere Local Tables (base tables not exposed for OData consumption) via the supported `/dwaas-core/api/v1/spaces/X/localtables` endpoint. These tables typically appear only as upstream lineage targets of views. Emitting them as Datasets closes the phantom-lineage gap. Their column schema is read from the per-table CSN when available (enabling column-level lineage to the base table); otherwise they are emitted as schema-less stubs carrying platform + subtype + container membership. Off by default to keep ingestion minimal; turn on when you want full catalog coverage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_remote_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, discover federated Remote Tables and emit each as a Dataset with upstream lineage to its external source object (parsed from the CSN `@DataWarehouse.remote.connection` / `@DataWarehouse.remote.entity` annotations). The external upstream is routed via `connection_to_platform_map` / `platform_type_defaults`; unmapped connections leave the remote table without upstream lineage (reported under `remote_table_source_unresolved`). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_replication_flows</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, discover SAP Datasphere Replication Flows and emit each as a DataJob with its source/target objects as input/output datasets and per-task column-level lineage. Replication Flows move data between two external systems; their source/target objects are routed to DataHub platforms via `connection_to_platform_map` / `platform_type_defaults` (objects on unmapped connections are skipped and reported under `flow_endpoints_unresolved`). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_task_chains</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | EXPERIMENTAL. If True, discover Task Chains and emit each as a DataJob. No live Task Chain payload was available to reverse-engineer the member/reference grammar, so chains are currently surfaced as IO-less jobs (their presence + subtype only) without lineage edges. Enable to catalogue the objects; please report payload samples so lineage can be added. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_transformation_flows</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | EXPERIMENTAL. If True, discover Transformation Flows and emit them as DataJobs. This path is parsed with the Data Flow process-graph reader but has NOT been verified against a live Transformation Flow payload, so lineage may be incomplete. Enable only if you can validate the output; please report payload samples so the parser can be hardened. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_view_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit each View's / Analytic Model's definition as the DataHub viewProperties aspect (shown in the View Definition tab). SQL-editor views emit their raw SQL with viewLanguage 'SQL'; graphical/modeled views emit the Core Schema Notation (CSN/CQN) query tree with viewLanguage 'CSN'. Requires fetching the CSN, which is also fetched when include_lineage is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retries for transient HTTP errors (5xx, connection errors) when calling SAP Datasphere APIs. Uses exponential backoff. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">max_workers_assets</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of parallel workers for per-asset metadata fetch (EDMX schema + CSN lineage). At 1M assets this is the dominant performance lever. Default 10 gives ~10x speedup vs serial. Set to 1 to disable parallelism. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">refresh_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | OAuth refresh token (authorization code flow). Takes priority over client credentials. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">request_timeout_sec</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Per-request timeout (seconds) for HTTP calls to the SAP Datasphere catalog and consumption APIs. Increase if you see frequent timeouts on large EDMX documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">resolve_external_urns_via_graph</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, reconcile external flow endpoint URNs (replication/data-flow sources and targets routed to another platform such as BigQuery) against the real physical URNs already in DataHub, using the graph on the configured sink. SAP's flow API reports logical names (lower-cased leaf, and sometimes without a source-added prefix like `w01_cds_`) that need not match the physical warehouse table, so the raw edge can dangle. When enabled, each endpoint is matched by its case-insensitive leaf within the same platform/platform_instance/env and dataset/schema; an unambiguous hit is rewritten to the real URN so the lineage stitches, and misses fall back to the original candidate. Requires a DataHub graph (REST sink or `datahub_api` configured); no-op otherwise. Cost: one scoped search per distinct external dataset/schema (cached per run). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Raw bearer token for dev/testing. Takes priority over all OAuth flows. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">xsuaa_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | XSUAA token endpoint base URL. Auto-derived from base_url if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">asset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">asset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionPlatformConfig)</span></div> | How to project a SAP Datasphere connection (or the tenant's own managed storage) <br /> onto a DataHub platform; one is resolved per asset to build its DatasetUrn.  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The DataHub platform name (e.g. 'hana', 'snowflake', 's3'). Required.  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lowercase external URNs (federated Remote Tables and replication-flow endpoints) routed to this platform. Defaults to True to match the case-insensitive lineage convention Snowflake and most DataHub sources follow. Set to False to preserve source case when the sibling native connector does, so the URNs stitch — e.g. BigQuery (`project.dataset.MyTable`) or a HANA connector left at its default (uppercase catalog identifiers). Independent of the connector's top-level `convert_urns_to_lowercase`, which governs managed `sap-datasphere` assets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional leading name segment prepended to external URNs routed here, ahead of the schema/dataset the Datasphere flow reports. Use it to supply a qualifier the Datasphere API omits — chiefly the BigQuery GCP project, which never appears in the connections/flow payloads. With `database: my-gcp-project`, a replication-flow object in dataset `staging` becomes `my-gcp-project.staging.<table>` so it stitches to the BigQuery connector. Because different connections of the same type can point at different projects, set this on a per-connection entry in `connection_to_platform_map` (keyed by the Datasphere connection name) rather than on a `platform_type_defaults` entry. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If False, assets routed to this connection are skipped (counted under `assets_skipped_disabled` in the ingestion report). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional DataHub platform_instance. Use this to align with the platform_instance a separate connector (e.g. the DataHub Snowflake connector) emits for the same physical system. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional environment override (PROD/DEV/STAGING/...). Falls back to the connector's top-level `env` when None. Must be a valid DataHub FabricType. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_type_defaults</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionPlatformConfig)</span></div> | How to project a SAP Datasphere connection (or the tenant's own managed storage) <br /> onto a DataHub platform; one is resolved per asset to build its DatasetUrn.  |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if platform_type_defaults is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The DataHub platform name (e.g. 'hana', 'snowflake', 's3'). Required.  |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lowercase external URNs (federated Remote Tables and replication-flow endpoints) routed to this platform. Defaults to True to match the case-insensitive lineage convention Snowflake and most DataHub sources follow. Set to False to preserve source case when the sibling native connector does, so the URNs stitch — e.g. BigQuery (`project.dataset.MyTable`) or a HANA connector left at its default (uppercase catalog identifiers). Independent of the connector's top-level `convert_urns_to_lowercase`, which governs managed `sap-datasphere` assets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional leading name segment prepended to external URNs routed here, ahead of the schema/dataset the Datasphere flow reports. Use it to supply a qualifier the Datasphere API omits — chiefly the BigQuery GCP project, which never appears in the connections/flow payloads. With `database: my-gcp-project`, a replication-flow object in dataset `staging` becomes `my-gcp-project.staging.<table>` so it stitches to the BigQuery connector. Because different connections of the same type can point at different projects, set this on a per-connection entry in `connection_to_platform_map` (keyed by the Datasphere connection name) rather than on a `platform_type_defaults` entry. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If False, assets routed to this connection are skipped (counted under `assets_skipped_disabled` in the ingestion report). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional DataHub platform_instance. Use this to align with the platform_instance a separate connector (e.g. the DataHub Snowflake connector) emits for the same physical system. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">platform_type_defaults.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional environment override (PROD/DEV/STAGING/...). Falls back to the connector's top-level `env` when None. Must be a valid DataHub FabricType. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">space_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">space_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config with stale metadata removal (soft-deletes spaces/assets that disappear between runs). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
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
    "ConnectionPlatformConfig": {
      "additionalProperties": false,
      "description": "How to project a SAP Datasphere connection (or the tenant's own managed storage)\nonto a DataHub platform; one is resolved per asset to build its DatasetUrn.",
      "properties": {
        "platform": {
          "description": "The DataHub platform name (e.g. 'hana', 'snowflake', 's3'). Required.",
          "title": "Platform",
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
          "description": "Optional DataHub platform_instance. Use this to align with the platform_instance a separate connector (e.g. the DataHub Snowflake connector) emits for the same physical system.",
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
          "description": "Optional environment override (PROD/DEV/STAGING/...). Falls back to the connector's top-level `env` when None. Must be a valid DataHub FabricType.",
          "title": "Env"
        },
        "enabled": {
          "default": true,
          "description": "If False, assets routed to this connection are skipped (counted under `assets_skipped_disabled` in the ingestion report).",
          "title": "Enabled",
          "type": "boolean"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Whether to lowercase external URNs (federated Remote Tables and replication-flow endpoints) routed to this platform. Defaults to True to match the case-insensitive lineage convention Snowflake and most DataHub sources follow. Set to False to preserve source case when the sibling native connector does, so the URNs stitch \u2014 e.g. BigQuery (`project.dataset.MyTable`) or a HANA connector left at its default (uppercase catalog identifiers). Independent of the connector's top-level `convert_urns_to_lowercase`, which governs managed `sap-datasphere` assets.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Optional leading name segment prepended to external URNs routed here, ahead of the schema/dataset the Datasphere flow reports. Use it to supply a qualifier the Datasphere API omits \u2014 chiefly the BigQuery GCP project, which never appears in the connections/flow payloads. With `database: my-gcp-project`, a replication-flow object in dataset `staging` becomes `my-gcp-project.staging.<table>` so it stitches to the BigQuery connector. Because different connections of the same type can point at different projects, set this on a per-connection entry in `connection_to_platform_map` (keyed by the Datasphere connection name) rather than on a `platform_type_defaults` entry.",
          "title": "Database"
        }
      },
      "required": [
        "platform"
      ],
      "title": "ConnectionPlatformConfig",
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
      "description": "Stateful Ingestion Config with stale metadata removal (soft-deletes spaces/assets that disappear between runs)."
    },
    "base_url": {
      "description": "SAP Datasphere tenant URL, e.g. https://foo.eu10.hcs.cloud.sap",
      "title": "Base Url",
      "type": "string"
    },
    "xsuaa_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "XSUAA token endpoint base URL. Auto-derived from base_url if not set.",
      "title": "Xsuaa Url"
    },
    "token": {
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
      "description": "Raw bearer token for dev/testing. Takes priority over all OAuth flows.",
      "title": "Token"
    },
    "refresh_token": {
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
      "description": "OAuth refresh token (authorization code flow). Takes priority over client credentials.",
      "title": "Refresh Token"
    },
    "client_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "XSUAA OAuth client ID for client credentials flow.",
      "title": "Client Id"
    },
    "client_secret": {
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
      "description": "XSUAA OAuth client secret for client credentials flow.",
      "title": "Client Secret"
    },
    "space_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter SAP Datasphere spaces by name. Spaces are emitted as DataHub containers. Example: `{allow: ['^PROD_.*']}` to ingest only spaces with names starting with 'PROD_'."
    },
    "asset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter assets (views, analytical models, tables) by name within each space. Applied after space_pattern."
    },
    "column_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to filter columns from emitted schemas. Applied to the EDMX property name. Reduces payload size for wide tables in large catalogs."
    },
    "request_timeout_sec": {
      "default": 30,
      "description": "Per-request timeout (seconds) for HTTP calls to the SAP Datasphere catalog and consumption APIs. Increase if you see frequent timeouts on large EDMX documents.",
      "maximum": 600,
      "minimum": 1,
      "title": "Request Timeout Sec",
      "type": "integer"
    },
    "max_retries": {
      "default": 3,
      "description": "Maximum number of retries for transient HTTP errors (5xx, connection errors) when calling SAP Datasphere APIs. Uses exponential backoff.",
      "maximum": 10,
      "minimum": 0,
      "title": "Max Retries",
      "type": "integer"
    },
    "max_workers_assets": {
      "default": 10,
      "description": "Maximum number of parallel workers for per-asset metadata fetch (EDMX schema + CSN lineage). At 1M assets this is the dominant performance lever. Default 10 gives ~10x speedup vs serial. Set to 1 to disable parallelism.",
      "maximum": 64,
      "minimum": 1,
      "title": "Max Workers Assets",
      "type": "integer"
    },
    "asset_batch_size": {
      "default": 5000,
      "description": "How many assets are submitted to the parallel executor at a time. The connector processes each space's assets in batches of this size so memory stays bounded regardless of how many assets a single space holds (the executor otherwise submits every task up front). Larger = slightly better parallelism overlap at higher memory; the default keeps peak memory low while preserving effectively full parallelism (batch size >> max_workers_assets). Only relevant when max_workers_assets > 1.",
      "maximum": 100000,
      "minimum": 1,
      "title": "Asset Batch Size",
      "type": "integer"
    },
    "expose_for_consumption_only": {
      "default": false,
      "description": "If True, only emit datasets that have an `assetRelationalMetadataUrl` set (i.e., assets exposed for consumption via the SAP Datasphere consumption API). Assets without an exposure URL are counted in `assets_filtered`. Useful when you want DataHub to only show consumable views.",
      "title": "Expose For Consumption Only",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": true,
      "description": "Whether to lowercase emitted dataset and container URNs. The original case is preserved in display names and custom properties. Defaults to True to match the convention used by Snowflake and other DataHub sources so lineage merges case-insensitively across connectors.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "include_lineage": {
      "default": false,
      "description": "Extract lineage from each asset's CSN. When enabled, the connector emits table-level upstream references (from SELECT FROM clauses and @remote.source annotations) AND column-level FineGrainedLineage (walking SELECT columns[] expressions). Uses the SAP-supported per-object-type endpoint under `/dwaas-core/api/v1/spaces/X/{views,analyticmodels}/Y` (same surface the official `datasphere` CLI uses); assets where CSN fetch fails are logged to `report.assets_csn_fetch_failed` and emitted without lineage. Cost: one extra HTTP call per asset.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "emit_sap_semantics_as_tags": {
      "default": true,
      "description": "If True (default), emit SAP CDS semantic annotations (@Analytics.Dimension/Measure, @Common.IsCalendar*, @Common.IsCurrency/IsUnit, @Analytics.DimensionType) as DataHub tags on the relevant schema fields or datasets. Tags are searchable/filterable in DataHub Search and render as colored badges in the UI. The existing `sap_*` custom properties are emitted in parallel (additive) for backward compat \u2014 no breaking change. Set to False to suppress tag emission entirely.",
      "title": "Emit Sap Semantics As Tags",
      "type": "boolean"
    },
    "include_local_tables": {
      "default": false,
      "description": "If True, ALSO discover Datasphere Local Tables (base tables not exposed for OData consumption) via the supported `/dwaas-core/api/v1/spaces/X/localtables` endpoint. These tables typically appear only as upstream lineage targets of views. Emitting them as Datasets closes the phantom-lineage gap. Their column schema is read from the per-table CSN when available (enabling column-level lineage to the base table); otherwise they are emitted as schema-less stubs carrying platform + subtype + container membership. Off by default to keep ingestion minimal; turn on when you want full catalog coverage.",
      "title": "Include Local Tables",
      "type": "boolean"
    },
    "include_data_flows": {
      "default": false,
      "description": "If True, discover SAP Datasphere Data Flows and emit each as its own DataFlow + DataJob (parented under the space container) with its source/target tables as input/output datasets and column-level lineage from the producer's attributeMappings. Data Flows are the primary lineage source for Local Tables populated by an ETL flow. Uses the supported `/dwaas-core/api/v1/spaces/X/dataflows` endpoint (one list call per space + one read call per flow).",
      "title": "Include Data Flows",
      "type": "boolean"
    },
    "include_replication_flows": {
      "default": false,
      "description": "If True, discover SAP Datasphere Replication Flows and emit each as a DataJob with its source/target objects as input/output datasets and per-task column-level lineage. Replication Flows move data between two external systems; their source/target objects are routed to DataHub platforms via `connection_to_platform_map` / `platform_type_defaults` (objects on unmapped connections are skipped and reported under `flow_endpoints_unresolved`).",
      "title": "Include Replication Flows",
      "type": "boolean"
    },
    "include_remote_tables": {
      "default": false,
      "description": "If True, discover federated Remote Tables and emit each as a Dataset with upstream lineage to its external source object (parsed from the CSN `@DataWarehouse.remote.connection` / `@DataWarehouse.remote.entity` annotations). The external upstream is routed via `connection_to_platform_map` / `platform_type_defaults`; unmapped connections leave the remote table without upstream lineage (reported under `remote_table_source_unresolved`).",
      "title": "Include Remote Tables",
      "type": "boolean"
    },
    "include_transformation_flows": {
      "default": false,
      "description": "EXPERIMENTAL. If True, discover Transformation Flows and emit them as DataJobs. This path is parsed with the Data Flow process-graph reader but has NOT been verified against a live Transformation Flow payload, so lineage may be incomplete. Enable only if you can validate the output; please report payload samples so the parser can be hardened.",
      "title": "Include Transformation Flows",
      "type": "boolean"
    },
    "include_task_chains": {
      "default": false,
      "description": "EXPERIMENTAL. If True, discover Task Chains and emit each as a DataJob. No live Task Chain payload was available to reverse-engineer the member/reference grammar, so chains are currently surfaced as IO-less jobs (their presence + subtype only) without lineage edges. Enable to catalogue the objects; please report payload samples so lineage can be added.",
      "title": "Include Task Chains",
      "type": "boolean"
    },
    "resolve_external_urns_via_graph": {
      "default": false,
      "description": "If True, reconcile external flow endpoint URNs (replication/data-flow sources and targets routed to another platform such as BigQuery) against the real physical URNs already in DataHub, using the graph on the configured sink. SAP's flow API reports logical names (lower-cased leaf, and sometimes without a source-added prefix like `w01_cds_`) that need not match the physical warehouse table, so the raw edge can dangle. When enabled, each endpoint is matched by its case-insensitive leaf within the same platform/platform_instance/env and dataset/schema; an unambiguous hit is rewritten to the real URN so the lineage stitches, and misses fall back to the original candidate. Requires a DataHub graph (REST sink or `datahub_api` configured); no-op otherwise. Cost: one scoped search per distinct external dataset/schema (cached per run).",
      "title": "Resolve External Urns Via Graph",
      "type": "boolean"
    },
    "include_view_definitions": {
      "default": true,
      "description": "Whether to emit each View's / Analytic Model's definition as the DataHub viewProperties aspect (shown in the View Definition tab). SQL-editor views emit their raw SQL with viewLanguage 'SQL'; graphical/modeled views emit the Core Schema Notation (CSN/CQN) query tree with viewLanguage 'CSN'. Requires fetching the CSN, which is also fetched when include_lineage is enabled.",
      "title": "Include View Definitions",
      "type": "boolean"
    },
    "connection_to_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionPlatformConfig"
      },
      "description": "Per-Datasphere-connection mapping to DataHub platform/platform_instance/env for FEDERATED Remote Tables. Keyed by the Datasphere connection name (as returned by the Datasphere REST connections API). Managed assets (Views, Analytical Models, Local Tables that live in the tenant's own HANA Cloud) always emit on the `sap_datasphere` platform regardless of this mapping. Use this only to align FEDERATED Datasphere assets' URNs with the URNs other DataHub connectors emit for the same physical sources (e.g. snowflake).",
      "title": "Connection To Platform Map",
      "type": "object"
    },
    "platform_type_defaults": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionPlatformConfig"
      },
      "description": "Fallback platform mapping keyed by Datasphere connection typeId (HANA, SNOWFLAKE, S3, etc.). Used when an asset's connection is NOT explicitly listed in `connection_to_platform_map`. Built-in defaults cover HANA, MSSQL, S3, GCS, ABAP, SAPS4HANACLOUD, SAPBWMODELTRANSFER; other typeIds default to disabled with a warning until you add them here.",
      "title": "Platform Type Defaults",
      "type": "object"
    }
  },
  "required": [
    "base_url"
  ],
  "title": "SapDatasphereConfig",
  "type": "object"
}
```





### Capabilities

#### Lineage extraction

When `include_lineage: true` is set on the recipe, the connector emits both
table-level and column-level lineage by walking the CSN (Core Schema Notation)
returned by the supported per-object-type endpoint under
`/dwaas-core/api/v1/spaces/{space}/{views,analyticmodels}/{name}` — the same
surface the official `datasphere` CLI uses.

**Table-level lineage** is derived from the asset's `query.SELECT.from` clause
(direct refs, joins, and inline subqueries) plus `@remote.source` annotations on
federated remote tables. For Analytic Models, the star-schema fact and dimension
sources come from the business layer, which is authoritative over the query's
`FROM`. `UNION` / `INTERSECT` / `EXCEPT` (`query.SET`) branches are flattened so
every branch's `FROM` contributes an upstream. CDS **associations /
compositions** (`cds.Association` / `cds.Composition` elements) are treated as
upstreams **only when actually referenced** — projected in `SELECT` / `columns`
or used in a `join`/`on` — so unused navigation links don't create phantom
edges.

**Column-level lineage** walks `query.SELECT.columns[]` and emits one
`FineGrainedLineage` entry per downstream column with at least one resolvable
upstream column. Supported expression shapes:

| CSN shape          | Example                                         | Result                                           |
| ------------------ | ----------------------------------------------- | ------------------------------------------------ |
| Direct ref         | `{ref: [AMOUNT]}`                               | `IDENTITY` transformation                        |
| Aliased ref        | `{ref: [BASE, X], as: y}`                       | `RENAME`                                         |
| Aggregate function | `{func: SUM, args: [{ref: [X]}], as: total}`    | `AGGREGATE`                                      |
| Arithmetic / xpr   | `{xpr: [{ref: [A]}, "+", {ref: [B]}], as: sum}` | `EXPRESSION`                                     |
| Function           | `{func: UPPER, args: [{ref: [X]}], as: u}`      | `TRANSFORMATION`                                 |
| Association nav    | `{ref: [_Customer, NAME], as: cust}`            | Attributed to the association's target entity    |
| UNION / SET        | `{SET: {op: union, args: [SELECT, SELECT]}}`    | Output column merges upstreams from every branch |
| Literal            | `{val: 5, as: const}`                           | No lineage emitted (no upstream column)          |

Unresolvable refs (e.g., `SELECT *` star expansion, or columns referring to
unknown aliases) are skipped and logged to `report.column_lineage_unresolved`.

Lineage extraction adds one HTTP call per asset (the per-object-type CSN
fetch). At 1M assets with `max_workers_assets=10`, this adds ~3 hours to the
total run time. The column-level walker itself is essentially free (in-memory
tree walk over an already-fetched JSON document).

#### Flow and Remote Table lineage (ETL)

The query-derived lineage above captures how views are computed, but not how
the tables they read are _populated_. Several opt-in flags surface the
design-time objects that move and transform data, so an otherwise phantom
upstream Local Table becomes a navigable node with real upstream lineage:

- **`include_data_flows: true`** — emits each Data Flow as its own DataFlow
  (pipeline) with a single DataJob, parented under the space container, with its
  source tables as inputs, its target tables as outputs, and column-level lineage
  from the flow's column mappings. Column lineage is attributed only when a
  target has a single input source.
- **`include_replication_flows: true`** — emits each Replication Flow as its own
  DataFlow + DataJob wiring its source object(s) to target object(s), with
  per-task column mappings. Source/target systems are routed to a DataHub platform via
  `connection_to_platform_map` / `platform_type_defaults` (using the flow's own
  `connectionType` when the connection isn't in the space's connections list).
- **`include_remote_tables: true`** — emits federated Remote Tables as datasets
  with upstream lineage to their external source object (parsed from the CSN
  `@DataWarehouse.remote.*` annotations and routed via the connection maps).
- **`include_task_chains: true`** — **experimental**. Task Chains _are_
  discovered against a live tenant and emitted as IO-less DataFlow + DataJob
  pairs (their presence + subtype), but their internal member/reference grammar has not been
  reverse-engineered, so no lineage edges are produced. Enable to catalogue the
  objects; please share a payload sample if you need lineage.
- **`include_transformation_flows: true`** — **experimental**. Parsed with the
  Data Flow process-graph reader, but not yet verified against a live
  Transformation Flow payload, so lineage may be incomplete. Enable only if you
  can validate the output.

Endpoints or sources that don't resolve to a platform are counted in
`report.flow_endpoints_unresolved` and `report.remote_table_source_unresolved`
rather than being silently dropped. Each flag adds one list call per space plus
one read call per object.

#### View definitions

With `include_view_definitions: true` (default), a View's or Analytic Model's
definition is emitted as the `viewProperties` aspect — `viewLanguage="SQL"` for
SQL-editor views (the raw SQL the modeler wrote) and `viewLanguage="CSN"` for
graphical/modeled views (the CSN/CQN query tree).

#### Analytic Models

Assets with `supportsAnalyticalQueries: true` are cataloged as the
`Analytic Model` subtype. Column schema for exposed assets is read from the
OData EDMX relational-metadata URL.

#### Tags (SAP CDS semantic annotations)

With `emit_sap_semantics_as_tags: true` (default), the connector emits DataHub
tags for the SAP CDS semantic annotations it parses from EDMX, so they're
searchable and filterable in DataHub Search and render as colored badges in the
UI:

| Tag URN                                                            | Source annotation          | Where attached                   |
| ------------------------------------------------------------------ | -------------------------- | -------------------------------- |
| `urn:li:tag:Dimension`                                             | `@Analytics.Dimension`     | dataset (entity-level) or column |
| `urn:li:tag:Measure`                                               | `@Analytics.Measure`       | dataset (entity-level) or column |
| `urn:li:tag:sap:semantic:currency`                                 | `@Common.IsCurrency`       | column                           |
| `urn:li:tag:sap:semantic:unit`                                     | `@Common.IsUnit`           | column                           |
| `urn:li:tag:sap:calendar:{year,month,quarter,week,date,yearmonth}` | `@Common.IsCalendar*`      | column                           |
| `urn:li:tag:sap:dimension_type:{Time,Customer,...}`                | `@Analytics.DimensionType` | dataset                          |

`Dimension` and `Measure` URNs are flat (no namespace) so they collide with the
equivalent tags from other DataHub connectors (Looker, Snowflake semantic views,
Mode, ThoughtSpot, ...). Customers get cross-connector pivot on a single
"Measure" tag. SAP-specific concepts are namespaced under `sap:` to avoid
collision.

The existing `sap_*` custom properties (`sap_is_dimension`, `sap_calendar_type`,
`sap_semantic`, `sap_dimension_type`, ...) continue to be emitted on every
asset for backward compat — tag emission is purely additive. The same
annotations also appear in field descriptions with `[sap_calendar_type=date]`-style
suffixes. Set `emit_sap_semantics_as_tags: false` to suppress tag emission entirely.

#### API support

All endpoints called by this connector are SAP-supported public APIs:

- `/api/v1/datasphere/consumption/catalog/...` — asset discovery (Catalog API)
- `/api/v1/datasphere/consumption/relational/.../$metadata` — EDMX schema
- `/api/v1/datasphere/spaces/X/connections` — Connections API
- `/dwaas-core/api/v1/spaces/X/{views,analyticmodels,localtables,remotetables}/{name}` —
  per-object-type CSN read (with `Accept: application/vnd.sap.datasphere.object.content+json`),
  the same surface the official `datasphere` CLI uses
- `/dwaas-core/api/v1/spaces/X/{dataflows,replicationflows,transformationflows,taskchains}/{name}` —
  per-flow design-time definitions (only called when the matching `include_*` flag is set)

Previous releases of this connector used the `/deepsea/repository/` endpoint
for lineage extraction, which SAP marks as internal-use-only in
[KBA #3517441](https://userapps.support.sap.com/sap/support/knowledge/en/3517441).
That dependency was removed in this release — `include_lineage: true` is now
a fully supported feature.

The catalog service caps a page at **500 records** (both the default and the
documented maximum). The connector paginates by following the server's
`@odata.nextLink` cursor (falling back to advancing its offset by the number of
records actually returned), so spaces with more than 500 exposed assets are
fully captured.

#### Performance and scale

The dominant cost is **HTTP calls, not memory**. For each exposed asset the
connector makes roughly **2 HTTP calls** — one CSN definition fetch (per-object
type endpoint) plus one EDMX `$metadata` schema fetch — on top of paginated
catalog listing. That works out to **~2N HTTP calls for N assets** (e.g.
~2,000,000 calls for 1M datasets), so expect long runtimes at very large scale.

Levers to reduce cost and scope:

- **`space_pattern` / `asset_pattern`** — filtering is applied **before** the
  per-asset HTTP calls, so filtered-out assets are nearly free. Scope the run to
  the spaces / assets you actually need.
- **`expose_for_consumption_only: true`** — skip assets that have no consumption
  exposure URL. By default the connector catalogs all assets in each Space
  regardless of whether they are exposed for OData consumption.
- **`include_view_definitions: false` AND `include_lineage: false`** — together
  these skip the per-asset CSN fetch, roughly **halving** the HTTP calls for
  users who only need catalog + schema (no lineage / view definitions).
- **`max_workers_assets`** — default 10; raise toward 64 if the tenant tolerates
  the request rate. EDMX schema and CSN lineage are fetched per-asset (the SAP
  Datasphere API has no batch metadata endpoint), so this is the main parallelism
  lever for large catalogs.

**Memory is bounded** regardless of catalog size:

- Workunits stream out with backpressure (the connector never buffers the whole
  run).
- The ingestion report uses capped `LossyList`s, so per-run diagnostics don't
  grow unbounded.
- Each space's assets are processed in batches of `asset_batch_size` (default 5000) before being submitted to the parallel executor, so peak memory does not
  grow with the number of assets in a single space.

### Limitations

#### Catalog business-enrichment metadata is not extracted

SAP Datasphere's **Catalog & Marketplace** application surfaces governance
metadata that this connector does **not** ingest:

- **Responsible Team**, **Business Contact Person**, **Purpose**
- User-defined **Tags** and related **Glossary Terms** / **KPIs**
- **Published By** / created-by / published-on (user-level ownership & audit)

These fields are **not exposed by any public, OAuth-accessible SAP API**. They
are served only by the Catalog's internal UI backend under `/deepsea/catalog/`
and `/deepsea/repository/`, which:

1. **Rejects OAuth.** Called with an XSUAA Bearer token (the connector's auth),
   `/deepsea/...` returns an HTML login page, not JSON — it only accepts
   interactive browser **session cookies**.
2. **Is an internal UI endpoint**, not a supported REST API (its OData service
   names are explicitly marked `...private...`), so it may change without notice.

The supported public surface the connector uses
(`/api/v1/datasphere/consumption/catalog/...`) returns an asset's technical name,
business name (label), description, space, consumption URLs, and capability flags
— but none of the governance-enrichment fields above. There is therefore no
supported path to ingest them today. Support will be added if and when SAP
publishes a public Catalog metadata API.

Note: the connector **does** extract the SAP CDS semantic annotations that _are_
available via the supported EDMX/CSN surface (Dimension/Measure, currency/unit,
calendar, dimension type) — see **Capabilities → Tags**. The gap is
specifically the Catalog-app governance fields, not all business metadata.

#### Schema and type limitations

These follow from what the supported consumption surface exposes:

- **Geospatial and binary types are reported as strings.** Per SAP's "OData
  Annotation Limitations", the consumption `$metadata` (EDMX) overwrites several
  CDS types to `Edm.String`: `cds.hana.ST_GEOMETRY`, `cds.hana.ST_POINT`,
  `cds.Binary`, `cds.LargeBinary`, `cds.hana.BINARY`, `cds.LargeString`,
  `cds.UUID`. Because the connector reads column types from this EDMX surface,
  such columns appear as string types in DataHub — the original geo/binary/UUID
  type cannot be recovered from the consumption API.
- **View input parameters are not enumerated.** The connector records whether an
  asset has parameters (the `sap_*` flag derived from the catalog's
  `hasParameters`) and surfaces Analytic Model **variables** (as the
  `sap_variables` custom property), but it does not yet emit the individual
  **input-parameter** definitions of a view as schema/metadata.

#### Lineage limitations

- Scalar subqueries in column expressions ARE supported (resolved against the
  subquery's own FROM clause). Correlated subqueries that reference outer-scope
  aliases are NOT — their refs land in `report.column_lineage_unresolved`.
- Cross-Space upstream resolution: a reference that is already Space-qualified
  (e.g. `OTHER_SPACE.OBJECT`) is detected and used as-is, so it is no longer
  double-prefixed into a phantom URN. It is still routed to the downstream's
  storage platform (managed cross-Space refs stay on `sap-datasphere`), and
  column matching assumes the upstream schema mirrors the downstream.
- `SELECT *` star expansion is not unrolled — those columns are listed in
  `report.column_lineage_unresolved`.
- A `SELECT FROM T AS t` with unqualified column refs (e.g. `{ref: [X]}`) does
  NOT auto-resolve through the alias; users either need to qualify the ref
  (`t.X`) or remove the explicit `AS` alias.

#### Stateful ingestion with large catalogs

The connector uses DataHub's standard `StaleEntityRemovalHandler` for soft-delete
detection. This handler keeps a checkpoint of every emitted URN in a single MCP
that is sent to GMS at the end of each run.

At default GMS / Kafka payload limits (1 MB Kafka message, 16 MB GMS REST), the
checkpoint can hold approximately **80,000 dataset URNs** before exceeding
limits. If your Datasphere catalog has more than ~50,000 datasets we recommend:

- Disable `stateful_ingestion` (set `enabled: false`) and use manual soft-delete
  cleanup via the DataHub UI / CLI.
- Or, partition the ingestion by `space_pattern` so each run stays under the
  ceiling.

The connector emits a `report.warning` when emitted entity count crosses 50,000
during a single run (via `_check_scale_warning`).

### Troubleshooting

#### Fewer objects than the Data Builder shows

This is the **#1 reason DataHub captures fewer objects than the Data Builder
shows**: a human browsing the Data Builder sees spaces they personally belong
to, while the ingestion principal only sees the spaces _it_ belongs to. If a
space is missing from DataHub, check the `report` warnings for the
_"Not a member of SAP Datasphere space"_ message and add the principal to that
space (see **Prerequisites → Space membership**).

#### HTTP 429 throttling

SAP Datasphere rate-limits the OData/consumption APIs to **~300 requests per
user per minute** (per the OData API documentation); exceeding it returns HTTP
429 with a `Retry-After` header. The connector's HTTP layer already retries 429
with exponential backoff (honoring `Retry-After`, bounded by `max_retries`), but
setting `max_workers_assets` too high can sustain throttling and slow the run —
tune it down if you see repeated 429 retries in the logs.

#### Local testing with a mock Datasphere (no tenant required)

You can exercise the full connector path — including the OAuth handshake —
without a real SAP tenant, using the bundled standalone mock server. It serves
**real recorded tenant responses** (catalog spaces/assets/connections,
per-view CSN, EDMX, local tables) plus a **permissive `/oauth/token`** endpoint,
so the connector's OAuth cold-start, data fetch, and lineage all run end-to-end.

Start the mock server (it prints a ready-to-run recipe and the command to run):

```bash
# Push into a running DataHub:
python tests/integration/sap_datasphere/mock_datasphere_server.py --sink http://localhost:8080

# Or omit --sink for a file sink (writes to /tmp/sap_datasphere_mock_out.json):
python tests/integration/sap_datasphere/mock_datasphere_server.py
```

Then, in another terminal, run ingestion against the generated recipe:

```bash
datahub ingest -c /tmp/sap_datasphere_mock_recipe.yml
```

> `mock_datasphere_server.py` needs `pytest-httpserver` (a test dependency). If
> it isn't importable, install it with `pip install pytest-httpserver` or
> install this package's test extra.

**UI-ingestion caveat:** the DataHub UI's ingestion executor runs in a
container, so `localhost` / `127.0.0.1` in the recipe won't reach a mock server
running on the host. Bind the server to all interfaces with `--host 0.0.0.0`
and point the recipe's `base_url` / `xsuaa_url` at `host.docker.internal:18000`
(Docker Desktop) or the host's LAN IP instead.


### Code Coordinates
- Class Name: `datahub.ingestion.source.sap_datasphere.source.SapDatasphereSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sap_datasphere/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for SAP Datasphere, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
