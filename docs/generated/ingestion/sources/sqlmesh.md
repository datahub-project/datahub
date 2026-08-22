


# SQLMesh

## Overview

[SQLMesh](https://sqlmesh.com) is an open-source DataOps framework for building and operating SQL transformation pipelines. It manages model versioning, virtual environments, incremental execution, and data quality audits across warehouses including Snowflake, BigQuery, Databricks, DuckDB, and others.

DataHub ingests SQLMesh model metadata—schema, lineage, column-level lineage, descriptions, and data quality audits—and links each model to its corresponding warehouse view as a sibling entity. This follows the same pattern as the dbt connector: the SQLMesh entity owns model definitions and lineage while the warehouse connector contributes runtime metadata such as query history, profiling, and usage. DataHub merges both views in the UI automatically.

## Concept Mapping

| SQLMesh concept     | DataHub entity / aspect                          |
| ------------------- | ------------------------------------------------ |
| Model               | Dataset (`urn:li:dataPlatform:sqlmesh,...`)      |
| Model depends_on    | UpstreamLineage (coarse-grained)                 |
| Column dependencies | FineGrainedLineage (column-level lineage)        |
| Model description   | DatasetProperties.description                    |
| Column descriptions | SchemaMetadata field descriptions                |
| Model tags          | GlobalTags                                       |
| Model owner         | Ownership                                        |
| Audit definition    | Assertion entity (AssertionInfo)                 |
| Audit run result    | AssertionRunEvent (pass / fail)                  |
| Warehouse view      | Sibling dataset on the target warehouse platform |
| Database / Schema   | Container hierarchy                              |


## Module `sqlmesh`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Supported when model descriptions are defined. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

The `sqlmesh` source plugin reads SQLMesh project metadata directly from the SQLMesh Python
API—no running SQLMesh server is required. It emits Dataset entities on the
`urn:li:dataPlatform:sqlmesh` platform and links each one to its corresponding warehouse
view (Snowflake, BigQuery, DuckDB, etc.) as a sibling, so DataHub merges both into a
single unified view in the UI.

### Prerequisites

- Python 3.9 or later
- The `sqlmesh` Python package installed in the ingestion environment:
  `pip install 'acryl-datahub[sqlmesh]'`
- Read access to the SQLMesh project directory (config files and model SQL)
- If using a remote gateway (Snowflake, BigQuery, etc.), valid gateway credentials
  in the SQLMesh project config—the source plugin loads the SQLMesh context which opens
  a connection to resolve model metadata

#### Concepts and setup

The rest of this section covers the concepts and setup you need before configuring
a recipe.

#### Project location: local, S3, or Git

`project_path` accepts three kinds of location, so the SQLMesh project does not have to
be checked out next to the ingestion process:

- **Local directory** (default) — a filesystem path, e.g. `project_path: /opt/sqlmesh_project`.
- **S3 prefix** — `project_path: s3://my-bucket/sqlmesh_project` with an `aws_connection`
  block for credentials. The entire prefix is downloaded to a temporary directory for the
  run, so the whole project tree (config, `models/`, `audits/`, `macros/`, seeds) must live
  under that prefix.
- **Git repository** — a `git_info` block shallow-clones the repo (authenticated with an SSH
  deploy key); `project_path` is then interpreted _relative to the checkout_ (`.`, the
  default, is the repo root; use a subdirectory such as `sqlmesh/` when the project isn't at
  the root).

```yaml
# S3
source:
  type: sqlmesh
  config:
    project_path: s3://my-bucket/sqlmesh_project
    aws_connection:
      aws_region: us-east-1
      # aws_access_key_id / aws_secret_access_key, aws_role, or an instance profile

# Git
source:
  type: sqlmesh
  config:
    git_info:
      repo: https://github.com/my-org/my-sqlmesh-repo
      branch: main
      deploy_key_file: /secrets/sqlmesh_deploy_key
    project_path: sqlmesh # relative to the repo root
```

#### Fingerprint table abstraction

SQLMesh internally manages model versions through **fingerprint tables**—versioned physical
tables with hashes in their names (e.g., `schema.model__47716296`). These are implementation
details that track snapshots and enable efficient incremental processing. The connector
automatically abstracts away this complexity: fingerprint tables are never exposed in DataHub.

Instead, users see only the **logical views** (clean published model names like `schema.model`)
and the **physical source tables** that feed into them. This keeps the data lineage graph
clean and focused on business semantics, not internal versioning mechanics.

#### Sibling stitching

For sibling URNs to stitch correctly, the `sqlmesh` connector and your warehouse connector
must agree on the dataset name. Key config options:

- **`target_platform_instance`** — must match the `platform_instance` in your warehouse recipe exactly.
- **`default_catalog`** — required when SQLMesh model names are two-part (`schema.model`) but
  your warehouse connector emits three-part URNs (`catalog.schema.table`).
- **`convert_urns_to_lowercase`** — auto-enabled for Snowflake; set manually for other
  case-folding warehouses.

#### Column-level lineage

SQLMesh parses all SQL through [SQLGlot](https://github.com/tobymao/sqlglot), so
column-level lineage is available natively for all SQL models without any additional
configuration. Python models (ibis, pandas) do not have column-level lineage available.

#### Data quality audits

SQLMesh audit definitions (`not_null`, `unique_values`, `unique_combination_of_columns`,
`number_of_rows`, `forall`, `accepted_range`, `accepted_values`) are emitted as typed
DataHub [Assertion](/docs/managed-datahub/observe/assertions)
entities and appear on the **Validation** tab of each dataset.

To surface pass/fail run results on the Validation tab, produce a JSON results file
and point `audit_results_path` at it. The expected format is described in the config
reference below.

#### Multi-gateway projects

SQLMesh projects can declare multiple gateways targeting different warehouses
(e.g. staging models on BigQuery, curated models on Snowflake). The connector
reads `ctx.engine_adapters` to discover every gateway and routes each model's
sibling URN to the correct warehouse platform automatically.

Configuration:

- Top-level `target_platform` / `target_platform_instance` / `default_catalog`
  continue to apply to the project's **default** gateway (`default_gateway:`
  in the SQLMesh config).
- `gateway_overrides:` provides per-gateway values for any non-default
  gateway. Each entry takes the same four fields, all optional. Anything
  omitted is auto-detected from the gateway's connection config or falls
  back to the project-level default.

Example multi-gateway recipe:

```yaml
source:
  type: sqlmesh
  config:
    project_path: /path/to/sqlmesh_project
    target_platform: snowflake # for the default gateway
    target_platform_instance: prod_snowflake
    default_catalog: analytics
    gateway_overrides:
      bigquery_lake:
        target_platform: bigquery
        target_platform_instance: prod_bigquery
        default_catalog: lake-prod
      duckdb_sandbox:
        target_platform: duckdb
        target_platform_instance: dev_duckdb
```

For a single-gateway project just omit `gateway_overrides` — behaviour is
unchanged.

#### Authentication and secrets

Two kinds of credentials matter for an ingest run:

| Credential                                  | Where it lives                                 | How to provide it                                                                           |
| ------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **DataHub GMS token**                       | DataHub sink config                            | `${DATAHUB_TOKEN}` interpolated in the recipe                                               |
| **Tobiko Cloud token** (Enterprise only)    | This connector's config                        | `tobiko_cloud_token: ${TOBIKO_TOKEN}` _or_ `tobiko_cloud_token_file: ${SECRETS_DIR}/tobiko` |
| **Warehouse credentials** (Snowflake, etc.) | SQLMesh project's `config.yaml` or `config.py` | SQLMesh's own env-var interpolation — `password: ${SNOWFLAKE_PASSWORD}` etc.                |

The two patterns the connector itself supports:

1. **Env-var inline** — every config field is interpolated by DataHub's YAML
   loader, so any `${ENV_VAR}` reference works. Use this for tokens that
   change rarely.

2. **Env-var-specified file path** — for tokens that rotate (Kubernetes
   projected secret volumes etc.) point a `*_file` field at the mount path:

   ```yaml
   source:
     type: sqlmesh
     config:
       tobiko_cloud_token_file: ${SECRETS_DIR}/tobiko-cloud-token
   ```

   File reads go through a 60-second TTL cache so the connector picks up
   rotated tokens within one cache window without restarting the ingestion
   process.

For multi-gateway projects, every gateway in the SQLMesh project must have
working credentials at ingest time — SQLMesh opens a connection per
gateway when loading the Context. Set those credentials via env vars in
the SQLMesh project's `config.yaml`:

```yaml
gateways:
  snowflake_prod:
    connection:
      type: snowflake
      user: ${SNOWFLAKE_USER}
      password: ${SNOWFLAKE_PASSWORD}
      account: ${SNOWFLAKE_ACCOUNT}
  bigquery_lake:
    connection:
      type: bigquery
      method: service-account
      keyfile: ${BIGQUERY_KEYFILE}
```

The connector inherits whatever credentials SQLMesh resolves; it never
reads warehouse creds directly. If a gateway can't open its connection
the corresponding models still get URN routing (sibling stitching uses
metadata only), but volume / freshness signals that need
`ctx.engine_adapter` will skip for that gateway and the report shows
`has_warehouse_query_access: False`.

#### Warehouse permissions

The connector inherits whatever SQLMesh's gateway has. The minimum the
SQLMesh user / service account needs at ingestion time:

- **Read on the SQLMesh state schema** (`sqlmesh__*`, or the configured
  `state_connection` schema). Used to read `snapshot.updated_ts` for
  freshness and `snapshot.table_name()` for the authoritative fingerprint
  name. Without this the connector still emits assertion **definitions**
  but skips the freshness OperationAspect and the volume row count.
- **`SELECT` on the fingerprint tables** themselves
  (`<catalog>.sqlmesh__<schema>.*`). Used for the `SELECT COUNT(*)`
  query that populates `DatasetProfile.rowCount` for the volume
  assertion baseline.
- **No write privileges needed** — the connector is read-only.

If you're using a Tobiko Cloud deployment the state-store read is
fulfilled by the cloud API token; the warehouse read still applies.

#### The SQLMesh project directory is required

Unlike state and warehouse access (which the connector can degrade
around), the **SQLMesh project files themselves are mandatory**.
SQLMesh's Python API loads everything — model SQL, audits, macros,
the gateway config — from a directory; there is no API path that
returns model metadata without it. If the connector can't read
`config.yaml` / `config.py` at `project_path`, ingestion fails fast.

This affects deployment in a few ways depending on where the project
lives:

- **Same repo as the warehouse / dbt project**: trivial — the project
  is in the working tree, point `project_path` at it.
- **Separate repo**: clone or sync the SQLMesh project alongside the
  recipe at ingest time. In CI this is typically a second
  `actions/checkout` for the SQLMesh repo. In Kubernetes a `git-sync`
  sidecar (or initContainer that does a shallow clone) is the standard
  pattern, both for keeping the project current and for avoiding a
  full image rebuild on every commit.
- **Tobiko Cloud-managed projects**: the source files still live in
  the user's git repo even when state is in Tobiko Cloud — Tobiko
  Cloud doesn't host the SQL. Same patterns apply.
- **Project doesn't fit on the ingestion host** (very large repos):
  the connector only reads the files; no SQLMesh-side execution
  happens during ingest. A sparse-checkout limited to the SQLMesh
  project subtree is sufficient.

What the connector reads from the project at ingestion time:

- `config.yaml` / `config.py` — for the gateway list and SQLMesh
  defaults
- `models/**/*.sql` and `models/**/*.py` — model definitions, audits,
  lineage
- `audits/**` and `macros/**` — referenced by models
- `external_models.yaml` — declared external sources (Category 2)

Optional / read separately from a configured path:

- `audit_results_path` JSON — produced externally by
  `sqlmesh audit --output` or your own tooling; outside `project_path`

#### What works with vs without state-store access

The connector probes three capabilities once at Context load
(`has_state`, `has_warehouse_query`, `has_graph`) and surfaces them in
the report. Different emissions depend on different probes.

**Happy path — all three available:**

| Emission                                        | Depends on                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------ |
| Dataset entities + schema + lineage             | nothing (just project files)                                                         |
| Sibling URN routing                             | nothing                                                                              |
| Assertion **definitions** (audit only)          | nothing                                                                              |
| Assertion **run events** (audit pass/fail)      | `audit_results_path` JSON file (no probes needed)                                    |
| Volume `DatasetProfile.rowCount`                | `has_state` (for authoritative fingerprint name) + `has_warehouse_query` (for COUNT) |
| Pipeline `OperationAspect.lastUpdatedTimestamp` | `has_state` (for `snapshot.updated_ts`)                                              |

**Minimal path — state store unavailable** (`has_state: false`):

The connector still emits the full metadata model — datasets, schema,
column-level lineage, audits, assertion definitions, siblings,
containers, ownership, tags. The audit assertions are `CUSTOM` (SQLMesh
runs them, DataHub records the definition and the pass/fail run events
from `audit_results_path`), so they populate the Validation tab
regardless of state availability. What you **lose**:

- `OperationAspect` for fingerprint rebuild timestamps — DataHub doesn't
  see "when did SQLMesh last apply this model".
- `DatasetProfile.rowCount` from this connector — your warehouse
  connector still profiles the underlying table, so volume baselines
  for Cloud Monitor flow through the warehouse connector instead.

Reasons you'd land in this state:

- **Tobiko Cloud without a token**: the connector falls back to a local
  DuckDB stub for state; `has_state` is technically `True` against the
  stub but useless. Set `tobiko_cloud_token` to recover.
- **State schema permissions** the SQLMesh role doesn't have.
- **Fresh project that has never run `plan/apply`**: the state schema
  exists but is empty; nothing to skip into.

Minimal recipe that explicitly opts out of state-dependent emissions
(useful for environments where the state store is off-limits and you
want a clean ingest report without the "skipped" warnings):

```yaml
source:
  type: sqlmesh
  config:
    project_path: /path/to/sqlmesh_project
    gateway: snowflake_prod
    target_platform_instance: prod_snowflake
    default_catalog: ANALYTICS
    sqlmesh_platform_instance: prod
    include_column_lineage: true
    convert_urns_to_lowercase: true
    # OperationAspect + DatasetProfile are emitted when state/warehouse are reachable.
    # Create freshness/volume monitors in DataHub against those timeseries.
    stateful_ingestion:
      enabled: true
```

What this gives you: **complete dataset metadata, the audit Validation tab, and
Operation/Profile timeseries for the monitors you create.** Strictly less than
the happy path, but still the bulk of the value.

#### Real-world configuration examples

End-to-end Snowflake setup, the most common case:

```yaml
# In your SQLMesh project's config.yaml
gateways:
  snowflake_prod:
    connection:
      type: snowflake
      user: ${SNOWFLAKE_USER}
      password: ${SNOWFLAKE_PASSWORD}
      account: ${SNOWFLAKE_ACCOUNT}
      warehouse: COMPUTE_WH
      role: SQLMESH_ROLE
      database: ANALYTICS # the catalog SQLMesh writes to
default_gateway: snowflake_prod
model_defaults:
  dialect: snowflake
```

```yaml
# DataHub ingestion recipe
source:
  type: sqlmesh
  config:
    project_path: /path/to/sqlmesh_project
    gateway: snowflake_prod
    # target_platform auto-detects to "snowflake" from the gateway dialect
    target_platform_instance: prod_snowflake # must match Snowflake connector
    default_catalog: ANALYTICS # set when SQLMesh model names are 2-part
    environment: prod # SQLMesh environment to ingest from
    sqlmesh_platform_instance: prod # namespace for urn:li:dataPlatform:sqlmesh
    include_column_lineage: true
    convert_urns_to_lowercase: true # Snowflake auto-folds — required for stitching
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: ${DATAHUB_GMS_URL}
    token: ${DATAHUB_TOKEN}
```

Env vars expected at runtime: `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_ACCOUNT`, `DATAHUB_GMS_URL`, `DATAHUB_TOKEN`.

##### BigQuery service-account JSON

```yaml
# SQLMesh config.yaml
gateways:
  bigquery_lake:
    connection:
      type: bigquery
      method: service-account
      keyfile: ${GOOGLE_APPLICATION_CREDENTIALS} # path to JSON key
      project: my-gcp-project
default_gateway: bigquery_lake
model_defaults:
  dialect: bigquery
```

`GOOGLE_APPLICATION_CREDENTIALS` may point at a path mounted by a
secret manager (GCP Secret Manager, K8s projected secret, etc.). The
file is read by the BigQuery client library directly; the connector
itself doesn't touch it.

##### Tobiko Cloud Enterprise

```yaml
source:
  type: sqlmesh
  config:
    project_path: /path/to/sqlmesh_project
    gateway: snowflake_prod # must be set so tobikodata knows which gateway
    # Either inline:
    tobiko_cloud_token: ${TOBIKO_CLOUD_TOKEN}
    # OR file-based (preferred for K8s with rotated tokens):
    tobiko_cloud_token_file: ${SECRETS_DIR}/tobiko-cloud-token
    # Only needed when the project's config.py doesn't already declare it
    # tobiko_cloud_url: https://my-org.tobiko.cloud
```

Without a token, the connector falls back to a local DuckDB stub for
state — model definitions still load from project files, but
`snapshot.updated_ts` and other state-derived signals are unavailable
(`has_state_store_access: False` in the report).

##### Running in Kubernetes

The connector ships no special K8s requirements. Common setup:

- Mount the SQLMesh project as a `configMap` or `git-sync` sidecar.
- Mount warehouse credentials and the Tobiko Cloud token via
  **projected secret volumes** under e.g. `/var/run/secrets/`. Point
  `tobiko_cloud_token_file` at the mount path. The 60-second TTL cache
  means token rotation is picked up within one cache window without
  pod restart.
- Set `SECRETS_DIR` so the env-var-interpolation pattern shown above
  works without hardcoding paths.
- Run the connector as a `CronJob` or via Airflow / Dagster /
  GitHub Actions on whatever cadence makes sense for your refresh
  rate (commonly once per day, aligned with the SQLMesh apply cron).

##### Running in CI/CD (GitHub Actions example)

```yaml
- name: Ingest SQLMesh metadata to DataHub
  env:
    SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
    SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
    SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
    DATAHUB_GMS_URL: ${{ secrets.DATAHUB_GMS_URL }}
    DATAHUB_TOKEN: ${{ secrets.DATAHUB_TOKEN }}
  run: |
    pip install 'acryl-datahub[sqlmesh]'
    datahub ingest -c recipe.yml
```

Run this step **after** your `sqlmesh plan` / `sqlmesh apply` step in
the same workflow so the freshness `OperationAspect` reflects the
just-completed rebuild, and so the optional `audit_results_path` JSON
(if you produce one with `sqlmesh audit --output`) shows the latest
pass/fail.


### Install the Plugin
```shell
pip install 'acryl-datahub[sqlmesh]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: sqlmesh
  config:
    project_path: /path/to/sqlmesh_project
    # gateway: my_gateway  # defaults to the project's default gateway
    # Tobiko Cloud projects: point at your cloud state store. The token is only
    # needed for CI/service accounts; SSO users authenticate via `tcloud auth login`.
    # tobiko_cloud_url: https://cloud.tobikodata.com/sqlmesh/<org>/<project>/
    # tobiko_cloud_token: "${TCLOUD_TOKEN}"
    # target_platform: snowflake  # auto-detected from gateway connection type
    # target_platform_instance: prod_snowflake  # must match warehouse connector
    # default_catalog: analytics  # required for 2-part model names + 3-part warehouse URNs
    environment: prod # SQLMesh environment to ingest from
    include_column_lineage: true
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    # token is optional for a local unauthenticated DataHub; uncomment for
    # authenticated / hosted (DataHub Cloud) instances.
    # token: "${DATAHUB_TOKEN}"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">audit_results_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to a JSON file containing SQLMesh audit pass/fail results. When set, the connector emits AssertionRunEvent aspects for each result, making pass/fail status visible on the DataHub Data Quality tab. The file must exist at ingestion time; results with no matching assertion definition are silently skipped. <br />  <br /> Expected JSON format:: <br />  <br />   { <br />     "metadata": {"generated_at": "2024-01-01T00:00:00Z"}, <br />     "results": [ <br />       { <br />         "model": "myschema.orders", <br />         "audit": "not_null", <br />         "columns": ["order_id"], <br />         "status": "pass", <br />         "failing_rows": 0 <br />       } <br />     ] <br />   } <br />  <br /> Valid ``status`` values: ``pass``, ``fail``, ``skip``. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">convert_column_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Force column names in field URNs to lowercase. Defaults to the same value as convert_urns_to_lowercase when not set. Set explicitly when column name casing in your warehouse connector differs from the dataset URN casing (e.g. Snowflake uppercases column names). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">default_catalog</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default catalog (database) to prepend to model names that are only two-part (schema.model). Required for sibling URN stitching when your warehouse connector emits three-part URNs (catalog.schema.table) but SQLMesh model names omit the catalog. Example: set to 'analytics' so that 'star.dim_developer' becomes 'analytics.star.dim_developer', matching what the Snowflake connector emits. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">detect_stale_fingerprints</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, detect SQLMesh fingerprint tables that haven't been regenerated recently (no plan/apply runs). Use this to monitor if SQLMesh transformations are running on their expected schedules. Reads snapshot timestamps from the SQLMesh state store; silently skipped when state is unreachable. When a fingerprint is stale, a custom property 'sqlmesh.fingerprint_stale' is added to the dataset. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">emit_incidents_on_failure</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit a DataHub Incident entity (``urn:li:incident:…``) every time ``_emit_audit_run_events`` reads a ``"fail"`` result from the ``audit_results_path`` JSON file. The incident links back to the assertion via ``IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`` so the Incidents tab on the dataset shows the failure history. Standard DataHub entity — works regardless of edition. Cloud additionally adds Slack threading and triage ML on top. Re-emitting the same incident is idempotent because the URN is derived from a hash of (assertion_urn, run_id). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | SQLMesh environment to ingest from (e.g. prod, dev). <div className="default-line default-line-with-docs">Default: <span className="default-value">prod</span></div> |
| <div className="path-line"><span className="path-main">fingerprint_staleness_threshold_hours</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of hours before a fingerprint table is considered stale. Only used when detect_stale_fingerprints=True. A fingerprint that hasn't been updated (via plan/apply) within this many hours will be flagged as stale. Default: 48 hours (2 days). <div className="default-line default-line-with-docs">Default: <span className="default-value">48</span></div> |
| <div className="path-line"><span className="path-main">gateway</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | SQLMesh gateway name. Defaults to the project's default gateway. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit column-level lineage derived from SQLMesh's SQL parsing (via SQLGlot). Available for all SQL models natively — no separate parsing step needed. Disable for very large projects where per-column analysis is too slow. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_database_name</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include the database/catalog component in warehouse sibling URNs. Set to false for platforms like Athena that omit the catalog from their URNs. When false, 'analytics.star.dim_developer' becomes 'star.dim_developer' in the warehouse URN. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit model-to-model lineage derived from SQLMesh DAG dependencies. Disable if lineage is managed by another connector or not needed. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_model_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit dataset properties (description, custom properties) for each model. Disable to ingest schema and lineage only. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit column schema metadata for each model. Disable to reduce ingestion volume when schema is already captured by a warehouse connector. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use patch/incremental lineage mode for non-SQLMesh entities (e.g. external warehouse tables referenced in lineage). When enabled, the plugin adds lineage edges without overwriting edges the warehouse connector previously discovered. Must match the warehouse connector's incremental_lineage setting. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">owner_extraction_pattern</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Regex pattern to extract the owner identity from the SQLMesh model ``owner`` field. The first capture group is used as the owner. Example: ``(.*)@.*`` extracts the username from an email address. When not set, the owner field value is used as-is. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">preview_urns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Before emitting metadata, print a sample of generated sqlmesh URNs and expected warehouse sibling URNs side-by-side to the log. Helps validate URN stitching before a full run. Set to true for a dry-run style check, or use --dry-run on the CLI. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">preview_urns_sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample models to include in the URN preview output. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">project_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Location of the SQLMesh project. One of: a local directory path; an ``s3://bucket/prefix`` pointing at the project tree (requires ``aws_connection``); or — when ``git_info`` is set — a path *relative to the cloned repository* (``.``, the default, is the repo root). <div className="default-line default-line-with-docs">Default: <span className="default-value">.</span></div> |
| <div className="path-line"><span className="path-main">skip_external_models_in_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When false (default), declared external models (defined in external_models.yaml) appear as SQLMesh 'Source' entities in the lineage graph. When true, lineage from managed models points directly to the warehouse URN for external models — skipping the SQLMesh entity. Produces a cleaner graph if external models are already well-represented by the warehouse connector. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">sqlmesh_is_primary_sibling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When true (default), the SQLMesh entity is the primary sibling — its name, description, and lineage take precedence in the merged UI view. The warehouse entity contributes runtime metadata (tags, query history, profiling, usage). Matches dbt's dbt_is_primary_sibling=true default. Set to false if your warehouse entity carries authoritative documentation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sqlmesh_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance for the sqlmesh entities themselves. Use this to namespace the urn:li:dataPlatform:sqlmesh entities and avoid collisions when multiple SQLMesh projects write to the same warehouse. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Prefix prepended to SQLMesh model tags when creating DataHub tags. Example: a model tag 'pii' becomes DataHub tag 'sqlmesh:pii'. Set to empty string to use tags as-is. <div className="default-line default-line-with-docs">Default: <span className="default-value">sqlmesh:</span></div> |
| <div className="path-line"><span className="path-main">target_platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Warehouse platform SQLMesh writes to (e.g. snowflake, bigquery, databricks). Auto-detected from the gateway connection type if not set — only specify this when auto-detection produces the wrong value. Must match the platform used in your warehouse connector recipe so that sibling URNs stitch correctly. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">target_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance for the target warehouse. Must exactly match the platform_instance configured in your warehouse connector recipe so that sibling URNs stitch correctly. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tobiko_cloud_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Tobiko Cloud API token. Set this when the SQLMesh project is configured against Tobiko Cloud (an ``EnterpriseConfig`` with a cloud state connection) and DataHub should read from the real cloud state store. Mutually exclusive with ``tobiko_cloud_token_file``. When neither is set, DataHub falls back to a local DuckDB stub so Context init succeeds without creds — model definitions still come from the project files, but anything that depends on remote state (snapshot history, environment promotions) is unavailable. Requires ``gateway`` to be set; the gateway name determines which ``SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__*`` variables get populated for tobikodata to read. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tobiko_cloud_token_file</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to a file containing the Tobiko Cloud API token (single line). Re-read with a 60-second cache TTL so projected Kubernetes secret mounts pick up rotated tokens without a process restart. Mutually exclusive with ``tobiko_cloud_token``. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tobiko_cloud_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Tobiko Cloud state-store URL. Only needed when the project's ``config.py`` does not already declare it on its cloud state connection. Used for both static-token and SSO auth, so it must be https:// whenever it is set (credentials/state travel over it). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_connection</span></div> <div className="type-name-line"><span className="type-name">One of AwsConnectionConfig, null</span></div> | AWS connection details for loading the project from an ``s3://`` ``project_path``. Required whenever ``project_path`` is an S3 URI. The entire prefix is downloaded to a temp directory for the run. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">gateway_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,GatewayOverride)</span></div> | Per-gateway overrides for warehouse-URN construction. <br />  <br /> SQLMesh projects can declare multiple gateways (e.g. Snowflake for some <br /> models, BigQuery for others). The top-level ``target_platform`` / <br /> ``target_platform_instance`` / ``default_catalog`` apply to the default <br /> gateway; ``gateway_overrides`` lets you set per-gateway values for the <br /> others. Anything left ``None`` falls back to auto-detection from <br /> ``ctx.engine_adapters[gateway].dialect``.  |
| <div className="path-line"><span className="path-prefix">gateway_overrides.`key`.</span><span className="path-main">target_platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Warehouse platform for this gateway. Auto-detected from the gateway connection type if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gateway_overrides.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Force-lowercase URNs for this gateway. Defaults to the project-level value, or True for Snowflake. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gateway_overrides.`key`.</span><span className="path-main">default_catalog</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | default_catalog for this gateway. Prepended to 2-part model names to build 3-part warehouse URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gateway_overrides.`key`.</span><span className="path-main">target_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | platform_instance for this gateway. Must match the warehouse connector's platform_instance for sibling URN stitching. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">git_info</span></div> <div className="type-name-line"><span className="type-name">One of GitInfo, null</span></div> | Git repository to shallow-clone (authenticated with an SSH deploy key) and load the SQLMesh project from. When set, ``project_path`` is interpreted relative to the checkout (e.g. ``project_path: sqlmesh/`` for a project in a repo subdirectory). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if git_info is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.  |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">clone_timeout</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Timeout in seconds for git clone operations. Set to None to disable the timeout. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">One of string(file-path), null</span></div> | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_subdir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">model_kind_filter</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Filter which model kinds to ingest. When set, only models whose kind matches one of the listed values are processed. Valid values: FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, INCREMENTAL_BY_PARTITION, SCD_TYPE_2_BY_TIME, SCD_TYPE_2_BY_COLUMN, VIEW, SEED, EXTERNAL, EMBEDDED. Default: all kinds. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">model_kind_filter.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">model_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">model_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "AwsAssumeRoleConfig": {
      "additionalProperties": true,
      "properties": {
        "RoleArn": {
          "description": "ARN of the role to assume.",
          "title": "Rolearn",
          "type": "string"
        },
        "ExternalId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "External ID to use when assuming the role.",
          "title": "Externalid"
        }
      },
      "required": [
        "RoleArn"
      ],
      "title": "AwsAssumeRoleConfig",
      "type": "object"
    },
    "AwsConnectionConfig": {
      "additionalProperties": false,
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "properties": {
        "aws_access_key_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Access Key Id"
        },
        "aws_secret_access_key": {
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
          "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Secret Access Key"
        },
        "aws_session_token": {
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
          "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Session Token"
        },
        "aws_role": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "items": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "$ref": "#/$defs/AwsAssumeRoleConfig"
                  }
                ]
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
          "title": "Aws Role"
        },
        "aws_profile": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
          "title": "Aws Profile"
        },
        "aws_region": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS region code.",
          "title": "Aws Region"
        },
        "aws_endpoint_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
          "title": "Aws Endpoint Url"
        },
        "aws_proxy": {
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
          "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
          "title": "Aws Proxy"
        },
        "aws_retry_num": {
          "default": 5,
          "description": "Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "title": "Aws Retry Num",
          "type": "integer"
        },
        "aws_retry_mode": {
          "default": "standard",
          "description": "Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "enum": [
            "legacy",
            "standard",
            "adaptive"
          ],
          "title": "Aws Retry Mode",
          "type": "string"
        },
        "read_timeout": {
          "default": 60,
          "description": "The timeout for reading from the connection (in seconds).",
          "title": "Read Timeout",
          "type": "number"
        },
        "aws_advanced_config": {
          "additionalProperties": true,
          "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
          "title": "Aws Advanced Config",
          "type": "object"
        }
      },
      "title": "AwsConnectionConfig",
      "type": "object"
    },
    "GatewayOverride": {
      "additionalProperties": false,
      "description": "Per-gateway overrides for warehouse-URN construction.\n\nSQLMesh projects can declare multiple gateways (e.g. Snowflake for some\nmodels, BigQuery for others). The top-level ``target_platform`` /\n``target_platform_instance`` / ``default_catalog`` apply to the default\ngateway; ``gateway_overrides`` lets you set per-gateway values for the\nothers. Anything left ``None`` falls back to auto-detection from\n``ctx.engine_adapters[gateway].dialect``.",
      "properties": {
        "target_platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Warehouse platform for this gateway. Auto-detected from the gateway connection type if not set.",
          "title": "Target Platform"
        },
        "target_platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "platform_instance for this gateway. Must match the warehouse connector's platform_instance for sibling URN stitching.",
          "title": "Target Platform Instance"
        },
        "default_catalog": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "default_catalog for this gateway. Prepended to 2-part model names to build 3-part warehouse URNs.",
          "title": "Default Catalog"
        },
        "convert_urns_to_lowercase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Force-lowercase URNs for this gateway. Defaults to the project-level value, or True for Snowflake.",
          "title": "Convert Urns To Lowercase"
        }
      },
      "title": "GatewayOverride",
      "type": "object"
    },
    "GitInfo": {
      "additionalProperties": false,
      "description": "A reference to a Git repository, including a deploy key that can be used to clone it.",
      "properties": {
        "repo": {
          "description": "Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.",
          "title": "Repo",
          "type": "string"
        },
        "branch": {
          "default": "main",
          "description": "Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
          "title": "Branch",
          "type": "string"
        },
        "url_subdir": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations.",
          "title": "Url Subdir"
        },
        "url_template": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}",
          "title": "Url Template"
        },
        "deploy_key_file": {
          "anyOf": [
            {
              "format": "file-path",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase.",
          "title": "Deploy Key File"
        },
        "deploy_key": {
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
          "description": "A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.",
          "title": "Deploy Key"
        },
        "repo_ssh_locator": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.",
          "title": "Repo Ssh Locator"
        },
        "clone_timeout": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 300,
          "description": "Timeout in seconds for git clone operations. Set to None to disable the timeout.",
          "title": "Clone Timeout"
        }
      },
      "required": [
        "repo"
      ],
      "title": "GitInfo",
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
      "description": "Stateful Ingestion Config"
    },
    "project_path": {
      "default": ".",
      "description": "Location of the SQLMesh project. One of: a local directory path; an ``s3://bucket/prefix`` pointing at the project tree (requires ``aws_connection``); or \u2014 when ``git_info`` is set \u2014 a path *relative to the cloned repository* (``.``, the default, is the repo root).",
      "title": "Project Path",
      "type": "string"
    },
    "aws_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/AwsConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS connection details for loading the project from an ``s3://`` ``project_path``. Required whenever ``project_path`` is an S3 URI. The entire prefix is downloaded to a temp directory for the run."
    },
    "git_info": {
      "anyOf": [
        {
          "$ref": "#/$defs/GitInfo"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Git repository to shallow-clone (authenticated with an SSH deploy key) and load the SQLMesh project from. When set, ``project_path`` is interpreted relative to the checkout (e.g. ``project_path: sqlmesh/`` for a project in a repo subdirectory)."
    },
    "gateway": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "SQLMesh gateway name. Defaults to the project's default gateway.",
      "title": "Gateway"
    },
    "tobiko_cloud_token": {
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
      "description": "Tobiko Cloud API token. Set this when the SQLMesh project is configured against Tobiko Cloud (an ``EnterpriseConfig`` with a cloud state connection) and DataHub should read from the real cloud state store. Mutually exclusive with ``tobiko_cloud_token_file``. When neither is set, DataHub falls back to a local DuckDB stub so Context init succeeds without creds \u2014 model definitions still come from the project files, but anything that depends on remote state (snapshot history, environment promotions) is unavailable. Requires ``gateway`` to be set; the gateway name determines which ``SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__*`` variables get populated for tobikodata to read.",
      "title": "Tobiko Cloud Token"
    },
    "tobiko_cloud_token_file": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to a file containing the Tobiko Cloud API token (single line). Re-read with a 60-second cache TTL so projected Kubernetes secret mounts pick up rotated tokens without a process restart. Mutually exclusive with ``tobiko_cloud_token``.",
      "title": "Tobiko Cloud Token File"
    },
    "tobiko_cloud_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tobiko Cloud state-store URL. Only needed when the project's ``config.py`` does not already declare it on its cloud state connection. Used for both static-token and SSO auth, so it must be https:// whenever it is set (credentials/state travel over it).",
      "title": "Tobiko Cloud Url"
    },
    "environment": {
      "default": "prod",
      "description": "SQLMesh environment to ingest from (e.g. prod, dev).",
      "title": "Environment",
      "type": "string"
    },
    "target_platform": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Warehouse platform SQLMesh writes to (e.g. snowflake, bigquery, databricks). Auto-detected from the gateway connection type if not set \u2014 only specify this when auto-detection produces the wrong value. Must match the platform used in your warehouse connector recipe so that sibling URNs stitch correctly.",
      "title": "Target Platform"
    },
    "target_platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Platform instance for the target warehouse. Must exactly match the platform_instance configured in your warehouse connector recipe so that sibling URNs stitch correctly.",
      "title": "Target Platform Instance"
    },
    "sqlmesh_platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Platform instance for the sqlmesh entities themselves. Use this to namespace the urn:li:dataPlatform:sqlmesh entities and avoid collisions when multiple SQLMesh projects write to the same warehouse.",
      "title": "Sqlmesh Platform Instance"
    },
    "default_catalog": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Default catalog (database) to prepend to model names that are only two-part (schema.model). Required for sibling URN stitching when your warehouse connector emits three-part URNs (catalog.schema.table) but SQLMesh model names omit the catalog. Example: set to 'analytics' so that 'star.dim_developer' becomes 'analytics.star.dim_developer', matching what the Snowflake connector emits.",
      "title": "Default Catalog"
    },
    "gateway_overrides": {
      "additionalProperties": {
        "$ref": "#/$defs/GatewayOverride"
      },
      "description": "Per-gateway overrides for multi-gateway SQLMesh projects (different models writing to different warehouses). Keyed by gateway name. The top-level ``target_platform`` / ``target_platform_instance`` / ``default_catalog`` continue to apply to the default gateway; this block sets values for the others. Any field left ``None`` is auto-detected from the gateway's connection config. Single-gateway projects can ignore this entirely.\n\nExample::\n\n  gateway_overrides:\n    bigquery_lake:\n      target_platform: bigquery\n      target_platform_instance: prod_bigquery\n      default_catalog: lake-prod\n    snowflake_dwh:\n      target_platform_instance: prod_snowflake\n",
      "title": "Gateway Overrides",
      "type": "object"
    },
    "sqlmesh_is_primary_sibling": {
      "default": true,
      "description": "When true (default), the SQLMesh entity is the primary sibling \u2014 its name, description, and lineage take precedence in the merged UI view. The warehouse entity contributes runtime metadata (tags, query history, profiling, usage). Matches dbt's dbt_is_primary_sibling=true default. Set to false if your warehouse entity carries authoritative documentation.",
      "title": "Sqlmesh Is Primary Sibling",
      "type": "boolean"
    },
    "include_schema": {
      "default": true,
      "description": "Emit column schema metadata for each model. Disable to reduce ingestion volume when schema is already captured by a warehouse connector.",
      "title": "Include Schema",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Emit model-to-model lineage derived from SQLMesh DAG dependencies. Disable if lineage is managed by another connector or not needed.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "skip_external_models_in_lineage": {
      "default": false,
      "description": "When false (default), declared external models (defined in external_models.yaml) appear as SQLMesh 'Source' entities in the lineage graph. When true, lineage from managed models points directly to the warehouse URN for external models \u2014 skipping the SQLMesh entity. Produces a cleaner graph if external models are already well-represented by the warehouse connector.",
      "title": "Skip External Models In Lineage",
      "type": "boolean"
    },
    "include_database_name": {
      "default": true,
      "description": "Whether to include the database/catalog component in warehouse sibling URNs. Set to false for platforms like Athena that omit the catalog from their URNs. When false, 'analytics.star.dim_developer' becomes 'star.dim_developer' in the warehouse URN.",
      "title": "Include Database Name",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Emit column-level lineage derived from SQLMesh's SQL parsing (via SQLGlot). Available for all SQL models natively \u2014 no separate parsing step needed. Disable for very large projects where per-column analysis is too slow.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "convert_column_urns_to_lowercase": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Force column names in field URNs to lowercase. Defaults to the same value as convert_urns_to_lowercase when not set. Set explicitly when column name casing in your warehouse connector differs from the dataset URN casing (e.g. Snowflake uppercases column names).",
      "title": "Convert Column Urns To Lowercase"
    },
    "include_model_properties": {
      "default": true,
      "description": "Emit dataset properties (description, custom properties) for each model. Disable to ingest schema and lineage only.",
      "title": "Include Model Properties",
      "type": "boolean"
    },
    "detect_stale_fingerprints": {
      "default": false,
      "description": "When enabled, detect SQLMesh fingerprint tables that haven't been regenerated recently (no plan/apply runs). Use this to monitor if SQLMesh transformations are running on their expected schedules. Reads snapshot timestamps from the SQLMesh state store; silently skipped when state is unreachable. When a fingerprint is stale, a custom property 'sqlmesh.fingerprint_stale' is added to the dataset.",
      "title": "Detect Stale Fingerprints",
      "type": "boolean"
    },
    "fingerprint_staleness_threshold_hours": {
      "default": 48,
      "description": "Number of hours before a fingerprint table is considered stale. Only used when detect_stale_fingerprints=True. A fingerprint that hasn't been updated (via plan/apply) within this many hours will be flagged as stale. Default: 48 hours (2 days).",
      "minimum": 0,
      "title": "Fingerprint Staleness Threshold Hours",
      "type": "integer"
    },
    "incremental_lineage": {
      "default": true,
      "description": "Use patch/incremental lineage mode for non-SQLMesh entities (e.g. external warehouse tables referenced in lineage). When enabled, the plugin adds lineage edges without overwriting edges the warehouse connector previously discovered. Must match the warehouse connector's incremental_lineage setting.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "audit_results_path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to a JSON file containing SQLMesh audit pass/fail results. When set, the connector emits AssertionRunEvent aspects for each result, making pass/fail status visible on the DataHub Data Quality tab. The file must exist at ingestion time; results with no matching assertion definition are silently skipped.\n\nExpected JSON format::\n\n  {\n    \"metadata\": {\"generated_at\": \"2024-01-01T00:00:00Z\"},\n    \"results\": [\n      {\n        \"model\": \"myschema.orders\",\n        \"audit\": \"not_null\",\n        \"columns\": [\"order_id\"],\n        \"status\": \"pass\",\n        \"failing_rows\": 0\n      }\n    ]\n  }\n\nValid ``status`` values: ``pass``, ``fail``, ``skip``.",
      "title": "Audit Results Path"
    },
    "preview_urns": {
      "default": false,
      "description": "Before emitting metadata, print a sample of generated sqlmesh URNs and expected warehouse sibling URNs side-by-side to the log. Helps validate URN stitching before a full run. Set to true for a dry-run style check, or use --dry-run on the CLI.",
      "title": "Preview Urns",
      "type": "boolean"
    },
    "preview_urns_sample_size": {
      "default": 10,
      "description": "Number of sample models to include in the URN preview output.",
      "title": "Preview Urns Sample Size",
      "type": "integer"
    },
    "model_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns to allow or deny specific models by fully-qualified name (matched after catalog qualification, before lowercasing). Also applied to lineage dependencies \u2014 denied models are excluded as upstream nodes."
    },
    "model_kind_filter": {
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
      "description": "Filter which model kinds to ingest. When set, only models whose kind matches one of the listed values are processed. Valid values: FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, INCREMENTAL_BY_PARTITION, SCD_TYPE_2_BY_TIME, SCD_TYPE_2_BY_COLUMN, VIEW, SEED, EXTERNAL, EMBEDDED. Default: all kinds.",
      "title": "Model Kind Filter"
    },
    "tag_prefix": {
      "default": "sqlmesh:",
      "description": "Prefix prepended to SQLMesh model tags when creating DataHub tags. Example: a model tag 'pii' becomes DataHub tag 'sqlmesh:pii'. Set to empty string to use tags as-is.",
      "title": "Tag Prefix",
      "type": "string"
    },
    "owner_extraction_pattern": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Regex pattern to extract the owner identity from the SQLMesh model ``owner`` field. The first capture group is used as the owner. Example: ``(.*)@.*`` extracts the username from an email address. When not set, the owner field value is used as-is.",
      "title": "Owner Extraction Pattern"
    },
    "emit_incidents_on_failure": {
      "default": true,
      "description": "Emit a DataHub Incident entity (``urn:li:incident:\u2026``) every time ``_emit_audit_run_events`` reads a ``\"fail\"`` result from the ``audit_results_path`` JSON file. The incident links back to the assertion via ``IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`` so the Incidents tab on the dataset shows the failure history. Standard DataHub entity \u2014 works regardless of edition. Cloud additionally adds Slack threading and triage ML on top. Re-emitting the same incident is idempotent because the URN is derived from a hash of (assertion_urn, run_id).",
      "title": "Emit Incidents On Failure",
      "type": "boolean"
    }
  },
  "title": "SqlmeshSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Supported model kinds

All SQLMesh model kinds are supported: `FULL`, `INCREMENTAL_BY_TIME_RANGE`,
`INCREMENTAL_BY_UNIQUE_KEY`, `INCREMENTAL_BY_PARTITION`, `SCD_TYPE_2`, `VIEW`, `SEED`,
`EXTERNAL`, and `EMBEDDED`. Each maps to a DataHub dataset subtype (`Model`, `Seed`,
`Source`, or `Embedded`).

#### Data quality assertions

SQLMesh audits become DataHub **`CUSTOM`** assertions attached to the SQLMesh
logical URN (sibling stitching surfaces them on the warehouse sibling in the
UI):

- Each built-in audit (`not_null`, `unique_values`,
  `unique_combination_of_columns`, `number_of_rows`, `forall`,
  `accepted_range`, `accepted_values`) and any unrecognised audit name becomes
  an `AssertionType.CUSTOM` assertion with `customAssertion.type = "SQLMesh"`.
- Useful semantics (scope / operator / aggregation / fields / kwargs) are
  carried as custom properties. The source plugin does **not** invent a SQL
  statement for DataHub to evaluate — SQLMesh executes the audits.
- Pass/fail history comes from an external `audit_results_path` JSON
  (`sqlmesh audit --output`). Failures can emit Incidents when
  `emit_incidents_on_failure` is enabled.

#### Freshness and volume signals

The source plugin does **not** emit `FRESHNESS` or `VOLUME` assertion definitions
(there is no Cloud path that auto-attaches a monitor to an ingested assertion).
Instead it emits the timeseries signals users can point monitors at:

- **`OperationAspect`** with `customOperationType=SQLMESH_FINGERPRINT_REBUILD`
  from `snapshot.updated_ts` (when the SQLMesh state store is reachable)
- **`DatasetProfile.rowCount`** from a warehouse `COUNT(*)` against the
  physical fingerprint table (when the gateway engine adapter is reachable)

`sqlmesh.cron` / `interval_unit` are still ingested as dataset custom
properties for reference; they are **not** mapped into a freshness SLA.

#### Stateful ingestion

When `stateful_ingestion.enabled: true`, the source plugin tracks emitted URNs across runs
and soft-deletes entities that have been removed from the SQLMesh project.

### Limitations

- **Python models**: Python-based SQLMesh models (ibis, pandas) are supported for
  schema extraction when `columns_to_types` is defined, but column-level lineage is
  not available.
- **Audit run results**: Pass/fail status on the Validation tab requires an external
  JSON results file (`audit_results_path`). The source plugin does not execute audits
  itself at ingestion time.
- **Sibling merging**: Sibling stitching requires the warehouse source plugin to be running
  and producing URNs that match this source plugin's output. Verify URN alignment using
  `preview_urns: true` before full ingestion. The warehouse sibling edge is patched
  (not overwritten) so an existing dbt sibling relationship is preserved.

### Troubleshooting

#### URNs do not stitch with warehouse entities

Run with `preview_urns: true` to print a sample of generated sqlmesh and warehouse URN
pairs before emitting. Compare these against the URNs your warehouse source plugin produces.
Common causes:

- `target_platform_instance` mismatch — must be identical in both recipes
- Missing `default_catalog` — needed when model names are two-part but warehouse URNs
  are three-part
- Case mismatch — enable `convert_urns_to_lowercase: true` for Snowflake and other
  case-folding warehouses

#### Context fails to load

If ingestion logs `Could not initialize SQLMesh context`, check:

- The `project_path` points to a valid SQLMesh project directory (contains a
  `config.yaml` or `config.py`)
- The specified `gateway` name matches a gateway defined in the project config
- All Python dependencies for Python models are installed in the ingestion environment
  (for example `ibis-framework` for ibis models)

#### DuckDB "cannot open file" error

If you see `IO Error: Cannot open file "...": No such file or directory` when using a DuckDB
gateway, your `config.yaml` contains a relative `database:` path (e.g. `db/myproject.db`).
SQLMesh resolves this path against the working directory of the process that loads it, not
against `project_path`.

Run the source plugin from the SQLMesh project directory:

```bash
cd /path/to/sqlmesh_project
datahub ingest -c recipe.yml
```

Or switch to an absolute path in your SQLMesh `config.yaml`:

```yaml
gateways:
  local:
    connection:
      type: duckdb
      database: /absolute/path/to/myproject.db
```


### Code Coordinates
- Class Name: `datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sqlmesh/sqlmesh_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for SQLMesh, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
