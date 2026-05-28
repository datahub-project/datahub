### Overview

The `sqlmesh` source plugin reads SQLMesh project metadata directly from the SQLMesh Python
API—no running SQLMesh server is required. It emits Dataset entities on the
`urn:li:dataPlatform:sqlmesh` platform and links each one to its corresponding warehouse
view (Snowflake, BigQuery, DuckDB, etc.) as a sibling, so DataHub merges both into a
single unified view in the UI.

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
DataHub [Assertion](https://docs.datahub.com/docs/managed-datahub/observe/assertions)
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

| Credential                                  | Where it lives                                              | How to provide it                                                                            |
| ------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **DataHub GMS token**                       | DataHub sink config                                         | `${DATAHUB_TOKEN}` interpolated in the recipe                                                |
| **Tobiko Cloud token** (Enterprise only)    | This connector's config                                     | `tobiko_cloud_token: ${TOBIKO_TOKEN}` _or_ `tobiko_cloud_token_file: ${SECRETS_DIR}/tobiko`  |
| **Warehouse credentials** (Snowflake, etc.) | SQLMesh project's `config.yaml` or `config.py`              | SQLMesh's own env-var interpolation — `password: ${SNOWFLAKE_PASSWORD}` etc.                 |

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

#### What works with vs without state-store access

The connector probes three capabilities once at Context load
(`has_state`, `has_warehouse_query`, `has_graph`) and surfaces them in
the report. Different emissions depend on different probes.

**Happy path — all three available:**

| Emission                              | Depends on               |
| ------------------------------------- | ------------------------ |
| Dataset entities + schema + lineage   | nothing (just project files) |
| Sibling URN routing                   | nothing                  |
| Assertion **definitions** (audit, freshness, volume) | nothing |
| Assertion **run events** (audit pass/fail) | `audit_results_path` JSON file (no probes needed) |
| Volume `DatasetProfile.rowCount`      | `has_state` (for authoritative fingerprint name) + `has_warehouse_query` (for COUNT) |
| Pipeline `OperationAspect.lastUpdatedTimestamp` | `has_state` (for `snapshot.updated_ts`) |

**Minimal path — state store unavailable** (`has_state: false`):

The connector still emits the full metadata model — datasets, schema,
column-level lineage, audits, assertion definitions, siblings,
containers, ownership, tags. Cloud Monitor evaluation against the
assertion definitions also continues to work (Monitor reads warehouse
state directly, not through us). What you **lose**:

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
    # Assertion definitions still go out; Monitor evaluates against the
    # warehouse connector's own DatasetProfile + Operation timeseries.
    emit_freshness_assertions: true
    emit_volume_assertions: true
    stateful_ingestion:
      enabled: true
```

What this gives you: **complete dataset metadata + audit Validation tab
+ assertion stubs that Cloud Monitor will fill in**. Strictly less
than the happy path, but still the bulk of the value.

### Real-world configuration examples

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
      database: ANALYTICS  # the catalog SQLMesh writes to
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

#### BigQuery service-account JSON

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

#### Tobiko Cloud Enterprise

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

#### Running in Kubernetes

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

#### Running in CI/CD (GitHub Actions example)

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

### Prerequisites

- Python 3.9 or later
- The `sqlmesh` Python package installed in the ingestion environment:
  `pip install 'acryl-datahub[sqlmesh]'`
- Read access to the SQLMesh project directory (config files and model SQL)
- If using a remote gateway (Snowflake, BigQuery, etc.), valid gateway credentials
  in the SQLMesh project config—the connector loads the SQLMesh context which opens
  a connection to resolve model metadata
