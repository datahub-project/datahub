### Overview

The `sqlmesh` source plugin reads SQLMesh project metadata directly from the SQLMesh Python
API—no running SQLMesh server is required. It emits Dataset entities on the
`urn:li:dataPlatform:sqlmesh` platform and links each one to its corresponding warehouse
view (Snowflake, BigQuery, DuckDB, etc.) as a sibling, so DataHub merges both into a
single unified view in the UI.

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

### Prerequisites

- Python 3.9 or later
- The `sqlmesh` Python package installed in the ingestion environment:
  `pip install 'acryl-datahub[sqlmesh]'`
- Read access to the SQLMesh project directory (config files and model SQL)
- If using a remote gateway (Snowflake, BigQuery, etc.), valid gateway credentials
  in the SQLMesh project config—the connector loads the SQLMesh context which opens
  a connection to resolve model metadata
