### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Metric Views

[Unity Catalog Metric Views](https://docs.databricks.com/aws/en/metric-views/) are first-class semantic layer assets that expose dimensions and measures via a YAML specification. DataHub ingests them as datasets with subtype `Metric View` when you opt in with `include_metric_views: true`.

```yaml
source:
  type: unity-catalog
  config:
    include_metric_views: true
    metric_view_pattern:
      allow:
        - my_catalog\.analytics\..*
```

When enabled, each metric view emits:

- A `Metric View` subtype so it is distinguishable from regular tables and views in the UI.
- A `ViewProperties` aspect carrying the raw YAML body with `viewLanguage: YAML`. The `materialized` flag is set when the YAML contains `materialization: materialized` (v0.1 string form) or any `materialization:` object (v1.1 form).
- The dataset description, taken from the YAML top-level `comment` when present (falls back to the underlying Unity Catalog table comment otherwise).
- Upstream lineage parsed from the YAML `source` and `joins[].source` fields. Both 3-part (`catalog.schema.table`) and 2-part (`schema.table`, resolved against the metric view's own catalog) identifiers are supported, as are backtick-quoted parts (`` `db.with.dots`.schema.table ``). Nested join hierarchies (snowflake-style joins) are walked recursively — every join target along the chain becomes an upstream, and every alias along the chain becomes resolvable in dimension and measure expressions.
- Column-level lineage parsed from each `dimensions[].expr` / `measures[].expr` using the Databricks SQL dialect (requires `include_column_lineage: true`, which is the default). Unqualified columns map to the source table; `<join_name>.column` references map through the join's `source`.
- Intra-view field-to-field lineage for `MEASURE(name)` composable measure references. A measure expressed as `MEASURE(total_revenue) / MEASURE(order_count)` emits two upstream edges to the `total_revenue` and `order_count` measures within the same metric view dataset. Matching is case-insensitive; the emitted URN uses the canonical case from the spec.
- A `Dimension` tag on schema fields matching a YAML `dimensions[].name`, and a `Measure` tag on those matching `measures[].name`. Measures with a non-empty `window:` block get an additional `Window Measure` tag alongside `Measure`.
- Per-column descriptions taken from the YAML `dimensions[].comment` / `measures[].comment` when present, with `description` accepted as a v0.1 fallback (falls back to the underlying Unity Catalog column comment otherwise).
- A filtered set of custom properties: the Spark engine config snapshot Unity Catalog injects as `view.sqlConfig.spark.*` keys (~150 entries per view) is dropped, since it is identical across views in a workspace and crowds the UI. All other source-table properties are preserved unchanged.

#### Metric view custom properties

The following spec-level properties are surfaced on the metric view dataset so the spec is inspectable without re-reading the YAML body:

- `metric_view.spec_version` — the spec version (e.g. `1.1`).
- `metric_view.filter` — the top-level `filter:` expression.
- `metric_view.joins` — the entire `joins:` hierarchy as a JSON string, preserving `on:` predicates, `using:` shorthand, and any nested joins.
- `metric_view.materialization.schedule` / `metric_view.materialization.mode` / `metric_view.materialization.materialized_views` — when `materialization:` is a v1.1 object, each subkey lands as a queryable property.

Per-field agent metadata (Databricks Runtime 17.3+, YAML 1.1) is exposed as dataset-level custom properties keyed `metric_view.field.<name>.*`:

- `display_name` — human-readable label for the field. Truncated to 255 characters; truncation events are counted in the ingestion report.
- `synonyms` — comma-joined alternative names. Each entry is limited to 255 characters and the list is capped at 10 entries per field (per the Databricks v1.1 spec). Per-item truncations, invalid-type drops, and the 10-cap are each recorded in the ingestion report.
- `format.type` and its subkeys (for dimensions and measures) — `number`, `currency`, `percentage`, `byte`, `date`, and `date_time` formats are supported. Each known subkey is exposed individually (including nested `decimal_places.type` and `decimal_places.places`); unknown subkeys are dropped and counted in the ingestion report.
- `window.order` / `window.range` / `window.semiadditive` for single-entry window measures, or `window` as a JSON string for multi-entry windows. Window properties are emitted on measures only.

If a metric view's `source` is a SQL subquery, or if it uses a 1-part identifier that DataHub can't resolve, the YAML lineage path is skipped and DataHub falls back to the Unity Catalog table-lineage REST API for upstream resolution.

`include_metric_views` is `false` by default for backwards compatibility — when the flag is off (or when the installed `databricks-sdk` predates `TableType.METRIC_VIEW`), metric views continue to be emitted as plain `Table` entities with no view body.

#### Usage statistics

Usage is enabled by default (`include_usage_statistics: true`). Choose how query history is read with `usage_data_source`:

- `AUTO` (default) — system tables when `warehouse_id` is set; otherwise the REST API.
- `SYSTEM_TABLES` — `system.query.history` only (requires `warehouse_id`).
- `API` — REST API only.

On the **system-tables path**, query history is joined to `system.access.table_lineage` on `statement_id`. When lineage rows exist, dataset references come from lineage; otherwise queries are parsed with sqlglot. Set `skip_sqlglot_when_system_table_lineage_missing: true` to skip queries with no lineage rows instead of parsing them.

- `include_operational_stats` (default `true`) — when `false`, only `SELECT` statements are fetched.
- `include_queries` / `include_query_usage_statistics` — emit Query entities and per-query popularity (system-tables path only).
- `include_column_usage_stats` (default `false`) — when `true`, force full sqlglot parsing of every usage query so column-level usage statistics (`fieldCounts`) are produced. This bypasses the faster preparsed system-table lineage path and is slower; it also overrides `push_down_database_pattern_access_history` and `skip_sqlglot_when_system_table_lineage_missing`.

`push_down_database_pattern_access_history: true` applies `catalog_pattern` filtering in `system.access.table_lineage` and semi-joins query history to statements that have lineage in the configured time window. Statements without lineage rows are not fetched (even when `catalog_pattern` allows all catalogs).

:::warning Coverage vs. speed tradeoff

`skip_sqlglot_when_system_table_lineage_missing` and `push_down_database_pattern_access_history` trade usage **coverage** for speed, not just parsing time. Databricks only records a `system.access.table_lineage` row for statements that emit a lineage event (typically a minority of warehouse/serverless queries) — `CREATE`, `DESCRIBE`, `SET`, and most other statements have no lineage row at all. Enabling either option therefore drops usage and operations for every statement that lacks lineage in the time window, which is usually the large majority of activity. They are off by default for this reason; leave them off unless you specifically want to restrict usage to the lineage-bearing subset in exchange for faster, lighter ingestion.

The default preparsed path emits table-level usage only (no column `fieldCounts`). Set `include_column_usage_stats: true` to regain column-level usage statistics via full sqlglot parsing at the cost of speed.

:::

#### Delta Lake External Tables

When `emit_siblings` is enabled (the default), the connector emits sibling relationships between Unity Catalog external tables and their corresponding `delta-lake` platform entities for tables stored on S3 or other object storage. This means you may see a second dataset entity for each external Delta table — one under the `databricks` platform and one under `delta-lake` — linked as siblings in DataHub. Set `emit_siblings: false` in your recipe to disable this behavior if you don't need cross-platform linkage.

#### Lakehouse Federation (foreign catalogs)

DataHub detects Unity Catalog **foreign catalogs** (Lakehouse Federation) and links their tables to the external source dataset each one mirrors (PostgreSQL, SQL Server, MySQL, Snowflake, Redshift, BigQuery, Oracle, Teradata, another Databricks workspace, or Glue/Hive).

- `include_federation_lineage` (default `true`) emits an upstream **COPY** lineage edge from each foreign-catalog table to the external source dataset it mirrors. Column-level lineage is added when `include_column_lineage` is set. Set it to `false` to skip the cross-platform link.
- `emit_federation_structured_properties` (default `true`) marks the foreign catalog with structured properties (`platform`, `remote_database`, `connection`, `catalog_type`) so federated catalogs are facetable in the UI.
- `include_federation_column_backfill` (default `true`) fills in a foreign-catalog table's columns from the external source when Unity Catalog has not synced them yet (structure only — governance is not copied).
- For the link to resolve, the external source must be ingested separately, and its `platform_instance` and case-folding must match. Use `federation_connection_details` (keyed by Unity Catalog connection name) to align them:

```yaml
source:
  type: unity-catalog
  config:
    include_federation_lineage: true
    federation_connection_details:
      pg_conn:
        platform_instance: prod-pg
        env: PROD
```

:::caution Dangling lineage to an un-ingested external source

The upstream lineage edge only resolves if the external source is **also ingested into DataHub** as its own recipe, using the exact same `platform_instance` (and `convert_urns_to_lowercase`) that you set in `federation_connection_details`. If the external source is never ingested, or is ingested with a different `platform_instance` or case-folding setting, the edge points at a dataset URN that DataHub never creates — a dangling external dataset that never reconciles with the real one.

:::

The `emit_siblings` option described under _Delta Lake External Tables_ above is unrelated: it governs only the Delta Lake (S3 external table) sibling path, not Lakehouse Federation.

#### External data quality tables

If a data-quality engine outside DataHub (for example, an in-house framework running as Databricks jobs) evaluates rules, it can publish them to DataHub by writing two tables that follow the **DataHub external DQ table contract (v1)**. The connector reads them and publishes each rule as an externally-managed assertion on the dataset it checks, with one run event per evaluation.

```yaml
source:
  type: unity-catalog
  config:
    warehouse_id: "<warehouse-id>"
    stateful_ingestion:
      enabled: true # read results incrementally; strongly recommended
    external_dq:
      enabled: true
      rules_table: main.governance.dq_rules
      results_table: main.governance.dq_results
```

```sql
CREATE TABLE main.governance.dq_rules (
  rule_id STRING NOT NULL,           -- stable across renames/threshold changes
  dataset_path ARRAY<STRING> NOT NULL, -- ["catalog", "schema", "table"]
  column_paths ARRAY<STRING>,         -- empty = table-level; several = multi-column rule
  rule_name STRING NOT NULL,
  rule_type STRING NOT NULL,
  rule_description STRING,
  dimension STRING,
  operator STRING,                    -- NOT_NULL, UNIQUE, BETWEEN, GREATER_THAN, LESS_THAN, EQUAL_TO
  threshold_min DOUBLE,
  threshold_max DOUBLE,
  threshold_value DOUBLE,
  logic STRING,
  severity STRING,                    -- LOW, MEDIUM, HIGH
  is_active BOOLEAN NOT NULL,         -- false retires the assertion
  rule_version STRING,
  external_url STRING,
  updated_at TIMESTAMP NOT NULL -- reserved; not yet used by DataHub
);

CREATE TABLE main.governance.dq_results (
  run_id STRING NOT NULL,
  rule_id STRING NOT NULL,
  executed_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,             -- SUCCESS, FAILURE, ERROR, INIT
  is_warning BOOLEAN,                 -- SUCCESS + true = non-blocking warning
  severity STRING,
  actual_value DOUBLE,
  evaluated_row_count BIGINT,
  failed_row_count BIGINT,
  missing_row_count BIGINT,
  operator_snapshot STRING,
  threshold_min_snapshot DOUBLE,
  threshold_max_snapshot DOUBLE,
  threshold_value_snapshot DOUBLE,
  rule_version_snapshot STRING,
  error_type STRING,
  error_message STRING,
  external_url STRING
);
```

- Every contract column must exist with a compatible type (widening such as `INT` for `BIGINT` or `DECIMAL` for `DOUBLE` is accepted; `TIMESTAMP_NTZ` is not). If either table does not match the contract, nothing is read from either table and the run reports a failure.
- Additional columns are allowed after the contract columns and are shown on the assertion (rules) or run (results).
- Assertion identity is `(platform instance, rule_namespace, rule_id)`, so renaming a table keeps the assertion's history.
- Results are append-only. With stateful ingestion, each result is published once, including results that arrive up to `late_arrival_minutes` late. Without it, the last `initial_lookback_days` of results are re-published on every run, which re-sends notifications to subscribers.
- `executed_at` should be when the evaluation finished. Lateness is measured against the newest `executed_at` already published, so a result stamped more than `late_arrival_minutes` earlier than that is not picked up. With stateful ingestion, the next run detects such results and reports a warning with how many were missed. Results dated more than `late_arrival_minutes` in the future are skipped with a warning.
- Rules for tables that were not ingested in the same run are skipped and reported.
- Results for a rule that is invalid or not yet published are retried while they are inside the late-arrival window, then dropped. Each run reports a warning naming each such `rule_id`.
- Setting `is_active` to `false` marks the assertion as removed, and no further results are published for it. Results already published stay as history.
- Both tables must be in Unity Catalog (not `hive_metastore`), and the ingestion principal needs `SELECT` on them.
- `column_paths` should name top-level columns. Column casing is matched to the ingested schema; nested (struct) fields are passed through as written and may not link to the column in DataHub.

#### Advanced

##### Multiple Databricks Workspaces

If you have multiple databricks workspaces **that point to the same Unity Catalog metastore**, our suggestion is to use separate recipes for ingesting the workspace-specific Hive Metastore catalog and Unity Catalog metastore's information schema.

To ingest Hive metastore information schema

- Setup one ingestion recipe per workspace
- Use platform instance equivalent to workspace name
- Ingest only hive_metastore catalog in the recipe using config `catalogs: ["hive_metastore"]`

To ingest Unity Catalog information schema

- Disable hive metastore catalog ingestion in the recipe using config `include_hive_metastore: False`
- Ideally, just ingest from one workspace
- To ingest from both workspaces (e.g. if each workspace has different permissions and therefore restricted view of the UC metastore):
  - Use same platform instance for all workspaces using same UC metastore
  - Ingest usage from only one workspace (you lose usage from other workspace)
  - Use filters to only ingest each catalog once, but shouldn’t be necessary

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

#### Lineage extraction is too slow

Unity Catalog REST API requires one call per table (table lineage) and one call per column (column lineage). To improve performance, disable column lineage with `include_column_lineage: false`.

Similarly, `include_table_constraints: true` adds one `tables.get()` call per non-Hive table to fetch primary key and foreign key constraints. For workspaces with thousands of tables this adds latency; leave the flag disabled (the default) if Primary Key / Foreign Key metadata is not needed.

#### Missing or incomplete usage statistics

- On the system-tables path, queries without rows in `system.access.table_lineage` are parsed with sqlglot unless `skip_sqlglot_when_system_table_lineage_missing: true`.
- With `push_down_database_pattern_access_history: true`, only statements with lineage in the time window are fetched. Disable pushdown or relax `catalog_pattern` if usage looks incomplete.
- If the ingestion report contains **Databricks query text is redacted**, Databricks returned `<REDACTED>` instead of SQL text. Behavior depends on the configured usage path:

  - On the default system-tables path (`usage_data_source: AUTO` with `warehouse_id` set, or `SYSTEM_TABLES`) with `include_column_usage_stats: false`: table-level usage statistics (`totalSqlQueries`, `uniqueUserCount`, `userCounts`) are preserved because upstream tables come from `system.access.table_lineage`. `Query` entities are not emitted for redacted queries, so column-level usage statistics (`fieldCounts`), operational statistics, per-query usage counts, and the top-SQL sample are absent for them. The top-SQL sample on affected tables may show a single `<REDACTED>` entry indicating how many queries were masked in the bucket.
  - On the REST API path (`usage_data_source: API`) or when `include_column_usage_stats: true`: redacted queries are dropped entirely and contribute nothing to usage — those paths require parsing SQL text.
  - Create the account-level group `databricks_pii_access` and add the ingestion principal to it, while retaining its existing system-table or Query History API permissions. You can verify access by running the following query as the ingestion principal:

  ```sql
  SELECT statement_text
  FROM system.query.history
  WHERE execution_status = 'FINISHED'
  ORDER BY start_time DESC
  LIMIT 5;
  ```

  The result should contain SQL text rather than `<REDACTED>`.
