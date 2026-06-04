### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Lineage Computation Details

- **Regular views** — lineage is extracted from each view's `CREATE VIEW` definition via the `SqlParsingAggregator` and the `postgres` sqlglot dialect (HANA SQL is close enough to ANSI for view definitions).
- **Calculation views (`include_calculation_views: true`)** — column-level lineage is parsed from the calc view's XML in `_SYS_REPO.ACTIVE_OBJECT`. The parser handles `ProjectionView`, `JoinView`, `AggregationView`, `UnionView`, `RankView`, and `SqlScriptView` nodes. For `SqlScriptView` (`calculationScenarioType="SCRIPT_BASED"`) views the parser also extracts table-level upstreams from the embedded HANA SQLScript body — column-level lineage from SQLScript is out of scope.
- **Stored procedures (`include_stored_procedures: true`)** — each procedure's body is parsed for table-level reads/writes and procedure-to-procedure lineage; the result is attached to the procedure's `DataJob` as input/output datasets.
- **Observed queries (`include_query_usage: true`)** — query history from `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` is fed to the aggregator as `ObservedQuery` entries. Each row represents one `(statement_hash, last_execution_timestamp)` observation — i.e. one distinct moment a cached plan was executed within `[start_time, end_time]`. The aggregator derives both query-driven lineage and (with `include_usage_stats: true`) `DatasetUsageStatistics` rollups bucketed by `bucket_duration`.

#### Usage Computation Details

When `include_query_usage: true`:

- The connector dedupes plan-cache snapshots by `(statement_hash, last_execution_timestamp)` so a steady-state query that has been observed by many statistics-service snapshots does not double-count. As a consequence, multiple executions that fall inside one snapshot window are observed as a single event; absolute execution counts are therefore a floor, not an exact count.
- System users (`SYS` and any `_SYS_*` user) and monitoring traffic against `SYS` / `_SYS_*` schemas are filtered out at the SQL layer.
- `usage_max_queries` caps the number of distinct observations returned per ingestion run.
- `bucket_duration`, `start_time`, and `end_time` come from `BaseUsageConfig` and behave the same as in Snowflake / Redshift.

#### Profiling Details

Profiling is implemented via `SQLAlchemySource`'s standard profiler and is opt-in (`profiling.enabled: true`). HANA's column-store makes most profiling queries cheap, but you can still cap the per-table sample size via `profile_table_level_only` or `profile.sample_size` for very wide / large tables.

### Limitations

- **`hdbcli` driver on `aarch64`/`arm64`** — SAP does not ship `aarch64` wheels for `hdbcli`. Run ingestion under an `x86_64` interpreter on Apple Silicon and Graviton hosts.
- **Calculation views are on-premise / self-managed only** — `include_calculation_views: true` requires the SAP HANA XS-classic repository (`_SYS_REPO`), which is not present on SAP HANA Cloud or HDI-only deployments. See the deployment-compatibility table in the prerequisites section for the full feature matrix; calc-view extraction warns and skips rather than failing on incompatible tenants.
- **Calc views that wrap stored procedures** — a `SqlScriptView` whose body only `CALL`s another procedure has no parsable `FROM`/`JOIN` references, so no table lineage is emitted for that view. The called procedure itself is still ingested (if `include_stored_procedures` is on) and reachable through procedure lineage.
- **SQLScript column-level lineage** — column-level lineage from SQLScript bodies (calc-view `SqlScriptView` nodes and stored procedures) is intentionally out of scope; sqlglot's HANA support is too partial for reliable column tracing.
- **Usage execution counts are a lower bound** — `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` snapshots the plan cache every few minutes, and multiple executions within one snapshot window collapse to one observation. Frequency rankings (top tables, top users) remain accurate; absolute totals are conservative. Also, `STATEMENT_STRING` is truncated at the HANA-side `sql_text_length` parameter (default 5000 chars), so extremely long statements may parse partially.

### Troubleshooting

#### `[89018] _SYS_REPO.ACTIVE_OBJECT not found`

The tenant does not have SAP HANA XS-classic repository content. Either disable calc-view extraction (`include_calculation_views: false`) or migrate to a tenant that has the repository deployed.

#### `[258] insufficient privilege` on `SYS.PROCEDURES` or `_SYS_REPO.ACTIVE_OBJECT`

Grant `SELECT` on the relevant system view to the ingestion user. The connector logs a warning and continues with the rest of the extraction path; failures here never abort ingestion.

#### Calc view column lineage missing

1. Verify the view is _activated_ (visible in `_SYS_BIC`). Unactivated design-time objects are skipped.
2. Check the ingestion logs for `Failed to parse calculation view` — that points at a parse failure (the source XML may be malformed, or it may be a `SqlScriptView` where column-level lineage is unsupported).
3. For `SqlScriptView` views, confirm the body uses fully-qualified, double-quoted references (`FROM "SCHEMA"."TABLE"`); HANA SQLScript table variables (`T_FREQ = SELECT …`) are intentionally skipped.

#### `hdbcli` import error

```
ImportError: dlopen(...) ... incompatible architecture
```

You are running an `aarch64` Python interpreter. Switch to an `x86_64` interpreter (Rosetta or an x86_64 container).

#### `[258] insufficient privilege` against `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`

Grant the `MONITORING` role (or `CATALOG READ` system privilege) to the ingestion user. The connector logs a warning and continues — usage extraction degrades to a no-op while the rest of metadata ingestion completes normally.

#### Usage extraction returns zero queries

1. Confirm the statistics service is running (`SELECT * FROM SYS.M_SERVICES WHERE SERVICE_NAME = 'statisticsserver'`). SAP note 2147247 has full diagnostics.
2. Confirm that `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` is populated (`SELECT COUNT(*) FROM _SYS_STATISTICS.HOST_SQL_PLAN_CACHE`). Newly-provisioned tenants take a few minutes to seed the first snapshot.
3. Widen the time window — the default is the last full day in UTC. Set `start_time: "-7 days"` for a longer lookback.
