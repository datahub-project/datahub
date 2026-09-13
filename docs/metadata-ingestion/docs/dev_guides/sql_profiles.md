
# SQL Profiling

SQL Profiling collects table level and column level statistics.
The SQL-based profiler does not run alone, but rather can be enabled for other SQL-based sources.
Enabling profiling will slow down ingestion runs.

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

## Capabilities

Extracts:

- Row and column counts for each table
- For each column, if applicable:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

## Supported Sources

| Source | Notes |
| ------ | ----- |
| [ABS Data Lake](../../../docs/generated/ingestion/sources/abs.md) | Optionally enabled via configuration. |
| [Apache Doris](../../../docs/generated/ingestion/sources/doris.md) | Optionally enabled via configuration. |
| [Athena](../../../docs/generated/ingestion/sources/athena.md) | Optionally enabled via configuration. Profiling uses sql queries on whole table which can be expensive operation. |
| [BigID](../../../docs/generated/ingestion/sources/bigid.md) | Column-level profiles from BigID columnProfile data. |
| [BigQuery](../../../docs/generated/ingestion/sources/bigquery.md) | Optionally enabled via configuration. |
| [ClickHouse `clickhouse-usage`](../../../docs/generated/ingestion/sources/clickhouse.md) | Optionally enabled via configuration. |
| [ClickHouse `clickhouse`](../../../docs/generated/ingestion/sources/clickhouse.md) | Optionally enabled via configuration. |
| [CockroachDB](../../../docs/generated/ingestion/sources/cockroachdb.md) | Optionally enabled via configuration. |
| [Databricks](../../../docs/generated/ingestion/sources/databricks.md) | Supported via the `profiling.enabled` config. |
| [Dremio](../../../docs/generated/ingestion/sources/dremio.md) | Optionally enabled via configuration. |
| [Excel](../../../docs/generated/ingestion/sources/excel.md) | Optionally enabled via configuration. |
| [Google Cloud Storage](../../../docs/generated/ingestion/sources/gcs.md) | Optionally enabled via configuration. |
| [IBM Db2](../../../docs/generated/ingestion/sources/db2.md) | Optionally enabled via configuration. |
| [Iceberg](../../../docs/generated/ingestion/sources/iceberg.md) | Optionally enabled via configuration. |
| [Informix](../../../docs/generated/ingestion/sources/informix.md) | Row counts only, via systables.nrows. |
| [Kafka](../../../docs/generated/ingestion/sources/kafka.md) | Optionally enabled via configuration `profiling.enabled`. |
| [MariaDB](../../../docs/generated/ingestion/sources/mariadb.md) | Optionally enabled via configuration. |
| [Microsoft SQL Server](../../../docs/generated/ingestion/sources/mssql.md) | Optionally enabled via configuration. |
| [MySQL](../../../docs/generated/ingestion/sources/mysql.md) | Optionally enabled via configuration. |
| [Postgres](../../../docs/generated/ingestion/sources/postgres.md) | Optionally enabled via configuration. |
| [PowerBI](../../../docs/generated/ingestion/sources/powerbi.md) | Optionally enabled via configuration profiling.enabled. |
| [Presto](../../../docs/generated/ingestion/sources/presto.md) | Optionally enabled via configuration. |
| [Redshift](../../../docs/generated/ingestion/sources/redshift.md) | Optionally enabled via configuration. |
| [S3 / Local Files](../../../docs/generated/ingestion/sources/s3.md) | Optionally enabled via configuration. |
| [Salesforce](../../../docs/generated/ingestion/sources/salesforce.md) | Only table level profiling is supported via `profiling.enabled` config field. |
| [SAP HANA](../../../docs/generated/ingestion/sources/hana.md) | Optionally enabled via configuration. |
| [Snowflake](../../../docs/generated/ingestion/sources/snowflake.md) | Optionally enabled via configuration `profiling.enabled`. |
| [SQLAlchemy](../../../docs/generated/ingestion/sources/sqlalchemy.md) | Optionally enabled via configuration. |
| [StarRocks](../../../docs/generated/ingestion/sources/starrocks.md) | Optionally enabled via configuration. |
| [Teradata](../../../docs/generated/ingestion/sources/teradata.md) | Optionally enabled via configuration. |
| [TiDB](../../../docs/generated/ingestion/sources/tidb.md) | Optionally enabled via configuration. |
| [TimescaleDB](../../../docs/generated/ingestion/sources/timescaledb.md) | Optionally enabled via configuration. |
| [Trino `trino`](../../../docs/generated/ingestion/sources/trino.md) | Optionally enabled via configuration. |
| [Vertica](../../../docs/generated/ingestion/sources/vertica.md) | Optionally enabled via configuration. |


## Profiler Implementation

DataHub uses a SQLAlchemy-based profiler for all SQL sources. It runs profiling queries directly against your SQL source's existing SQLAlchemy connection and emits the table- and column-level statistics listed under [Capabilities](#capabilities). No additional dependencies are required beyond the SQL connector itself.

No configuration is required to use it — any SQL source with profiling enabled will use the SQLAlchemy profiler automatically:

```yaml
source:
  config:
    profiling:
      enabled: true
```

:::note

The legacy Great Expectations profiler (`profiling.method: ge`) has been removed. SQLAlchemy is now the only SQL profiler; the `profiling.method` option no longer has any effect and can be dropped from recipes.

:::

## Reducing profiling cost

Profiling issues one query per metric per column, so a wide table can cost hundreds of round trips and hundreds of table scans. Three independent options reduce that; they address different costs and can be combined.

### Query combining

`profiling.query_combiner_enabled` (on by default) batches queries that each return exactly one row into a single round trip, by wrapping each in a CTE and cross-joining them. This cuts **round trips**, not table scans — each CTE is still its own aggregate over the table, so the database may scan once per metric.

### Aggregate flattening

`profiling.query_combiner_flatten_enabled` (off by default) goes further for same-shape aggregates over the same table: instead of one CTE per metric, it emits a single `SELECT count(*), min(v), max(v) FROM t`. That collapses many scans into one, which matters most on row stores such as MySQL where each scan reads the whole table.

```yaml
source:
  config:
    profiling:
      enabled: true
      query_combiner_enabled: true # required — flattening runs inside the combiner
      query_combiner_flatten_enabled: true
```

Only single-aggregate-over-a-whole-table queries are flattened. Anything the profiler builds itself — a filtered count, a sampled row count, a median fallback — falls back to the CTE path, correct but not collapsed. `COUNT(DISTINCT)` columns are capped per statement, because each one builds a distinct-value tree in server memory; the gain is therefore largest for cheap aggregates and smaller for unique counts.

`max_distinct_per_statement` (default 5) caps how many `COUNT(DISTINCT)` columns share one statement. The default is a starting point rather than a measured optimum.

#### Reading the report

Flattening trades round trips for scans, so `combined_queries_issued` can rise while scans fall — read it together with `scans_avoided` rather than treating the rise as a regression.

| counter                       | meaning                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| `scans_avoided`               | table scans saved; the success signal, counted only after a flat statement's results are extracted |
| `flat_queries_issued`         | flat statements attempted, counted before execution                                                |
| `flatten_rejected`            | queries the profiler built itself (filtered, sampled, or multi-row) so they were never eligible    |
| `flatten_singletons`          | queries alone in their table group, sent to the CTE path because flattening one saves nothing      |
| `flat_group_failures`         | flat statements that failed and fell back                                                          |
| `flat_group_cte_recoveries`   | of those, how many the CTE path recovered in one round trip                                        |
| `flat_group_serial_fallbacks` | of those, how many ended up one query per round trip                                               |

If `scans_avoided` is low, those last four say why. High `flatten_singletons` means the workload has little to merge; a non-zero `flat_group_serial_fallbacks` means flattening is costing round trips rather than saving scans, and the flag is better off.

### Sampling

For very large tables, `profiling.use_sampling` (supported on BigQuery and Snowflake) profiles a sample rather than the full table. This reduces the cost of each scan, where the two options above reduce how many queries and scans are issued — so sampling composes with both, and on a supported platform you can enable all three.

The difference that matters when choosing: sampling changes the numbers you get. Distinct counts in particular are computed over the sample, so `uniqueCount` becomes an estimate. Query combining and flattening only change how the queries are issued — the statistics they produce are identical to running each query on its own.
