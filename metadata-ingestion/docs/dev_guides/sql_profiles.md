---
description: "SQL Profiling in DataHub collects table-level and column-level statistics for relational sources during ingestion."
---

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

{{ inline /docs/generated/ingestion/sql_profiling_support_table.md.snippet }}

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

`max_queries_to_combine` (default 40) caps how many queries share one statement at all, so a table wider than that takes one statement, and one scan, per group of 40 columns. Raising it cuts scans further on wide tables; the trade is that a statement which fails is retried one query at a time, so a bigger batch means a bigger retry.

### Skipping the exact row count

An exact `COUNT(*)` costs a full scan of its own on a large table. `profiling.profile_table_row_count_estimate_only` reads the row count from the catalog instead — `information_schema.tables.table_rows` on MySQL, `pg_class.reltuples` on Postgres — which costs nothing. The count becomes an estimate, which for profiling is usually close enough.

### Bounding how long one statement runs

Combining and flattening reduce how many statements run, but not how long one of them takes, and a single aggregate over a very large table holds a read view for its whole duration — growing the InnoDB undo log on MySQL, blocking `VACUUM` on Postgres.

`profiling.query_timeout_seconds` puts a server-side limit on each profiling statement (`max_execution_time` on MySQL, `statement_timeout` on Postgres). A statement that exceeds it fails, and that table is reported and left unprofiled rather than holding the read view open. The limit is set on the profiling connection and cleared when the table is done, so it never leaks to the connection pool that metadata extraction shares.

Size it with the retry in mind: a failed combined statement is retried one query at a time, so a table that times out can spend up to `max_queries_to_combine` times the limit before it is given up on.

### Transactions

`profiling.profiling_isolation_level: AUTOCOMMIT` makes each profiling statement self-contained, so no transaction spans a whole table's profile. Set this rather than `options.connect_args.autocommit` — the latter is a driver-level default that the connection pool resets, while `profiling_isolation_level` is applied to each profiling connection as it is checked out. Setting both is harmless but redundant.

Reading a database audit log afterwards, expect to see the pool's cleanup, not the setup: SQLAlchemy restores a connection's default isolation level when it returns to the pool, which on MySQL emits `SET AUTOCOMMIT = 0` followed by `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`. Those appear _after_ a table's profiling queries, not around them. The matching `SET AUTOCOMMIT = 1` is often absent because drivers skip it when the connection is already in autocommit.

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

#### `rowCount` and the column statistics are measured over different things

A profile whose `partitionSpec.type` is not `FULL_TABLE` — which covers both a sampled profile and a partitioned one — carries two kinds of number that do **not** come from the same set of rows:

| field                                                                              | measured over                                   |
| ---------------------------------------------------------------------------------- | ----------------------------------------------- |
| `rowCount`                                                                         | the whole dataset, taken from source metadata   |
| every field profile (`nullCount`, `nullProportion`, `uniqueCount`, min/max/mean/…) | only the sample, or only the profiled partition |

**The consequence is the part that catches people out: `nullCount` is not a fraction of `rowCount`.** It is `sample_rows - non_null_rows`, so on a billion-row table sampled to 10,000 rows a column that is 50% null reports `nullCount: 5000` against `rowCount: 1000000000`. Dividing one by the other is meaningless. `nullProportion` is the figure to use — it is computed over the sample and is therefore a valid estimate of the whole. The same applies to `uniqueCount` (a sample's distinct count, not scaled up — distinct counts do not grow linearly with the sampling rate) and to the min, max, mean, median and stdev.

When the source metadata has no row count — views, external tables, or anywhere it is otherwise unavailable — the profile carries no `rowCount` at all rather than the sample's size. That is expected, not a failure.
