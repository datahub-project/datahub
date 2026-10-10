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

## Profiling and long transactions

By default a MySQL profile runs as one transaction per table, so under `REPEATABLE READ` — MySQL's default — every stage reads one snapshot, until the query combiner rolls back to recover from a failed statement. InnoDB holds that read view, and the undo history behind it, for as long as the table takes to profile. `profiling.profiling_isolation_level: AUTOCOMMIT` runs each profiling statement on its own instead.

The cost is consistency. On a table being written concurrently the stages see different rows, so a profile can disagree with itself — `uniqueCount` above `rowCount`, for instance. Postgres-family sources already connect in `AUTOCOMMIT`, so the option changes nothing there unless `options.isolation_level` has overridden it.

Prefer it to `options.connect_args.autocommit`. The driver option does work, but it applies to every connection the source opens rather than just profiling, SQLAlchemy cannot see it, and it is not validated at config-parse time. The two interact badly: SQLAlchemy records the server's default isolation level at first connect and, after any checkout that set an isolation level through SQLAlchemy, restores it on return to the pool — and that restore turns driver-level autocommit back off.

### What this looks like in a MySQL log

With `profiling_isolation_level: AUTOCOMMIT`, each profiled table brackets like this:

```
SET AUTOCOMMIT = 1                                        -- checkout
<the profiling queries>
ROLLBACK
SET AUTOCOMMIT = 0                                        -- return to the pool
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ
COMMIT
```

The last four lines are SQLAlchemy restoring the connection as it returns to the pool, **after** the table's queries — not a transaction wrapped around them. The isolation level named is whatever the server's default is, not necessarily `REPEATABLE READ`.

If you also set `options.connect_args.autocommit`, the opening `SET AUTOCOMMIT = 1` disappears for the first table on each connection, because the connection is already in autocommit and the driver skips a statement that would change nothing. A log read without that in mind looks like the setting being ignored when it is not.

## Reducing profiling cost

Profiling issues one query per metric per column, so a wide table can cost hundreds of round trips and hundreds of table scans. Four independent options reduce that; they address different costs and can be combined.

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

### Skipping the exact row count

`profiling.profile_table_row_count_estimate_only` replaces the profiler's own `COUNT(*)` — a full scan per table — with a single catalog lookup: `information_schema.tables.table_rows` on MySQL, `pg_class.reltuples` on Postgres, `svv_table_info.tbl_rows` on Redshift. It is ignored on every other source.

The error reaches further than `rowCount`. `nullCount` is `rowCount` minus an exact non-null count, so it and `nullProportion` inherit it. Because the subtraction is clamped at zero, an under-reported `rowCount` understates a column's nulls — to none at all once the shortfall exceeds them — and an over-reported one invents nulls, even in a `NOT NULL` column.

The estimate is approximate by design, not only when it is stale:

- **MySQL.** For InnoDB, `TABLE_ROWS` is a sampled estimate that MySQL documents as varying from the true count by up to 40–50%. On 8.0 and later it is additionally served from a cache refreshed by `ANALYZE TABLE` or after `information_schema_stats_expiry` (24 hours by default), so a table that has grown since also under-reports. Views report `TABLE_ROWS` as `0` or `NULL`.
- **Postgres.** On 14 and later, `reltuples` is `-1` for a table that has never been analyzed, and stays `-1` for a partitioned parent until someone runs `ANALYZE` on the parent, because autovacuum does not. That `-1` is carried through as `rowCount`. On 13 and earlier the sentinel is `0`, which is indistinguishable from an empty table.
- **Redshift.** `tbl_rows` counts rows deleted but not yet vacuumed, so it over-reports after deletes. `SVV_TABLE_INFO` is also visible only to superusers unless you grant it: without `GRANT SELECT ON SVV_TABLE_INFO TO <user>`, the profiling user sees no rows and **every table reads as 0**, which clamps every column's `nullCount` to zero.
- A lookup that fails, or that finds no row, is reported as `0`.

See [`rowCount` and the column statistics are measured over different things](#rowcount-and-the-column-statistics-are-measured-over-different-things) for the related case where the two are measured over different row sets.

### Sampling

For very large tables, `profiling.use_sampling` (supported on BigQuery and Snowflake) profiles a sample rather than the full table. This reduces the cost of each scan, where the options above reduce how many queries and scans are issued — so sampling composes with them, and on a supported platform you can enable several together.

The difference that matters when choosing: sampling changes the numbers you get. Distinct counts in particular are computed over the sample, so `uniqueCount` becomes an estimate. Query combining and flattening only change how the queries are issued — the statistics they produce are identical to running each query on its own.

#### `rowCount` and the column statistics are measured over different things

A profile whose `partitionSpec.type` is not `FULL_TABLE` — which covers both a sampled profile and a partitioned one — carries two kinds of number that do **not** come from the same set of rows:

| field                                                                              | measured over                                   |
| ---------------------------------------------------------------------------------- | ----------------------------------------------- |
| `rowCount`                                                                         | the whole dataset, taken from source metadata   |
| every field profile (`nullCount`, `nullProportion`, `uniqueCount`, min/max/mean/…) | only the sample, or only the profiled partition |

**The consequence is the part that catches people out: `nullCount` is not a fraction of `rowCount`.** It is `sample_rows - non_null_rows`, so on a billion-row table sampled to 10,000 rows a column that is 50% null reports `nullCount: 5000` against `rowCount: 1000000000`. Dividing one by the other is meaningless. `nullProportion` is the figure to use — it is computed over the sample and is therefore a valid estimate of the whole. The same applies to `uniqueCount` (a sample's distinct count, not scaled up — distinct counts do not grow linearly with the sampling rate) and to the min, max, mean, median and stdev.

When the source metadata has no row count — views, external tables, or anywhere it is otherwise unavailable — the profile carries no `rowCount` at all rather than the sample's size. That is expected, not a failure.
