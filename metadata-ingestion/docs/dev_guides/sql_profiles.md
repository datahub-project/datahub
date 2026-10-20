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

Profiling issues one query per metric per column, so a wide table can cost hundreds of round trips and hundreds of table scans. Five independent options reduce that; they address different costs and can be combined.

### Query combining

`profiling.query_combiner_enabled` (on by default) batches queries that each return exactly one row into a single round trip, by wrapping each in a CTE and cross-joining them. This cuts **round trips**, not table scans — each CTE is still its own aggregate over the table, so the database may scan once per metric.

`max_queries_to_combine` (default 40) caps how many **queries** share one statement — not columns. A 12-column numeric table issues 109 queries under default settings, 85 of them combinable, because each column contributes several and an unsampled median adds more; a table of strings queues far fewer. Raising it cuts round trips, and with flattening on it also cuts scans, because a flattened statement is one aggregate list over one table. With flattening off each merged query is still its own aggregate, so only round trips fall.

The database bounds how far it can usefully be raised: a merged statement cross-joins one subquery per query, and MySQL allows 61 tables in a join, SQLite 64. Past that every merged statement fails and is retried one query at a time. A value above the limit is therefore slower than not combining at all; the profiler warns at startup when it can tell, which it can for MySQL, MariaDB and SQLite.

A flattened statement does not join, so its normal path is not subject to that limit. Its recovery is: a flat statement that fails is re-routed through the joining path, so above the limit that recovery skips straight to one query at a time.

The other trade is that a statement which fails is retried one query at a time, and that retry is scoped to the batch that failed — so a bigger batch means a bigger retry.

Measured on a 20-column numeric SQLite table (101 queued queries, with sample values, median and stddev off):

| `max_queries_to_combine` | flattening off | flattening on, flat form rejected |
| ------------------------ | -------------- | --------------------------------- |
| 40                       | 3 statements   | 6 statements                      |
| 100 (above SQLite's 64)  | 102 statements | 83 statements                     |

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

If `scans_avoided` is low, `flatten_singletons` through `flat_group_serial_fallbacks` say why. High `flatten_singletons` means the workload has little to merge; a non-zero `flat_group_serial_fallbacks` means flattening is costing round trips rather than saving scans, and the flag is better off.

Two more counters are about the row-count gate rather than flattening, and a high value in either is a saving rather than a problem:

| counter                       | meaning                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| `queries_skipped_after_gate`  | queries never issued because the table's row count failed on its own, so it could not be read      |
| `queries_skipped_empty_table` | queries never issued because the exact row count was 0, so their results would have been discarded |

One more is about `query_timeout_seconds` rather than flattening:

| counter          | meaning                                                                          |
| ---------------- | -------------------------------------------------------------------------------- |
| `query_timeouts` | statements the server ended on `query_timeout_seconds`, including serial retries |

### Skipping the exact row count

`profiling.profile_table_row_count_estimate_only` replaces the profiler's own `COUNT(*)` — a full scan per table — with a single catalog lookup: `information_schema.tables.table_rows` on MySQL, `pg_class.reltuples` on Postgres, `svv_table_info.tbl_rows` on Redshift. It is ignored on every other source.

The error reaches further than `rowCount`. `nullCount` is `rowCount` minus an exact non-null count, so it and `nullProportion` inherit it. Because the subtraction is clamped at zero, an under-reported `rowCount` understates a column's nulls — to none at all once the shortfall exceeds them — and an over-reported one invents nulls, even in a `NOT NULL` column.

The estimate is approximate by design, not only when it is stale:

- **MySQL.** For InnoDB, `TABLE_ROWS` is a sampled estimate that MySQL documents as varying from the true count by up to 40–50%. On 8.0 and later it is additionally served from a cache refreshed by `ANALYZE TABLE` or after `information_schema_stats_expiry` (24 hours by default), so a table that has grown since also under-reports. Views report `TABLE_ROWS` as `0` or `NULL`.
- **Postgres.** On 14 and later, `reltuples` is `-1` for a table that has never been analyzed, and stays `-1` for a partitioned parent until someone runs `ANALYZE` on the parent, because autovacuum does not. That `-1` is carried through as `rowCount`. On 13 and earlier the sentinel is `0`, which is indistinguishable from an empty table.
- **Redshift.** `tbl_rows` counts rows deleted but not yet vacuumed, so it over-reports after deletes. `SVV_TABLE_INFO` is also visible only to superusers unless you grant it: without `GRANT SELECT ON SVV_TABLE_INFO TO <user>`, the profiling user sees no rows and **every table reads as 0**, which clamps every column's `nullCount` to zero.
- A lookup that fails, or that finds no row, is reported as `0`.

See [`rowCount` and the column statistics are measured over different things](#rowcount-and-the-column-statistics-are-measured-over-different-things) for the related case where the two are measured over different row sets.

### Bounding how long one statement runs

Combining and flattening reduce how many statements run, but not how long one of them takes, and a single aggregate over a very large table holds a read view for its whole duration — growing the InnoDB undo log on MySQL, blocking `VACUUM` on Postgres.

`profiling.query_timeout_seconds` puts a server-side limit on each profiling statement (`max_execution_time` on MySQL, `statement_timeout` on Postgres; only these two are implemented, and every other platform — MariaDB included, despite having `max_statement_time` — ignores the option). The limit is set on the profiling connection and the session's previous value put back when the table is done, so it neither leaks to the connection pool that metadata extraction shares nor discards a limit the session already carried. It is a session setting rather than a per-query one, so while it is installed it bounds every statement profiling issues on that connection — reflection, sampling, the row count — not only the column aggregates.

**Pair it with `profiling_isolation_level: AUTOCOMMIT`.** The isolation level decides whether the limit does its job, and the profiler warns when it does not:

- On Postgres a timed-out statement leaves the transaction aborted, at every level below `AUTOCOMMIT`. The combiner rolls back before each of its own retries, but the per-column queries that follow — quantiles, histograms, value frequencies, sample values — do not, so the whole table loses them. Under AUTOCOMMIT only the slow column loses its metrics.
- On MySQL nothing is lost that way at any level, but the default `REPEATABLE READ` holds the read view from the first statement until the commit, so a per-statement limit does not bound the thing this option exists to bound. `AUTOCOMMIT` or `READ COMMITTED` restores the bound, and neither is warned about.

Under AUTOCOMMIT, a statement that exceeds the limit does not cost the whole table. It is retried one query at a time, so columns whose own query then succeeds are still profiled and only the slow ones lose their metrics; a table is left without field profiles only when the row count itself times out.

Size it with that retry in mind: a table whose columns are each slow enough to time out costs roughly the limit **per profiling query**, which on a wide table is many multiples of it.

**Do not pair it with `profile_table_row_count_estimate_only: true` on a table that cannot be scanned inside the limit.** The exact row count is what stops such a table early: it runs first, times out, and the rest of the table is skipped. Replace it with a catalog estimate and no query can fail first, so every query pays the limit on its own. Measured on a table whose full scan takes about 1.5 s with a 1 s limit: 4 statements and 6.2 s with no limit, against 64 statements and 62.6 s with one — ten times the server work, and the field profiles lost either way. `query_timeouts` in the report is the signal: it counts both the batch that timed out and each retry that timed out after it, so on a table in this state it grows with the number of profiling queries rather than staying at one per batch.

Behind a connection pooler in transaction mode (PgBouncer and similar) the limit is not reliable: the `SET` is committed in its own transaction and so is the restore, and each may land on a different server connection.

### Sampling

For very large tables, `profiling.use_sampling` (supported on BigQuery and Snowflake) profiles a sample rather than the full table. This reduces the cost of each scan, where the four options above reduce how many queries and scans are issued, or how long one may run — so sampling composes with them, and on a supported platform you can enable several together.

The difference that matters when choosing: sampling changes the numbers you get. Distinct counts in particular are computed over the sample, so `uniqueCount` becomes an estimate. Query combining and flattening only change how the queries are issued — the statistics they produce are identical to running each query on its own.

#### `rowCount` and the column statistics are measured over different things

A profile whose `partitionSpec.type` is not `FULL_TABLE` — which covers both a sampled profile and a partitioned one — carries two kinds of number that do **not** come from the same set of rows:

| field                                                                              | measured over                                   |
| ---------------------------------------------------------------------------------- | ----------------------------------------------- |
| `rowCount`                                                                         | the whole dataset, taken from source metadata   |
| every field profile (`nullCount`, `nullProportion`, `uniqueCount`, min/max/mean/…) | only the sample, or only the profiled partition |

**The consequence is the part that catches people out: `nullCount` is not a fraction of `rowCount`.** It is `sample_rows - non_null_rows`, so on a billion-row table sampled to 10,000 rows a column that is 50% null reports `nullCount: 5000` against `rowCount: 1000000000`. Dividing one by the other is meaningless. `nullProportion` is the figure to use — it is computed over the sample and is therefore a valid estimate of the whole. The same applies to `uniqueCount` (a sample's distinct count, not scaled up — distinct counts do not grow linearly with the sampling rate) and to the min, max, mean, median and stdev.

When the source metadata has no row count — views, external tables, or anywhere it is otherwise unavailable — the profile carries no `rowCount` at all rather than the sample's size. That is expected, not a failure.
