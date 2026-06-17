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

DataHub uses a SQLAlchemy-based profiler by default for all SQL sources.

### Default: SQLAlchemy Profiler

The default profiler runs profiling queries directly against your SQL source's existing SQLAlchemy connection and emits the table- and column-level statistics listed under [Capabilities](#capabilities). No additional dependencies are required beyond the SQL connector itself.

No configuration is required to use it — any SQL source with profiling enabled will use the SQLAlchemy profiler automatically:

```yaml
source:
  config:
    profiling:
      enabled: true
```

### Optional: Great Expectations Profiler (Deprecated)

:::warning

The Great Expectations profiler is **deprecated** and is planned for removal in a future release. The SQLAlchemy profiler above is the recommended replacement and has feature parity for all dataset- and column-level metrics. Existing users still relying on `method: ge` should plan to migrate.

:::

To use the legacy GE profiler, install the optional `profiling-ge` extra and set `profiling.method` explicitly:

```bash
pip install 'acryl-datahub[profiling-ge]'
```

```yaml
source:
  config:
    profiling:
      enabled: true
      method: ge
```

If you set `profiling.method: ge` without installing the extra, the ingestion will fail with a `ConfigurationError` pointing at the fix.

### Differences

The two profilers produce equivalent dataset- and column-level statistics. The only known difference is histogram bucket layout (controlled by `include_field_histogram`, off by default): the SQLAlchemy profiler uses 10 equal-width buckets, while the GE profiler uses Great Expectations' adaptive partitioning.
