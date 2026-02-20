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

SQL profiling is supported for all SQL sources. Check the individual source page to verify if it supports profiling.

## Profiler Implementation

DataHub is transitioning from Great Expectations (GE) based profiling to a custom SQLAlchemy profiler.

### Current State

Two profiler implementations are available:

1. **GE Profiler** (default): Uses Great Expectations library
2. **SQLAlchemy Profiler** (opt-in): Custom implementation with no external GE dependency

The SQLAlchemy profiler can be enabled via `profile.method = "sqlalchemy"`:

```yaml
source:
  config:
    profiling:
      enabled: true
      method: sqlalchemy
```

### Rollout Plan

- **Phase 1 (Current):** SQLAlchemy profiler available as opt-in. Users can test and validate.
- **Phase 2 (Future):** SQLAlchemy profiler becomes the default. GE profiler still available for compatibility.
- **Phase 3 (Future):** GE profiler removed from codebase to reduce maintenance and dependencies.
