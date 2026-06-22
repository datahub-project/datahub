### Capabilities

#### Hypertables and continuous aggregates

Hypertables are emitted as `Dataset` entities (subtype `Table`) tagged `hypertable`, with `is_hypertable`, `num_dimensions`, `num_chunks`, `compression_enabled`, `retention_period`, and a dimensions JSON in `customProperties`.

Continuous aggregates are emitted as `Dataset` entities (subtype `View`) tagged `continuous_aggregate`, with refresh policy details in `customProperties` and upstream lineage to their source hypertable.

#### Background jobs (optional)

By default, TimescaleDB policy procedures (`policy_refresh_continuous_aggregate`, `policy_retention`, `policy_compression`, `policy_reorder`) are filtered out of stored-procedure ingestion.

Set `include_background_jobs: true` to emit them as job entities instead:

```yaml
source:
  type: timescaledb
  config:
    include_background_jobs: true
```

When enabled, per schema:

- One `DataFlow` of subtype `Background Jobs` grouping the jobs.
- One `DataJob` per background job, with `schedule_interval`, `max_runtime`, `max_retries`, `retry_period`, and target hypertable as custom properties. Continuous-aggregate refresh jobs emit lineage from source hypertable to materialized continuous aggregate.
- `DataProcessInstance` entities for recent runs, pulled from `timescaledb_information.job_stats`.

### Limitations

- Only the supported `timescaledb_information` schema is read. The internal catalogs (`_timescaledb_catalog`, `_timescaledb_internal`) are not queried.
- Background jobs are off by default.
- TimescaleDB and PostgreSQL share the `postgresql://` URI scheme, so URI-based reverse-lookups (e.g. BI tool lineage attribution) resolve to `postgres`.
- All limitations of the Postgres source apply.

### Troubleshooting

#### TimescaleDB metadata is missing from ingested datasets

Verify the extension is installed and the user has access:

```sql
SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';
SELECT hypertable_schema, hypertable_name FROM timescaledb_information.hypertables LIMIT 5;
```

If the first query returns nothing, the extension is not installed and the source falls back to plain Postgres. If the second fails with a permissions error, grant access:

```sql
GRANT USAGE ON SCHEMA timescaledb_information TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;
```

#### Background jobs are not appearing in DataHub

Set `include_background_jobs: true` in the recipe.

#### SSL connection errors against Tiger Cloud

Set `sslmode: require` under `options.connect_args`.
