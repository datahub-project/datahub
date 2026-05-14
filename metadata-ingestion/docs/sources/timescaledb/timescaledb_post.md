### Capabilities

Use the **Important Capabilities** table above as the source of truth for which features are supported and whether additional configuration is required.

#### Hypertables and continuous aggregates

These are emitted as regular `Dataset` entities (tables and views respectively), so they show up in search, lineage, and lineage-based impact analysis exactly like other Postgres relations. To make them easy to filter on, the connector adds:

- A `hypertable` tag on every hypertable, plus custom properties (`is_hypertable`, `num_dimensions`, `num_chunks`, `compression_enabled`, `retention_period`, dimensions JSON).
- A `continuous_aggregate` tag on every continuous aggregate, plus custom properties (`is_continuous_aggregate`, `materialized_only`, `source_hypertable`, `refresh_interval`, refresh policy details).

Continuous aggregates emit upstream lineage to their source hypertable automatically. Standard PostgreSQL views also emit lineage based on `pg_depend`, identical to the Postgres source.

#### TimescaleDB background jobs (optional)

By default, TimescaleDB's policy procedures (`policy_refresh_continuous_aggregate`, `policy_retention`, `policy_compression`, `policy_reorder`) are **excluded** from stored-procedure ingestion, because they are system-managed jobs rather than user-defined logic.

Set `include_background_jobs: true` to surface them as first-class job entities:

```yaml
source:
  type: timescaledb
  config:
    host_port: "localhost:5432"
    database: tsdb
    username: datahub_user
    password: "${TIMESCALEDB_PASSWORD}"

    # Emit TimescaleDB background jobs as DataJob entities
    include_background_jobs: true
```

When enabled, the connector emits, per schema:

- A `DataFlow` of subtype **Background Jobs** that groups every TimescaleDB background job in that schema.
- A `DataJob` of subtype **Background Job** per job, with the job's `schedule_interval`, `max_runtime`, `max_retries`, `retry_period`, and target hypertable as custom properties. Continuous-aggregate refresh jobs emit lineage from their source hypertable to the materialized continuous aggregate.
- `DataProcessInstance` entities for the most recent runs, including last-run status, success/failure timestamps, and consecutive-failure counts pulled from `timescaledb_information.job_stats`.

#### Graceful degradation

The connector is designed so that TimescaleDB-specific extraction never blocks the underlying Postgres ingestion:

- If the `timescaledb` extension is not installed, the source operates as a plain PostgreSQL source.
- If the user is missing privileges on `timescaledb_information` or `pg_extension`, the connector logs a warning and continues with whatever metadata it can access.
- Errors fetching hypertable / continuous-aggregate / job metadata are logged but do not abort the run.

### Limitations

- The connector only reads from the supported `timescaledb_information` schema. The legacy internal catalogs (`_timescaledb_catalog`, `_timescaledb_internal`) are not used and metadata exposed only there is not extracted.
- Background jobs are off by default. They must be explicitly enabled with `include_background_jobs: true`.
- TimescaleDB and PostgreSQL share the same wire protocol and URI scheme (`postgresql://`). When DataHub reverse-maps a discovered SQLAlchemy URI to a platform (e.g. for BI-tool lineage attribution), it resolves to `postgres`. Configure the platform explicitly in the recipe (`type: timescaledb`) when you want TimescaleDB-specific behavior.
- All standard limitations of the Postgres source apply.

### Troubleshooting

If ingestion fails, first validate credentials, network reachability, and database name. Then review the ingestion logs for source-specific warnings.

#### TimescaleDB metadata is missing from ingested datasets

Most commonly this is a permissions issue. Verify the user can read from the supported metadata schemas:

```sql
-- Should return rows
SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';

-- Should return your hypertables
SELECT hypertable_schema, hypertable_name
FROM timescaledb_information.hypertables
LIMIT 5;
```

If the first query returns nothing, the `timescaledb` extension is not installed on this database; the connector will fall back to plain Postgres ingestion.

If the first query succeeds but the second fails with a permissions error, grant access:

```sql
GRANT USAGE ON SCHEMA timescaledb_information TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;
```

#### Background jobs are not appearing in DataHub

Make sure `include_background_jobs: true` is set in the recipe. With the flag disabled the underlying `policy_*` procedures are still filtered out of the stored-procedure ingestion (so that they don't show up as user procedures), but they are not emitted as `DataJob` entities either.

#### SSL connection errors against Tiger Cloud

Tiger Cloud rejects non-TLS connections. Set `sslmode: require` (or stricter) under `options.connect_args`:

```yaml
source:
  type: timescaledb
  config:
    options:
      connect_args:
        sslmode: require
```
