### Overview

This plugin extracts:

- Standard PostgreSQL metadata (databases, schemas, tables, views, stored procedures, column types and comments).
- Hypertables, with dimensions, chunk counts, compression, and retention policies.
- Continuous aggregates, with refresh policies and upstream lineage to their source hypertables.
- Optionally, TimescaleDB background jobs (refresh, compression, retention, reorder) as `DataJob` entities, with recent execution history.
- Table, row, and column statistics via optional SQL profiling.

Self-hosted TimescaleDB (2.0+) and Tiger Cloud are both supported. If the `timescaledb` extension is not installed, the source falls back to plain PostgreSQL ingestion.

### Prerequisites

The DataHub user needs the following privileges:

```sql
GRANT CONNECT ON DATABASE your_database TO datahub_user;

GRANT USAGE ON SCHEMA public TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;

GRANT USAGE ON SCHEMA timescaledb_information TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;

GRANT SELECT ON pg_extension TO datahub_user;
```

Tiger Cloud requires SSL. Set `sslmode: require` in the recipe:

```yaml
source:
  type: timescaledb
  config:
    options:
      connect_args:
        sslmode: require
```

Tiger Cloud connection details: [Find your connection details](https://docs.tigerdata.com/integrations/latest/find-connection-details/).
