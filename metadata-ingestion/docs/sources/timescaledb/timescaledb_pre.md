### Overview

The TimescaleDB source extends the Postgres source and extracts:

- All standard PostgreSQL metadata (databases, schemas, tables, views, stored procedures, column-level types and comments).
- Hypertables, with their dimensions, chunk counts, compression status, and retention policies (tagged `hypertable`).
- Continuous aggregates, with their refresh policies and upstream source hypertables (tagged `continuous_aggregate` and emitted with view lineage).
- Optionally, TimescaleDB background jobs (refresh, compression, retention, reorder) as `DataJob` entities grouped under a per-schema `DataFlow`, with recent execution history as `DataProcessInstance` entities.
- Table, row, and column statistics via optional SQL profiling.

Both self-hosted TimescaleDB (2.0+) and Tiger Cloud (managed TimescaleDB) are supported. The connector automatically detects which environment it is connected to.

If the `timescaledb` extension is not installed on the target database the connector degrades to a standard PostgreSQL ingestion — no TimescaleDB-specific aspects are emitted.

#### See Also

- [TimescaleDB documentation](https://docs.tigerdata.com/)
- [Postgres source configuration](https://docs.datahub.com/docs/generated/ingestion/sources/postgres) — all Postgres options apply here too

### Prerequisites

#### Required privileges

The DataHub user needs the following privileges on the target database:

```sql
-- Connect to the database
GRANT CONNECT ON DATABASE your_database TO datahub_user;

-- Access to user schemas
GRANT USAGE ON SCHEMA public TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;

-- Access to TimescaleDB metadata
GRANT USAGE ON SCHEMA timescaledb_information TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;

-- Access to extension metadata
GRANT SELECT ON pg_extension TO datahub_user;
```

The connector uses the `timescaledb_information` schema, which is the supported public interface in TimescaleDB 2.0+ and Tiger Cloud. The legacy `_timescaledb_catalog` and `_timescaledb_internal` schemas are **not** required.

#### Authentication

Standard PostgreSQL username / password authentication is used. For Tiger Cloud (managed TimescaleDB) SSL is required:

```yaml
source:
  type: timescaledb
  config:
    host_port: "<your-tiger-cloud-host>:5432"
    database: tsdb
    username: tsdbadmin
    password: "${TIMESCALEDB_PASSWORD}"
    options:
      connect_args:
        sslmode: require
```

Tiger Cloud connection details can be found in the [Tiger Cloud console](https://docs.tigerdata.com/integrations/latest/find-connection-details/).

#### Supported versions

- TimescaleDB **2.0** or later
- PostgreSQL **12 – 16**
- Tiger Cloud (managed TimescaleDB)
