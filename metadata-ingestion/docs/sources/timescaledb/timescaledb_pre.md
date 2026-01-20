### Prerequisites

In order to execute this source, the user credentials need the following privileges:

```sql
-- Connect to database
GRANT CONNECT ON DATABASE your_database TO datahub_user;

-- Access to schemas
GRANT USAGE ON SCHEMA public TO datahub_user;
GRANT USAGE ON SCHEMA timescaledb_information TO datahub_user;

-- Access to tables and views
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;

-- Access to extension metadata
GRANT SELECT ON pg_extension TO datahub_user;

-- Grant permissions on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
```

**Note:** The connector uses the `timescaledb_information` schema which is the standard interface for both self-hosted TimescaleDB (2.0+) and Tiger Cloud (Managed TimescaleDB). The legacy `_timescaledb_catalog` and `_timescaledb_internal` schemas are not required.

### Authentication

Authentication is done via standard PostgreSQL username and password.

For Tiger Cloud (Managed TimescaleDB), SSL connections are required by default. The connector will automatically use SSL when connecting to Tiger Cloud instances.

### Environment Compatibility

#### Self-Hosted TimescaleDB

**Supported versions:**

- TimescaleDB 2.0+
- PostgreSQL 12, 13, 14, 15, 16

The connector automatically detects self-hosted TimescaleDB by checking for the TimescaleDB extension and available schemas.

#### Tiger Cloud (Managed TimescaleDB)

Tiger Cloud is fully supported with automatic environment detection. No manual configuration is needed.

**Connection requirements:**

- SSL is required and enabled by default
- Connection details can be found in the [Tiger Cloud console](https://docs.tigerdata.com/integrations/latest/find-connection-details/)

### TimescaleDB Features

The connector extends PostgreSQL source capabilities with TimescaleDB-specific metadata:

**Automatically detected:**

- Hypertables with dimensions and chunk information
- Continuous aggregates (materialized views with automatic refresh)
- Compression policies and settings
- Data retention policies

**Optional features:**

- Background jobs (continuous aggregate refresh, compression, retention) can be included as DataJob entities by setting `include_background_jobs: true`
- Background jobs are system-managed automated tasks, distinct from user-defined stored procedures

### Error Handling

The connector implements graceful degradation:

- Errors in TimescaleDB-specific metadata extraction do not stop the entire ingestion
- If the user lacks permissions to query TimescaleDB metadata, the connector will log warnings and continue
- Basic PostgreSQL metadata is always extracted even if TimescaleDB features fail

### Caveats

- If the TimescaleDB extension is not installed, the connector will operate as a standard PostgreSQL connector
- The `timescaledb_information` schema requires `USAGE` and `SELECT` permissions for full metadata extraction
- Background jobs require `include_background_jobs: true` to be extracted as DataJob entities (disabled by default)
