### Prerequisites

In order to execute this source, the user credentials need the following privileges:

- `GRANT SELECT_PRIV ON DATABASE.* TO 'USERNAME'@'%'`
- `GRANT SHOW_VIEW_PRIV ON DATABASE.* TO 'USERNAME'@'%'`

`SELECT_PRIV` is required to read table structures and perform profiling operations.

### Compatibility Notes

Apache Doris uses the MySQL protocol for client connections, making it compatible with MySQL tools and drivers. However, there are some important differences:

**Port Configuration:**

- Default Doris query port: **9030** (not MySQL's 3306)
- Use `host_port: doris-server:9030` in your configuration

**Stored Procedures:**

- Apache Doris does not support stored procedures
- The `information_schema.ROUTINES` table exists for MySQL compatibility but is always empty
- The connector automatically handles this limitation

**Data Types:**

- Doris includes additional data types not in MySQL: `HLL` (HyperLogLog), `BITMAP`, `ARRAY`, `JSONB`, `QUANTILE_STATE`
- The connector properly handles these types and maps them to appropriate DataHub types:
  - `HLL`, `BITMAP`, `QUANTILE_STATE` → Bytes (binary data)
  - `ARRAY` → Array type
  - `JSONB` → Record/Struct type
- These types will be correctly ingested and displayed in DataHub with proper type information

**System Tables:**

- Doris provides MySQL-compatible system tables via `information_schema`
- Some advanced MySQL system tables may not be available or may be read-only

### Recommended Configuration

For optimal results with Apache Doris:

```yaml
source:
  type: doris
  config:
    host_port: doris-server:9030 # Note: Use Doris query port (9030)
    database: your_database
    username: datahub_user
    password: ${DORIS_PASSWORD}

    # Recommended settings
    include_tables: true
    include_views: true

    # Profiling (optional)
    profiling:
      enabled: true
      profile_table_level_only: true

    # Stateful ingestion for deletion detection
    stateful_ingestion:
      enabled: true
```

### SSL/TLS Configuration

Apache Doris supports SSL connections using the same configuration as MySQL:

```yaml
source:
  type: doris
  config:
    host_port: doris-server:9030
    database: your_database
    username: datahub_user
    password: ${DORIS_PASSWORD}

    options:
      connect_args:
        ssl_ca: "/path/to/ca-cert.pem"
        ssl_cert: "/path/to/client-cert.pem"
        ssl_key: "/path/to/client-key.pem"
```

### Performance Tuning

For large Doris deployments:

- Use `table_pattern` and `schema_pattern` to limit the scope of ingestion
- Enable `profile_table_level_only: true` to skip column-level profiling
- Consider using stateful ingestion to only process changed tables

### Troubleshooting

**Connection Issues:**

- Verify you're using port 9030 (query port), not 9050 (HTTP port)
- Check that the Doris FE (Frontend) node is accessible
- Ensure the user has `NODE_PRIV` if querying cluster metadata

**Permission Errors:**

- Grant `SELECT_PRIV` on target databases
- Grant `SHOW_VIEW_PRIV` if ingesting views
- For profiling, ensure user can read `information_schema.tables`

**Missing Metadata:**

- Verify `information_schema` access
- Check Doris version compatibility (2.0+ recommended)
- Review Doris FE logs for any backend issues

For more technical details about MySQL compatibility, see [Apache Doris MySQL Compatibility Documentation](https://doris.apache.org/docs/query-data/mysql-compatibility/).
