### Prerequisites

In order to execute this source, the user credentials need the following privileges:

```sql
-- Grant necessary privileges to the DataHub user
GRANT SELECT_PRIV ON your_database.* TO 'datahub_user'@'%';
GRANT SHOW_VIEW_PRIV ON your_database.* TO 'datahub_user'@'%';

-- For profiling (optional, if profiling is enabled)
GRANT SELECT_PRIV ON your_database.* TO 'datahub_user'@'%';
```

**Note:** `SELECT_PRIV` is required to read table structures and perform profiling operations. `SHOW_VIEW_PRIV` is required to ingest views.

#### Apache Doris Compatibility Notes

Apache Doris uses the MySQL protocol for client connections, but with some key differences:

**Port Configuration:**

- Default Doris query port: **9030** (FE MySQL protocol port)
- **Not** MySQL's default 3306
- Ensure you use `host_port: doris-server:9030` in your configuration

**Architecture:**

- Doris uses a Frontend (FE) and Backend (BE) architecture
- DataHub connects to the FE node on port 9030
- Ensure the FE node is accessible and healthy

**Data Types:**

- Doris includes additional data types: `HLL`, `BITMAP`, `ARRAY`, `JSONB`, `QUANTILE_STATE`
- These types are automatically mapped to appropriate DataHub types
- No additional configuration needed

**Stored Procedures:**

- Apache Doris does not support stored procedures
- The `information_schema.ROUTINES` table is a MySQL compatibility stub (always empty)
- The connector automatically handles this limitation

### Troubleshooting

#### Connection Issues

**Problem:** `Can't connect to MySQL server` or connection timeouts

**Solutions:**

- Verify you're using port **9030** (query port), not 9050 (HTTP port) or 3306 (MySQL default)
- Check that the Doris FE (Frontend) node is running: `curl http://fe-host:8030/api/bootstrap`
- Ensure network connectivity and firewall rules allow connections to port 9030
- Verify the FE node has registered BE nodes: `SHOW BACKENDS;`

**Problem:** `Access denied for user`

**Solutions:**

- Verify the user has been granted `SELECT_PRIV` and `SHOW_VIEW_PRIV`
- Check grants with: `SHOW GRANTS FOR 'datahub_user'@'%';`
- Ensure the user is allowed to connect from your host: use `'%'` for any host or specify the IP

#### Missing Metadata

**Problem:** Tables or views are not being ingested

**Solutions:**

- Verify the user has `SELECT_PRIV` on the target databases/tables
- Check that tables exist and are visible: `SHOW TABLES IN your_database;`
- Review `schema_pattern` and `table_pattern` in your recipe configuration
- Ensure the database is not filtered out by your configuration

**Problem:** Column types showing as UNKNOWN

**Solutions:**

- This typically happens with Doris-specific types in older DataHub versions
- Ensure you're using the latest DataHub version which includes Doris type mappings
- Check Doris FE logs for any metadata query errors

#### Performance Issues

**Problem:** Ingestion is slow or timing out

**Solutions:**

- Use `schema_pattern` and `table_pattern` to limit scope: `schema_pattern: {"allow": ["important_db"]}`
- Enable table-level-only profiling: `profiling.profile_table_level_only: true`
- Disable profiling if not needed: `profiling.enabled: false`
- Increase query timeouts if you have very large tables: `options.connect_timeout: 300`

**Problem:** Doris FE or BE is overloaded during ingestion

**Solutions:**

- Reduce profiling sample size: `profiling.max_number_of_fields_to_profile: 10`
- Schedule ingestion during off-peak hours
- Increase `profiling.query_combiner_enabled: false` to avoid complex queries

#### Profiling Issues

**Problem:** Profiling fails or returns no statistics

**Solutions:**

- Verify user has `SELECT_PRIV` on target tables
- Check that tables contain data (empty tables have no statistics)
- Ensure Doris statistics are up to date: `ANALYZE TABLE your_table;`
- Review Doris FE logs for query errors during profiling

#### Doris-Specific Issues

**Problem:** Warnings about `DUPLICATE KEY` or `DISTRIBUTED BY HASH`

**Solutions:**

- These are informational warnings from SQLAlchemy parsing Doris-specific table properties
- They do not affect ingestion and can be safely ignored
- The connector handles these properties correctly

**Problem:** View lineage not being captured

**Solutions:**

- Ensure `include_view_lineage: true` (enabled by default)
- Verify views are created with proper table references
- Check that referenced tables are accessible to the DataHub user
- Review `include_view_column_lineage` configuration

For additional support, consult the [Apache Doris documentation](https://doris.apache.org/docs) or reach out to the DataHub community.
