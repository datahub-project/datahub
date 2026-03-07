### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Connection Issues

**Problem**: `Could not connect to metastore database`

**Solutions**:

- Verify `host_port`, `database`, and `scheme` are correct
- Check network connectivity: `telnet <host> <port>`
- Verify firewall rules allow connections
- For PostgreSQL: Check `pg_hba.conf` allows connections from your IP
- For MySQL: Check `bind-address` in `my.cnf`

#### Authentication Failures

**Problem**: `Authentication failed` or `Access denied`

**Solutions**:

- Verify username and password are correct
- Check user has CONNECT/LOGIN privileges
- For Azure: Ensure username includes `@server-name` suffix
- Review database logs for detailed error messages

#### Missing Tables

**Problem**: Not all tables appear in DataHub

**Solutions**:

- Verify database user has SELECT on all metastore tables
- Check if tables are filtered by `schema_pattern`, `database_pattern`, or `table_pattern`
- Query metastore directly to verify tables exist:
  ```sql
  SELECT d.name as db_name, t.tbl_name as table_name, t.tbl_type
  FROM TBLS t
  JOIN DBS d ON t.db_id = d.db_id
  WHERE d.name = 'your_database';
  ```

#### Presto/Trino Views Not Appearing

**Problem**: Views defined in Presto/Trino don't show up

**Solutions**:

- Check view definitions exist in metastore:
  ```sql
  SELECT d.name as db_name, t.tbl_name as view_name, tp.param_value
  FROM TBLS t
  JOIN DBS d ON t.db_id = d.db_id
  JOIN TABLE_PARAMS tp ON t.tbl_id = tp.tbl_id
  WHERE t.tbl_type = 'VIRTUAL_VIEW'
  AND tp.param_key = 'presto_view'
  LIMIT 10;
  ```
- Review ingestion logs for parsing errors
- Verify view JSON is valid

#### Storage Lineage Not Appearing

**Problem**: No storage lineage relationships visible

**Solutions**:

- Verify `emit_storage_lineage: true` is set
- Check tables have storage locations in metastore:
  ```sql
  SELECT d.name as db_name, t.tbl_name as table_name, s.location
  FROM TBLS t
  JOIN DBS d ON t.db_id = d.db_id
  JOIN SDS s ON t.sd_id = s.sd_id
  WHERE s.location IS NOT NULL
  LIMIT 10;
  ```
- Review logs for "Failed to parse storage location" warnings
- See the "Storage Lineage" section above for troubleshooting tips

#### Slow Ingestion

**Problem**: Ingestion takes too long

**Solutions**:

- Use schema filtering to reduce scope
- Enable stateful ingestion to only process changes
- Check database query performance (may need indexes on metastore tables)
- Ensure low latency network connection to metastore database
- Consider disabling column lineage if not needed

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
