## Migration from MySQL Connector

If you were previously ingesting Doris using the MySQL connector, switch to the dedicated Doris connector for better support:

**Configuration changes:**

- Change `type: mysql` → `type: doris`
- Change port: `3306` → `9030`

**Important:** Dataset URNs will change from `platform:mysql` to `platform:doris`. This creates new entities in DataHub. Enable stateful ingestion with `remove_stale_metadata: true` to automatically clean up old MySQL-based entities.

## Troubleshooting

### Types showing as BLOB instead of HLL/BITMAP

If Doris-specific types (HLL, BITMAP, QUANTILE_STATE) appear as generic BLOB types in DataHub, enable debug logging to investigate:

```bash
datahub ingest -c doris_recipe.yml --debug
```

Look for these log messages:

- `"Type preservation: X Doris-specific columns..."` - Types are being preserved correctly ✓
- `"DESCRIBE query failed..."` - The connector is falling back to MySQL types. This may indicate permission issues or unsupported table types.

### Connection Issues

#### Connection refused on port 9030

**Cause**: The Doris Frontend (FE) is not accessible.

**Solution**:

1. Verify the FE is running: `docker ps` or check Doris logs
2. Confirm the FE query port is `9030` (default)
3. Test connectivity: `telnet <doris-host> 9030`
4. Check firewall rules between DataHub and Doris

#### Authentication failed

Verify the user credentials and permissions:

```sql
-- Check user exists
SELECT user, host FROM mysql.user WHERE user='datahub';

-- Verify permissions
SHOW GRANTS FOR 'datahub'@'%';
```

### Performance

#### Slow ingestion for large schemas

For Doris deployments with many tables, use filtering to reduce ingestion scope:

```yaml
schema_pattern:
  allow: ["production.*"]
  deny: ["test_.*", "tmp_.*"]
table_pattern:
  deny: [".*_backup$", ".*_archive$"]
```

### View Lineage

View definitions in DataHub automatically include lineage to source tables. The connector extracts view SQL from Doris and cleans it for better readability:

- Removes the `internal` catalog prefix (Doris's default catalog)
- Example: `` `internal`.`db`.`table` `` → `` `db`.`table` ``

This ensures view definitions match how users typically write queries and enables accurate lineage tracking.

## Advanced Configuration

### Performance Tuning for Large Deployments

For Doris clusters with 1000+ tables, consider these optimizations:

**Option 1: Table-level profiling only**

Skip expensive column-level profiling:

```yaml
profiling:
  enabled: true
  profile_table_level_only: true
```

**Option 2: Selective profiling**

Profile only important tables:

```yaml
profiling:
  enabled: true
  profile_pattern:
    allow: ["production\.fact_.*", "production\.dim_.*"]
```

**Option 3: Schema/table filtering**

Reduce ingestion scope:

```yaml
schema_pattern:
  allow: ["production.*"]
  deny: ["test_.*", "tmp_.*"]
table_pattern:
  deny: [".*_backup$", ".*_archive$"]
```

### Multi-Cluster Deployments

When ingesting from multiple Doris clusters, use `platform_instance` to distinguish them:

```yaml
source:
  type: doris
  config:
    host_port: prod-doris-01:9030
    platform_instance: production-us-west
```

This creates unique URNs per cluster:

```
urn:li:dataset:(urn:li:dataPlatform:doris,production-us-west.dbname.tablename,PROD)
```
