# Apache Doris

For configuration details, see the [Doris source documentation](https://datahubproject.io/docs/generated/ingestion/sources/doris).

## Troubleshooting

### Common Issues

#### Types showing as BLOB instead of HLL/BITMAP

**Cause**: DESCRIBE query may be failing or dialect patching issue.

**Solution**: Enable debug logging:

```bash
datahub ingest -c doris_recipe.yml --debug
```

Look for:

- `"Type preservation: X Doris-specific columns..."` - Types are being preserved ✓
- `"DESCRIBE query failed..."` - Graceful fallback to MySQL types (investigate if frequent)

#### Connection refused on port 9030

**Cause**: Doris FE not running or wrong port.

**Solution**:

- Verify FE is running: `docker ps` or check Doris logs
- Confirm port: Doris FE query port is `9030`
- Check firewall rules

#### Slow ingestion for large schemas

**Cause**: DESCRIBE query runs for every table.

**Solution**: Use schema/table patterns to limit scope:

```yaml
schema_pattern:
  allow: ["production.*"]
table_pattern:
  deny: [".*_backup$", "tmp_.*"]
```

### Observability

The connector provides detailed logging:

**Successful Type Preservation:**

```
INFO  datahub.ingestion.source.sql.doris - Created Doris engine for database 'analytics'
INFO  datahub.ingestion.source.sql.doris - Type preservation: 3 Doris-specific columns in analytics.user_behavior
DEBUG datahub.ingestion.source.sql.doris - Preserved Doris type for analytics.user_behavior.user_ids_hll: HLL
```

**Graceful Degradation (DESCRIBE failure):**

```
WARNING datahub.ingestion.source.sql.doris - DESCRIBE query failed for analytics.old_table: Table doesn't exist. Falling back to MySQL types.
```

## Advanced Configuration

### Performance Tuning

For large Doris deployments (1000+ tables):

1. **Limit profiling scope**:

   ```yaml
   profiling:
     enabled: true
     profile_table_level_only: true  # Skip expensive column profiling
     profile_pattern:
       allow: ["production\.fact_.*"]
   ```

2. **Filter schemas/tables**:
   ```yaml
   schema_pattern:
     allow: ["production.*"]
   table_pattern:
     deny: [".*_backup$", "tmp_.*"]
   ```

### Multi-Cluster Deployments

Use `platform_instance` to distinguish clusters:

```yaml
source:
  type: doris
  config:
    host_port: prod-doris-01:9030
    platform_instance: production-cluster-01
```

Creates URNs like:

```
urn:li:dataset:(urn:li:dataPlatform:doris,production-cluster-01.dbname.tablename,PROD)
```

## Migration from MySQL Connector

### URN Changes

Dataset URNs will change:

**Before (MySQL):**

```
urn:li:dataset:(urn:li:dataPlatform:mysql,dbname.tablename,PROD)
```

**After (Doris):**

```
urn:li:dataset:(urn:li:dataPlatform:doris,dbname.tablename,PROD)
```

### Cleanup Strategy

Enable stateful ingestion to automatically remove old MySQL entities:

```yaml
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

Adjust threshold for removal timing as needed.

**Recommendation**: Test with a small schema first to verify new entities look correct.
