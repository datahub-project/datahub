# Apache Doris

For configuration details, see the [Doris source documentation](https://datahubproject.io/docs/generated/ingestion/sources/doris).

## Troubleshooting

### Common Issues

#### Types showing as BLOB instead of HLL/BITMAP

**Cause**: DESCRIBE query may be failing or dialect patching issue.

**Solution**: Enable debug logging to see what's happening:

```bash
datahub ingest -c doris_recipe.yml --debug
```

Look for log messages like:

- `"Type preservation: X Doris-specific columns..."` - Types are being preserved
- `"DESCRIBE query failed..."` - Graceful fallback to MySQL types

#### Connection refused on port 9030

**Cause**: Doris FE (Frontend) not running or wrong port.

**Solution**:

- Verify FE is running: `docker ps` or check Doris logs
- Confirm port: Doris FE query port is `9030` by default, not MySQL's `3306`
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

See the [recipe documentation](https://datahubproject.io/docs/generated/ingestion/sources/doris) for additional performance tuning options.

### Reporting Issues

If you encounter bugs or compatibility issues:

1. **Gather Information**:

   - Doris version: `SELECT version();`
   - SQLAlchemy version: `pip show sqlalchemy`
   - DataHub version: `datahub version`

2. **Enable Debug Logging**:

   ```bash
   datahub ingest -c recipe.yml --debug 2>&1 | tee debug.log
   ```

3. **Create GitHub Issue**: https://github.com/datahub-project/datahub/issues/new

   Include:

   - Doris version
   - SQLAlchemy version
   - Error message and full stack trace
   - Sample table DDL (if related to type mapping)
   - Relevant debug logs

### Testing

The connector is tested against:

- **Doris versions**: 3.0.8 (primary), 2.1.x, 2.0.x
- **SQLAlchemy versions**: 1.4.x and 2.0.x
- **Python versions**: 3.8, 3.9, 3.10, 3.11

If you're using an untested combination, it may still work but please report any issues.

## Advanced Configuration

### SSL/TLS

To enable SSL/TLS:

```yaml
source:
  type: doris
  config:
    host_port: doris.example.com:9030
    username: user
    password: pass
    options:
      connect_args:
        ssl_ca: "/path/to/server-ca.pem"
        ssl_cert: "/path/to/client-cert.pem"
        ssl_key: "/path/to/client-key.pem"
```

### Performance Tuning

For large Doris deployments (1000+ tables), consider:

1. **Limit profiling scope** to reduce load:

   ```yaml
   profiling:
     enabled: true
     profile_table_level_only: true  # Skip expensive column profiling
     profile_pattern:
       allow: ["production\.fact_.*"]  # Only critical tables
   ```

2. **Filter schemas/tables** to reduce DESCRIBE queries:
   ```yaml
   schema_pattern:
     allow: ["production.*"]
   table_pattern:
     deny: [".*_backup$", "tmp_.*"]
   ```

For additional options including connection pooling, see the [example recipe](doris_recipe.yml).

### Multi-Cluster Deployments

Use `platform_instance` to distinguish multiple Doris clusters:

```yaml
source:
  type: doris
  config:
    host_port: prod-doris-01:9030
    platform_instance: production-cluster-01
    # ... other config ...
```

This creates URNs like:

```
urn:li:dataset:(urn:li:dataPlatform:doris,production-cluster-01.dbname.tablename,PROD)
```

## Migration Notes

### From MySQL Connector

**URN Changes**

When migrating from the MySQL connector to the Doris connector, dataset URNs will change:

**Before (MySQL):**

```
urn:li:dataset:(urn:li:dataPlatform:mysql,dbname.tablename,PROD)
```

**After (Doris):**

```
urn:li:dataset:(urn:li:dataPlatform:doris,dbname.tablename,PROD)
```

This creates **new entities** in DataHub. Old MySQL entities will remain as orphaned metadata unless cleaned up.

**Cleanup Options**:

1. **Soft delete**: DataHub will mark old entities as deleted after they're not seen in ingestion for a configured period (if stateful ingestion is enabled)

2. **Manual cleanup**: Use DataHub CLI to delete old MySQL platform entities:

   ```bash
   datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:mysql,*,PROD)" --hard
   ```

3. **No cleanup**: Keep both if you want historical reference (not recommended for large schemas)

**Recommendation**: Test with a small schema first, verify new Doris entities look correct, then proceed with full migration.

## Development

### Running Tests

```bash
# Unit tests
cd metadata-ingestion
pytest tests/unit/doris/ -v

# Integration tests (requires Docker)
pytest tests/integration/doris/ -v
```

### Contributing

Contributions are welcome! Please:

1. Add tests for new features
2. Update documentation
3. Follow the existing code style
4. Run linting: `../gradlew :metadata-ingestion:lint`
