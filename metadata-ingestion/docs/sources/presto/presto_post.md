### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Catalog and Schema Filtering

Presto can connect to many different catalogs (Hive, PostgreSQL, MySQL, etc.). Use filtering to control what gets ingested:

##### Catalog Filtering

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user

    # Only ingest specific catalogs
    database_pattern:
      allow:
        - "^hive$"
        - "^postgresql$"
      deny:
        - "system"
        - "information_schema"
```

##### Schema Filtering

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user
    database: hive # Default catalog

    # Filter schemas within catalogs
    schema_pattern:
      allow:
        - "^production_.*"
        - "analytics"
      deny:
        - ".*_test$"
```

##### Table Filtering

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user

    # Filter specific tables
    table_pattern:
      allow:
        - "^fact_.*"
        - "^dim_.*"
      deny:
        - ".*_tmp$"
        - ".*_staging$"
```

#### Platform Instances

When ingesting from multiple Presto clusters, use `platform_instance`:

```yaml
source:
  type: presto
  config:
    host_port: prod-presto.company.com:8080
    platform_instance: "prod-presto"
```

This creates URNs like:

```
urn:li:dataset:(urn:li:dataPlatform:presto,catalog.schema.table,prod-presto)
```

#### Data Profiling

The Presto connector supports optional data profiling:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user

    # Enable profiling
    profiling:
      enabled: true
      profile_table_level_only: false # Include column-level stats

      # Limit profiling scope
      profile_pattern:
        allow:
          - "^production_.*"
```

**Warning**: Profiling can be expensive on large tables. Start with `profile_table_level_only: true` and expand as needed.

#### Performance Considerations

##### Large Presto Deployments

For Presto clusters with many catalogs and tables:

1. **Catalog Filtering**: Limit ingestion to specific catalogs:

   ```yaml
   database_pattern:
     allow:
       - "hive"
       - "postgresql"
   ```

2. **Disable Profiling**: Or limit it to specific tables:

   ```yaml
   profiling:
     enabled: true
     profile_table_level_only: true
   ```

3. **Stateful Ingestion**: Only process changes:
   ```yaml
   stateful_ingestion:
     enabled: true
     remove_stale_metadata: true
   ```

##### Query Performance

- The connector queries Presto's `information_schema` tables
- Ensure your Presto cluster has sufficient resources
- Consider running ingestion during off-peak hours for large deployments

#### Migration from presto-on-hive

If you're currently using the deprecated `presto-on-hive` source:

**Old Configuration**:

```yaml
source:
  type: presto-on-hive # ← Deprecated
  config:
    host_port: metastore-db:3306
    # ...
```

**New Configuration** (Recommended):

```yaml
source:
  type: hive-metastore # ← Use this instead
  config:
    host_port: metastore-db:3306
    mode: presto # ← Set mode to 'presto'
    emit_storage_lineage: true # ← Now available!
    # ...
```

**Benefits of Migration**:

- Access to storage lineage features
- Better Presto view parsing
- Improved performance
- Active maintenance and new features

#### Comparison: Presto vs. Hive Metastore Connector

| Feature             | `presto` Connector   | `hive-metastore` (mode: presto) |
| ------------------- | -------------------- | ------------------------------- |
| **Connection**      | Direct to Presto     | Direct to metastore database    |
| **Catalogs**        | All Presto catalogs  | Only Hive-backed catalogs       |
| **Storage Lineage** | Not supported        | Supported                       |
| **Column Lineage**  | Limited              | Full support                    |
| **View Parsing**    | Basic                | Enhanced Presto view parsing    |
| **Performance**     | Good                 | Better (direct DB access)       |
| **Data Profiling**  | Supported            | Not supported                   |
| **Use Case**        | Multi-catalog Presto | Presto-on-Hive with lineage     |

#### Best Practices

1. **Choose the Right Connector**:

   - Use `presto` for multi-catalog Presto deployments
   - Use `hive-metastore` (mode: presto) for Hive-backed tables with storage lineage

2. **Filter Appropriately**:

   - Exclude system catalogs: `system`, `information_schema`
   - Use patterns to include only relevant data

3. **Enable Stateful Ingestion**:

   - Only process changes on subsequent runs
   - Reduces ingestion time and resource usage

4. **Test First**:

   - Start with a small subset of catalogs/schemas
   - Verify metadata quality before expanding scope

5. **Monitor Presto Load**:
   - Ingestion queries can impact Presto performance
   - Run during off-peak hours for large deployments

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Storage Lineage

**Not Supported**: The Presto connector cannot extract storage lineage because it doesn't have access to underlying storage locations.

**Solution**: Use the [Hive Metastore connector](../hive-metastore) with `mode: presto` to get storage lineage for Presto views backed by Hive.

#### View Definitions

- **Simple Views**: Fully supported with SQL extraction
- **Complex Presto Views**: Views with Presto-specific SQL functions may have limited lineage
- **Cross-Catalog Views**: Views referencing multiple catalogs are supported

#### Connector-Specific Tables

Presto's catalog connectors (Hive, PostgreSQL, etc.) may have different metadata available. The connector extracts common metadata that works across all connectors.

### Troubleshooting

1. **Information Schema Latency**: Presto's `information_schema` may have delays in reflecting recent DDL changes.

2. **Large Result Sets**: Catalogs with 10,000+ tables may be slow to ingest.

3. **View Lineage Parsing**: Complex Presto SQL with window functions, CTEs, or Presto-specific syntax may have incomplete lineage.

4. **Connector-Specific Metadata**: Some Presto connectors (e.g., Cassandra) have limited metadata available through `information_schema`.

#### Connection Issues

**Problem**: `Could not connect to Presto`

**Solutions**:

- Verify `host_port` is correct and points to the Presto coordinator
- Check firewall rules allow traffic on the Presto port
- Confirm Presto service is running: `curl http://<host>:<port>/v1/info`
- Check Presto logs for connection errors

#### Authentication Failures

**Problem**: `Authentication failed`

**Solutions**:

- Verify username and password are correct
- Check authentication method matches Presto configuration
- For Kerberos: Ensure valid ticket exists (`klist`)
- Review Presto coordinator logs: `/var/log/presto/`

#### Missing Catalogs or Tables

**Problem**: Not all catalogs/tables appear in DataHub

**Solutions**:

- Verify user has access to catalogs: `SHOW CATALOGS;` in Presto
- Check if catalogs are filtered by `database_pattern`
- Ensure catalog connectors are properly configured in Presto
- Review warnings in DataHub ingestion logs

#### Slow Ingestion

**Problem**: Metadata extraction takes too long

**Solutions**:

- Use catalog/schema filtering to reduce scope
- Disable profiling or limit to specific tables
- Enable stateful ingestion
- Ensure Presto cluster has adequate resources
- Check Presto query queue and resource groups

#### View Lineage Not Appearing

**Problem**: No lineage for Presto views

**Solutions**:

- Complex Presto SQL may have limited lineage extraction
- For Hive-backed views, consider using the [Hive Metastore connector](../hive-metastore) with `mode: presto`
- Review logs for SQL parsing warnings
- Simplify view definitions if possible

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
