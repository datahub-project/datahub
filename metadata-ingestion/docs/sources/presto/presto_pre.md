### Prerequisites

The Presto source connects directly to your Presto cluster via SQL to extract metadata about tables, views, schemas, and catalogs.

Before configuring the DataHub connector, ensure you have:

1. **Network Access**: The machine running DataHub ingestion must be able to reach your Presto coordinator on the configured port (typically 8080 or 443 for HTTPS).

2. **Presto User Account**: A Presto user with appropriate permissions to query metadata.

3. **PyHive Dependencies**: The connector uses PyHive for connectivity. Install the appropriate dependencies:
   ```bash
   pip install 'acryl-datahub[presto]'
   ```

### Important: Presto vs. Presto-on-Hive

There are **two different ways** to ingest Presto metadata into DataHub, depending on your use case:

#### Option 1: Presto Connector (This Source)

**Use when**: You want to connect directly to Presto to extract metadata from **all catalogs** (not just Hive).

**Capabilities**:

- Extracts tables and views from all Presto catalogs (Hive, PostgreSQL, MySQL, Cassandra, etc.)
- Supports table and view metadata
- Supports data profiling
- Extracts view SQL definitions
- **Does NOT support storage lineage** (no access to underlying storage locations)
- Limited view lineage for complex Presto-specific SQL

**Configuration**:

```yaml
source:
  type: presto # ← This connector
  config:
    host_port: presto-coordinator.company.com:8080
    username: datahub_user
    password: ${PRESTO_PASSWORD}
```

#### Option 2: Hive Metastore Connector with Presto Mode

**Use when**: You want to ingest **Presto views that use the Hive metastore** and need storage lineage.

**Capabilities**:

- Extracts Presto views stored in Hive metastore
- **Supports storage lineage** from S3/HDFS/Azure to Hive tables to Presto views
- Better Presto view definition parsing
- Column-level lineage support
- Faster metadata extraction (direct database access)
- Only works with Hive-backed catalogs

**Configuration**:

```yaml
source:
  type: hive-metastore # ← Use this for storage lineage
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    scheme: "postgresql+psycopg2"
    mode: presto # ← Set mode to 'presto'

    # Enable storage lineage
    emit_storage_lineage: true
    hive_storage_lineage_direction: upstream
```

**For complete details**, see:

- [Hive Metastore Connector Documentation](../hive-metastore)

### Required Permissions

The Presto user account used by DataHub needs minimal permissions:

```sql
-- Presto uses catalog-level permissions
-- The user needs SELECT access to system information tables
-- This is typically granted by default to all users
```

**Recommendation**: Use a read-only service account with access to all catalogs you want to ingest.

### Authentication

#### Basic Authentication (Username/Password)

The most common authentication method:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user
    password: ${PRESTO_PASSWORD}
    database: hive # Optional: default catalog
```

#### LDAP Authentication

For LDAP-based authentication:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user
    password: ${LDAP_PASSWORD}
    database: hive
```

#### HTTPS/TLS Connection

For secure connections:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:443
    username: datahub_user
    password: ${PRESTO_PASSWORD}
    database: hive
    options:
      connect_args:
        protocol: https
```

#### Kerberos Authentication

For Kerberos-secured Presto clusters:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    database: hive
    options:
      connect_args:
        auth: KERBEROS
        kerberos_service_name: presto
```

**Requirements**:

- Valid Kerberos ticket (use `kinit` before running ingestion)
- PyKerberos package installed

### Catalog and Schema Filtering

Presto can connect to many different catalogs (Hive, PostgreSQL, MySQL, etc.). Use filtering to control what gets ingested:

#### Catalog Filtering

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

#### Schema Filtering

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

#### Table Filtering

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

### Platform Instances

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

### Data Profiling

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

### Performance Considerations

#### Large Presto Deployments

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

#### Query Performance

- The connector queries Presto's `information_schema` tables
- Ensure your Presto cluster has sufficient resources
- Consider running ingestion during off-peak hours for large deployments

### Caveats and Limitations

#### Storage Lineage

**Not Supported**: The Presto connector cannot extract storage lineage because it doesn't have access to underlying storage locations.

**Solution**: Use the [Hive Metastore connector](../hive-metastore) with `mode: presto` to get storage lineage for Presto views backed by Hive.

#### View Definitions

- **Simple Views**: Fully supported with SQL extraction
- **Complex Presto Views**: Views with Presto-specific SQL functions may have limited lineage
- **Cross-Catalog Views**: Views referencing multiple catalogs are supported

#### Connector-Specific Tables

Presto's catalog connectors (Hive, PostgreSQL, etc.) may have different metadata available. The connector extracts common metadata that works across all connectors.

#### Known Issues

1. **Information Schema Latency**: Presto's `information_schema` may have delays in reflecting recent DDL changes.

2. **Large Result Sets**: Catalogs with 10,000+ tables may be slow to ingest.

3. **View Lineage Parsing**: Complex Presto SQL with window functions, CTEs, or Presto-specific syntax may have incomplete lineage.

4. **Connector-Specific Metadata**: Some Presto connectors (e.g., Cassandra) have limited metadata available through `information_schema`.

### Troubleshooting

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

### Migration from presto-on-hive

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

### Comparison: Presto vs. Hive Metastore Connector

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

### Best Practices

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

### Related Documentation

- [Presto Configuration Examples](presto_recipe.yml)
- [Hive Metastore Connector](../hive-metastore) - For Presto-on-Hive with storage lineage
- [Trino Connector](../trino) - Similar connector for Trino (Presto's successor)
- [PyHive Documentation](https://github.com/dropbox/PyHive) - Underlying connection library
