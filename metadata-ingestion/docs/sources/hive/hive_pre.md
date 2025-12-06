### Prerequisites

The Hive source connects directly to the HiveServer2 service to extract metadata. Before configuring the DataHub connector, ensure you have:

1. **Network Access**: The machine running DataHub ingestion must be able to reach your HiveServer2 instance on the configured port (typically 10000 or 10001 for TLS).

2. **Hive User Account**: A Hive user account with appropriate permissions to read metadata from the databases and tables you want to ingest.

3. **PyHive Dependencies**: The connector uses PyHive for connectivity. Install the appropriate dependencies:
   ```bash
   pip install 'acryl-datahub[hive]'
   ```

### Required Permissions

The Hive user account used by DataHub needs the following permissions:

#### Minimum Permissions (Metadata Only)

```sql
-- Grant SELECT on all databases you want to ingest
GRANT SELECT ON DATABASE <database_name> TO USER <datahub_user>;

-- Grant SELECT on tables/views for schema extraction
GRANT SELECT ON TABLE <database_name>.* TO USER <datahub_user>;
```

#### Additional Permissions for Storage Lineage

If you plan to enable storage lineage, the connector needs to read table location information:

```sql
-- Grant DESCRIBE on tables to read storage locations
GRANT SELECT ON <database_name>.* TO USER <datahub_user>;
```

#### Recommendations

- **Read-only Access**: DataHub only needs read permissions. Never grant `INSERT`, `UPDATE`, `DELETE`, or `DROP` privileges.
- **Database Filtering**: If you only need to ingest specific databases, use the `database` config parameter to limit scope and reduce the permissions required.

### Authentication

The Hive connector supports multiple authentication methods through PyHive. Configure authentication using the recipe parameters described below.

#### Basic Authentication (Username/Password)

The simplest authentication method using a username and password:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    username: datahub_user
    password: ${HIVE_PASSWORD} # Use environment variables for sensitive data
```

#### LDAP Authentication

For LDAP-based authentication:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    username: datahub_user
    password: ${LDAP_PASSWORD}
    options:
      connect_args:
        auth: LDAP
```

#### Kerberos Authentication

For Kerberos-secured Hive clusters:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    options:
      connect_args:
        auth: KERBEROS
        kerberos_service_name: hive
```

**Requirements**:

- Valid Kerberos ticket (use `kinit` before running ingestion)
- Kerberos configuration file (`/etc/krb5.conf` or specified via `KRB5_CONFIG` environment variable)
- PyKerberos or requests-kerberos package installed

#### TLS/SSL Connection

For secure connections over HTTPS:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10001
    scheme: "hive+https"
    username: datahub_user
    password: ${HIVE_PASSWORD}
    options:
      connect_args:
        auth: BASIC
```

#### Azure HDInsight

For Microsoft Azure HDInsight clusters:

```yaml
source:
  type: hive
  config:
    host_port: <cluster_name>.azurehdinsight.net:443
    scheme: "hive+https"
    username: admin
    password: ${HDINSIGHT_PASSWORD}
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC
```

#### Databricks (via PyHive)

For Databricks clusters using the Hive connector:

```yaml
source:
  type: hive
  config:
    host_port: <workspace-url>:443
    scheme: "databricks+pyhive"
    username: token # or your Databricks username
    password: ${DATABRICKS_TOKEN} # Personal access token or password
    options:
      connect_args:
        http_path: "sql/protocolv1/o/xxxyyyzzzaaasa/1234-567890-hello123"
```

**Note**: For comprehensive Databricks support, consider using the dedicated [Databricks Unity Catalog](../databricks) connector instead, which provides enhanced features.

### Storage Lineage

DataHub can extract lineage between Hive tables and their underlying storage locations (S3, Azure Blob, HDFS, GCS, etc.). This feature creates relationships showing data flow from raw storage to Hive tables.

#### Quick Start

Enable storage lineage with minimal configuration:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    username: datahub_user
    password: ${HIVE_PASSWORD}

    # Enable storage lineage
    emit_storage_lineage: true
```

This will:

- Extract storage locations from Hive table metadata
- Create lineage from storage (S3, HDFS, etc.) to Hive tables
- Include column-level lineage by default

#### Configuration Options

Storage lineage behavior is controlled by four parameters:

| Parameter                        | Type    | Default      | Description                                                                 |
| -------------------------------- | ------- | ------------ | --------------------------------------------------------------------------- |
| `emit_storage_lineage`           | boolean | `false`      | Master toggle to enable/disable storage lineage                             |
| `hive_storage_lineage_direction` | string  | `"upstream"` | Direction: `"upstream"` (storage → Hive) or `"downstream"` (Hive → storage) |
| `include_column_lineage`         | boolean | `true`       | Enable column-level lineage from storage paths to Hive columns              |
| `storage_platform_instance`      | string  | `None`       | Platform instance for storage URNs (e.g., `"prod-s3"`, `"dev-hdfs"`)        |

#### Supported Storage Platforms

The connector automatically detects and creates lineage for:

- **Amazon S3**: `s3://`, `s3a://`, `s3n://`
- **HDFS**: `hdfs://`
- **Google Cloud Storage**: `gs://`
- **Azure Blob Storage**: `wasb://`, `wasbs://`
- **Azure Data Lake (Gen1)**: `adl://`
- **Azure Data Lake (Gen2)**: `abfs://`, `abfss://`
- **Databricks File System**: `dbfs://`
- **Local File System**: `file://` or absolute paths

#### Complete Documentation

See the sections above for detailed configuration examples, troubleshooting, and best practices.

### Platform Instances

When ingesting from multiple Hive environments (e.g., production, staging, development), use `platform_instance` to distinguish them:

```yaml
source:
  type: hive
  config:
    host_port: prod-hive.company.com:10000
    platform_instance: "prod-hive"
```

This creates URNs like:

```
urn:li:dataset:(urn:li:dataPlatform:hive,database.table,prod-hive)
```

**Best Practice**: Combine with `storage_platform_instance` for complete environment isolation:

```yaml
source:
  type: hive
  config:
    platform_instance: "prod-hive" # Hive environment
    storage_platform_instance: "prod-s3" # Storage environment
    emit_storage_lineage: true
```

### Performance Considerations

#### Large Hive Deployments

For Hive clusters with thousands of tables, consider:

1. **Database Filtering**: Limit ingestion to specific databases:

   ```yaml
   database: "production_db" # Only ingest one database
   ```

2. **Incremental Ingestion**: Use DataHub's stateful ingestion to only process changes:

   ```yaml
   stateful_ingestion:
     enabled: true
     remove_stale_metadata: true
   ```

3. **Disable Column Lineage**: If not needed, disable to improve performance:

   ```yaml
   emit_storage_lineage: true
   include_column_lineage: false # Faster ingestion
   ```

4. **Connection Pooling**: The connector uses a single connection by default. For better performance with large schemas, ensure your HiveServer2 has sufficient resources.

#### Network Latency

- If DataHub is running far from your Hive cluster, expect slower metadata extraction
- Consider running ingestion from a machine closer to your Hive infrastructure
- Use scheduled ingestion during off-peak hours for large deployments

### Caveats and Limitations

#### Hive Version Support

- **Supported Versions**: Hive 1.x, 2.x, and 3.x
- **HiveServer2 Required**: The connector connects to HiveServer2, not the metastore database directly
- For direct metastore access, use the [Hive Metastore](../hive-metastore) connector instead

#### View Definitions

- **Simple Views**: Fully supported with SQL lineage extraction
- **Complex Views**: Views with complex SQL (CTEs, subqueries) are supported via SQL parsing
- **Presto/Trino Views**: Not directly accessible via this connector. Use the [Hive Metastore](../hive-metastore) connector for Presto/Trino view support

#### Storage Lineage Limitations

- **Location Required**: Only tables with defined storage locations (`LOCATION` clause) will have storage lineage
- **External Tables**: Best supported (always have explicit locations)
- **Managed Tables**: Lineage created for default warehouse locations
- **Temporary Tables**: Not supported (no persistent storage location)

#### Partitioned Tables

- Partition information is extracted and included in schema metadata
- Partition-level storage lineage is aggregated at the table level
- Individual partition lineage is not currently supported

#### Authentication Limitations

- **No Proxy Support**: Direct connection to HiveServer2 required
- **Token-Based Auth**: Not natively supported (use Kerberos or basic auth)
- **Multi-Factor Authentication**: Not supported

#### Known Issues

1. **Session Timeout**: Long-running ingestion may hit HiveServer2 session timeouts. Configure `hive.server2.session.timeout` appropriately on the Hive side.

2. **Large Schemas**: Tables with 1000+ columns may be slow to ingest due to schema extraction overhead.

3. **Case Sensitivity**:
   - Hive is case-insensitive by default
   - DataHub automatically lowercases URNs for consistency
4. **View Lineage Parsing**: Complex views using non-standard SQL may not have complete lineage extracted.

### Troubleshooting

#### Connection Issues

**Problem**: `Could not connect to HiveServer2`

**Solutions**:

- Verify `host_port` is correct and accessible
- Check firewall rules allow traffic on the Hive port
- Confirm HiveServer2 service is running: `beeline -u jdbc:hive2://<host>:<port>`

#### Authentication Failures

**Problem**: `Authentication failed`

**Solutions**:

- Verify username and password are correct
- Check authentication method matches your Hive configuration
- For Kerberos: Ensure valid ticket exists (`klist`)
- Review HiveServer2 logs for detailed error messages

#### Missing Tables

**Problem**: Not all tables appear in DataHub

**Solutions**:

- Verify user has SELECT permissions on missing tables
- Check if tables are in filtered databases
- Review warnings in ingestion logs
- Ensure tables are not temporary or views with complex definitions

#### Storage Lineage Not Appearing

**Problem**: No storage lineage relationships visible

**Solutions**:

- Verify `emit_storage_lineage: true` is set
- Check tables have defined storage locations: `DESCRIBE FORMATTED <table>`
- Review logs for "Failed to parse storage location" warnings
- See the "Storage Lineage" section above for more troubleshooting tips

### Related Documentation

- [Hive Source Configuration](hive_recipe.yml) - Configuration examples
- [Hive Metastore Connector](../hive-metastore) - Alternative connector for direct metastore access
- [PyHive Documentation](https://github.com/dropbox/PyHive) - Underlying connection library
