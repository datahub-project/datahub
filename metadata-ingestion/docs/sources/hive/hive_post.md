### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Storage Lineage

DataHub can extract lineage between Hive tables and their underlying storage locations (S3, Azure Blob, HDFS, GCS, etc.). This feature creates relationships showing data flow from raw storage to Hive tables.

##### Quick Start

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

##### Configuration Options

Storage lineage behavior is controlled by four parameters:

| Parameter                        | Type    | Default      | Description                                                                 |
| -------------------------------- | ------- | ------------ | --------------------------------------------------------------------------- |
| `emit_storage_lineage`           | boolean | `false`      | Master toggle to enable/disable storage lineage                             |
| `hive_storage_lineage_direction` | string  | `"upstream"` | Direction: `"upstream"` (storage → Hive) or `"downstream"` (Hive → storage) |
| `include_column_lineage`         | boolean | `true`       | Enable column-level lineage from storage paths to Hive columns              |
| `storage_platform_instance`      | string  | `None`       | Platform instance for storage URNs (e.g., `"prod-s3"`, `"dev-hdfs"`)        |

##### Supported Storage Platforms

The connector automatically detects and creates lineage for:

- **Amazon S3**: `s3://`, `s3a://`, `s3n://`
- **HDFS**: `hdfs://`
- **Google Cloud Storage**: `gs://`
- **Azure Blob Storage**: `wasb://`, `wasbs://`
- **Azure Data Lake (Gen1)**: `adl://`
- **Azure Data Lake (Gen2)**: `abfs://`, `abfss://`
- **Databricks File System**: `dbfs://`
- **Local File System**: `file://` or absolute paths

#### Platform Instances

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

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

### Troubleshooting

1. **Session Timeout**: Long-running ingestion may hit HiveServer2 session timeouts. Configure `hive.server2.session.timeout` appropriately on the Hive side.

2. **Large Schemas**: Tables with 1000+ columns may be slow to ingest due to schema extraction overhead.

3. **Case Sensitivity**:
   - Hive is case-insensitive by default
   - DataHub automatically lowercases URNs for consistency
4. **View Lineage Parsing**: Complex views using non-standard SQL may not have complete lineage extracted.
