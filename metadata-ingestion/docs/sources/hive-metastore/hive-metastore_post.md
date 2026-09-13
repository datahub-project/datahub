### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Thrift Connection Mode

Use `connection_type: thrift` when you cannot access the metastore database directly but have access to the HMS Thrift API (typically port 9083). This is common in:

- Kerberized Hadoop clusters where database access is restricted
- Cloud-managed Hive services that only expose the Thrift API
- Environments with strict network segmentation

##### Thrift Mode Prerequisites

Before using Thrift mode, ensure:

1. **Network Access**: The machine running DataHub ingestion can reach HMS on port 9083
2. **HMS Service Running**: The Hive Metastore service is running and accepting Thrift connections
3. **For Kerberos**: A valid Kerberos ticket is available (see Kerberos section below)

**Verify connectivity**:

```bash
# Test network connectivity to HMS
telnet hms.company.com 9083

# For Kerberos environments, verify ticket
klist
```

##### Thrift Mode Dependencies

```bash
# Install with Thrift support
pip install 'acryl-datahub[hive-metastore]'

# For Kerberos authentication, also install:
pip install thrift-sasl pyhive[hive-pure-sasl]
```

##### Thrift Configuration Options

| Option                        | Type      | Default | Required         | Description                                                                    |
| ----------------------------- | --------- | ------- | ---------------- | ------------------------------------------------------------------------------ |
| `connection_type`             | string    | `sql`   | Yes (for Thrift) | Set to `thrift` to enable Thrift mode                                          |
| `host_port`                   | string    | -       | Yes              | HMS host and port (e.g., `hms.company.com:9083`)                               |
| `use_kerberos`                | boolean   | `false` | No               | Enable Kerberos/SASL authentication                                            |
| `kerberos_service_name`       | string    | `hive`  | No               | Kerberos service principal name                                                |
| `kerberos_hostname_override`  | string    | -       | No               | Override hostname for Kerberos principal (for load balancers)                  |
| `kerberos_qop`                | string    | `auth`  | No               | Kerberos Quality of Protection: `auth`, `auth-int`, or `auth-conf` (see below) |
| `timeout_seconds`             | int       | `60`    | No               | Connection timeout in seconds                                                  |
| `max_retries`                 | int       | `3`     | No               | Maximum retry attempts for transient failures                                  |
| `catalog_name`                | string    | -       | No               | HMS 3.x catalog name (e.g., `spark_catalog`)                                   |
| `include_catalog_name_in_ids` | boolean   | `false` | No               | Include catalog in dataset URNs                                                |
| `database_pattern`            | AllowDeny | -       | No               | Filter databases by regex pattern                                              |
| `table_pattern`               | AllowDeny | -       | No               | Filter tables by regex pattern                                                 |

**Note**: SQL `WHERE` clause options (`tables_where_clause_suffix`, `views_where_clause_suffix`, `schemas_where_clause_suffix`) have been **deprecated** for security reasons (SQL injection risk) and are no longer supported. Use `database_pattern` and `table_pattern` instead.

##### Basic Thrift Configuration

```yaml
source:
  type: hive-metastore
  config:
    connection_type: thrift
    host_port: hms.company.com:9083
```

##### Thrift with Kerberos Authentication

Ensure you have a valid Kerberos ticket (`kinit -kt /path/to/keytab user@REALM`) before running ingestion:

```yaml
source:
  type: hive-metastore
  config:
    connection_type: thrift
    host_port: hms.company.com:9083
    use_kerberos: true
    kerberos_service_name: hive # Change if HMS uses different principal
    # kerberos_hostname_override: hms-internal.company.com  # If using load balancer
    # catalog_name: spark_catalog  # For HMS 3.x multi-catalog
    # kerberos_qop: auth-conf # For Kerberos QOP authentication + integrity + encryption
    database_pattern:
      allow:
        - "^prod_.*"
```

##### Kerberos Quality of Protection (QOP)

If your Hive Metastore is configured with `hadoop.rpc.protection` set to `integrity` or `privacy`, you must configure the matching QOP level:

| `hadoop.rpc.protection` | `kerberos_qop` | Description                             |
| ----------------------- | -------------- | --------------------------------------- |
| `authentication`        | `auth`         | Authentication only (default)           |
| `integrity`             | `auth-int`     | Authentication + integrity checking     |
| `privacy`               | `auth-conf`    | Authentication + integrity + encryption |

##### Thrift Mode Limitations

- **No Presto/Trino view lineage**: View SQL parsing requires SQL mode
- **No WHERE clause filtering**: Use `database_pattern`/`table_pattern` instead
- **Kerberos ticket required**: Must have valid ticket before running (not embedded in config)
- **HMS version compatibility**: Tested with HMS 2.x and 3.x

##### Kerberos with Remote Executor

When running Hive Metastore ingestion on a **DataHub Cloud Remote Executor**, `kinit` is not run
automatically. The executor Docker image ships Kerberos client tools (`krb5`, `kinit`, `klist`,
`cyrus-sasl-mit`), but you must supply a keytab and configure an init container to obtain a ticket
before ingestion starts.

> **Note**: The executor's `extraSidecars` field is defined in `values.yaml` but is not rendered in
> any executor deployment template. Do not attempt sidecar-based ticket renewal — use the init
> container approach below.

**Step 1 — Create Kubernetes resources:**

```bash
# Secret for the keytab
kubectl create secret generic executor-krb5-keytab \
  --from-file=user.keytab=/path/to/user.keytab \
  --namespace <executor-namespace>

# ConfigMap for krb5.conf
kubectl create configmap executor-krb5-config \
  --from-file=krb5.conf=/etc/krb5.conf \
  --namespace <executor-namespace>
```

**Step 2 — Helm values override:**

```yaml
extraVolumes:
  - name: krb5-keytab
    secret:
      secretName: executor-krb5-keytab
      defaultMode: 0400
  - name: krb5-config
    configMap:
      name: executor-krb5-config
  - name: krb5-ccache
    emptyDir: {}

extraInitContainers:
  - name: kerberos-init
    image: <your-executor-image>
    command: ["/bin/bash", "-c"]
    args:
      - |
        kinit -kt /etc/kerberos/keytab/user.keytab user@YOUR.REALM
        klist # Fail fast if kinit failed
    env:
      - name: KRB5CCNAME
        value: FILE:/tmp/krb5cc/krb5cc_default
      - name: KRB5_CONFIG
        value: /etc/kerberos/krb5.conf
    volumeMounts:
      - name: krb5-keytab
        mountPath: /etc/kerberos/keytab
        readOnly: true
      - name: krb5-config
        mountPath: /etc/kerberos/krb5.conf
        subPath: krb5.conf
        readOnly: true
      - name: krb5-ccache
        mountPath: /tmp/krb5cc

extraVolumeMounts:
  - name: krb5-ccache
    mountPath: /tmp/krb5cc

extraEnvs:
  - name: KRB5CCNAME
    value: FILE:/tmp/krb5cc/krb5cc_default
  - name: KRB5_CONFIG
    value: /etc/kerberos/krb5.conf
```

**Step 3 — Ingestion recipe:**

```yaml
source:
  type: hive-metastore
  config:
    connection_type: thrift
    host_port: hms.company.com:9083
    use_kerberos: true
    kerberos_service_name: hive
    kerberos_qop: auth # or auth-int / auth-conf to match hadoop.rpc.protection
```

**TGT lifetime**: Kerberos TGTs typically expire after 10 hours, which is sufficient for most
ingestion runs. For long-running ingestions, coordinate with your KDC administrator to issue a
renewable ticket or extend the maximum ticket lifetime.

#### Storage Lineage

The Hive Metastore connector supports the same storage lineage features as the Hive connector, with enhanced performance due to direct database access.

##### Quick Start

Enable storage lineage with minimal configuration:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "postgresql+psycopg2"

    # Enable storage lineage
    emit_storage_lineage: true
```

##### Configuration Options

Storage lineage is controlled by the same parameters as the Hive connector:

| Parameter                        | Type    | Default      | Description                                                                 |
| -------------------------------- | ------- | ------------ | --------------------------------------------------------------------------- |
| `emit_storage_lineage`           | boolean | `false`      | Master toggle to enable/disable storage lineage                             |
| `hive_storage_lineage_direction` | string  | `"upstream"` | Direction: `"upstream"` (storage → Hive) or `"downstream"` (Hive → storage) |
| `include_column_lineage`         | boolean | `true`       | Enable column-level lineage from storage paths to Hive columns              |
| `storage_platform_instance`      | string  | `None`       | Platform instance for storage URNs (e.g., `"prod-s3"`, `"dev-hdfs"`)        |

##### Supported Storage Platforms

All storage platforms supported by the Hive connector are also supported here:

- Amazon S3 (`s3://`, `s3a://`, `s3n://`)
- HDFS (`hdfs://`)
- Google Cloud Storage (`gs://`)
- Azure Blob Storage (`wasb://`, `wasbs://`)
- Azure Data Lake (`adl://`, `abfs://`, `abfss://`)
- Databricks File System (`dbfs://`)
- Local File System (`file://`)

See the sections above for complete configuration details.

#### Presto and Trino View Support

A key advantage of the Hive Metastore connector is its ability to extract metadata from Presto and Trino views that are stored in the metastore.

##### How It Works

1. **View Detection**: The connector identifies views by checking the `TABLE_PARAMS` table for Presto/Trino view definitions.

2. **View Parsing**: Presto/Trino view JSON is parsed to extract:

   - Original SQL text
   - Referenced tables
   - Column metadata and types

3. **Lineage Extraction**: SQL is parsed using `sqlglot` to create table-to-view lineage.

4. **Storage Lineage Integration**: If storage lineage is enabled, the connector also creates lineage from storage → tables → views.

##### Configuration

Presto/Trino view support is automatically enabled when ingesting from a metastore that contains Presto/Trino views. No additional configuration is required.

##### Example

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "postgresql+psycopg2"

    # Enable storage lineage for complete lineage chain
    emit_storage_lineage: true
```

This configuration will create complete lineage:

```
S3 Bucket → Hive Table → Presto View
```

##### Limitations

- **Presto/Trino Version**: The connector supports Presto 0.200+ and Trino view formats
- **Complex SQL**: Very complex SQL with non-standard syntax may have incomplete lineage
- **Cross-Database References**: Lineage is extracted for references within the same Hive metastore

#### Schema Filtering

For large metastore deployments with many databases, use filtering to limit ingestion scope:

##### Database Filtering

```yaml
source:
  type: hive-metastore
  config:
    # ... connection config ...

    # Only ingest from specific databases
    schema_pattern:
      allow:
        - "^production_.*" # All databases starting with production_
        - "analytics" # Specific database
      deny:
        - ".*_test$" # Exclude test databases
```

##### Table Filtering with SQL

For filtering by database name, use pattern-based filtering:

```yaml
source:
  type: hive-metastore
  config:
    # ... connection config ...

    # Filter to specific databases using regex patterns
    database_pattern:
      allow:
        - "^production_db$"
        - "^analytics_db$"
      deny:
        - "^test_.*"
        - ".*_staging$"
```

**Note**: The deprecated `*_where_clause_suffix` options have been removed for security reasons. Use `database_pattern` and `table_pattern` for filtering.

#### Performance Considerations

##### Advantages Over HiveServer2 Connector

The Hive Metastore connector is significantly faster than the Hive connector because:

1. **Direct Database Access**: No HiveServer2 overhead
2. **Batch Queries**: Fetches all metadata in optimized SQL queries
3. **No Query Execution**: Doesn't run Hive queries to extract metadata
4. **Parallel Processing**: Can process multiple databases concurrently

**Performance Comparison** (approximate):

- **10 databases, 1000 tables**: ~2 minutes (Metastore) vs ~15 minutes (HiveServer2)
- **100 databases, 10,000 tables**: ~15 minutes (Metastore) vs ~2 hours (HiveServer2)

##### Optimization Tips

1. **Database Connection Pooling**: The connector uses SQLAlchemy's default connection pooling. For very large deployments, consider tuning pool size:

   ```yaml
   options:
     pool_size: 10
     max_overflow: 20
   ```

2. **Schema Filtering**: Use `schema_pattern` to limit scope and reduce query time.

3. **Stateful Ingestion**: Enable to only process changes:

   ```yaml
   stateful_ingestion:
     enabled: true
     remove_stale_metadata: true
   ```

4. **Disable Column Lineage**: If not needed:
   ```yaml
   emit_storage_lineage: true
   include_column_lineage: false # Faster
   ```

##### Network Considerations

- **Latency**: Low latency to the metastore database is important
- **Bandwidth**: Minimal bandwidth required (only metadata, no data transfer)
- **Connection Limits**: Ensure metastore database can handle additional read connections

#### Platform Instances

When ingesting from multiple metastores (e.g., different clusters or environments), use `platform_instance`:

```yaml
source:
  type: hive-metastore
  config:
    host_port: prod-metastore-db.company.com:5432
    database: metastore
    platform_instance: "prod-hive"
```

**Best Practice**: Combine with `storage_platform_instance`:

```yaml
source:
  type: hive-metastore
  config:
    platform_instance: "prod-hive" # Hive tables
    storage_platform_instance: "prod-hdfs" # Storage locations
    emit_storage_lineage: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

##### Metastore Schema Compatibility

- **Hive Versions**: Tested with Hive 1.x, 2.x, and 3.x metastore schemas
- **Schema Variations**: Different Hive versions may have slightly different metastore schemas
- **Custom Tables**: If your organization has added custom metastore tables, they won't be processed

##### Database Support

- **Supported**: PostgreSQL, MySQL, MariaDB
- **Not Supported**: Oracle, MSSQL (may work but untested)
- **Derby**: Not recommended (embedded metastore, typically single-user)

##### View Lineage Parsing

- **Simple SQL**: Fully supported with accurate lineage
- **Complex SQL**: Best-effort parsing; some edge cases may have incomplete lineage
- **Non-standard SQL**: Presto/Trino-specific functions may not be fully parsed

##### Permissions Limitations

- **Read-Only**: The connector only needs SELECT permissions
- **No Write Operations**: Never requires INSERT, UPDATE, or DELETE
- **Metastore Locks**: Read operations don't acquire metastore locks

##### Storage Lineage Limitations

Same as the Hive connector:

- Only tables with defined storage locations have lineage
- Temporary tables are not supported
- Partition-level lineage is aggregated at table level

### Troubleshooting

1. **Large Column Lists**: Tables with 500+ columns may be slow to process due to metastore query complexity.

2. **View Definition Encoding**: Some older Hive versions store view definitions in non-UTF-8 encoding, which may cause parsing issues.

3. **Case Sensitivity**:

   - PostgreSQL metastore: Case-sensitive identifiers
   - MySQL metastore: Case-insensitive by default
   - DataHub automatically lowercases URNs for consistency

4. **Concurrent Metastore Writes**: If the metastore is being actively modified during ingestion, some metadata may be inconsistent.

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

-
- Review ingestion logs for parsing errors
- Verify view JSON is valid

#### Storage Lineage Not Appearing

**Problem**: No storage lineage relationships visible

**Solutions**:

- Verify `emit_storage_lineage: true` is set
- Check tables have storage locations in metastore:
-

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

#### Kerberos Ticket Not Found (Remote Executor)

**Problem**: Ingestion fails with `GSS initiate failed`, `No Kerberos credentials available`, or
`TTransportException` when using Thrift + Kerberos on a Remote Executor.

**Solutions**:

- **Check init container logs**: `kubectl logs <pod> -c kerberos-init` — look for `kinit` errors
  such as bad keytab path, wrong principal, or KDC unreachable.
- **Verify `KRB5CCNAME`**: Both the init container and the main executor container must set
  `KRB5CCNAME=FILE:/tmp/krb5cc/krb5cc_default` (paths must match exactly).
- **Verify emptyDir mounts**: Confirm the `krb5-ccache` volume is mounted at the same `mountPath`
  in both `extraInitContainers.volumeMounts` and `extraVolumeMounts`.
- **Verify keytab secret**: Run `kubectl get secret executor-krb5-keytab -o yaml` and confirm the
  `user.keytab` key is present.
- **Run `klist`**: Add `klist` as the last command in the init container `args` — it exits non-zero
  if the ticket cache is empty, causing the pod to fail fast rather than silently.
- **Check `kerberos_qop`**: Ensure the `kerberos_qop` value in your recipe matches
  `hadoop.rpc.protection` on the HMS server (see the QOP table in the Thrift with Kerberos section
  above).
