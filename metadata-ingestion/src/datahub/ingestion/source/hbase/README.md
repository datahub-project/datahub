# HBase Source

## Overview

The HBase source connector extracts metadata from Apache HBase clusters through the Thrift API using the `happybase` Python library.

## Supported Features

- **Namespaces**: Discovered as containers
- **Tables**: Extracted with full metadata
- **Column Families**: Schema extraction with column family details
- **Table Properties**: Compression, versions, TTL, and other configurations
- **Filtering**: Namespace and table patterns for selective ingestion
- **Stateful Ingestion**: Automatic detection of deleted entities

## Installation

The HBase source requires the `happybase` library:

```bash
pip install 'acryl-datahub[hbase]'
# or
pip install happybase
```

## Prerequisites

1. **HBase Thrift Server**: Must be running and accessible
2. **Network Access**: Connector must be able to reach the Thrift server (default port: 9090)
3. **Permissions**: Read access to HBase tables and metadata

### Starting HBase Thrift Server

If the Thrift server is not already running:

```bash
# HBase 2.x
hbase thrift start -p 9090

# Or as a daemon
hbase-daemon.sh start thrift
```

## Configuration

### Basic Configuration

```yaml
source:
  type: hbase
  config:
    host: localhost
    port: 9090
```

### Full Configuration

```yaml
source:
  type: hbase
  config:
    # Required
    host: hbase.example.com

    # Optional
    port: 9090 # Default: 9090
    use_ssl: false # Default: false
    timeout: 30000 # Connection timeout in milliseconds, default: 30000

    # Filtering
    namespace_pattern:
      allow:
        - "prod_.*"
        - "staging"
      deny:
        - ".*_test"

    table_pattern:
      allow:
        - ".*"
      deny:
        - ".*_temp"
        - ".*_backup"

    # Schema extraction
    include_column_families: true # Default: true

    # DataHub configuration
    env: PROD # Default: PROD
    platform_instance: hbase-cluster-1 # Optional

    # Stateful ingestion
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
```

## Configuration Options

| Option                    | Type    | Required | Default   | Description                            |
| ------------------------- | ------- | -------- | --------- | -------------------------------------- |
| `host`                    | string  | ✅       | -         | HBase Thrift server hostname or IP     |
| `port`                    | integer | ❌       | 9090      | HBase Thrift server port               |
| `use_ssl`                 | boolean | ❌       | false     | Use SSL/TLS for connection             |
| `timeout`                 | integer | ❌       | 30000     | Connection timeout in milliseconds     |
| `namespace_pattern`       | object  | ❌       | allow all | Regex patterns for namespace filtering |
| `table_pattern`           | object  | ❌       | allow all | Regex patterns for table filtering     |
| `include_column_families` | boolean | ❌       | true      | Include column families in schema      |
| `env`                     | string  | ❌       | PROD      | Environment for URN construction       |
| `platform_instance`       | string  | ❌       | None      | Platform instance identifier           |

## Extracted Metadata

### Namespaces

- Container entities representing HBase namespaces
- Default namespace for tables without explicit namespace
- Namespace descriptions and properties

### Tables

- Dataset entities for each HBase table
- Qualified names with namespace prefix (e.g., `namespace:table`)
- Display names and descriptions

### Schema

- **Row Key**: Always included as the primary key field
- **Column Families**: Extracted as schema fields when `include_column_families: true`
- Field types and nullability information

### Properties

For each column family:

- `maxVersions`: Maximum number of cell versions
- `compression`: Compression algorithm (NONE, SNAPPY, GZ, LZO, etc.)
- `inMemory`: Whether data is kept in memory
- `blockCacheEnabled`: Whether block cache is enabled
- `timeToLive`: TTL setting

## Examples

### Example 1: Basic Ingestion

```yaml
source:
  type: hbase
  config:
    host: localhost
    port: 9090

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

### Example 2: Production Environment with Filtering

```yaml
source:
  type: hbase
  config:
    host: hbase-prod.company.com
    port: 9090
    use_ssl: true
    timeout: 60000

    namespace_pattern:
      allow:
        - "prod_.*"
      deny:
        - "prod_test_.*"

    table_pattern:
      deny:
        - ".*_backup$"
        - ".*_temp$"

    env: PROD
    platform_instance: prod-cluster-1

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: https://datahub.company.com
```

### Example 3: Multiple Clusters

Ingest from multiple HBase clusters by creating separate recipe files:

**cluster1-recipe.yml:**

```yaml
source:
  type: hbase
  config:
    host: hbase-cluster1.company.com
    port: 9090
    platform_instance: cluster1
```

**cluster2-recipe.yml:**

```yaml
source:
  type: hbase
  config:
    host: hbase-cluster2.company.com
    port: 9090
    platform_instance: cluster2
```

## Generated URNs

- **Namespace Container**: `urn:li:container:<hash>` (based on namespace name)
- **Table Dataset**: `urn:li:dataset:(urn:li:dataPlatform:hbase,<namespace>.<table>,<env>)`

For tables in the default namespace:

- Name: `table_name`
- Qualified Name: `table_name`

For tables in named namespaces:

- Name: `namespace.table_name`
- Qualified Name: `namespace:table_name`

## Limitations

1. **Column Qualifiers**: Individual column qualifiers are not extracted (only column families)
2. **Data Sampling**: No data profiling or sampling is performed
3. **Table Relationships**: Foreign key relationships are not extracted
4. **Row Count**: Table row counts are not extracted
5. **Authentication**: Currently supports unauthenticated connections only

## Troubleshooting

### Connection Timeout

**Error**: `Failed to connect to HBase: Connection timed out`

**Solutions**:

- Verify HBase Thrift server is running: `netstat -an | grep 9090`
- Check network connectivity: `telnet hbase-host 9090`
- Increase timeout: `timeout: 60000`
- Verify firewall rules allow connection

### Import Error

**Error**: `Failed to import happybase library`

**Solution**:

```bash
pip install happybase
```

### Empty Results

**Issue**: No tables are ingested

**Solutions**:

- Check namespace/table patterns are not too restrictive
- Verify tables exist: `echo "list" | hbase shell`
- Check HBase permissions
- Review ingestion report for dropped entities

### Schema Not Extracted

**Issue**: Tables ingested without schema information

**Solution**:

- Ensure `include_column_families: true` (default)
- Verify column families exist on tables
- Check for errors in ingestion report

## Performance Considerations

1. **Large Clusters**: For clusters with many tables, use filtering to reduce ingestion time
2. **Network Latency**: Higher latency networks may need increased timeout values
3. **Concurrent Access**: Multiple ingestions can run in parallel using different platform instances

## Testing

### Unit Tests

```bash
pytest tests/unit/test_hbase_config.py -v
pytest tests/unit/test_hbase_source.py -v
```

### Integration Tests

Requires Docker:

```bash
cd tests/integration/hbase
docker-compose up -d
pytest test_hbase_integration.py -v
docker-compose down -v
```

See `tests/integration/hbase/README.md` for detailed testing instructions.

## Questions & Support

For questions or issues:

1. Check the [DataHub documentation](https://datahubproject.io/)
2. Search [existing issues](https://github.com/datahub-project/datahub/issues)
3. Open a new issue with the `hbase-source` label

## Related Documentation

- [HBase Official Documentation](https://hbase.apache.org/)
- [HBase Thrift API](https://hbase.apache.org/book.html#thrift)
- [happybase Documentation](https://happybase.readthedocs.io/)
- [DataHub Sources Overview](https://datahubproject.io/docs/metadata-ingestion/)
