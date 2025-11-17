# Iceberg REST Catalog Sink

The Iceberg REST sink allows you to write DataHub metadata to an Apache Iceberg REST Catalog. This enables you to:
- Store DataHub metadata in Iceberg tables for SQL querying
- Archive and backup metadata in a standard data lakehouse format
- Integrate DataHub metadata with your existing Iceberg-based analytics infrastructure
- Query metadata using Spark, DuckDB, or other Iceberg-compatible engines

## Installation

To use the Iceberg REST sink, install the `iceberg-rest-sink` extra:

```bash
pip install 'acryl-datahub[iceberg-rest-sink]'
```

This installs PyIceberg and all required dependencies, including PyArrow.

## Configuration

The sink stores all DataHub metadata aspects in a single Iceberg table with the following schema:

| Column          | Type      | Description                                              |
|-----------------|-----------|----------------------------------------------------------|
| urn             | string    | Entity URN (e.g., dataset, dashboard URN)                |
| entity_type     | string    | Entity type extracted from URN (e.g., dataset, chart, dashboard) |
| aspect          | string    | Name of the aspect (e.g., datasetProperties, ownership)  |
| metadata        | string    | JSON-serialized aspect value (the actual metadata)       |
| systemmetadata  | string    | JSON-serialized system metadata (optional)               |
| version         | long      | Aspect version (0 = latest version)                      |
| createdon       | timestamp | Timestamp when the record was written                    |

The table is partitioned by `entity_type` for efficient querying by entity type (e.g., all datasets, all charts).

### Configuration Options

| Field                        | Type    | Required | Default                  | Description                                                                 |
|------------------------------|---------|----------|--------------------------|-----------------------------------------------------------------------------|
| uri                          | string  | ✅       |                          | Iceberg REST Catalog endpoint URL                                           |
| warehouse                    | string  | ✅       |                          | Warehouse name in the catalog                                               |
| namespace                    | string  |          | `datahub_metadata`       | Namespace to store metadata tables                                          |
| table_name                   | string  |          | `metadata_aspects_v2`    | Name of the metadata table                                                  |
| token                        | string  |          |                          | Authentication token (e.g., DataHub Personal Access Token)                  |
| aws_role_arn                 | string  |          |                          | AWS role ARN for vended credentials                                         |
| s3.region                    | string  |          |                          | AWS S3 region                                                               |
| s3.access-key-id             | string  |          |                          | AWS S3 access key ID                                                        |
| s3.secret-access-key         | string  |          |                          | AWS S3 secret access key                                                    |
| s3.endpoint                  | string  |          |                          | S3 endpoint (for MinIO or custom S3-compatible storage)                     |
| create_table_if_not_exists   | boolean |          | `true`                   | Create table and namespace if they don't exist                              |
| verify_warehouse             | boolean |          | `true`                   | Verify warehouse exists before writing (requires pre-created warehouse)     |
| batch_size                   | integer |          | `10000`                  | Number of records to batch before writing (10k-50k recommended)             |
| connection.timeout           | integer |          | `120`                    | Connection timeout in seconds                                               |
| connection.retry.total       | integer |          | `3`                      | Total number of retry attempts                                              |
| connection.retry.backoff_factor | float |       | `0.1`                    | Backoff factor for retries                                                  |

## Warehouse Setup

### DataHub Catalogs

When using DataHub's Iceberg catalog (DataHub Cloud or self-hosted), the warehouse **must be created before running the sink**:

```bash
# Create a warehouse in DataHub
datahub iceberg create \
  -w datahub_metadata_warehouse \
  -d s3://my-bucket/warehouse-root \
  -i ${AWS_ACCESS_KEY_ID} \
  --client_secret ${AWS_SECRET_ACCESS_KEY} \
  --region us-east-1 \
  --role arn:aws:iam::123456789012:role/DataHubIcebergRole
```

The sink will automatically verify the warehouse exists on startup (controlled by `verify_warehouse: true`).

### Other Iceberg Catalogs

For external Iceberg REST catalogs (Polaris, Tabular, etc.), the warehouse is typically just a configuration parameter pointing to a storage location. No pre-creation is required - the catalog manages the warehouse configuration internally.

## Usage Examples

### Example 1: Writing to DataHub Cloud's Iceberg Catalog

This example shows how to write metadata from one DataHub instance to DataHub Cloud's built-in Iceberg catalog:

```yaml
source:
  type: datahub
  config:
    database_connection:
      host_port: "source-postgres:5432"
      database: "datahub"
      username: "datahub"
      password: "${DB_PASSWORD}"

sink:
  type: iceberg-rest
  config:
    # DataHub Cloud Iceberg catalog endpoint
    uri: "https://your-instance.acryl.io/gms/iceberg/"
    warehouse: "datahub_metadata_warehouse"
    namespace: "metadata_backup"
    table_name: "aspects"
    
    # Authenticate with Personal Access Token
    token: "${DATAHUB_CLOUD_PAT}"
    
    # S3 configuration (where DataHub Cloud stores data)
    s3.region: "us-east-1"
    aws_role_arn: "arn:aws:iam::123456789012:role/DataHubIcebergRole"
```

### Example 2: Writing to Local DataHub's Iceberg Catalog

Writing metadata to a locally running DataHub instance with its Iceberg catalog:

```yaml
source:
  type: datahub
  config:
    database_connection:
      host_port: "source-postgres:5432"
      database: "datahub"
      username: "datahub"
      password: "${DB_PASSWORD}"

sink:
  type: iceberg-rest
  config:
    uri: "http://localhost:8080/iceberg/"
    warehouse: "local_warehouse"
    namespace: "datahub_metadata"
    table_name: "metadata_aspects_v2"
    
    # Authenticate with local DataHub PAT
    token: "${DATAHUB_PAT}"
    
    s3.region: "us-west-2"
    s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
    s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```

### Example 3: Writing to External Iceberg REST Catalog (e.g., Polaris)

Using an external Iceberg REST catalog like Snowflake Polaris:

```yaml
source:
  type: datahub
  config:
    database_connection:
      host_port: "source-postgres:5432"
      database: "datahub"
      username: "datahub"
      password: "${DB_PASSWORD}"

sink:
  type: iceberg-rest
  config:
    uri: "https://polaris.snowflakecomputing.com/api/catalog"
    warehouse: "datahub_warehouse"
    namespace: "datahub"
    table_name: "metadata"
    
    # Polaris OAuth token
    token: "${POLARIS_TOKEN}"
    
    s3.region: "us-east-1"
    
    # Connection resiliency
    connection:
      timeout: 180
      retry:
        total: 5
        backoff_factor: 0.5
```

### Example 4: Using MinIO as Storage

Writing to an Iceberg catalog with MinIO backend:

```yaml
source:
  type: datahub
  config:
    database_connection:
      host_port: "source-postgres:5432"
      database: "datahub"
      username: "datahub"
      password: "${DB_PASSWORD}"

sink:
  type: iceberg-rest
  config:
    uri: "http://iceberg-rest:8181"
    warehouse: "minio_warehouse"
    
    # MinIO configuration
    s3.endpoint: "http://minio:9000"
    s3.access-key-id: "minioadmin"
    s3.secret-access-key: "minioadmin"
    s3.region: "us-east-1"
```

### Example 5: Using DataHub Source Filters

Leverage DataHub source configuration to filter what gets written:

```yaml
source:
  type: datahub
  config:
    database_connection:
      host_port: "source-postgres:5432"
      database: "datahub"
      username: "datahub"
      password: "${DB_PASSWORD}"
    
    # Only ingest specific aspects
    exclude_aspects:
      - dataHubIngestionSourceKey
      - dataHubIngestionSourceInfo
      - testResults
    
    # Only specific entity types
    urn_pattern:
      allow:
        - "urn:li:dataset:.*"
        - "urn:li:chart:.*"

sink:
  type: iceberg-rest
  config:
    uri: "http://localhost:8080/iceberg/"
    warehouse: "filtered_warehouse"
    token: "${DATAHUB_PAT}"
    s3.region: "us-west-2"
```

## Querying Metadata

Once metadata is written to the Iceberg table, you can query it using any Iceberg-compatible engine.

### Using DuckDB

```python
from pyiceberg.catalog import load_catalog
import duckdb

# Load the catalog
catalog = load_catalog(
    "datahub",
    uri="http://localhost:8080/iceberg/",
    warehouse="local_warehouse",
    token="your-pat-token",
)

# Load the table
table = catalog.load_table("datahub_metadata.metadata_aspects_v2")

# Query with DuckDB
con = table.scan().to_duckdb(table_name="metadata")

# Example queries
print("Count by aspect type:")
print(con.execute("SELECT aspect, COUNT(*) FROM metadata GROUP BY aspect").fetchall())

print("\nRecent ownership changes:")
print(con.execute("""
    SELECT urn, createdon 
    FROM metadata 
    WHERE aspect = 'ownership' 
    ORDER BY createdon DESC 
    LIMIT 10
""").fetchall())
```

### Using Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.datahub", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.datahub.type", "rest") \
    .config("spark.sql.catalog.datahub.uri", "http://localhost:8080/iceberg/") \
    .config("spark.sql.catalog.datahub.warehouse", "local_warehouse") \
    .config("spark.sql.catalog.datahub.token", "your-pat-token") \
    .getOrCreate()

# Query the metadata
spark.sql("""
    SELECT aspect, COUNT(*) as count
    FROM datahub.datahub_metadata.metadata_aspects_v2
    GROUP BY aspect
    ORDER BY count DESC
""").show()
```

### SQL Analysis Examples

```sql
-- Find all datasets with their descriptions
SELECT 
    urn,
    JSON_EXTRACT(metadata, '$.description') as description
FROM datahub.datahub_metadata.metadata_aspects_v2
WHERE aspect = 'datasetProperties'
    AND urn LIKE 'urn:li:dataset:%';

-- Count aspects by entity type
SELECT 
    REGEXP_EXTRACT(urn, 'urn:li:([^:]+):', 1) as entity_type,
    aspect,
    COUNT(*) as count
FROM datahub.datahub_metadata.metadata_aspects_v2
GROUP BY entity_type, aspect
ORDER BY entity_type, count DESC;

-- Track ownership changes over time
SELECT 
    DATE_TRUNC('day', createdon) as day,
    COUNT(*) as ownership_updates
FROM datahub.datahub_metadata.metadata_aspects_v2
WHERE aspect = 'ownership'
GROUP BY day
ORDER BY day DESC;
```

## Troubleshooting

### Connection Issues

**Problem:** "Connection refused" or timeout errors

**Solution:**
- Verify the Iceberg REST catalog is running and accessible
- Check network connectivity and firewall rules
- Increase the timeout value in configuration:
  ```yaml
  connection:
    timeout: 300
  ```

### Authentication Failures

**Problem:** "Unauthorized" or "Authentication failed" errors

**Solution:**
- Verify your token is valid (for DataHub, generate a new Personal Access Token)
- For AWS role-based authentication, ensure the role ARN is correct
- Check that the token has necessary permissions to create namespaces and tables

### Table Creation Failures

**Problem:** "Permission denied" when creating table or namespace

**Solution:**
- Ensure your user has the required DataHub privileges:
  - `DATA_MANAGE_NAMESPACES_PRIVILEGE`
  - `DATA_MANAGE_TABLES_PRIVILEGE`
- For external catalogs, verify catalog-level permissions

### Namespace/Table Already Exists

**Problem:** Previous versions might fail with "ValidationException" or "already exists" errors when re-running

**Solution:**
- The sink now implements "create if not exists" logic
- It checks for existing namespaces/tables before attempting creation
- Re-running the same ingestion pipeline is now idempotent
- If you encounter issues, the sink will:
  - Skip creation if namespace/table already exists
  - Log informational messages rather than failing
  - Continue with data writing to existing tables

### Iceberg Commit Failures

**Problem:** "CommitFailedException: Requirement failed: branch main was created concurrently"

**Solution:**
- The sink automatically reloads the table metadata after each batch write
- This prevents optimistic concurrency conflicts when writing multiple batches
- The table is refreshed to get the latest snapshot ID before the next batch
- This is handled transparently - no configuration needed

### Write Performance Issues

**Problem:** Slow write performance

**Solution:**
- The sink batches records (default: 10,000 per batch) to optimize Iceberg performance
- For high-volume ingestion, increase `batch_size` to 20,000-50,000 records
- Larger batches create fewer Parquet files, improving query performance
- Ensure adequate resources for the Iceberg catalog and storage backend
- Consider using a closer S3 region to reduce latency

**Example configuration for high-volume ingestion:**
```yaml
sink:
  type: iceberg-rest
  config:
    # ... other config ...
    batch_size: 50000  # Large batches for better performance
```

### Schema Evolution

**Problem:** "Schema incompatible" errors

**Solution:**
- The table schema is fixed. If you need to modify it, you'll need to:
  1. Drop the existing table
  2. Let the sink recreate it with the new schema
  3. Or manually alter the table schema in the catalog

### Checking Write Progress

To monitor writes in real-time:

```python
# Check the sink report
from datahub.ingestion.run.pipeline import Pipeline

pipeline = Pipeline.create({"source": {...}, "sink": {...}})
pipeline.run()

report = pipeline.sink.get_report()
print(f"Total written: {report.total_records_written}")
print(f"Write errors: {report.write_errors}")
print(f"Table created: {report.table_created}")
```

### S3 Access Issues

**Problem:** "Access Denied" errors when writing to S3

**Solution:**
- Verify AWS credentials have write permissions to the S3 bucket
- Check bucket policies and IAM role permissions
- For vended credentials, ensure the role ARN is correct and the trust relationship is configured

## Advanced Configuration

### Custom Retry Logic

```yaml
sink:
  type: iceberg-rest
  config:
    uri: "http://iceberg-rest:8181"
    warehouse: "warehouse"
    connection:
      timeout: 180
      retry:
        total: 10
        backoff_factor: 1.0
        status_forcelist: [429, 500, 502, 503, 504]
```

### Multiple Warehouses

To write to multiple Iceberg catalogs, run separate ingestion pipelines:

```bash
# Write to production warehouse
datahub ingest -c prod-iceberg-sink.yaml

# Write to backup warehouse
datahub ingest -c backup-iceberg-sink.yaml
```

## Performance Considerations

- **Batch Writing**: The sink batches records before writing to Iceberg (default: 10,000 records). This creates appropriately-sized Parquet files and avoids the "small files problem" that hurts query performance.
  - **Small batches (1,000-5,000)**: Lower memory usage, more files, slower queries
  - **Medium batches (10,000-20,000)**: Balanced approach (default: 10,000)
  - **Large batches (20,000-50,000)**: Best query performance, higher memory usage
- **File Size**: Each batch creates one Parquet file. Target 100-500 MB per file for optimal performance.
- **Table Metadata Refresh**: After each batch write, the sink automatically reloads the table metadata to prevent Iceberg optimistic concurrency conflicts. This adds minimal latency (~100-200ms per batch) but ensures reliable multi-batch writes.
- **Partition Pruning**: Queries filtering by `entity_type` will benefit from partition pruning.
- **Storage Format**: Data is stored as Parquet files, providing efficient compression and columnar access.
- **Catalog Overhead**: REST catalog operations add latency; use connection pooling and retries for reliability.
- **Memory Usage**: Approximate memory per batch = `batch_size × average_record_size`. Typical DataHub records are 1-10 KB.

## Compatibility

- **PyIceberg**: Requires version 0.8.0 or higher
- **Iceberg REST Catalogs**: Compatible with any REST catalog implementing the Iceberg REST specification
  - DataHub (1.0.0+)
  - Snowflake Polaris
  - Apache Iceberg REST
  - Tabular
  - And others

## See Also

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [DataHub Iceberg Catalog](https://docs.datahub.com/docs/iceberg-catalog)
- [DataHub Source Documentation](../sources/datahub/datahub.md)

