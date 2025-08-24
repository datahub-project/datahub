## Dremio

DataHub supports extracting metadata from Dremio, including:

- Tables, views, and virtual datasets (VDS)
- Schema and column information with data types
- Table and column descriptions
- Data lineage from view definitions and queries
- Usage statistics and query patterns
- Data profiling and classification
- Custom tags and glossary terms
- Container hierarchy (projects, sources, spaces, folders)
- Intelligent processing of large view definitions (auto-detects Dremio limits)

### Supported Dremio Deployments

- **Dremio Cloud**: Fully supported with cloud-specific features
- **Dremio Software**: Self-hosted deployments (Version 21.0+ required)
- **Dremio Enterprise**: On-premises enterprise deployments (Version 21.0+ required)

**Note**: Dremio versions prior to 21.0 are not supported and may not work correctly with this connector.

### Quick Start

#### Basic Configuration

```yaml
source:
  type: dremio
  config:
    # Connection details
    host: your-dremio-host.com
    port: 9047 # Default port, 443 for Dremio Cloud
    username: datahub_user
    password: your_password

    # For Dremio Cloud
    is_dremio_cloud: true
    dremio_cloud_project_id: your-project-id

    # Authentication (optional)
    authentication_method: PAT # or password (default)
```

#### Dremio Cloud Configuration

```yaml
source:
  type: dremio
  config:
    # Dremio Cloud specific settings
    is_dremio_cloud: true
    dremio_cloud_project_id: "12345678-1234-1234-1234-123456789abc"

    # Authentication using Personal Access Token (recommended)
    authentication_method: PAT
    username: your_username
    password: your_personal_access_token

    # Enable advanced features
    include_query_lineage: true
    profiling:
      enabled: true
```

#### Enterprise/Software Configuration

```yaml
source:
  type: dremio
  config:
    # Connection details
    host: dremio.company.com
    port: 9047
    username: datahub_user
    password: secure_password

    # SSL configuration (if enabled)
    use_ssl: true

    # Source mappings for lineage
    source_mappings:
      - platform: s3
        source_name: data-lake
        root_path: /analytics
      - platform: snowflake
        source_name: warehouse
        database_name: ANALYTICS_DB
```

### Configuration Options

#### Connection Configuration

| Field                     | Type    | Default    | Description                  |
| ------------------------- | ------- | ---------- | ---------------------------- |
| `host`                    | string  |            | Dremio coordinator hostname  |
| `port`                    | int     | 9047       | Dremio port (443 for Cloud)  |
| `username`                | string  |            | Dremio username              |
| `password`                | string  |            | Dremio password or PAT token |
| `use_ssl`                 | boolean | false      | Use SSL/TLS connection       |
| `is_dremio_cloud`         | boolean | false      | Set to true for Dremio Cloud |
| `dremio_cloud_project_id` | string  |            | Required for Dremio Cloud    |
| `authentication_method`   | string  | "password" | "password" or "PAT"          |

#### Filtering Configuration

| Field                   | Type             | Default   | Description                     |
| ----------------------- | ---------------- | --------- | ------------------------------- |
| `schema_pattern`        | AllowDenyPattern | Allow all | Filter for schemas/containers   |
| `table_pattern`         | AllowDenyPattern | Allow all | Filter for tables               |
| `view_pattern`          | AllowDenyPattern | Allow all | Filter for views                |
| `include_system_tables` | boolean          | false     | Include system tables and views |

**Example Filtering:**

```yaml
# Include only specific patterns
schema_pattern:
  allow:
    - "Analytics.*" # All containers under Analytics
    - "DataLake.prod.*" # Production data in DataLake
  deny:
    - ".*temp.*" # Exclude temporary containers
    - ".*test.*" # Exclude test containers

table_pattern:
  allow:
    - "fact_.*" # Include fact tables
    - "dim_.*" # Include dimension tables
  deny:
    - ".*_backup" # Exclude backup tables
```

#### Advanced Features

##### Format-Based Platform URNs (New Feature)

Generate platform-specific URNs based on table format (Delta Lake, Iceberg, etc.):

```yaml
# Enable format-based platform URNs
use_format_based_platform_urns: true

# Customize format mappings (optional)
format_platform_mapping:
  delta: "delta-lake"
  iceberg: "iceberg"
  hudi: "hudi"
  parquet: "s3"
```

When enabled:

- Delta Lake tables get `delta-lake` platform URNs
- Iceberg tables get `iceberg` platform URNs
- Regular tables continue to use `dremio` platform URNs
- Container URNs always use `dremio` platform for consistency

##### Query Lineage and Usage

```yaml
# Enable query-based lineage extraction
include_query_lineage: true

# Usage statistics configuration
usage:
  enabled: true
  start_time: "2024-01-01T00:00:00Z"
  end_time: "2024-12-31T23:59:59Z"
  bucket_duration: DAY
```

##### Data Profiling

```yaml
profiling:
  enabled: true
  # Profile only specific tables
  profile_table_level_only: true
  # Limit profiling to smaller tables
  max_number_of_fields_to_profile: 20
  # Sample configuration
  profile_sample_size: 10000
```

##### Performance and Memory Management

```yaml
# File-backed caching to prevent OOM errors
enable_file_backed_cache: true
file_backed_cache_size: 1000
cache_eviction_batch_size: 100

# Intelligent view chunking (automatically detects Dremio limits)
# Requires access to sys.options - see Prerequisites documentation
# Falls back to 32KB chunks if system access unavailable

# Processing limits
max_workers: 5
max_containers_per_batch: 100
```

##### Source Mappings for Lineage

Map Dremio sources to external platforms for cross-platform lineage:

```yaml
source_mappings:
  # S3 data lake
  - platform: s3
    source_name: s3-data-lake
    root_path: /analytics

  # Snowflake warehouse
  - platform: snowflake
    source_name: snowflake-prod
    database_name: ANALYTICS

  # PostgreSQL database
  - platform: postgres
    source_name: postgres-oltp
    database_name: application_db
```

##### Domain Assignment

Automatically assign domains based on dataset paths:

```yaml
domain:
  finance:
    allow:
      - "Finance.*"
      - "Accounting.*"
  analytics:
    allow:
      - "Analytics.*"
      - "DataScience.*"
  operations:
    allow:
      - "Operations.*"
      - "Logistics.*"
```

### Complete Configuration Example

```yaml
source:
  type: dremio
  config:
    # === Connection Configuration ===
    host: dremio.company.com
    port: 9047
    username: datahub_service
    password: ${DREMIO_PASSWORD}
    use_ssl: true

    # === Dremio Cloud (uncomment if using Cloud) ===
    # is_dremio_cloud: true
    # dremio_cloud_project_id: "your-project-id"
    # authentication_method: PAT

    # === Filtering Configuration ===
    schema_pattern:
      allow:
        - "Analytics.*"
        - "Finance.*"
        - "Operations.*"
      deny:
        - ".*temp.*"
        - ".*test.*"
        - ".*dev.*"

    table_pattern:
      allow:
        - ".*"
      deny:
        - ".*_backup"
        - ".*_archive"

    # === Advanced Features ===

    # Format-based platform URNs
    use_format_based_platform_urns: true
    format_platform_mapping:
      delta: "delta-lake"
      iceberg: "iceberg"
      hudi: "hudi"

    # Query lineage and usage
    include_query_lineage: true
    usage:
      enabled: true
      bucket_duration: DAY

    # Data profiling
    profiling:
      enabled: true
      profile_table_level_only: true
      max_number_of_fields_to_profile: 50

    # Performance optimization
    enable_file_backed_cache: true
    file_backed_cache_size: 2000
    enable_sql_chunking: true
    max_workers: 8

    # Source mappings for lineage
    source_mappings:
      - platform: s3
        source_name: data-lake-prod
        root_path: /warehouse
      - platform: snowflake
        source_name: snowflake-dw
        database_name: ANALYTICS_DW

    # Domain assignment
    domain:
      finance:
        allow: ["Finance.*", "Accounting.*"]
      analytics:
        allow: ["Analytics.*", "ML.*"]
      operations:
        allow: ["Operations.*", "Supply.*"]

    # System configuration
    include_system_tables: false
    ingest_owner: true

    # Platform instance (optional)
    platform_instance: prod-dremio
    env: PROD

# === Sink Configuration ===
sink:
  type: datahub-rest
  config:
    server: http://datahub-gms:8080
```

### Troubleshooting

#### Common Issues

**1. Permission Denied Errors**

```
Error: Access denied to source/table/view
```

- Verify user has required privileges (see Prerequisites)
- Check parent object permissions (project, source, folder)
- Ensure user is not limited to PUBLIC role only

**2. Connection Timeouts**

```
Error: Connection timeout to Dremio
```

- Verify host and port configuration
- Check network connectivity and firewall rules
- For Dremio Cloud, verify project ID is correct

**3. Missing Objects**

```
Warning: Expected objects not found
```

- Review schema_pattern, table_pattern, view_pattern filters
- Check include_system_tables setting
- Verify user has access to expected objects

**4. Memory Issues**

```
Error: OutOfMemoryError during ingestion
```

- Enable file-backed caching: `enable_file_backed_cache: true`
- Reduce batch sizes: `max_containers_per_batch: 50`
  **5. Large View Definition Issues**

```
Warning: View definition truncated or incomplete
```

The connector uses intelligent chunking that automatically detects Dremio's limits:

- **Ensure system access**: Grant `SELECT` on `sys.options` (Enterprise/Software/Community) or `sys.project.options` (Cloud)
- **Check logs**: Look for "Detected Dremio limits" or "Using fallback" messages
- **Verify detection**: The connector will log the detected `single_field_size_bytes` limit
- **Fallback behavior**: Without system access, gracefully uses safe 32KB chunks across all editions

If views are still truncated, check Dremio's configuration:

```sql
-- Check current limit in Dremio
SELECT name, string_val, num_val
FROM sys.options
WHERE name = 'planner.single_field_size_bytes';
```

#### Debug Configuration

Enable detailed logging for troubleshooting:

```yaml
# Add to your DataHub configuration
logging:
  loggers:
    datahub.ingestion.source.dremio:
      level: DEBUG
    datahub.ingestion.source.dremio.dremio_api:
      level: DEBUG
```

#### Performance Tuning

For large Dremio deployments:

1. **Enable caching and intelligent chunking**:

   ```yaml
   enable_file_backed_cache: true
   # Intelligent chunking is automatic - ensure sys.options access for best performance
   ```

2. **Adjust worker limits**:

   ```yaml
   max_workers: 10 # Increase for faster processing
   ```

3. **Use filtering to reduce scope**:

   ```yaml
   schema_pattern:
     allow: ["ProductionData.*"] # Only production data
   ```

4. **Enable stateful ingestion** for incremental updates:
   ```yaml
   stateful_ingestion:
     enabled: true
   ```

### Migration from Other Connectors

If migrating from other data catalog tools, consider:

1. **URN Consistency**: Use `platform_instance` to maintain URN compatibility
2. **Filtering**: Map existing filters to Dremio's pattern syntax
3. **Lineage**: Configure source mappings to maintain cross-platform lineage
4. **Domains**: Use domain assignment to maintain data organization

### Best Practices

1. **Security**:

   - Use Personal Access Tokens instead of passwords
   - Create dedicated service accounts with minimal required privileges
   - Regularly rotate credentials

2. **Performance**:

   - Enable file-backed caching for large deployments
   - Use appropriate filtering to limit scope
   - Configure reasonable batch sizes

3. **Monitoring**:

   - Monitor ingestion performance and memory usage
   - Set up alerts for failed ingestions
   - Review logs regularly for warnings

4. **Maintenance**:
   - Keep DataHub and connector versions updated
   - Review and update filtering patterns as data grows
   - Periodically audit user privileges in Dremio
