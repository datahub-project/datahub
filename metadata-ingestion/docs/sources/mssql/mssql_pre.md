### Overview

The `mssql` module ingests metadata from Mssql into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Requires specific privileges to ingest SQL Server Jobs and stored procedures.

The connector automatically detects your environment and uses the optimal method:

- **RDS/Managed SQL Server**: Stored procedures (recommended for managed environments)
- **On-premises SQL Server**: Direct table access (typically faster)
- **Automatic fallback**: Tries alternative method if primary fails

#### Permissions for All Environments

```sql
-- Core permissions for stored procedures (required)
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'

-- Stored procedure permissions (required for RDS/managed environments)
GRANT EXECUTE ON msdb.dbo.sp_help_job TO 'USERNAME'
GRANT EXECUTE ON msdb.dbo.sp_help_jobstep TO 'USERNAME'

-- Permissions for stored procedure code and lineage
USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```

#### RDS SQL Server Specific Notes

For **Amazon RDS SQL Server** environments, the stored procedure approach is preferred and typically the only method that works due to restricted table access. Ensure the following permissions are granted:

```sql
-- Essential for RDS environments
GRANT EXECUTE ON msdb.dbo.sp_help_job TO 'USERNAME'
GRANT EXECUTE ON msdb.dbo.sp_help_jobstep TO 'USERNAME'
```

**Production Recommendation**: Explicitly set `is_aws_rds: true` in your configuration to avoid automatic detection issues:

```yaml
source:
  type: mssql
  config:
    is_aws_rds: true # Recommended for RDS environments
    # ... other config
```

The connector automatically detects RDS environments by analyzing the server name (e.g., `*.rds.amazonaws.com`), but explicit configuration is more reliable and avoids potential false positives or negatives.

#### On-Premises SQL Server Notes

For **on-premises SQL Server** installations, direct table access is typically available and faster. The source will automatically use direct queries when possible, with stored procedures as fallback.

**Production Recommendation**: Explicitly set `is_aws_rds: false` to ensure optimal performance:

```yaml
source:
  type: mssql
  config:
    is_aws_rds: false # Recommended for on-premises
    # ... other config
```

#### Troubleshooting Permissions

If you encounter permission errors:

1. **RDS environments**: Ensure stored procedure execute permissions are granted
2. **On-premises environments**: Verify both table select and stored procedure execute permissions
3. **Mixed environments**: Grant all permissions listed above for maximum compatibility

The DataHub source will automatically handle fallback between methods and provide detailed error messages with specific permission requirements if issues occur.

---

### Query-Based Lineage and Usage Statistics

Extracts lineage and usage statistics by analyzing SQL queries:

- **Table-level lineage**: Tables read from and written to
- **Column-level lineage**: Data flow between columns
- **Usage patterns**: Frequently accessed tables and user activity
- **Query performance**: Execution counts and timing

### Known Limitations

- **User attribution not supported**: SQL Server Query Store and DMVs do not preserve historical user session context. Query extraction focuses on query content, frequency, and performance metrics.

### Prerequisites for Query-Based Lineage

#### SQL Server Version Requirements

Query-based lineage requires **SQL Server 2016 or later**.

#### Permission Requirements

Grant the DataHub user `VIEW SERVER STATE` permission:

```sql
USE master;
GRANT VIEW SERVER STATE TO [datahub_user];
GO
```

This permission is required to access:

- Query Store (preferred method, SQL Server 2016+)
- Dynamic Management Views (DMVs) as fallback

**Verify permissions:**

```sql
-- Check server-level permissions
SELECT
    state_desc,
    permission_name
FROM sys.server_permissions
WHERE grantee_principal_id = SUSER_ID('datahub_user');
-- Should show VIEW SERVER STATE
```

### Query Store Setup (Recommended)

Query Store is the preferred method for query extraction. It provides better query history retention and performance.

#### Enable Query Store

```sql
-- Enable Query Store for your database
ALTER DATABASE [YourDatabase] SET QUERY_STORE = ON;

-- Configure Query Store settings (recommended production values)
-- Source: SQL Server defaults since 2016/2017, based on Microsoft's extensive testing
-- Reference: https://learn.microsoft.com/en-us/sql/relational-databases/performance/best-practice-with-the-query-store
ALTER DATABASE [YourDatabase] SET QUERY_STORE (
    OPERATION_MODE = READ_WRITE,
    DATA_FLUSH_INTERVAL_SECONDS = 900,     -- 15 min: balances durability vs I/O overhead
    INTERVAL_LENGTH_MINUTES = 60,          -- 1 hour: sufficient granularity for lineage, lower storage overhead
    MAX_STORAGE_SIZE_MB = 1000,            -- 1 GB: ~30-90 days retention for typical workloads (100-1000 queries/day)
    QUERY_CAPTURE_MODE = AUTO,             -- Captures frequent queries only; filters ad-hoc noise
    SIZE_BASED_CLEANUP_MODE = AUTO         -- Auto-removes old queries at capacity; prevents read-only mode
);
```

**Adjust for your environment:**

- **Low volume (<100 queries/day):** `MAX_STORAGE_SIZE_MB = 100`
- **High volume (>10,000 queries/day):** `MAX_STORAGE_SIZE_MB = 2000`, `INTERVAL_LENGTH_MINUTES = 30`
- **Dev/Test:** `MAX_STORAGE_SIZE_MB = 100`
- **OLTP with ad-hoc queries:** `QUERY_CAPTURE_MODE = ALL` (monitor storage)

#### Verify Query Store Status

```sql
SELECT
    name,
    is_query_store_on,
    query_store_state_desc
FROM sys.databases
WHERE name = 'YourDatabase';
```

**Expected output:**

- `is_query_store_on`: 1 (enabled)
- `query_store_state_desc`: "READ_WRITE" (active)

### Configuration

Enable query-based lineage in your DataHub recipe:

```yaml
source:
  type: mssql
  config:
    host_port: localhost:1433
    database: YourDatabase
    username: datahub_user
    password: your_password

    # Enable query-based lineage extraction
    include_query_lineage: true

    # Maximum number of queries to extract (default: 1000, max: 10000)
    max_queries_to_extract: 1000

    # Minimum query execution count to include (default: 1)
    # Higher values reduce noise from rarely-executed queries
    min_query_calls: 5

    # Exclude system and temporary queries (comprehensive list)
    query_exclude_patterns:
      - "%sys.%" # System tables
      - "%tempdb.%" # Temp database
      - "%INFORMATION_SCHEMA%" # Metadata views
      - "%msdb.%" # SQL Agent database
      - "%master.%" # Master database
      - "%model.%" # Model database
      - "%#%" # Temporary tables
      - "%sp_reset_connection%" # JDBC connection resets
      - "%SET ANSI_%" # Connection setup
      - "%SELECT @@%" # Driver metadata queries
      - "%ReportServer%" # SSRS system tables

    # Enable usage statistics (requires graph connection)
    # include_usage_statistics: true

sink:
  # Your sink config
```

### Configuration Options

| Option                     | Type         | Default | Description                                                       |
| -------------------------- | ------------ | ------- | ----------------------------------------------------------------- |
| `include_query_lineage`    | boolean      | `false` | Enable query-based lineage extraction                             |
| `max_queries_to_extract`   | integer      | `1000`  | Maximum queries to analyze (range: 1-10000)                       |
| `min_query_calls`          | integer      | `1`     | Minimum execution count to include query                          |
| `query_exclude_patterns`   | list[string] | `[]`    | SQL LIKE patterns to exclude queries (max 100 patterns)           |
| `include_usage_statistics` | boolean      | `false` | Extract usage statistics (requires `include_query_lineage: true`) |

### Query Extraction Methods

DataHub automatically selects the best available method:

1. **Query Store (Preferred)** - SQL Server 2016+

   - Provides comprehensive query history
   - Better performance and reliability
   - Requires Query Store to be enabled

2. **DMV Fallback** - All supported versions
   - Uses `sys.dm_exec_cached_plans` and related DMVs
   - Limited to queries currently in plan cache
   - Smaller query history window

The source will automatically detect and use the appropriate method based on your SQL Server version and configuration.

### Best Practices

1. **Start Conservative**: Begin with `max_queries_to_extract: 1000` and `min_query_calls: 5`
2. **Monitor Query Store Size**: Set appropriate retention policies
3. **Use Exclude Patterns**: Filter out system queries and temporary tables (see comprehensive list in Configuration section)
4. **Regular Extraction**: Run ingestion at least daily for accurate usage statistics
5. **Test Before Production**: Validate permissions and Query Store setup in a non-production environment first

### Performance Considerations

- **Query Store Impact**: Query Store has minimal overhead when configured appropriately
- **Extraction Performance**: Query Store typically performs better than DMV method
- **Storage**: Query Store storage usage depends on retention settings and query volume
- **Parsing Time**: Scales with query complexity and volume; monitor debug logs for timing
