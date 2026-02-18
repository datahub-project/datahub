### Prerequisites

If you want to ingest MSSQL Jobs and stored procedures (with code) the user credentials needs the proper privileges.

The DataHub MSSQL source automatically detects your environment and uses the optimal method:

- **RDS/Managed SQL Server**: Uses stored procedures (recommended for managed environments)
- **On-premises SQL Server**: Uses direct table access (typically faster when available)
- **Automatic fallback**: If the primary method fails, it automatically tries the alternative approach

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

## Query-Based Lineage and Usage Statistics

DataHub can extract lineage and usage statistics by analyzing SQL queries executed on your MS SQL Server. This provides insights into:

- **Table-level lineage**: Understand which tables are read from and written to
- **Column-level lineage**: Track how data flows between specific columns
- **Usage patterns**: Identify frequently accessed tables and user activity
- **Query performance**: Monitor execution counts and timing

### Known Limitations

- **User attribution not supported**: SQL Server Query Store and DMVs do not preserve historical user session context. Query extraction focuses on query content, frequency, and performance metrics.

### Prerequisites for Query-Based Lineage

#### SQL Server Version Requirements

Query-based lineage requires **SQL Server 2016 or later**.

#### Permission Requirements

Grant the DataHub user `VIEW SERVER STATE` permission:

```sql
GRANT VIEW SERVER STATE TO [datahub_user];
```

This permission is required to access:

- Query Store (preferred method, SQL Server 2016+)
- Dynamic Management Views (DMVs) as fallback

### Query Store Setup (Recommended)

Query Store is the preferred method for query extraction. It provides better query history retention and performance.

#### Enable Query Store

```sql
-- Enable Query Store for your database
ALTER DATABASE [YourDatabase] SET QUERY_STORE = ON;

-- Configure Query Store settings (recommended)
ALTER DATABASE [YourDatabase] SET QUERY_STORE (
    OPERATION_MODE = READ_WRITE,
    DATA_FLUSH_INTERVAL_SECONDS = 900,
    INTERVAL_LENGTH_MINUTES = 60,
    MAX_STORAGE_SIZE_MB = 1000,
    QUERY_CAPTURE_MODE = AUTO,
    SIZE_BASED_CLEANUP_MODE = AUTO
);
```

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

    # Exclude system and temporary queries
    query_exclude_patterns:
      - "%sys.%"
      - "%tempdb.%"
      - "%INFORMATION_SCHEMA%"
      - "%msdb.%"
      - "%#%" # Temporary tables

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

### Troubleshooting

#### Issue: "Query Store is not enabled"

**Solution:**
Enable Query Store on your database:

```sql
ALTER DATABASE [YourDatabase] SET QUERY_STORE = ON;
```

Verify it's enabled:

```sql
SELECT name, is_query_store_on
FROM sys.databases
WHERE name = 'YourDatabase';
```

#### Issue: "VIEW SERVER STATE permission denied"

**Solution:**
Grant the required permission:

```sql
GRANT VIEW SERVER STATE TO [datahub_user];
```

Verify permissions:

```sql
SELECT
    state_desc,
    permission_name
FROM sys.database_permissions
WHERE grantee_principal_id = USER_ID('datahub_user')
AND permission_name = 'VIEW SERVER STATE';
```

#### Issue: "SQL Server version 2014 detected, but 2016+ is required"

**Solution:**

- Upgrade to SQL Server 2016 or later for Query Store support
- The DMV fallback method has limited query history and is not recommended as the primary method

#### Issue: Query Store Returns No Results

**Possible causes:**

1. **No queries in history:**

   - Queries may have been cleared from Query Store
   - Query Store retention settings may be too aggressive
   - Solution: Execute some queries and wait for them to appear in Query Store

2. **All queries filtered by exclude patterns:**

   - Review your `query_exclude_patterns` configuration
   - Solution: Reduce or remove overly broad exclusion patterns

3. **Queries below min_query_calls threshold:**

   - Queries executed fewer times than `min_query_calls` are excluded
   - Solution: Lower the `min_query_calls` value or execute queries more frequently

4. **Query Store query capture mode:**

   - If set to `NONE` or `CUSTOM` with restrictive filters
   - Solution: Set to `AUTO` or `ALL`:
     ```sql
     ALTER DATABASE [YourDatabase]
     SET QUERY_STORE (QUERY_CAPTURE_MODE = AUTO);
     ```

5. **Query Store data not yet captured:**
   - Query Store captures queries asynchronously
   - Solution: Wait 5-10 minutes after query execution, then check:
     ```sql
     SELECT COUNT(*) AS query_count
     FROM sys.query_store_query;
     ```

#### How to Verify Permissions Work

**Test VIEW SERVER STATE permission:**

```sql
-- Test Query Store access
SELECT TOP 1 query_id, query_sql_text
FROM sys.query_store_query_text;

-- Test DMV access
SELECT TOP 1 sql_handle
FROM sys.dm_exec_query_stats;
```

If these queries run successfully, permissions are correct. If you get permission errors:

```sql
-- Check current user's server permissions
SELECT *
FROM fn_my_permissions(NULL, 'SERVER')
WHERE permission_name = 'VIEW SERVER STATE';

-- Grant permission if missing
GRANT VIEW SERVER STATE TO [datahub_user];
```

#### Issue: Hit max_queries_to_extract Limit

**Symptoms:**

- Ingestion report shows exactly `max_queries_to_extract` queries processed
- Warning in logs: "Reached max_queries_to_extract limit"

**What happens:**

- Only the top N queries by execution time are extracted
- Remaining queries are not processed for lineage
- Less frequently executed queries may be missed

**Impact:**

- Lineage may be incomplete for less active tables
- Usage statistics will only reflect the most expensive queries

**Solutions:**

1. **Increase the limit** (if performance allows):

   ```yaml
   source:
     config:
       max_queries_to_extract: 5000 # Default is 1000
   ```

2. **Use exclude patterns** to filter out noise:

   ```yaml
   source:
     config:
       query_exclude_patterns:
         - "%sp_reset_connection%" # Filter JDBC connection resets
         - "%SET%" # Filter SET statements
         - "%DECLARE%" # Filter variable declarations
   ```

3. **Increase min_query_calls** to focus on frequently executed queries:
   ```yaml
   source:
     config:
       min_query_calls: 10 # Only extract queries executed 10+ times
   ```

#### Performance Impact of Extracting Many Queries

Performance varies based on SQL Server version, hardware, query complexity, and system load. Start with conservative limits and adjust based on your environment.

**Recommendations:**

- Start with `max_queries_to_extract: 1000` (default)
- Monitor extraction time in logs
- Adjust based on your requirements

**If extraction is slow:**

- Reduce `max_queries_to_extract`
- Add `query_exclude_patterns` to filter unnecessary queries
- Increase `min_query_calls` to focus on frequently-executed queries

#### Issue: SQL aggregator initialization failed

**Error message:**

```
RuntimeError: Failed to initialize SQL aggregator for query-based lineage,
but include_query_lineage: true was explicitly enabled
```

**Possible causes:**

- Graph connection missing when `include_usage_statistics: true`
- Invalid platform instance configuration

**Solution:**

- If using `include_usage_statistics`, ensure your sink is configured with a DataHub GMS connection
- Verify your source configuration is valid

### Performance Considerations

- **Query Store Impact**: Query Store has minimal overhead when configured appropriately
- **Extraction Performance**: Query Store typically performs better than DMV method
- **Storage**: Query Store storage usage depends on retention settings and query volume

### Best Practices

1. **Start Conservative**: Begin with `max_queries_to_extract: 1000` and `min_query_calls: 5`
2. **Monitor Query Store Size**: Set appropriate retention policies
3. **Use Exclude Patterns**: Filter out system queries and temporary tables
4. **Regular Extraction**: Run ingestion at least daily for accurate usage statistics
5. **Test Before Production**: Validate permissions and Query Store setup in a non-production environment first

### Example: Complete Configuration for Production

```yaml
source:
  type: mssql
  config:
    # Connection
    host_port: prod-mssql.example.com:1433
    database: ProductionDB
    username: datahub_readonly
    password: ${MSSQL_PASSWORD}

    # Basic settings
    convert_urns_to_lowercase: true

    # Query-based lineage (optimized for production)
    include_query_lineage: true
    max_queries_to_extract: 5000
    min_query_calls: 10 # Only include queries executed 10+ times

    # Exclude system and temporary objects
    query_exclude_patterns:
      - "%sys.%"
      - "%tempdb.%"
      - "%INFORMATION_SCHEMA%"
      - "%msdb.%"
      - "%#%" # Temporary tables
      - "%master.%" # Master database
      - "%model.%" # Model database
      - "%ReportServer%" # SSRS system tables

sink:
  type: datahub-rest
  config:
    server: http://datahub-gms:8080
```
