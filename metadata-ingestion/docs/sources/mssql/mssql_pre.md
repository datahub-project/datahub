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

### Troubleshooting

#### Debug Mode

Enable debug logging to see detailed information about query extraction and parsing:

```bash
datahub ingest -c recipe.yml --debug
```

Look for these key log messages:

- `INFO: Extracted X queries from query_store` - Confirms queries were fetched
- `INFO: Processed X queries for lineage extraction (X failed)` - Shows parsing results
- `INFO: Generated X lineage workunits from queries` - Confirms lineage generation
- `DEBUG: Query extraction completed in X.XX seconds` - Performance metrics

#### Common Issues and Solutions

#### Issue: "Query Store is not enabled"

**Solution:**
Follow the Query Store setup instructions above (see "Enable Query Store" section).

#### Issue: "VIEW SERVER STATE permission denied"

**Solution:**
Follow the permission grant instructions above (see "Permission Requirements" section).

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
   - Solution: Wait 5-10 minutes after query execution, then verify:
     ```sql
     SELECT COUNT(*) AS query_count
     FROM sys.query_store_query;
     -- Should return > 0
     ```

#### Issue: Queries Extracted but No Lineage Appears

**Symptoms:**

- Logs show "Extracted X queries" but "Generated 0 lineage workunits"
- Or many queries show parsing failures

**Possible causes:**

1. **SQL parsing errors:**

   - Complex SQL syntax not supported (CTEs, window functions, vendor-specific syntax)
   - Solution: Check debug logs for `SqlUnderstandingError` or `UnsupportedStatementTypeError`

2. **Tables filtered by patterns:**

   - Tables in queries match your `table_pattern` deny list
   - Solution: Review your `table_pattern` and `schema_pattern` configuration

3. **Tables not yet ingested:**

   - Lineage references tables that haven't been discovered yet
   - Solution: Ensure base table ingestion runs before query lineage, or run ingestion twice

4. **Database name mismatch:**
   - Queries reference different databases than configured
   - Solution: Use `database: null` to ingest all databases, or adjust `database` config

**Debug steps:**

```bash
# Run with debug logging
datahub ingest -c recipe.yml --debug 2>&1 | grep -i "lineage\|parsing\|query"

# Look for these patterns:
# - "Unable to parse query" - indicates SQL parsing issues
# - "Table X not found" - indicates missing base tables
# - "Filtered table" - indicates pattern matching issues
```

#### Issue: Authentication or Connection Errors

**Error message:**

```
Login failed for user 'datahub_user'
Cannot open database "YourDatabase" requested by the login
```

**Solutions:**

1. **Login failure:**

   ```sql
   -- Verify user exists and has correct password
   SELECT name, type_desc, is_disabled
   FROM sys.server_principals
   WHERE name = 'datahub_user';
   ```

2. **Database access denied:**

   ```sql
   -- Grant database access
   USE [YourDatabase];
   CREATE USER [datahub_user] FOR LOGIN [datahub_user];
   GRANT CONNECT TO [datahub_user];
   ```

3. **Connection timeout:**
   - Increase timeout in connection string:
     ```yaml
     source:
       config:
         uri_args:
           connect_timeout: 30
           timeout: 30
     ```

#### Issue: Temporary Tables Not Recognized

**Symptoms:**

- Lineage shows temp table names like `#temp_table` as actual tables
- Temp tables pollute your lineage graph

**Solution:**

MSSQL temp tables (starting with `#`) are automatically filtered by default. If you see temp tables in lineage:

1. **Verify temp table patterns are configured:**

   ```yaml
   source:
     config:
       temporary_tables_pattern:
         - ".*#.*" # Built-in default pattern
   ```

2. **Add custom temp table patterns:**
   ```yaml
   source:
     config:
       temporary_tables_pattern:
         - ".*#.*"  # Standard SQL Server temp tables
         - ".*\.temp_.*"  # Custom naming pattern
         - ".*\.staging_.*"  # ETL staging tables
   ```

#### Issue: Performance Problems (Slow Extraction or Hit Query Limit)

**Symptoms:**

- Query extraction takes >30 seconds
- Ingestion times out or hangs during query extraction
- Ingestion report shows exactly `max_queries_to_extract` queries processed
- Warning in logs: "Reached max_queries_to_extract limit"

**Understanding the Problem:**

When you hit the limit, only the top N queries by execution time are extracted. This means:

- Remaining queries are not processed for lineage
- Less frequently executed queries may be missed
- Lineage may be incomplete for less active tables

**Performance Tuning Solutions:**

1. **Reduce query limit** (if experiencing slowness):

   ```yaml
   source:
     config:
       max_queries_to_extract: 500 # Reduced from default 1000
   ```

2. **Increase query limit** (if you need more coverage and can afford the time):

   ```yaml
   source:
     config:
       max_queries_to_extract: 5000 # Increased from default 1000
   ```

3. **Filter out noise with exclude patterns** (see comprehensive list in Configuration section above)

4. **Focus on frequently-executed queries**:

   ```yaml
   source:
     config:
       min_query_calls: 10 # Only extract queries executed 10+ times
   ```

5. **Check Query Store performance:**
   ```sql
   -- Check Query Store size and query count
   SELECT
       actual_state_desc,
       readonly_reason,
       current_storage_size_mb,
       max_storage_size_mb,
       query_count = (SELECT COUNT(*) FROM sys.query_store_query)
   FROM sys.database_query_store_options;
   ```

**Recommendations:**

- Start with `max_queries_to_extract: 1000` (default)
- Monitor extraction time in debug logs
- Adjust based on your requirements and performance constraints
- Use exclude patterns to filter unnecessary queries before hitting the limit

#### Issue: DMV Fallback But Empty Results

**Symptoms:**

- Logs show "falling back to DMV-based extraction"
- But then "Extracted 0 queries from dmv"

**Cause:**
DMVs only contain queries currently in the plan cache. The cache clears on:

- SQL Server restart
- Memory pressure
- Manual cache clearing

**Solutions:**

1. **Enable Query Store (recommended)** - See "Enable Query Store" section above

2. **Execute representative queries before ingestion:**

   - Run your typical ETL jobs
   - Execute common queries
   - Wait 5-10 minutes, then run ingestion

3. **Check plan cache size:**
   ```sql
   SELECT
       size_in_bytes/1024/1024 AS cache_size_mb,
       name,
       type
   FROM sys.dm_os_memory_clerks
   WHERE type = 'CACHESTORE_SQLCP';
   ```

#### Issue: Configuration Validation Errors

**Error messages:**

```
ValidationError: max_queries_to_extract must be positive
ValidationError: query_exclude_patterns cannot exceed 100 patterns
```

**Valid ranges:**

- `max_queries_to_extract`: 1 to 10000
- `min_query_calls`: 0 to any positive integer
- `query_exclude_patterns`: Maximum 100 patterns, each up to 500 characters

#### Issue: SQL Aggregator Initialization Failed

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

#### How to Verify Query Lineage is Working End-to-End

Follow these steps to confirm everything is configured correctly:

1. **Verify permissions:**

   ```sql
   -- Should return at least one row with VIEW SERVER STATE
   SELECT * FROM fn_my_permissions(NULL, 'SERVER')
   WHERE permission_name = 'VIEW SERVER STATE';
   ```

2. **Verify Query Store has data** (see "Verify Query Store Status" section above)

3. **Test Query Store access:**

   ```sql
   -- Test Query Store access
   SELECT TOP 1 query_id, query_sql_text
   FROM sys.query_store_query_text;

   -- Test DMV access
   SELECT TOP 1 sql_handle
   FROM sys.dm_exec_query_stats;
   ```

4. **Run ingestion with debug logging:**

   ```bash
   datahub ingest -c recipe.yml --debug 2>&1 | tee ingestion.log
   ```

5. **Check for success indicators in logs:**

   ```bash
   grep -i "extracted.*queries" ingestion.log
   grep -i "processed.*queries" ingestion.log
   grep -i "generated.*lineage workunits" ingestion.log
   ```

6. **Verify in DataHub UI:**
   - Navigate to a table that appears in your queries
   - Check the "Lineage" tab for upstream/downstream relationships
   - Check the "Queries" tab to see associated SQL queries

#### Still Having Issues?

If you've tried the above steps and still experiencing issues:

1. **Collect diagnostic information:**

   ```bash
   # Run ingestion with debug logging
   datahub ingest -c recipe.yml --debug > ingestion.log 2>&1

   # Check SQL Server version
   # Check Query Store status
   # Check user permissions
   ```

2. **Check DataHub GitHub issues:**
   - Search for similar problems: https://github.com/datahub-project/datahub/issues
3. **Ask for help:**
   - Include your DataHub version
   - Include SQL Server version
   - Include sanitized config (remove passwords!)
   - Include relevant log excerpts
   - Describe expected vs actual behavior

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
