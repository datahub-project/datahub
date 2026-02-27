# Query-Based Lineage for PostgreSQL

DataHub can extract table-level lineage from your PostgreSQL query history using the `pg_stat_statements` extension. This feature analyzes executed SQL queries to automatically discover upstream and downstream dataset dependencies.

## Overview

The query-based lineage feature:

- **Extracts lineage** from `INSERT...SELECT`, `CREATE TABLE AS SELECT`, `CREATE VIEW`, and other DML/DDL statements
- **Prioritizes important queries** by execution frequency and time
- **Respects your filters** using configurable exclude patterns
- **Generates usage statistics** showing which tables are queried and by whom

## Prerequisites

### 0. PostgreSQL Version Requirement

**PostgreSQL 13 or later is required** for query-based lineage extraction.

**Why:** PostgreSQL 13 changed column names in the `pg_stat_statements` view:

- PostgreSQL 12 and earlier: `total_time`, `min_time`, `max_time`, `mean_time`
- PostgreSQL 13+: `total_exec_time`, `min_exec_time`, `max_exec_time`, `mean_exec_time`

The DataHub connector uses the PostgreSQL 13+ column names. If you attempt to use this feature with PostgreSQL 12 or earlier, you'll receive a clear error message:

```
PostgreSQL version 12.0 detected. Query-based lineage requires PostgreSQL 13+
due to column name changes in pg_stat_statements (total_time -> total_exec_time).
Please upgrade to PostgreSQL 13 or later.
```

**Solution:** Upgrade to PostgreSQL 13 or later to use query-based lineage extraction.

### 1. Enable pg_stat_statements Extension

The `pg_stat_statements` extension must be installed and loaded. This extension tracks query execution statistics.

**Step 1: Load the extension in postgresql.conf**

Add or update the following line in your `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_statements'
```

If you already have other extensions loaded (e.g., `'pg_cron,pg_stat_statements'`), append `pg_stat_statements` to the comma-separated list.

**Step 2: Restart PostgreSQL**

The extension requires a database restart to load:

```bash
# On Linux (systemd)
sudo systemctl restart postgresql

# On macOS (Homebrew)
brew services restart postgresql

# Or using pg_ctl
pg_ctl restart -D /path/to/data/directory
```

**Step 3: Create the extension in your database**

Connect to each database you want to monitor and create the extension:

```sql
-- Connect to your database
\c your_database

-- Create the extension (requires superuser or CREATE privileges)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Verify installation
SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
```

### 2. Grant Required Permissions

The DataHub user needs permission to read from `pg_stat_statements`.

**Option 1: Grant pg_read_all_stats role (PostgreSQL 10+, recommended)**

```sql
-- Grant the pg_read_all_stats role to your DataHub user
GRANT pg_read_all_stats TO datahub_user;
```

**Option 2: Use a superuser account**

If your PostgreSQL version doesn't have `pg_read_all_stats`, you can use a superuser account. However, this is not recommended for production due to security implications.

**Verify permissions**

```sql
-- Check if user has the required role
SELECT
    pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') as has_stats_role,
    usesuper as is_superuser
FROM pg_user
WHERE usename = current_user;
```

The query should return `true` for at least one column.

### 3. Configure Query Retention (Optional)

By default, `pg_stat_statements` stores the last 5000 queries. You can adjust this in `postgresql.conf`:

```ini
# Maximum number of queries tracked
pg_stat_statements.max = 10000

# Track nested statements (functions, procedures)
pg_stat_statements.track = all
```

After changing these settings, restart PostgreSQL.

## Configuration

Enable query-based lineage in your DataHub recipe:

```yaml
source:
  type: postgres
  config:
    host_port: "localhost:5432"
    database: "your_database"
    username: "datahub_user"
    password: "your_password"

    # Enable query-based lineage extraction
    include_query_lineage: true

    # Optional: Configure lineage extraction
    max_queries_to_extract: 1000 # Default: 1000
    min_query_calls: 10 # Only extract queries executed ≥10 times

    # Optional: Exclude specific query patterns
    query_exclude_patterns:
      - "%pg_catalog%" # Exclude system catalog queries
      - "%temp_%" # Exclude temporary table queries
      - "%staging%" # Exclude staging queries

    # Optional: Enable usage statistics
    include_usage_statistics: true
```

### Configuration Options

| Option                     | Type         | Default | Description                                                                                                                                                                           |
| -------------------------- | ------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `include_query_lineage`    | boolean      | `false` | Enable query-based lineage extraction from `pg_stat_statements`                                                                                                                       |
| `max_queries_to_extract`   | integer      | `1000`  | Maximum number of queries to extract. Queries are prioritized by execution time and frequency.                                                                                        |
| `min_query_calls`          | integer      | `1`     | Minimum number of times a query must be executed to be included in lineage analysis. Higher values focus on frequently-used queries.                                                  |
| `query_exclude_patterns`   | list[string] | `[]`    | SQL LIKE patterns to exclude queries. Patterns are case-insensitive. Example: `"%pg_catalog%"` excludes all queries containing `pg_catalog`.                                          |
| `include_usage_statistics` | boolean      | `false` | Generate dataset usage metrics from query history. Requires `include_query_lineage: true`. Shows unique user counts, query frequencies, and column access patterns in the DataHub UI. |

## Supported Lineage Patterns

The lineage extractor recognizes common SQL patterns:

### INSERT...SELECT

```sql
INSERT INTO target_table (col1, col2)
SELECT col1, col2 FROM source_table;
```

**Lineage:** `source_table` → `target_table`

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE new_table AS
SELECT a.col1, b.col2
FROM table_a a
JOIN table_b b ON a.id = b.id;
```

**Lineage:** `table_a`, `table_b` → `new_table`

### CREATE VIEW

```sql
CREATE VIEW customer_summary AS
SELECT c.customer_id, COUNT(o.order_id) as order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id;
```

**Lineage:** `customers`, `orders` → `customer_summary`

### Complex JOINs and CTEs

```sql
WITH monthly_revenue AS (
    SELECT customer_id, SUM(amount) as revenue
    FROM transactions
    WHERE date >= '2024-01-01'
    GROUP BY customer_id
)
INSERT INTO customer_metrics (customer_id, total_revenue)
SELECT mr.customer_id, mr.revenue
FROM monthly_revenue mr
JOIN customers c ON mr.customer_id = c.id
WHERE c.active = true;
```

**Lineage:** `transactions`, `customers` → `customer_metrics`

## Verification

After running ingestion, verify that lineage was extracted:

**1. Check ingestion logs**

Look for messages like:

```
INFO - Prerequisites check: Prerequisites met
INFO - Extracted 850 queries from pg_stat_statements in 1.23 seconds
INFO - Processed 850 queries for lineage extraction (12 failed) in 4.56 seconds
```

**2. Query pg_stat_statements directly**

```sql
-- Check if queries are being tracked
SELECT COUNT(*) FROM pg_stat_statements;

-- View most frequently executed queries
SELECT
    calls,
    total_exec_time / 1000 as total_seconds,
    query
FROM pg_stat_statements
ORDER BY calls DESC
LIMIT 10;
```

**3. Verify in DataHub UI**

Navigate to a dataset in DataHub and check the "Lineage" tab. You should see upstream and downstream dependencies derived from query history.

## Troubleshooting

### PostgreSQL Version Too Old

**Error message:**

```
ERROR - PostgreSQL version 12.0 detected. Query-based lineage requires PostgreSQL 13+
due to column name changes in pg_stat_statements (total_time -> total_exec_time).
Please upgrade to PostgreSQL 13 or later.
```

**Solution:**

Upgrade your PostgreSQL installation to version 13 or later. PostgreSQL 13 was released in September 2020 and introduced breaking changes to the `pg_stat_statements` view column names.

**Check your current version:**

```sql
SELECT version();
-- Or
SHOW server_version;
```

**Upgrade path:**

- [PostgreSQL Upgrade Documentation](https://www.postgresql.org/docs/current/upgrading.html)
- Use `pg_upgrade` for in-place upgrades
- Consider managed services (AWS RDS, Google Cloud SQL, Azure Database) which support easy version upgrades

### Extension Not Installed

**Error message:**

```
ERROR - pg_stat_statements extension not installed. Install with: CREATE EXTENSION pg_stat_statements;
```

**Solution:**

1. Verify `shared_preload_libraries` includes `pg_stat_statements` in `postgresql.conf`
2. Restart PostgreSQL
3. Connect to your database and run `CREATE EXTENSION pg_stat_statements;`

### Permission Denied

**Error message:**

```
ERROR - Insufficient permissions. Grant pg_read_all_stats role: GRANT pg_read_all_stats TO <user>;
```

**Solution:**

```sql
-- Grant the required role
GRANT pg_read_all_stats TO datahub_user;

-- Or verify current permissions
SELECT
    pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') as has_stats_role,
    usesuper as is_superuser
FROM pg_user
WHERE usename = current_user;
```

### No Queries Extracted

**Possible causes:**

1. **No queries in pg_stat_statements**

   - The extension tracks queries since the last reset or restart
   - Run some queries against your database, then re-run ingestion

2. **Queries excluded by filters**

   - Check `min_query_calls` - lower this value to include less-frequent queries
   - Review `query_exclude_patterns` - ensure you're not excluding too broadly

3. **Empty pg_stat_statements**

   ```sql
   -- Check if queries are being tracked
   SELECT COUNT(*) FROM pg_stat_statements;

   -- If 0, reset and run some test queries
   SELECT pg_stat_statements_reset();

   -- Run sample queries
   SELECT * FROM your_table LIMIT 10;

   -- Verify queries were tracked
   SELECT COUNT(*) FROM pg_stat_statements;
   ```

### Query Text Truncated

By default, PostgreSQL truncates query text to 1024 characters. Increase this limit in `postgresql.conf`:

```ini
# Increase max query length tracked (requires restart)
track_activity_query_size = 4096
```

### Performance Considerations

**Memory usage:**

- Each tracked query consumes ~1KB in shared memory
- Default 5000 queries = ~5MB memory
- Adjust `pg_stat_statements.max` based on your available memory

**Query overhead:**

- The extension adds minimal overhead (<1%) to query execution
- Statistics are updated asynchronously

**Ingestion performance:**

- Extracting 1000 queries takes 1-5 seconds depending on database load
- Use `max_queries_to_extract` to limit extraction time
- Schedule ingestion during off-peak hours for large query volumes

## Limitations

1. **Historical data only**

   - Lineage is extracted from executed queries, not from schema definitions
   - Queries must have been executed since the last `pg_stat_statements_reset()`

2. **Dynamic SQL**

   - Parameterized queries show parameter placeholders, not actual values
   - Example: `SELECT * FROM users WHERE id = $1` (value not captured)

3. **Complex transformations**

   - The extractor may not parse extremely complex queries with nested CTEs or exotic syntax
   - Failed queries are logged but don't block ingestion

4. **No column-level lineage**
   - Currently supports table-level lineage only
   - Column-level lineage may be added in future releases

## Best Practices

1. **Reset pg_stat_statements periodically**

   ```sql
   -- Reset statistics (caution: clears all tracked queries)
   SELECT pg_stat_statements_reset();
   ```

   This prevents unbounded memory growth and focuses on recent query patterns.

2. **Use meaningful filters**

   - Exclude test, temporary, and system queries with `query_exclude_patterns`
   - Set `min_query_calls` to focus on production workloads

3. **Monitor memory usage**

   ```sql
   -- Check current query count
   SELECT COUNT(*) as query_count FROM pg_stat_statements;

   -- Check memory usage
   SELECT pg_size_pretty(
       pg_database_size('your_database')
   );
   ```

4. **Schedule regular ingestion**
   - Run ingestion daily or weekly to capture lineage from new queries
   - More frequent ingestion provides more up-to-date lineage graphs

## See Also

- [Postgres Source Configuration](https://datahubproject.io/docs/generated/ingestion/sources/postgres)
- [pg_stat_statements Documentation](https://www.postgresql.org/docs/current/pgstatstatements.html)
- [DataHub Lineage Concepts](https://datahubproject.io/docs/generated/lineage/lineage-feature-guide)
