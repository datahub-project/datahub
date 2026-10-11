### Overview

This plugin extracts the following:

- Metadata for databases, schemas, views, tables, and stored procedures
- Column types associated with each table
- Also supports PostGIS extensions
- Table, row, and column statistics via optional SQL profiling

#### See Also

- [Postgres Source Configuration](https://datahubproject.io/docs/generated/ingestion/sources/postgres)
- [pg_stat_statements Documentation](https://www.postgresql.org/docs/current/pgstatstatements.html)
- [DataHub Lineage Concepts](https://datahubproject.io/docs/generated/lineage/lineage-feature-guide)

### Prerequisites

#### 0. PostgreSQL Version Requirement

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

#### 1. Enable pg_stat_statements Extension

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

#### 2. Grant Required Permissions

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

#### 3. Configure Query Retention (Optional)

By default, `pg_stat_statements` stores the last 5000 queries. You can adjust this in `postgresql.conf`:

```ini
# Maximum number of queries tracked
pg_stat_statements.max = 10000

# Track nested statements (functions, procedures)
pg_stat_statements.track = all
```

After changing these settings, restart PostgreSQL.
