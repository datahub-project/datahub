## Troubleshooting

### Schema Discovery Issues

If you're not seeing all schemas or tables after following the setup steps, check the following:

#### Missing Schemas

**1. Check schema filtering configuration:**

```yaml
# In your recipe, ensure schema patterns are correct
schema_pattern:
  allow:
    - "your_schema_name"
    - "public"
  # Remove deny patterns that might be blocking schemas
```

**2. Verify permissions on specific schemas:**

```sql
-- Test if you can see schemas
SELECT schema_name, schema_type
FROM svv_redshift_schemas
WHERE database_name = 'your_database';

-- Test external schemas
SELECT schemaname, eskind, databasename
FROM SVV_EXTERNAL_SCHEMAS;
```

**3. Check for external schemas:**
External schemas (Redshift Spectrum) require both permissions:

```sql
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub_user;
```

#### Missing Tables Within Schemas

**1. Check table filtering:**

```yaml
table_pattern:
  allow:
    - "your_schema.your_table"
  # Ensure no overly restrictive deny patterns
```

**2. Test table visibility:**

```sql
-- For regular tables
SELECT schemaname, tablename, tabletype
FROM pg_tables
WHERE schemaname = 'your_schema';

-- For views
SELECT schemaname, viewname
FROM pg_views
WHERE schemaname = 'your_schema';

-- For external tables
SELECT schemaname, tablename
FROM SVV_EXTERNAL_TABLES
WHERE schemaname = 'your_schema';
```

#### Configuration Issues

**1. Database specification:**
Ensure you're connecting to the correct database - Redshift ingestion works per database:

```yaml
database: "your_actual_database_name" # Not the cluster name
```

**2. Schema access permissions:**
Ensure you have USAGE permissions on the schemas you want to discover:

```sql
-- Check if you have USAGE on schemas
SELECT n.nspname as schema_name,
       has_schema_privilege('datahub_user', n.nspname, 'USAGE') as has_usage
FROM pg_catalog.pg_namespace n
WHERE n.nspname NOT LIKE 'pg_%'
  AND n.nspname != 'information_schema';

-- Grant USAGE if missing
GRANT USAGE ON SCHEMA your_schema_name TO datahub_user;
```

**3. Shared database configuration:**
If using datashare consumers, add:

```yaml
is_shared_database: true
```

#### Permission Test Queries

Run these to verify your permissions are working:

```sql
-- Test core permissions
SELECT COUNT(*) FROM svv_redshift_schemas WHERE database_name = 'your_database';
SELECT COUNT(*) FROM svv_table_info WHERE database = 'your_database';

-- Test external permissions
SELECT COUNT(*) FROM svv_external_schemas;
SELECT COUNT(*) FROM svv_external_tables;
```

### Data Profiling Issues

#### Profile Data Not Appearing

**1. Check data access permissions:**
Ensure you have `USAGE` on schemas and `SELECT` on tables:

```sql
-- Test schema access
SELECT has_schema_privilege('datahub_user', 'your_schema', 'USAGE');

-- Test table access
SELECT has_table_privilege('datahub_user', 'your_schema.your_table', 'SELECT');
```

**2. Enable table-level profiling only:**
If you cannot grant `SELECT` on tables, use table-level profiling:

```yaml
profiling:
  profile_table_level_only: true
```

### Lineage Issues

#### Missing Lineage Information

**1. Check lineage configuration:**

```yaml
table_lineage_mode: stl_scan_based # or sql_based, mixed
include_usage_statistics: true
```

**2. Verify SYSLOG ACCESS:**

```sql
-- Check if user has SYSLOG ACCESS
SELECT usename, usesyslog
FROM pg_user
WHERE usename = 'datahub_user';
-- usesyslog should be 't' (true)
```

#### Cross-Cluster Lineage (Data Sharing)

For lineage across datashares, ensure:

1. DataHub user has `SHARE` privileges on datashares
2. Both producer and consumer clusters are ingested
3. `include_share_lineage: true` in configuration

```sql
-- Check datashare access
SELECT * FROM svv_datashares WHERE share_name = 'your_share';
```
