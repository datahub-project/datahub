### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Permission Overview

Three categories of permissions:

1. **System Table Access** - Access to Redshift system tables for lineage and usage statistics
2. **System View Access** - Access to system views for metadata discovery
3. **Data Access** - Access to user schemas and tables for profiling and classification

#### System Table and View Permissions

Execute as a superuser or user with grant privileges:

```sql
-- Core system access (required for lineage and usage statistics)
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Core metadata extraction (always required)
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;    -- Database information and properties
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;      -- Schema information within databases
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;      -- External schemas (Spectrum, federated)
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;           -- Table metadata, statistics, and properties
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;       -- External table definitions (Spectrum)
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;      -- External table column information
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;            -- Table creation timestamps and basic info

-- Essential pg_catalog tables for table discovery
GRANT SELECT ON pg_catalog.pg_class TO datahub;                 -- Table and view definitions
GRANT SELECT ON pg_catalog.pg_namespace TO datahub;             -- Schema namespace information
GRANT SELECT ON pg_catalog.pg_description TO datahub;           -- Table and column descriptions/comments
GRANT SELECT ON pg_catalog.pg_database TO datahub;              -- Database catalog information
GRANT SELECT ON pg_catalog.pg_attribute TO datahub;             -- Column definitions and properties
GRANT SELECT ON pg_catalog.pg_attrdef TO datahub;               -- Column default values
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership

-- Datashare lineage (enabled by default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information

-- Choose ONE based on your Redshift type:
-- For Provisioned Clusters:
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)

-- For Serverless Workgroups:
-- GRANT SELECT ON pg_catalog.svv_user_info TO datahub;          -- User information (serverless alternative)
-- GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;           -- Materialized view information (serverless)

-- Schema access (required to read tables in each schema)
GRANT USAGE ON SCHEMA <schema_to_ingest> TO datahub;             -- Replace with actual schema names
```

#### Detailed Permission Breakdown

Feature-specific permission requirements:

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Schema Discovery Issues

If you're not seeing all schemas or tables after following the setup steps, check the following:

##### Missing Schemas

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

##### Missing Tables Within Schemas

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

##### Configuration Issues

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

##### Permission Test Queries

Run these to verify your permissions are working:

```sql
-- Test core permissions
SELECT COUNT(*) FROM svv_redshift_schemas WHERE database_name = 'your_database';
SELECT COUNT(*) FROM svv_table_info WHERE database = 'your_database';

-- Test external permissions
SELECT COUNT(*) FROM svv_external_schemas;
SELECT COUNT(*) FROM svv_external_tables;
```

#### Data Profiling Issues

##### Profile Data Not Appearing

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

#### Lineage Issues

##### Missing Lineage Information

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

##### Cross-Cluster Lineage (Datashares)

For lineage across datashares, ensure:

1. DataHub user has `SHARE` privileges on datashares
2. Both producer and consumer clusters are ingested
3. `include_share_lineage: true` in configuration

```sql
-- Check datashare access
SELECT * FROM svv_datashares WHERE share_name = 'your_share';
```
