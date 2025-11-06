---
title: Setup
---

# Redshift Ingestion Guide: Setup & Prerequisites

To configure ingestion from Redshift, you'll need a [User](https://docs.aws.amazon.com/redshift/latest/gsg/t_adding_redshift_user_cmd.html) configured with the proper permission sets.

This setup guide will walk you through the steps you'll need to take in your Amazon Redshift cluster.

## Redshift Prerequisites

1. Connect to your Amazon Redshift cluster using an SQL client such as SQL Workbench/J or Amazon Redshift Query Editor with your Admin user.
2. Create a [Redshift User](https://docs.aws.amazon.com/redshift/latest/gsg/t_adding_redshift_user_cmd.html) that will be used to perform the metadata extraction if you don't have one already.
   For example:

```sql
CREATE USER datahub WITH PASSWORD 'Datahub1234';
```

## Redshift Setup

1. Grant the following permissions to your `datahub` user. For most users, the **recommended set** below will be sufficient:

### Recommended Permissions

For a typical provisioned cluster with default settings:

```sql
-- Core system access (required for lineage and usage statistics)
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Core metadata extraction (always required)
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;

-- Essential pg_catalog tables for table discovery
GRANT SELECT ON pg_catalog.pg_class TO datahub;
GRANT SELECT ON pg_catalog.pg_namespace TO datahub;
GRANT SELECT ON pg_catalog.pg_description TO datahub;
GRANT SELECT ON pg_catalog.pg_database TO datahub;
GRANT SELECT ON pg_catalog.pg_attribute TO datahub;
GRANT SELECT ON pg_catalog.pg_attrdef TO datahub;

-- Datashare lineage (enabled by default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;

-- Provisioned cluster materialized views
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;
```

### Additional Permissions Based on Your Configuration

**For Serverless Workgroups:**

```sql
-- Use these instead of stv_mv_info (from Provisioned section above)
GRANT SELECT ON pg_catalog.svv_user_info TO datahub;
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;
```

**For Shared Databases (Datashare Consumers):**

```sql
-- Required when is_shared_database = True
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub;
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub;
```

### Data Access Permissions (Required for Profiling/Classification)

**Important**: The above permissions only provide access to metadata. For data profiling, classification, or any feature that reads actual table data, you need:

```sql
-- Schema access (required to access tables within schemas)
GRANT USAGE ON SCHEMA public TO datahub;
GRANT USAGE ON SCHEMA your_schema_name TO datahub;

-- Table data access (required for profiling and classification)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;

-- For production environments (future tables/views):
-- IMPORTANT: Default privileges only apply to objects created by the user who runs this command
-- Option 1: If you (as admin) will create all future tables/views:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON TABLES TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON VIEWS TO datahub;

-- Option 2: If other users will create tables/views, run this for each user:
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;

-- Option 3: For all future users (requires superuser):
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;
```

:::caution Data Access vs Metadata Access

**The permissions are split into two categories:**

1. **System table permissions** (above) - Required for metadata extraction, lineage, and usage statistics
2. **Data access permissions** (this section) - Required for data profiling, classification, and any feature that reads actual table content

**Default privileges only apply to objects created by the user who ran the ALTER DEFAULT PRIVILEGES command.** If multiple users create tables in your schemas, you need to:

1. **Run the commands as each user**, OR
2. **Use `FOR ROLE other_user_name`** for each user who creates objects, OR
3. **Use `FOR ALL ROLES`** (requires superuser privileges)

**Common gotcha**: If User A runs `ALTER DEFAULT PRIVILEGES` and User B creates a table, DataHub won't have access to User B's table unless you used Option 2 or 3 above.

**Alternative approach**: Instead of default privileges, consider using a scheduled job to periodically grant access to new tables:

```sql
-- Run this periodically to catch new tables
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;
```

:::

## Next Steps

Once you've confirmed all of the above in Redshift, it's time to [move on](configuration.md) to configure the actual ingestion source within the DataHub UI.
