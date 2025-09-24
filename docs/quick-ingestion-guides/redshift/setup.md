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

1. Grant the following permissions to your `datahub` user. For most users, the **minimal set** below will be sufficient:

### Minimal Required Permissions (Recommended)

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

-- Datashare lineage (enabled by default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;

-- Choose ONE based on your Redshift type:
-- For Provisioned Clusters:
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;

-- For Serverless Workgroups:
-- GRANT SELECT ON pg_catalog.svv_user_info TO datahub;
-- GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;
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
-- IMPORTANT: Only works for objects created by the user running this command
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON TABLES TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON VIEWS TO datahub;
--
-- Alternative: Run this periodically to catch all new objects regardless of creator:
-- GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;
```

### Additional Permissions (Only if needed)

```sql
-- Only if using shared databases (datashare consumers):
-- GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub;
-- GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub;
```

## Next Steps

Once you've confirmed all of the above in Redshift, it's time to [move on](configuration.md) to configure the actual ingestion source within the DataHub UI.
