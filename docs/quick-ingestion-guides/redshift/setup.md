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

1. Grant the following permissions to your `datahub` user:

```sql
-- Enable access to system log tables (STL_*, SVL_*, SYS_*)
-- Required for lineage extraction and usage statistics
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Database and schema metadata
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;

-- Table and column metadata
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub;
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub;
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;

-- External table support (Amazon Redshift Spectrum)
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;

-- User information for usage statistics
GRANT SELECT ON pg_catalog.svv_user_info TO datahub;      -- Serverless workgroups
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;      -- Provisioned clusters

-- Materialized view information
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;        -- Provisioned clusters
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;        -- Serverless workgroups

-- Data sharing (cross-cluster lineage)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;

-- Table creation timestamps (provisioned clusters)
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;
```

## Next Steps

Once you've confirmed all of the above in Redshift, it's time to [move on](configuration.md) to configure the actual ingestion source within the DataHub UI.
