### Prerequisites

The DataHub Redshift connector requires specific database privileges to extract metadata, lineage, and usage statistics from your Amazon Redshift cluster.

## Permission Overview

DataHub requires three categories of permissions:

1. **System Table Access** - Access to Redshift system tables for lineage and usage statistics
2. **System View Access** - Access to system views for metadata discovery
3. **Data Access** - Access to user schemas and tables for profiling and classification

## System Table and View Permissions

Execute the following commands as a database superuser or user with sufficient privileges to grant these permissions:

```sql
-- Enable access to system log tables (STL_*, SVL_*, SYS_*)
-- Required for lineage extraction and usage statistics
ALTER USER datahub_user WITH SYSLOG ACCESS UNRESTRICTED;

-- Database and schema metadata
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub_user;
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub_user;

-- Table and column metadata
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub_user;
GRANT SELECT ON pg_catalog.svv_table_info TO datahub_user;

-- External table support (Amazon Redshift Spectrum)
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub_user;

-- User information for usage statistics
GRANT SELECT ON pg_catalog.svv_user_info TO datahub_user;      -- Serverless workgroups
GRANT SELECT ON pg_catalog.svl_user_info TO datahub_user;      -- Provisioned clusters

-- Materialized view information
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub_user;        -- Provisioned clusters
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub_user;        -- Serverless workgroups

-- Datashares (cross-cluster lineage)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub_user;

-- Table creation timestamps (provisioned clusters)
GRANT SELECT ON pg_catalog.pg_class_info TO datahub_user;
```

## Detailed Permission Breakdown

The following sections provide detailed information about which permissions are required for specific features and configurations.

### Core System Views (Always Required)

These system views are accessed in all DataHub configurations:

```sql
-- Schema discovery
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub_user;

-- Database information
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub_user;

-- Table metadata and statistics
GRANT SELECT ON pg_catalog.svv_table_info TO datahub_user;

-- External table support
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub_user;

-- Table creation timestamps
GRANT SELECT ON pg_catalog.pg_class_info TO datahub_user;
```

### Conditional System Tables (Feature Dependent)

#### Shared Database (Datashare Consumer)

```sql
-- Required when is_shared_database = True
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub_user;
```

#### Redshift Serverless Workgroups

```sql
-- Required for serverless workgroups
GRANT SELECT ON pg_catalog.svv_user_info TO datahub_user;
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub_user;
```

#### Redshift Provisioned Clusters

```sql
-- Required for provisioned clusters
GRANT SELECT ON pg_catalog.svl_user_info TO datahub_user;    -- Covered by SYSLOG ACCESS
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub_user;
```

#### Datashares Lineage

```sql
-- Required when include_share_lineage: true (default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub_user;
```

### Recommended Permission Set

For a typical provisioned cluster with default settings:

```sql
-- Core system access
ALTER USER datahub_user WITH SYSLOG ACCESS UNRESTRICTED;

-- Always required (7 grants)
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub_user;
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub_user;
GRANT SELECT ON pg_catalog.svv_table_info TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub_user;
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub_user;
GRANT SELECT ON pg_catalog.pg_class_info TO datahub_user;

-- Datashares (since include_share_lineage defaults to true)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub_user;

-- Provisioned cluster materialized views
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub_user;
```

#### Data Access Privileges (Required for Data Profiling and Classification)

**Important**: The system table permissions above only provide access to metadata. To enable data profiling, classification, or any feature that reads actual table data, you must grant additional privileges:

```sql
-- Grant USAGE privilege on schemas (required to access schema objects)
GRANT USAGE ON SCHEMA public TO datahub_user;
GRANT USAGE ON SCHEMA your_schema_name TO datahub_user;

-- Grant SELECT privilege on existing tables for data access
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub_user;

-- Grant privileges on future objects (recommended for production)
-- IMPORTANT: These must be run by each user who will create tables/views
-- OR by a superuser with FOR ROLE clause

-- Option 1: If you (as admin) will create all future tables/views:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON TABLES TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON VIEWS TO datahub_user;

-- Option 2: If other users will create tables/views, run this for each user:
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON VIEWS TO datahub_user;

-- Option 3: For all future users (requires superuser):
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub_user;
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
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub_user;
```

:::

#### Optional: Datashare Privileges

To enable cross-cluster lineage through datashares, grant the following privileges:

```sql
-- Grant SHARE privilege on datashares (replace with actual datashare names)
GRANT SHARE ON your_datashare_name TO datahub_user;
```

## Ingestion of multiple redshift databases, namespaces

- If multiple databases are present in the Redshift namespace (or provisioned cluster),
  you would need to set up a separate ingestion per database.

- Ingestion recipes of all databases in a particular redshift namespace should use same platform instance.

- If you've multiple redshift namespaces that you want to ingest within DataHub, it is highly recommended that
  you specify a platform_instance equivalent to namespace in recipe. It can be same as namespace id or other
  human readable name however it should be unique across all your redshift namespaces.

## Lineage

There are multiple lineage collector implementations as Redshift does not support table lineage out of the box.

### stl_scan_based

The stl_scan based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) and [stl_scan](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_SCAN.html) system tables to
discover lineage between tables.

**Pros:**

- Fast
- Reliable

**Cons:**

- Does not work with Spectrum/external tables because those scans do not show up in stl_scan table.
- If a table is depending on a view then the view won't be listed as dependency. Instead the table will be connected with the view's dependencies.

### sql_based

The sql_based based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) to discover all the insert queries
and uses sql parsing to discover the dependencies.

**Pros:**

- Works with Spectrum tables
- Views are connected properly if a table depends on it

**Cons:**

- Slow.
- Less reliable as the query parser can fail on certain queries

### mixed

Using both collector above and first applying the sql based and then the stl_scan based one.

**Pros:**

- Works with Spectrum tables
- Views are connected properly if a table depends on it
- A bit more reliable than the sql_based one only

**Cons:**

- Slow
- May be incorrect at times as the query parser can fail on certain queries

:::note

The redshift stl redshift tables which are used for getting data lineage retain at most seven days of log history, and sometimes closer to 2-5 days. This means you cannot extract lineage from queries issued outside that window.

:::

## Datashares Lineage

This is enabled by default, can be disabled via setting `include_share_lineage: False`

It is mandatory to run redshift ingestion of datashare producer namespace at least once so that lineage
shows up correctly after datashare consumer namespace is ingested.

## Profiling

Profiling runs sql queries on the redshift cluster to get statistics about the tables. To be able to do that, the user needs to have read access to the tables that should be profiled.

If you don't want to grant read access to the tables you can enable table level profiling which will get table statistics without reading the data.

```yaml
profiling:
  profile_table_level_only: true
```

### Caveats

:::note

**System table access**: The `SYSLOG ACCESS UNRESTRICTED` privilege gives the user visibility to data generated by other users. For example, STL_QUERY and STL_QUERYTEXT contain the full text of INSERT, UPDATE, and DELETE statements.

:::

:::note

**Datashare lineage**: For cross-cluster lineage through datashares, the DataHub user requires `SHARE` privileges on datashares in both producer and consumer namespaces. See the [Amazon Redshift datashare documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARES.html) for more information.

:::
