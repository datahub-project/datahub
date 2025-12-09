---
sidebar_position: 59
title: Redshift
slug: /generated/ingestion/sources/redshift
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/redshift.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Redshift
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database, Schema. |
| [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md) | ✅ | Optionally enabled via `classification.enabled`. |
| Column-level Lineage | ✅ | Optionally enabled via configuration (`mixed` or `sql_based` lineage needs to be enabled). |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration. |
| Dataset Usage | ✅ | Optionally enabled via `include_usage_statistics`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Optionally enabled via configuration. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:

- Metadata for databases, schemas, views and tables
- Column types associated with each table
- Table, row, and column statistics via optional SQL profiling
- Table lineage
- Usage statistics

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

**Important**: Make sure to grant USAGE on any schema you want to ingest from:

```sql
GRANT USAGE ON SCHEMA <schema_to_ingest> TO datahub;
```

## Detailed Permission Breakdown

The following sections provide detailed information about which permissions are required for specific features and configurations.

### Core System Views (Always Required)

These system views are accessed in all DataHub configurations:

```sql
-- Schema discovery
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;      -- Schema information within databases
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;      -- External schemas (Spectrum, federated)

-- Database information
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;    -- Database information and properties

-- Table metadata and statistics
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;           -- Table metadata, statistics, and properties

-- External table support
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;       -- External table definitions (Spectrum)
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;      -- External table column information

-- Table creation timestamps
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;            -- Table creation timestamps and basic info
```

### Conditional System Tables (Feature Dependent)

#### Shared Database (Datashare Consumer)

```sql
-- Required when is_shared_database = True
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub;       -- Table information in shared databases
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub;      -- Column information in shared databases
```

#### Redshift Serverless Workgroups

```sql
-- Required for serverless workgroups
GRANT SELECT ON pg_catalog.svv_user_info TO datahub;            -- User information (serverless alternative)
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;             -- Materialized view information (serverless)
```

#### Redshift Provisioned Clusters

```sql
-- Required for provisioned clusters
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership (superuser table)
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)
```

#### Datashares Lineage

```sql
-- Required when include_share_lineage: true (default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information
```

### Recommended Permission Set

For a typical provisioned cluster with default settings:

```sql
-- Core system access
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Always required
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
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership (superuser table)

-- Datashares (since include_share_lineage defaults to true)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information

-- Provisioned cluster materialized views
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)

-- Schema access (required to read tables in each schema)
GRANT USAGE ON SCHEMA <schema_to_ingest> TO datahub;             -- Replace with actual schema names
```

#### Data Access Privileges (Required for Data Profiling and Classification)

**Important**: The system table permissions above only provide access to metadata. To enable data profiling, classification, or any feature that reads actual table data, you must grant additional privileges:

```sql
-- Grant USAGE privilege on schemas (required to access schema objects)
GRANT USAGE ON SCHEMA public TO datahub;
GRANT USAGE ON SCHEMA your_schema_name TO datahub;

-- Grant SELECT privilege on existing tables for data access
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;

-- Grant privileges on future objects (recommended for production)
-- IMPORTANT: These must be run by each user who will create tables/views
-- OR by a superuser with FOR ROLE clause

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

#### Optional: Datashare Privileges

To enable cross-cluster lineage through datashares, grant the following privileges:

```sql
-- Grant SHARE privilege on datashares (replace with actual datashare names)
GRANT SHARE ON your_datashare_name TO datahub;
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

**Datashare lineage**: For cross-cluster lineage through datashares, the `datahub` user requires `SHARE` privileges on datashares in both producer and consumer namespaces. See the [Amazon Redshift datashare documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARES.html) for more information.

:::

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: redshift
  config:
    # Coordinates
    host_port: example.something.us-west-2.redshift.amazonaws.com:5439
    database: DemoDatabase

    # Credentials
    username: user
    password: pass

    # Options
    options:
      # driver_option: some-option

    include_table_lineage: true
    include_usage_statistics: true
    # The following options are only used when include_usage_statistics is true
    # it appends the domain after the redshift username which is extracted from the Redshift audit history
    # in the format username@email_domain
    email_domain: mydomain.com

    profiling:
      enabled: true
      # Only collect table level profiling information
      profile_table_level_only: true

  sink:
  # sink configs

#------------------------------------------------------------------------------
# Extra options when running Redshift behind a proxy</summary>
# This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
# See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15
#------------------------------------------------------------------------------

source:
  type: redshift
  config:
    host_port: my-proxy-hostname:5439

    options:
      connect_args:
        # check all available options here: https://pypi.org/project/redshift-connector/
        ssl_insecure: "false" # Specifies if IDP hosts server certificate will be verified

sink:
  # sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">host_port</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | host URL  |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | database <div className="default-line default-line-with-docs">Default: <span className="default-value">dev</span></div> |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The default schema to use if the sql parser fails to parse the schema with `sql_based` lineage collector <div className="default-line default-line-with-docs">Default: <span className="default-value">public</span></div> |
| <div className="path-line"><span className="path-main">email_domain</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Email domain of your organisation so users can be displayed on UI appropriately. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_profiling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_usage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">extra_client_options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |  <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract column level lineage. This config works with rest-sink only. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_copy_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether lineage should be collected from copy commands <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_share_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether lineage should be collected from datashares <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether table lineage should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_rename_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether we should follow `alter table ... rename to` statements when computing lineage.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_unload_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether lineage should be collected from unload commands <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Generate usage statistic. email_domain config parameter needs to be set if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates view->view and table->view lineage using DataHub's sql parser. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">is_serverless</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether target Redshift instance is serverless (alternative is provisioned cluster) <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">lineage_generate_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to generate queries entities for the SQL-based lineage collector. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">match_fully_qualified_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether `schema_pattern` is matched against fully qualified schema name `<database>.<schema>`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.  |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | password <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">patch_custom_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to patch custom properties on existing datasets rather than replace. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">resolve_temp_table_in_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to resolve temp table appear in lineage to upstream permanent tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">skip_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to skip EXTERNAL tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">sqlalchemy_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">table_lineage_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "sql_based", "stl_scan_based", "mixed"  |
| <div className="path-line"><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">use_file_backed_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use a file backed cache for the view definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">s3_lineage_config</span></div> <div className="type-name-line"><span className="type-name">S3LineageProviderConfig</span></div> | Any source that produces s3 lineage from/to Datasets should inherit this class.  |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.</span><span className="path-main">ignore_non_path_spec_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ignore paths that are not match in path_specs. It only applies if path_specs are specified. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.</span><span className="path-main">strip_urls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Strip filename from s3 url. It only applies if path_specs are not specified. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.</span><span className="path-main">path_specs</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of PathSpec. See below the details about PathSpec <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.</span><span className="path-main">PathSpec</span></div> <div className="type-name-line"><span className="type-name">PathSpec</span></div> |   |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">include</span>&nbsp;<abbr title="Required if PathSpec is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.  |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">allow_double_stars</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Allow double stars in the include path. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">autodetect_partitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024` <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">default_extension</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">enable_compression</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable or disable processing compressed files. Currently .gz and .bz files are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">include_hidden_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include hidden folders in the traversal (folders starting with . or _ <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">sample_files</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">table_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Display name of the dataset.Combination of named variables from include path and strings <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">traversal_method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ALL", "MIN_MAX", "MAX"  |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">exclude</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | list of paths in glob pattern which will be excluded while scanning for the datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.exclude.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">file_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;csv&#x27;, &#x27;tsv&#x27;, &#x27;json&#x27;, &#x27;parquet&#x27;, &#x27;avro&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.file_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.</span><span className="path-main">tables_filter_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">s3_lineage_config.path_specs.PathSpec.tables_filter_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes to use for classification. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">14</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#123;&#x27;type&#x27;: &#x27;datahub&#x27;, &#x27;config&#x27;: None&#125;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">DynamicTypedClassifierConfig</span></div> <div className="type-name-line"><span className="type-name">DynamicTypedClassifierConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if DynamicTypedClassifierConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. For DataHub,  use `datahub`  |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">config</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | The configuration required for initializing the classifier. If not specified, uses defaults for classifer type. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GEProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">70</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `Snowflake`, `BigQuery` and `Databricks`. Supported for `Oracle` based on calculated size from gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "ClassificationConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether classification should be used to auto-detect glossary terms",
          "title": "Enabled",
          "type": "boolean"
        },
        "sample_size": {
          "default": 100,
          "description": "Number of sample values used for classification.",
          "title": "Sample Size",
          "type": "integer"
        },
        "max_workers": {
          "default": 14,
          "description": "Number of worker processes to use for classification. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "table_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
        },
        "column_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format."
        },
        "info_type_to_term": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Optional mapping to provide glossary term identifier for info type",
          "title": "Info Type To Term",
          "type": "object"
        },
        "classifiers": {
          "default": [
            {
              "type": "datahub",
              "config": null
            }
          ],
          "description": "Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
          "items": {
            "$ref": "#/$defs/DynamicTypedClassifierConfig"
          },
          "title": "Classifiers",
          "type": "array"
        }
      },
      "title": "ClassificationConfig",
      "type": "object"
    },
    "DynamicTypedClassifierConfig": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "description": "The type of the classifier to use. For DataHub,  use `datahub`",
          "title": "Type",
          "type": "string"
        },
        "config": {
          "anyOf": [
            {},
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.",
          "title": "Config"
        }
      },
      "required": [
        "type"
      ],
      "title": "DynamicTypedClassifierConfig",
      "type": "object"
    },
    "FolderTraversalMethod": {
      "enum": [
        "ALL",
        "MIN_MAX",
        "MAX"
      ],
      "title": "FolderTraversalMethod",
      "type": "string"
    },
    "GEProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 70,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `Snowflake`, `BigQuery` and `Databricks`. Supported for `Oracle` based on calculated size from gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Fixed list of tags to ignore sampling. If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        }
      },
      "title": "GEProfilingConfig",
      "type": "object"
    },
    "LineageMode": {
      "enum": [
        "sql_based",
        "stl_scan_based",
        "mixed"
      ],
      "title": "LineageMode",
      "type": "string"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "PathSpec": {
      "additionalProperties": false,
      "properties": {
        "include": {
          "description": "Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.",
          "title": "Include",
          "type": "string"
        },
        "exclude": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": [],
          "description": "list of paths in glob pattern which will be excluded while scanning for the datasets",
          "title": "Exclude"
        },
        "file_types": {
          "default": [
            "csv",
            "tsv",
            "json",
            "parquet",
            "avro"
          ],
          "description": "Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
          "items": {
            "type": "string"
          },
          "title": "File Types",
          "type": "array"
        },
        "default_extension": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
          "title": "Default Extension"
        },
        "table_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Display name of the dataset.Combination of named variables from include path and strings",
          "title": "Table Name"
        },
        "enable_compression": {
          "default": true,
          "description": "Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
          "title": "Enable Compression",
          "type": "boolean"
        },
        "sample_files": {
          "default": true,
          "description": "Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
          "title": "Sample Files",
          "type": "boolean"
        },
        "allow_double_stars": {
          "default": false,
          "description": "Allow double stars in the include path. This can affect performance significantly if enabled",
          "title": "Allow Double Stars",
          "type": "boolean"
        },
        "autodetect_partitions": {
          "default": true,
          "description": "Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024`",
          "title": "Autodetect Partitions",
          "type": "boolean"
        },
        "traversal_method": {
          "$ref": "#/$defs/FolderTraversalMethod",
          "default": "MAX",
          "description": "Method to traverse the folder. ALL: Traverse all the folders, MIN_MAX: Traverse the folders by finding min and max value, MAX: Traverse the folder with max value"
        },
        "include_hidden_folders": {
          "default": false,
          "description": "Include hidden folders in the traversal (folders starting with . or _",
          "title": "Include Hidden Folders",
          "type": "boolean"
        },
        "tables_filter_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "The tables_filter_pattern configuration field uses regular expressions to filter the tables part of the Pathspec for ingestion, allowing fine-grained control over which tables are included or excluded based on specified patterns. The default setting allows all tables."
        }
      },
      "required": [
        "include"
      ],
      "title": "PathSpec",
      "type": "object"
    },
    "S3LineageProviderConfig": {
      "additionalProperties": false,
      "description": "Any source that produces s3 lineage from/to Datasets should inherit this class.",
      "properties": {
        "path_specs": {
          "default": [],
          "description": "List of PathSpec. See below the details about PathSpec",
          "items": {
            "$ref": "#/$defs/PathSpec"
          },
          "title": "Path Specs",
          "type": "array"
        },
        "strip_urls": {
          "default": true,
          "description": "Strip filename from s3 url. It only applies if path_specs are not specified.",
          "title": "Strip Urls",
          "type": "boolean"
        },
        "ignore_non_path_spec_path": {
          "default": false,
          "description": "Ignore paths that are not match in path_specs. It only applies if path_specs are specified.",
          "title": "Ignore Non Path Spec Path",
          "type": "boolean"
        }
      },
      "title": "S3LineageProviderConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'"
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "classification": {
      "$ref": "#/$defs/ClassificationConfig",
      "default": {
        "enabled": false,
        "sample_size": 100,
        "max_workers": 14,
        "table_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "column_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "info_type_to_term": {},
        "classifiers": [
          {
            "config": null,
            "type": "datahub"
          }
        ]
      },
      "description": "For details, refer to [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md)."
    },
    "enable_stateful_profiling": {
      "default": true,
      "description": "Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated. ",
      "title": "Enable Stateful Profiling",
      "type": "boolean"
    },
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. ",
      "title": "Enable Stateful Lineage Ingestion",
      "type": "boolean"
    },
    "bucket_duration": {
      "$ref": "#/$defs/BucketDuration",
      "default": "DAY",
      "description": "Size of the time window to aggregate usage stats."
    },
    "end_time": {
      "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
      "format": "date-time",
      "title": "End Time",
      "type": "string"
    },
    "start_time": {
      "default": null,
      "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
      "format": "date-time",
      "title": "Start Time",
      "type": "string"
    },
    "enable_stateful_usage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. ",
      "title": "Enable Stateful Usage Ingestion",
      "type": "boolean"
    },
    "top_n_queries": {
      "default": 10,
      "description": "Number of top queries to save to each table.",
      "exclusiveMinimum": 0,
      "title": "Top N Queries",
      "type": "integer"
    },
    "user_email_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for user emails to filter in usage."
    },
    "include_operational_stats": {
      "default": true,
      "description": "Whether to display operational stats.",
      "title": "Include Operational Stats",
      "type": "boolean"
    },
    "include_read_operational_stats": {
      "default": false,
      "description": "Whether to report read operational stats. Experimental.",
      "title": "Include Read Operational Stats",
      "type": "boolean"
    },
    "format_sql_queries": {
      "default": false,
      "description": "Whether to format sql queries",
      "title": "Format Sql Queries",
      "type": "boolean"
    },
    "include_top_n_queries": {
      "default": true,
      "description": "Whether to ingest the top_n_queries.",
      "title": "Include Top N Queries",
      "type": "boolean"
    },
    "email_domain": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Email domain of your organisation so users can be displayed on UI appropriately.",
      "title": "Email Domain"
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "s3_lineage_config": {
      "$ref": "#/$defs/S3LineageProviderConfig",
      "default": {
        "path_specs": [],
        "strip_urls": true,
        "ignore_non_path_spec_path": false
      },
      "description": "Common config for S3 lineage generation"
    },
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "options": {
      "additionalProperties": true,
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.",
      "title": "Options",
      "type": "object"
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered."
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "include_views": {
      "default": true,
      "description": "Whether views should be ingested.",
      "title": "Include Views",
      "type": "boolean"
    },
    "include_tables": {
      "default": true,
      "description": "Whether tables should be ingested.",
      "title": "Include Tables",
      "type": "boolean"
    },
    "include_table_location_lineage": {
      "default": true,
      "description": "If the source supports it, include table lineage to the underlying storage location.",
      "title": "Include Table Location Lineage",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "Populates view->view and table->view lineage using DataHub's sql parser.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "include_view_column_lineage": {
      "default": true,
      "description": "Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled.",
      "title": "Include View Column Lineage",
      "type": "boolean"
    },
    "use_file_backed_cache": {
      "default": true,
      "description": "Whether to use a file backed cache for the view definitions.",
      "title": "Use File Backed Cache",
      "type": "boolean"
    },
    "profiling": {
      "$ref": "#/$defs/GEProfilingConfig",
      "default": {
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "limit": null,
        "offset": null,
        "profile_table_level_only": false,
        "include_field_null_count": true,
        "include_field_distinct_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": true,
        "include_field_stddev_value": true,
        "include_field_quantiles": false,
        "include_field_distinct_value_frequencies": false,
        "include_field_histogram": false,
        "include_field_sample_values": true,
        "max_workers": 70,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null,
        "use_sampling": true,
        "sample_size": 10000,
        "profile_external_tables": false,
        "tags_to_ignore_sampling": null,
        "profile_nested_fields": false
      }
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "username",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "password",
      "title": "Password"
    },
    "host_port": {
      "description": "host URL",
      "title": "Host Port",
      "type": "string"
    },
    "database": {
      "default": "dev",
      "description": "database",
      "title": "Database",
      "type": "string"
    },
    "sqlalchemy_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
      "title": "Sqlalchemy Uri"
    },
    "default_schema": {
      "default": "public",
      "description": "The default schema to use if the sql parser fails to parse the schema with `sql_based` lineage collector",
      "title": "Default Schema",
      "type": "string"
    },
    "is_serverless": {
      "default": false,
      "description": "Whether target Redshift instance is serverless (alternative is provisioned cluster)",
      "title": "Is Serverless",
      "type": "boolean"
    },
    "lineage_generate_queries": {
      "default": true,
      "description": "Whether to generate queries entities for the SQL-based lineage collector.",
      "title": "Lineage Generate Queries",
      "type": "boolean"
    },
    "include_table_lineage": {
      "default": true,
      "description": "Whether table lineage should be ingested.",
      "title": "Include Table Lineage",
      "type": "boolean"
    },
    "include_copy_lineage": {
      "default": true,
      "description": "Whether lineage should be collected from copy commands",
      "title": "Include Copy Lineage",
      "type": "boolean"
    },
    "include_share_lineage": {
      "default": true,
      "description": "Whether lineage should be collected from datashares",
      "title": "Include Share Lineage",
      "type": "boolean"
    },
    "include_usage_statistics": {
      "default": false,
      "description": "Generate usage statistic. email_domain config parameter needs to be set if enabled",
      "title": "Include Usage Statistics",
      "type": "boolean"
    },
    "include_unload_lineage": {
      "default": true,
      "description": "Whether lineage should be collected from unload commands",
      "title": "Include Unload Lineage",
      "type": "boolean"
    },
    "include_table_rename_lineage": {
      "default": true,
      "description": "Whether we should follow `alter table ... rename to` statements when computing lineage. ",
      "title": "Include Table Rename Lineage",
      "type": "boolean"
    },
    "table_lineage_mode": {
      "$ref": "#/$defs/LineageMode",
      "default": "mixed",
      "description": "Which table lineage collector mode to use. Available modes are: [stl_scan_based, sql_based, mixed]"
    },
    "extra_client_options": {
      "additionalProperties": true,
      "default": {},
      "title": "Extra Client Options",
      "type": "object"
    },
    "match_fully_qualified_names": {
      "default": false,
      "description": "Whether `schema_pattern` is matched against fully qualified schema name `<database>.<schema>`.",
      "title": "Match Fully Qualified Names",
      "type": "boolean"
    },
    "extract_column_level_lineage": {
      "default": true,
      "description": "Whether to extract column level lineage. This config works with rest-sink only.",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "patch_custom_properties": {
      "default": true,
      "description": "Whether to patch custom properties on existing datasets rather than replace.",
      "title": "Patch Custom Properties",
      "type": "boolean"
    },
    "resolve_temp_table_in_lineage": {
      "default": true,
      "description": "Whether to resolve temp table appear in lineage to upstream permanent tables.",
      "title": "Resolve Temp Table In Lineage",
      "type": "boolean"
    },
    "skip_external_tables": {
      "default": false,
      "description": "Whether to skip EXTERNAL tables.",
      "title": "Skip External Tables",
      "type": "boolean"
    }
  },
  "required": [
    "host_port"
  ],
  "title": "RedshiftConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

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

#### Cross-Cluster Lineage (Datashares)

For lineage across datashares, ensure:

1. DataHub user has `SHARE` privileges on datashares
2. Both producer and consumer clusters are ingested
3. `include_share_lineage: true` in configuration

```sql
-- Check datashare access
SELECT * FROM svv_datashares WHERE share_name = 'your_share';
```

### Code Coordinates
- Class Name: `datahub.ingestion.source.redshift.redshift.RedshiftSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/redshift/redshift.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Redshift, feel free to ping us on [our Slack](https://datahub.com/slack).
