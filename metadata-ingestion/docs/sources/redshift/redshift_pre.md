### Prerequisites

To execute this source, your DataHub user will need specific privileges to access system tables and views for metadata extraction, lineage, and usage statistics.

The DataHub user requires two types of access:

1. **System table access** via `SYSLOG ACCESS UNRESTRICTED` privilege for lineage and usage data
2. **System view privileges** via explicit `GRANT SELECT` statements for metadata extraction

Run the following commands as a superuser or user with sufficient privileges to grant these permissions:

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

-- Data sharing (cross-cluster lineage)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub_user;

-- Table creation timestamps (provisioned clusters)
GRANT SELECT ON pg_catalog.pg_class_info TO datahub_user;
```

#### Optional: Data Profiling Privileges

To enable data profiling, grant the following privileges on your schemas and tables:

```sql
-- Grant USAGE privilege on schemas (required to access schema objects)
GRANT USAGE ON SCHEMA public TO datahub_user;
GRANT USAGE ON SCHEMA your_schema_name TO datahub_user;

-- Grant SELECT privilege on tables for profiling
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub_user;

-- Optional: Grant privileges on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON TABLES TO datahub_user;
```

#### Optional: Data Sharing Privileges

To enable cross-cluster lineage through data sharing, grant the following privileges:

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

## Data Sharing Lineage

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

**Data sharing lineage**: For cross-cluster lineage through data sharing, the DataHub user requires `SHARE` privileges on datashares in both producer and consumer namespaces. See the [Amazon Redshift data sharing documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARES.html) for more information.

:::
