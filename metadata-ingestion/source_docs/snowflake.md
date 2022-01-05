# Snowflake

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake]'`.

### Prerequisites

In order to execute this source, your Snowflake user will need to have specific privileges granted to it for reading metadata
from your warehouse. You can create a DataHub-specific role, assign it the required privileges, and assign it to a new DataHub user 
by executing the following Snowflake commands from a user with the `ACCOUNTADMIN` role: 

```sql
create or replace role datahub_role;

// Grant privileges to use and select from your target warehouses / dbs / schemas / tables
grant operate, usage on warehouse <your-warehouse> to role datahub_role;
grant usage on <your-database> to role datahub_role;
grant usage on all schemas in database <your-database> to role datahub_role; 
grant select on all tables in database <your-database> to role datahub_role; 
grant select on all external tables in database <your-database> to role datahub_role;
grant select on all views in database <your-database> to role datahub_role;

// Grant privileges for all future schemas and tables created in a warehouse 
grant usage on future schemas in database "<your-database>" to role datahub_role;
grant select on future tables in database "<your-database>" to role datahub_role;

// Create a new DataHub user and assign the DataHub role to it 
create user datahub_user display_name = 'DataHub' password='' default_role = datahub_role default_warehouse = '<your-warehouse>';

// Grant the datahub_role to the new DataHub user. 
grant role datahub_role to user datahub_user;
```

This represents the bare minimum privileges required to extract databases, schemas, views, tables from Snowflake. 

If you plan to enable extraction of table lineage, via the `include_table_lineage` config flag, you'll also need to grant privileges
to access the Snowflake Account Usage views. You can execute the following using the `ACCOUNTADMIN` role to do so:

```sql
grant imported privileges on database snowflake to role datahub_role;
```

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views and tables
- Column types associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)
- Table lineage.

:::tip

You can also get fine-grained usage statistics for Snowflake using the `snowflake-usage` source described below.

:::

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: snowflake
  config:
    # Coordinates
    host_port: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: user
    password: pass
    role: "accountadmin"

sink:
  # sink configs
```

## Config details

Like all SQL-based sources, the Snowflake integration supports:
- Stale Metadata Deletion: See [here](./stateful_ingestion.md) for more details on configuration.
- SQL Profiling: See [here](./sql_profiles.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                         | Required | Default                                                                     | Description                                                                                                                                                                             |
| ----------------------------- | -------- | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`                    |          |                                                                             | Snowflake username.                                                                                                                                                                     |
| `password`                    |          |                                                                             | Snowflake password.                                                                                                                                                                     |
| `host_port`                   | ✅       |                                                                             | Snowflake host URL.                                                                                                                                                                     |
| `warehouse`                   |          |                                                                             | Snowflake warehouse.                                                                                                                                                                    |
| `role`                        |          |                                                                             | Snowflake role.                                                                                                                                                                         |
| `env`                         |          | `"PROD"`                                                                    | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`            |          |                                                                             | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `database_pattern.allow`      |          |                                                                             | List of regex patterns for databases to include in ingestion.                                                                                                                           |
| `database_pattern.deny`       |          | `"^UTIL_DB$" `<br />`"^SNOWFLAKE$"`<br />`"^SNOWFLAKE_SAMPLE_DATA$"`        | List of regex patterns for databases to exclude from ingestion.                                                                                                                         |
| `database_pattern.ignoreCase` |          | `True`                                                                      | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `table_pattern.allow`         |          |                                                                             | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`          |          |                                                                             | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`    |          | `True`                                                                      | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`        |          |                                                                             | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`         |          |                                                                             | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase`   |          | `True`                                                                      | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`          |          |                                                                             | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`           |          |                                                                             | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`     |          | `True`                                                                      | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`              |          | `True`                                                                      | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`               |          | `True`                                                                      | Whether views should be ingested.                                                                                                                                                       |
| `include_table_lineage`       |          | `True`                                                                      | Whether table lineage should be ingested.                                                                                                                                               |
| `bucket_duration`             |          | `"DAY"`                                                                     | Duration to bucket lineage data extraction by. Can be `"DAY"` or `"HOUR"`.                                                                                                              |
| `start_time`                  |          | Start of last full day in UTC (or hour, depending on `bucket_duration`)     | Earliest time of lineage data to consider.                                                                                                                                              |
| `end_time`                    |          | End of last full day in UTC (or hour, depending on `bucket_duration`)       | Latest time of lineage data to consider.                                                                                                                                                |
| `profiling`                   |          | See the defaults for [profiling config](./sql_profiles.md#Config-details).   | See [profiling config](./sql_profiles.md#Config-details).                                                                                                                              |
| `include_table_lineage`       |          | `True`                                                                      | If enabled, populates the snowflake table-to-table lineage. Requires role to be `accountadmin`                                                                                          |     


## Compatibility

Table lineage requires Snowflake's [Access History](https://docs.snowflake.com/en/user-guide/access-history.html) feature.

# Snowflake Usage Stats

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake-usage]'`.

### Prerequisites 

In order to execute the snowflake-usage source, your Snowflake user will need to have specific privileges granted to it. Specifically,
you'll need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables, using which the DataHub source extracts information. Assuming
you've followed the steps outlined above to create a DataHub-specific User & Role, you'll simply need to execute the following commands
in Snowflake from a user with the `ACCOUNTADMIN` role: 

```sql
grant imported privileges on database snowflake to role datahub_role;
```

## Capabilities

This plugin extracts the following:

- Statistics on queries issued and tables and columns accessed (excludes views)
- Aggregation of these statistics into buckets, by day or hour granularity

Note: the user/role must have access to the account usage table. The "accountadmin" role has this by default, and other roles can be [granted this permission](https://docs.snowflake.com/en/sql-reference/account-usage.html#enabling-account-usage-for-other-roles).

Note: the underlying access history views that we use are only available in Snowflake's enterprise edition or higher.

:::note

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source described above.

:::

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: snowflake-usage
  config:
    # Coordinates
    host_port: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: user
    password: pass
    role: "sysadmin"

    # Options
    top_n_queries: 10
    email_domain: mycompany.com
sink:
  # sink configs
```

## Config details

Snowflake integration also supports prevention of redundant reruns for the same data. See [here](./stateful_ingestion.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.


| Field              | Required | Default                                                             | Description                                                                      |
|--------------------|----------|---------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `username`         |          |                                                                     | Snowflake username.                                                              |
| `password`         |          |                                                                     | Snowflake password.                                                              |
| `host_port`        | ✅        |                                                                     | Snowflake host URL.                                                              |
| `warehouse`        |          |                                                                     | Snowflake warehouse.                                                             |
| `role`             |          |                                                                     | Snowflake role.                                                                  |
| `env`              |          | `"PROD"`                                                            | Environment to use in namespace when constructing URNs.                          |
| `bucket_duration`  |          | `"DAY"`                                                             | Duration to bucket usage events by. Can be `"DAY"` or `"HOUR"`.                  |
| `email_domain`     |          |                                                                     | Email domain of your organisation so users can be displayed on UI appropriately. |
| `start_time`       |          | Last full day in UTC (or hour, depending on `bucket_duration`)      | Earliest date of usage logs to consider.                                         |
| `end_time`         |          | Last full day in UTC (or hour, depending on `bucket_duration`)      | Latest date of usage logs to consider.                                           |
| `top_n_queries`    |          | `10`                                                                | Number of top queries to save to each table.                                     |
| `database_pattern` |          | `"^UTIL_DB$" `<br />`"^SNOWFLAKE$"`<br />`"^SNOWFLAKE_SAMPLE_DATA$" | Allow/deny patterns for db in snowflake dataset names.                           |
| `schema_pattern`   |          |                                                                     | Allow/deny patterns for schema in snowflake dataset names.                       |
| `view_pattern`     |          |                                                                     | Allow/deny patterns for views in snowflake dataset names.                        |
| `table_pattern`    |          |                                                                     | Allow/deny patterns for tables in snowflake dataset names.                       |

:::caution

User's without email address will be ignored from usage if you don't set `email_domain` property.

:::



# Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
