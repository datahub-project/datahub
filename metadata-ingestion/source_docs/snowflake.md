# Snowflake

## Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake]'`.

## Capabilities

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

:::tip

You can also get fine-grained usage statistics for Snowflake using the `snowflake-usage` source described below.

:::

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: snowflake
  config:
    username: user
    password: pass
    host_port: account_name

    warehouse: "COMPUTE_WH" # optional
    role: "sysadmin" # optional

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # driver_option: some-option

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                    | Required | Default                                                              | Description                                                                                                                                                                             |
| ------------------------ | -------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`               |          |                                                                      | Snowflake username.                                                                                                                                                                     |
| `password`               |          |                                                                      | Snowflake password.                                                                                                                                                                     |
| `host_port`              | ✅       |                                                                      | Snowflake host URL.                                                                                                                                                                     |
| `warehouse`              |          |                                                                      | Snowflake warehouse.                                                                                                                                                                    |
| `role`                   |          |                                                                      | Snowflake role.                                                                                                                                                                         |
| `env`                    |          | `"PROD"`                                                             | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`       |          |                                                                      | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `database_pattern.allow` |          |                                                                      | Regex pattern for databases to include in ingestion.                                                                                                                                    |
| `database_pattern.deny`  |          | `"^UTIL_DB$" `<br />`"^SNOWFLAKE$"`<br />`"^SNOWFLAKE_SAMPLE_DATA$"` | Regex pattern for databases to exclude from ingestion.                                                                                                                                  |
| `table_pattern.allow`    |          |                                                                      | Regex pattern for tables to include in ingestion.                                                                                                                                       |
| `table_pattern.deny`     |          |                                                                      | Regex pattern for tables to exclude from ingestion.                                                                                                                                     |
| `schema_pattern.allow`   |          |                                                                      | Regex pattern for schemas to include in ingestion.                                                                                                                                      |
| `schema_pattern.deny`    |          |                                                                      | Regex pattern for schemas to exclude from ingestion.                                                                                                                                    |
| `view_pattern.allow`     |          |                                                                      | Regex pattern for views to include in ingestion.                                                                                                                                        |
| `view_pattern.deny`      |          |                                                                      | Regex pattern for views to exclude from ingestion.                                                                                                                                      |
| `include_tables`         |          | `True`                                                               | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`          |          | `True`                                                               | Whether views should be ingested.                                                                                                                                                       |

# Snowflake Usage Stats

## Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake-usage]'`.

## Capabilities

This plugin extracts the following:

- Fetch a list of queries issued
- Fetch a list of tables and columns accessed (excludes views)
- Aggregate these statistics into buckets, by day or hour granularity

Note: the user/role must have access to the account usage table. The "accountadmin" role has this by default, and other roles can be [granted this permission](https://docs.snowflake.com/en/sql-reference/account-usage.html#enabling-account-usage-for-other-roles).

Note: the underlying access history views that we use are only available in Snowflake's enterprise edition or higher.

:::note

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source described above.

:::

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: snowflake-usage
  config:
    username: user
    password: pass
    host_port: account_name
    role: ACCOUNTADMIN
    env: PROD

    bucket_duration: "DAY"
    start_time: ~ # defaults to the last full day in UTC (or hour)
    end_time: ~ # defaults to the last full day in UTC (or hour)

    top_n_queries: 10 # number of queries to save for each table
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
