# ClickHouse

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[clickhouse]'`.

## Capabilities

This plugin extracts the following:

- Metadata for tables, views, materialized views and dictionaries
- Column types associated with each table(except *AggregateFunction and DateTime with timezone)
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)
- Table, view, materialized view and dictionary(with CLICKHOUSE source_type) lineage

:::tip

You can also get fine-grained usage statistics for ClickHouse using the `clickhouse-usage` source described below.

:::

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: clickhouse
  config:
    # Coordinates
    host_port: localhost:9000

    # Credentials
    username: user
    password: pass

    # Options
    platform_instance: DatabaseNameToBeIngested

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True

sink:
  # sink configs
```

<details>
  <summary>Extra options to use encryption connection or different interface</summary>

For the HTTP interface:
```yml
source:
  type: clickhouse
  config:
    host_port: localhost:8443
    protocol: https

```

For the Native interface:
```yml
source:
  type: clickhouse
  config:
    host_port: localhost:9440
    scheme: clickhouse+native
    secure: True
```

</details>

## Config details

Like all SQL-based sources, the ClickHouse integration supports:
- Stale Metadata Deletion: See [here](./stateful_ingestion.md) for more details on configuration.
- SQL Profiling: See [here](./sql_profiles.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                       | Required | Default                                                                    | Description                                                                                                                                                                             |
|-----------------------------|----------|----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                  |          |                                                                            | ClickHouse username.                                                                                                                                                                    |
| `password`                  |          |                                                                            | ClickHouse password.                                                                                                                                                                    |
| `host_port`                 | ✅        |                                                                            | ClickHouse host URL.                                                                                                                                                                    |
| `database`                  |          |                                                                            | ClickHouse database to connect.                                                                                                                                                         |
| `sqlalchemy_uri`            |          |          | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. |
| `env`                       |          | `"PROD"`                                                                   | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`         |          | None                                                                       | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`          |          |                                                                            | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`       |          |                                                                            | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`        |          |                                                                            | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`  |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`      |          |                                                                            | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`       |          |                                                                            | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase` |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`        |          |                                                                            | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`         |          |                                                                            | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`   |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`            |          | `True`                                                                     | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`             |          | `True`                                                                     | Whether views should be ingested.                                                                                                                                                       |
| `include_table_lineage`     |          | `True`                                                                     | Whether table lineage should be ingested.                                                                                                                                               |
| `profiling`                 |          | See the defaults for [profiling config](./sql_profiles.md#Config-details). | See [profiling config](./sql_profiles.md#Config-details).                                                                                                                               |


# ClickHouse Usage Stats

This plugin extracts usage statistics for datasets in ClickHouse. For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

Note: Usage information is computed by querying the system.query_log table. In case you have a cluster or need to apply additional transformation/filters you can create a view and put to the `query_log_table` setting.

## Setup
To install this plugin, run `pip install 'acryl-datahub[clickhouse-usage]'`.

## Capabilities
This plugin has the below functionalities -
1. For a specific dataset this plugin ingests the following statistics -
   1. top n queries.
   2. top users.
   3. usage of each column in the dataset.
2. Aggregation of these statistics into buckets, by day or hour granularity.

:::note

This source only does usage statistics. To get the tables, views, and schemas in your ClickHouse warehouse, ingest using the `clickhouse` source described above.

:::

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: clickhouse-usage
  config:
    # Coordinates
    host_port: db_host:port
    platform_instance: dev_cluster
    email_domain: acryl.io

    # Credentials
    username: username
    password: "password"

sink:
# sink configs
```

## Config details
Note that a `.` is used to denote nested fields in the YAML recipe.

By default, we extract usage stats for the last day, with the recommendation that this source is executed every day.

| Field                       | Required | Default                                                        | Description                                                                                                                                                                             |
|-----------------------------|----------|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                  |          |                                                                | ClickHouse username.                                                                                                                                                                    |
| `password`                  |          |                                                                | ClickHouse password.                                                                                                                                                                    |
| `host_port`                 | ✅       |                                                                | ClickHouse host URL.                                                                                                                                                                    |
| `database`                  |          |                                                                | ClickHouse database to connect.                                                                                                                                                         |
| `env`                       |          | `"PROD"`                                                       | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`         |          | None                                                           | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`          |          |                                                                | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `email_domain`              | ✅       |                                                                | Email domain of your organisation so users can be displayed on UI appropriately.                                                                                                        |
| `start_time`                |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Earliest date of usage to consider.                                                                                                                                                     |   
| `end_time`                  |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Latest date of usage to consider.                                                                                                                                                       |
| `top_n_queries`             |          | `10`                                                           | Number of top queries to save to each table.                                                                                                                                            |
| `include_top_n_queries`     |          | `True`                                                         | Whether to ingest top_n_queries.                                                                                                                                                        |
| `include_operational_stats` |          | `True`                                                         | Whether to ingest operational stats.                                                                                                                                                    |
| `bucket_duration`           |          | `"DAY"`                                                        | Size of the time window to aggregate usage stats.                                                                                                                                       |
| `format_sql_queries`        |          | `False`                                                        | Whether to format sql queries                                                                                                                                                           |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
