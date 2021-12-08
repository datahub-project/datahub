# BigQuery

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)
- Table level lineage.

:::tip

You can also get fine-grained usage statistics for BigQuery using the `bigquery-usage` source described below.

:::

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: bigquery
  config:
    # Coordinates
    project_id: my_project_id

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

As a SQL-based service, the Athena integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                       | Required | Default                                                                  | Description                                                                                                                                                                             |
| --------------------------- | -------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `project_id`                |          | Autodetected                                                             | Project ID to ingest from. If not specified, will infer from environment.                                                                                                               |
| `env`                       |          | `"PROD"`                                                                 | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`          |          |                                                                          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`       |          |                                                                          | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`        |          |                                                                          | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`  |          | `True`                                                                   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`      |          |                                                                          | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`       |          |                                                                          | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase` |          | `True`                                                                   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`        |          |                                                                          | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`         |          |                                                                          | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`   |          | `True`                                                                   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`            |          | `True`                                                                   | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`             |          | `True`                                                                   | Whether views should be ingested.                                                                                                                                                       |
| `include_table_lineage`     |          | `True`                                                                   | Whether table level lineage should be ingested and processed.                                                                                                                           |
| `max_query_duration`        |          | `15`                                                                     | A time buffer in minutes to adjust start_time and end_time while querying Bigquery audit logs.                                                                                          |
| `start_time`                |          | Start of last full day in UTC (or hour, depending on `bucket_duration`)  | Earliest time of lineage data to consider.                                                                                                                                              |
| `end_time`                  |          | End of last full day in UTC (or hour, depending on `bucket_duration`)    | Latest time of lineage data to consider.                                                                                                                                                |
| `extra_client_options`      |          |                                                                          | Additional options to pass to `google.cloud.logging_v2.client.Client`.                                                                                                                  |

The following parameters are only relevant if include_table_lineage is set to true:

- max_query_duration 
- start_time 
- end_time 
- extra_client_options

Note: Since bigquery source also supports dataset level lineage, the auth client will require additional permissions to be able to access the google audit logs. Refer the permissions section in bigquery-usage section below which also accesses the audit logs.

## Compatibility

Coming soon!

## BigQuery Usage Stats

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery-usage]'`.

### Capabilities

This plugin extracts the following:

- Statistics on queries issued and tables and columns accessed (excludes views)
- Aggregation of these statistics into buckets, by day or hour granularity

Note: the client must have one of the following OAuth scopes, and should be authorized on all projects you'd like to ingest usage stats from.

- https://www.googleapis.com/auth/logging.read
- https://www.googleapis.com/auth/logging.admin
- https://www.googleapis.com/auth/cloud-platform.read-only
- https://www.googleapis.com/auth/cloud-platform

:::note

1. This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` source described above.
2. Depending on the compliance policies setup for the bigquery instance, sometimes logging.read permission is not sufficient. In that case, use either admin or private log viewer permission. 

:::

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: bigquery-usage
  config:
    # Coordinates
    projects:
      - project_id_1
      - project_id_2

    # Options
    top_n_queries: 10

sink:
  # sink configs
```

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

By default, we extract usage stats for the last day, with the recommendation that this source is executed every day.

| Field                  | Required | Default                                                        | Description                                                                                                                                                                                                                                                                                                                                                                            |
| ---------------------- | -------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `projects`             |          |                                                                |                                                                                                                                                                                                                                                                                                                                                                                        |
| `extra_client_options` |          |                                                                |                                                                                                                                                                                                                                                                                                                                                                                        |
| `env`                  |          | `"PROD"`                                                       | Environment to use in namespace when constructing URNs.                                                                                                                                                                                                                                                                                                                                |
| `start_time`           |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Earliest date of usage logs to consider.                                                                                                                                                                                                                                                                                                                                               |
| `end_time`             |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Latest date of usage logs to consider.                                                                                                                                                                                                                                                                                                                                                 |
| `top_n_queries`        |          | `10`                                                           | Number of top queries to save to each table.                                                                                                                                                                                                                                                                                                                                           |
| `extra_client_options` |          |                                                                | Additional options to pass to `google.cloud.logging_v2.client.Client`.                                                                                                                                                                                                                                                                                                                 |
| `query_log_delay`     |          |                                                                | To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least `query_log_delay` additional events to be processed before attempting to resolve BigQuery job information from the logs. If `query_log_delay` is `None`, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage. |
| `max_query_duration`   |          | `15`                                                           | Correction to pad `start_time` and `end_time` with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.                                                                                                                                                                            |
| `table_pattern.allow`  |          |                                                                | List of regex patterns for tables to include in ingestion.                                                                                                                                                                                                                                                                                                                             |
| `table_pattern.deny`  |          |                                                                | List of regex patterns for tables to exclude in ingestion.                                                                                                                                                                                                                                                                                                                              |

### Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
