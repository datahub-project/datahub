# BigQuery

## Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery]'`.

## Capabilities

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

:::tip

You can also get fine-grained usage statistics for BigQuery using the `bigquery-usage` source described below.

:::

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: bigquery
  config:
    project_id: "my_project_id"
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                  | Required | Default  | Description                                                                                                                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `project_id`           |  | Autodetected | Project ID to ingest from. If not specified, will infer from environment. |
| `env`                  |        | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`     |        |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`  |        |          | Regex pattern for tables to include in ingestion.                                                                                                                                       |
| `table_pattern.deny`   |        |          | Regex pattern for tables to exclude from ingestion.                                                                                                                                     |
| `schema_pattern.allow` |        |          | Regex pattern for schemas to include in ingestion.                                                                                                                                      |
| `schema_pattern.deny`  |        |          | Regex pattern for schemas to exclude from ingestion.                                                                                                                                    |
| `view_pattern.allow`   |        |          | Regex pattern for views to include in ingestion.                                                                                                                                        |
| `view_pattern.deny`    |        |          | Regex pattern for views to exclude from ingestion.                                                                                                                                      |
| `include_tables`       |        | `True`   | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`        |        | `True`   | Whether views should be ingested.                                                                                                                                                       |

# BigQuery Usage Stats

## Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery-usage]'`.

## Capabilities

This plugin extracts the following:

- Fetch a list of queries issued
- Fetch a list of tables and columns accessed
- Aggregate these statistics into buckets, by day or hour granularity

Note: the client must have one of the following OAuth scopes, and should be authorized on all projects you'd like to ingest usage stats from.

- https://www.googleapis.com/auth/logging.read
- https://www.googleapis.com/auth/logging.admin
- https://www.googleapis.com/auth/cloud-platform.read-only
- https://www.googleapis.com/auth/cloud-platform

:::note

This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` source described above.

:::

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: bigquery-usage
  config:
    projects: # optional - can autodetect a single project from the environment
      - project_id_1
      - project_id_2
    options:
      # See https://googleapis.dev/python/logging/latest/client.html for details.
      credentials: ~ # optional - see docs

    # Common usage stats options
    bucket_duration: "DAY"
    start_time: ~ # defaults to the last full day in UTC (or hour)
    end_time: ~ # defaults to the last full day in UTC (or hour)

    top_n_queries: 10 # number of queries to save for each table

    env: PROD

    # Additional options to pass to google.cloud.logging_v2.client.Client
    extra_client_options:

    # To account for the possibility that the query event arrives after
    # the read event in the audit logs, we wait for at least `query_log_delay`
    # additional events to be processed before attempting to resolve BigQuery
    # job information from the logs. If `query_log_delay` is None, it gets treated
    # as an unlimited delay, which prioritizes correctness at the expense of memory usage.
    query_log_delay:

    # Correction to pad start_time and end_time with.
    # For handling the case where the read happens within our time range but the query
    # completion event is delayed and happens after the configured end time.
    max_query_duration:
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
