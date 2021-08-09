# SQL Profiles

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[sql-profiles]'`.

The SQL-based profiler does not run alone, but rather can be enabled for other SQL-based sources.
Enabling profiling will slow down ingestion runs.

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

## Capabilities

Extracts:

- Row and column counts for each table
- For each column, if applicable:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

Supported SQL sources:

- AWS Athena
- BigQuery
- Druid
- Hive
- Microsoft SQL Server
- MySQL
- Oracle
- Postgres
- Redshift
- Snowflake
- Generic SQLAlchemy source

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: <sql-source> # can be bigquery, snowflake, etc - see above for the list
  config:
    # ... any other source-specific options ...

    # Options
    profiling:
      enabled: true

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                   | Required | Default | Description                                                             |
| ----------------------- | -------- | ------- | ----------------------------------------------------------------------- |
| `profiling.enabled`     |          | `False` | Whether profiling should be done.                                       |
| `profiling.limit`       |          |         | Max number of documents to profile. By default, profiles all documents. |
| `profiling.offset`      |          |         | Offset in documents to profile. By default, uses no offset.             |
| `profile_pattern.allow` |          |         | Regex pattern for tables to profile.                                    |
| `profile_pattern.deny`  |          |         | Regex pattern for tables to not profile.                                |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
