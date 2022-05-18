# SQL Profiles

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[sql-profiles]'` (prior to datahub version `0.8.16.0`).
In the versions after `0.8.16.0`, this gets installed along with the SQL-based source itself.

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

- [AWS Athena](./athena.md)
- [BigQuery](./bigquery.md)
- [Druid](./druid.md)
- [Hive](./hive.md)
- [Microsoft SQL Server](./mssql.md)
- [MySQL](./mysql.md)
- [Oracle](./oracle.md)
- [Postgres](./postgres.md)
- [Redshift](./redshift.md)
- [Snowflake](./snowflake.md)
- [Generic SQLAlchemy source](./sqlalchemy.md)
- [Trino](./trino.md)

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

| Field                                                | Required | Default              | Description                                                                                                                                                                                                                                                                                                                                                        |
|------------------------------------------------------|----------|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `profiling.enabled`                                  |          | `False`              | Whether profiling should be done.                                                                                                                                                                                                                                                                                                                                  |
| `profiling.bigquery_temp_table_schema`               |          |                      | On bigquery for profiling partitioned tables needs to create temporary views. You have to define a schema where these will be created. Views will be cleaned up after profiler runs. (Great expectation tech details about this [here](https://legacy.docs.greatexpectations.io/en/0.9.0/reference/integrations/bigquery.html#custom-queries-with-sql-datasource). |
| `profiling.limit`                                    |          |                      | Max number of documents to profile. By default, profiles all documents.                                                                                                                                                                                                                                                                                            |
| `profiling.offset`                                   |          |                      | Offset in documents to profile. By default, uses no offset.                                                                                                                                                                                                                                                                                                        |
| `profiling.max_workers`                              |          | `5 * os.cpu_count()` | Number of worker threads to use for profiling. Set to 1 to disable.                                                                                                                                                                                                                                                                                                |
| `profiling.query_combiner_enabled`                   |          | `True`               | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.                                                                                                                                                              |
| `profile_pattern.allow`                              |          | `*`                  | List of regex patterns for tables or table columns to profile. Defaults to all.                                                                                                                                                                                                                                                                                    |
| `profile_pattern.deny`                               |          |                      | List of regex patterns for tables or table columns to not profile. Defaults to none.                                                                                                                                                                                                                                                                               |
| `profile_pattern.ignoreCase`                         |          | `True`               | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                                                        |
| `profiling.turn_off_expensive_profiling_metrics`     |          | False                | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.                                                                                                                                                     |
| `profiling.max_number_of_fields_to_profile`          |          | `None`               | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.                                                                                                                                                     |
| `profiling.profile_table_level_only`                 |          | False                | Whether to perform profiling at table-level only, or include column-level profiling as well.                                                                                                                                                                                                                                                                       |
| `profiling.include_field_null_count`                 |          | `True`               | Whether to profile for the number of nulls for each column.                                                                                                                                                                                                                                                                                                        |
| `profiling.include_field_min_value`                  |          | `True`               | Whether to profile for the min value of numeric columns.                                                                                                                                                                                                                                                                                                           |
| `profiling.include_field_max_value`                  |          | `True`               | Whether to profile for the max value of numeric columns.                                                                                                                                                                                                                                                                                                           |
| `profiling.include_field_mean_value`                 |          | `True`               | Whether to profile for the mean value of numeric columns.                                                                                                                                                                                                                                                                                                          |
| `profiling.include_field_median_value`               |          | `True`               | Whether to profile for the median value of numeric columns.                                                                                                                                                                                                                                                                                                        |
| `profiling.include_field_stddev_value`               |          | `True`               | Whether to profile for the standard deviation of numeric columns.                                                                                                                                                                                                                                                                                                  |
| `profiling.include_field_quantiles`                  |          | `False`              | Whether to profile for the quantiles of numeric columns.                                                                                                                                                                                                                                                                                                           |
| `profiling.include_field_distinct_value_frequencies` |          | `False`              | Whether to profile for distinct value frequencies.                                                                                                                                                                                                                                                                                                                 |
| `profiling.include_field_histogram`                  |          | `False`              | Whether to profile for the histogram for numeric fields.                                                                                                                                                                                                                                                                                                           |
| `profiling.include_field_sample_values`              |          | `True`               | Whether to profile for the sample values for all columns.                                                                                                                                                                                                                                                                                                          |
| `profiling.partition_datetime`                       |          |                      | For partitioned datasets profile only the partition which matches the datetime or profile the latest one if not set. Only Bigquery supports this.                                                                                                                                                                                                                  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
