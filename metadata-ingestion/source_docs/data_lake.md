# Data lake files

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[data-lake]'`.

The data lake profiler reads files stored in a number of file types (see below for an exhaustive list).
For each file, the module will also generate profiles on the table itself as well as any columns similar to the
[SQL profiler](./sql_profiles.md).

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

Supported file types:

- CSV
- Parquet
- JSON
- ORC
- Avro

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: data-lake
  config:
    # Options
    profiling:
      enabled: true

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                | Required | Default              | Description                                                                                                                                                                                                    |
| ---------------------------------------------------- | -------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `profiling.enabled`                                  |          | `False`              | Whether profiling should be done.                                                                                                                                                                              |
| `profiling.limit`                                    |          |                      | Max number of documents to profile. By default, profiles all documents.                                                                                                                                        |
| `profiling.offset`                                   |          |                      | Offset in documents to profile. By default, uses no offset.                                                                                                                                                    |
| `profiling.max_workers`                              |          | `5 * os.cpu_count()` | Number of worker threads to use for profiling. Set to 1 to disable.                                                                                                                                            |
| `profiling.query_combiner_enabled`                   |          | `True`               | _This feature is still experimental and can be disabled if it causes issues._ Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.          |
| `profile_pattern.allow`                              |          | `*`                  | List of regex patterns for tables or table columns to profile. Defaults to all.                                                                                                                                |
| `profile_pattern.deny`                               |          |                      | List of regex patterns for tables or table columns to not profile. Defaults to none.                                                                                                                           |
| `profile_pattern.ignoreCase`                         |          | `True`               | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                    |
| `profiling.turn_off_expensive_profiling_metrics`     |          | False                | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. |
| `profiling.max_number_of_fields_to_profile`          |          | `None`               | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. |
| `profiling.profile_table_level_only`                 |          | False                | Whether to perform profiling at table-level only, or include column-level profiling as well.                                                                                                                   |
| `profiling.include_field_null_count`                 |          | `True`               | Whether to profile for the number of nulls for each column.                                                                                                                                                    |
| `profiling.include_field_min_value`                  |          | `True`               | Whether to profile for the min value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_max_value`                  |          | `True`               | Whether to profile for the max value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_mean_value`                 |          | `True`               | Whether to profile for the mean value of numeric columns.                                                                                                                                                      |
| `profiling.include_field_median_value`               |          | `True`               | Whether to profile for the median value of numeric columns.                                                                                                                                                    |
| `profiling.include_field_stddev_value`               |          | `True`               | Whether to profile for the standard deviation of numeric columns.                                                                                                                                              |
| `profiling.include_field_quantiles`                  |          | `True`               | Whether to profile for the quantiles of numeric columns.                                                                                                                                                       |
| `profiling.include_field_distinct_value_frequencies` |          | `True`               | Whether to profile for distinct value frequencies.                                                                                                                                                             |
| `profiling.include_field_histogram`                  |          | `True`               | Whether to profile for the histogram for numeric fields.                                                                                                                                                       |
| `profiling.include_field_sample_values`              |          | `True`               | Whether to profile for the sample values for all columns.                                                                                                                                                      |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
