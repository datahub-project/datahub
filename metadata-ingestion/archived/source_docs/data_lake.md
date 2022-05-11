# Data lake files

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

:::caution

This source is going to be deprecated. Please, use [S3 Data Lake](./s3_data_lake.md) instead of this.
:::

## Setup

To install this plugin, run `pip install 'acryl-datahub[data-lake]'`. Note that because the profiling is run with PySpark, we require Spark 3.0.3 with Hadoop 3.2 to be installed (see [compatibility](#compatibility) for more details). If profiling, make sure that permissions for **s3a://** access are set because Spark and Hadoop use the s3a:// protocol to interface with AWS (schema inference outside of profiling requires s3:// access).

The data lake connector extracts schemas and profiles from a variety of file formats (see below for an exhaustive list).
Individual files are ingested as tables, and profiles are computed similar to the [SQL profiler](./sql_profiles.md).

Enabling profiling will slow down ingestion runs.

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

Because data lake files often have messy paths, we provide the built-in option to transform names into a more readable format via the `path_spec` option. This option extracts identifiers from paths through a format string specifier where extracted components are denoted as `{name[index]}`.

For instance, suppose we wanted to extract the files `/base_folder/folder_1/table_a.csv` and `/base_folder/folder_2/table_b.csv`. To ingest, we could set `base_path` to `/base_folder/` and `path_spec` to `./{name[0]}/{name[1]}.csv`, which would extract tables with names `folder_1.table_a` and `folder_2.table_b`. You could also ignore the folder component by using a `path_spec` such as `./{folder_name}/{name[0]}.csv`, which would just extract tables with names `table_a` and `table_b` – note that any component without the form `{name[index]}` is ignored.

If you would like to write a more complicated function for resolving file names, then a [transformer](../transformers.md) would be a good fit.

## Capabilities

Extracts:

- Row and column counts for each table
- For each column, if profiling is enabled:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

This connector supports both local files as well as those stored on AWS S3 (which must be identified using the prefix `s3://`). Supported file types are as follows:

- CSV
- TSV
- JSON
- Parquet
- Apache Avro

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSON) are inferred. For CSV and TSV files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

:::caution

If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

| Capability        | Status | Details                                  |
| ----------------- | ------ | ---------------------------------------- |
| Platform Instance | 🛑     | [link](../../docs/platform-instances.md) |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: data-lake
  config:
    env: "PROD"
    platform: "local-data-lake"
    base_path: "/path/to/data/folder"
    profiling:
      enabled: true

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                | Required                 | Default      | Description                                                                                                                                                                                                    |
| ---------------------------------------------------- | ------------------------ | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `env`                                                |                          | `PROD`       | Environment to use in namespace when constructing URNs.                                                                                                                                                        |
| `platform`                                           |                          | Autodetected | Platform to use in namespace when constructing URNs. If left blank, local paths will correspond to `file` and S3 paths will correspond to `s3`.                                                                |
| `base_path`                                          | ✅                       |              | Path of the base folder to crawl. Unless `schema_patterns` and `profile_patterns` are set, the connector will ingest all files in this folder.                                                                 |
| `path_spec`                                          |                          |              | Format string for constructing table identifiers from the relative path. See the above [setup section](#setup) for details.                                                                                    |
| `use_relative_path`                                  |                          | `False`      | Whether to use the relative path when constructing URNs. Has no effect when a `path_spec` is provided.                                                                                                         |
| `ignore_dotfiles`                                    |                          | `True`       | Whether to ignore files that start with `.`. For instance, `.DS_Store`, `.bash_profile`, etc.                                                                                                                  |
| `spark_driver_memory`                                |                          | `4g`         | Max amount of memory to grant Spark.                                                                                                                                                                           |
| `aws_config.aws_region`                              | If ingesting from AWS S3 |              | AWS region code.                                                                                                                                                                                               |
| `aws_config.aws_access_key_id`                       |                          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `aws_config.aws_secret_access_key`                   |                          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `aws_config.aws_session_token`                       |                          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `max_rows`                                           |                          | `100`        | Maximum number of rows to use when inferring schemas for TSV and CSV files.                                                                                                                                    |
| `schema_patterns.allow`                              |                          | `*`          | List of regex patterns for tables to ingest. Defaults to all.                                                                                                                                                  |
| `schema_patterns.deny`                               |                          |              | List of regex patterns for tables to not ingest. Defaults to none.                                                                                                                                             |
| `schema_patterns.ignoreCase`                         |                          | `True`       | Whether to ignore case sensitivity during pattern matching of tables to ingest.                                                                                                                                |
| `profile_patterns.allow`                             |                          | `*`          | List of regex patterns for tables to profile (a must also be ingested for profiling). Defaults to all.                                                                                                         |
| `profile_patterns.deny`                              |                          |              | List of regex patterns for tables to not profile (a must also be ingested for profiling). Defaults to none.                                                                                                    |
| `profile_patterns.ignoreCase`                        |                          | `True`       | Whether to ignore case sensitivity during pattern matching of tables to profile.                                                                                                                               |
| `profiling.enabled`                                  |                          | `False`      | Whether profiling should be done.                                                                                                                                                                              |
| `profiling.spark_cluster_manager`                    |                          | `None`       | Spark master URL. See [Spark docs](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) for details.                                                                                 |
| `profiling.profile_table_level_only`                 |                          | `False`      | Whether to perform profiling at table-level only or include column-level profiling as well.                                                                                                                    |
| `profiling.max_number_of_fields_to_profile`          |                          | `None`       | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. |
| `profiling.include_field_null_count`                 |                          | `True`       | Whether to profile for the number of nulls for each column.                                                                                                                                                    |
| `profiling.include_field_min_value`                  |                          | `True`       | Whether to profile for the min value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_max_value`                  |                          | `True`       | Whether to profile for the max value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_mean_value`                 |                          | `True`       | Whether to profile for the mean value of numeric columns.                                                                                                                                                      |
| `profiling.include_field_median_value`               |                          | `True`       | Whether to profile for the median value of numeric columns.                                                                                                                                                    |
| `profiling.include_field_stddev_value`               |                          | `True`       | Whether to profile for the standard deviation of numeric columns.                                                                                                                                              |
| `profiling.include_field_quantiles`                  |                          | `True`       | Whether to profile for the quantiles of numeric columns.                                                                                                                                                       |
| `profiling.include_field_distinct_value_frequencies` |                          | `False`      | Whether to profile for distinct value frequencies.                                                                                                                                                             |
| `profiling.include_field_histogram`                  |                          | `False`      | Whether to profile for the histogram for numeric fields.                                                                                                                                                       |
| `profiling.include_field_sample_values`              |                          | `True`       | Whether to profile for the sample values for all columns.                                                                                                                                                      |

## Compatibility

Profiles are computed with PyDeequ, which relies on PySpark. Therefore, for computing profiles, we currently require Spark 3.0.3 with Hadoop 3.2 to be installed and the `SPARK_HOME` and `SPARK_VERSION` environment variables to be set. The Spark+Hadoop binary can be downloaded [here](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz).

For an example guide on setting up PyDeequ on AWS, see [this guide](https://aws.amazon.com/blogs/big-data/testing-data-quality-at-scale-with-pydeequ/).

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
