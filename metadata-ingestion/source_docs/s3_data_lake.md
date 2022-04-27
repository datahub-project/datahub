# S3 Data Lake

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

:::caution

This source is in **Beta** and under active development. Not yet considered ready for production.

:::

## Setup

To install this plugin, run `pip install 'acryl-datahub[s3]'`. Note that because the profiling is run with PySpark, we require Spark 3.0.3 with Hadoop 3.2 to be installed (see [compatibility](#compatibility) for more details). If profiling, make sure that permissions for **s3a://** access are set because Spark and Hadoop use the s3a:// protocol to interface with AWS (schema inference outside of profiling requires s3:// access).

The s3 connector extracts schemas and profiles from a variety of file formats (see below for an exhaustive list).
Based on configuration, individual files or folders are ingested as tables, and profiles are computed similar to the [SQL profiler](./sql_profiles.md).
Enabling profiling will slow down ingestion runs.

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

| Capability        | Status | Details                                  |
| ----------------- | ------ | ---------------------------------------- |
| Platform Instance | ✔️     | [link](../../docs/platform-instances.md) |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: s3
  config:
    path_spec:
      include: "s3://covid19-lake/covid_knowledge_graph/csv/nodes/*.*"
    aws_config:
      aws_access_key_id: *****
      aws_secret_access_key: *****
      aws_region: us-east-2
    env: "PROD"
    profiling:
      enabled: false

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                                | Required                 | Default                                   | Description                                                                                                                                                                                                    |
| ---------------------------------------------------- | ------------------------ | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path_spec.include`                                  | ✅                       |                                           | Path to table (s3 or local file system). Name variable {table} is used to mark the folder with dataset. In absence of {table}, file level dataset will be created. Check below examples for more details.      |
| `path_spec.exclude`                                  |                          |                                           | list of paths in glob pattern which will be excluded while scanning for the datasets                                                                                                                           |
| `path_spec.table_name`                               |                          | {table}                                   | Display name of the dataset.Combination of named variableds from include path and strings                                                                                                                      |
| `path_spec.file_types`                               |                          | ["csv", "tsv", "json", "parquet", "avro"] | Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.                                                                           |
| `env`                                                |                          | `PROD`                                    | Environment to use in namespace when constructing URNs.                                                                                                                                                        |
| `platform`                                           |                          | Autodetected                              | Platform to use in namespace when constructing URNs. If left blank, local paths will correspond to `file` and S3 paths will correspond to `s3`.                                                                |
| `platform_instance`                                  |                          |                                           | Platform instance for datasets and containers                                                                                                                                                                  |
| `spark_driver_memory`                                |                          | `4g`                                      | Max amount of memory to grant Spark.                                                                                                                                                                           |
| `aws_config.aws_region`                              | If ingesting from AWS S3 |                                           | AWS region code.                                                                                                                                                                                               |
| `aws_config.aws_access_key_id`                       |                          | Autodetected (Required for s3 profiling)  | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `aws_config.aws_secret_access_key`                   |                          | Autodetected (Required for s3 profiling)  | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `aws_config.aws_session_token`                       |                          | Autodetected                              | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                                                                                             |
| `use_s3_bucket_tags`                                 |                          | None                                      | If an S3 Buckets Tags should be created for the Tables ingested by S3. Please Note that this will not apply tags to any folders ingested, only the files.                                                      |
| `use_s3_object_tags`                                 |                          | None                                      | If an S3 Objects Tags should be created for the Tables ingested by S3. Please Note that this will not apply tags to any folders ingested, only the files.                                                      |
| `aws_config.aws_endpoint_url`                        |                          | Autodetected                              | See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html                                                                                                                        |
| `aws_config.aws_proxy`                               |                          | Autodetected                              | See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html                                                                                                                        |
| `max_rows`                                           |                          | `100`                                     | Maximum number of rows to use when inferring schemas for TSV and CSV files.                                                                                                                                    |
| `profile_patterns.allow`                             |                          | `*`                                       | List of regex patterns for tables to profile (a must also be ingested for profiling). Defaults to all.                                                                                                         |
| `profile_patterns.deny`                              |                          |                                           | List of regex patterns for tables to not profile (a must also be ingested for profiling). Defaults to none.                                                                                                    |
| `profile_patterns.ignoreCase`                        |                          | `True`                                    | Whether to ignore case sensitivity during pattern matching of tables to profile.                                                                                                                               |
| `profiling.enabled`                                  |                          | `False`                                   | Whether profiling should be done.                                                                                                                                                                              |
| `profiling.profile_table_level_only`                 |                          | `False`                                   | Whether to perform profiling at table-level only or include column-level profiling as well.                                                                                                                    |
| `profiling.max_number_of_fields_to_profile`          |                          | `None`                                    | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. |
| `profiling.include_field_null_count`                 |                          | `True`                                    | Whether to profile for the number of nulls for each column.                                                                                                                                                    |
| `profiling.include_field_min_value`                  |                          | `True`                                    | Whether to profile for the min value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_max_value`                  |                          | `True`                                    | Whether to profile for the max value of numeric columns.                                                                                                                                                       |
| `profiling.include_field_mean_value`                 |                          | `True`                                    | Whether to profile for the mean value of numeric columns.                                                                                                                                                      |
| `profiling.include_field_median_value`               |                          | `True`                                    | Whether to profile for the median value of numeric columns.                                                                                                                                                    |
| `profiling.include_field_stddev_value`               |                          | `True`                                    | Whether to profile for the standard deviation of numeric columns.                                                                                                                                              |
| `profiling.include_field_quantiles`                  |                          | `True`                                    | Whether to profile for the quantiles of numeric columns.                                                                                                                                                       |
| `profiling.include_field_distinct_value_frequencies` |                          | `False`                                   | Whether to profile for distinct value frequencies.                                                                                                                                                             |
| `profiling.include_field_histogram`                  |                          | `False`                                   | Whether to profile for the histogram for numeric fields.                                                                                                                                                       |
| `profiling.include_field_sample_values`              |                          | `True`                                    | Whether to profile for the sample values for all columns.                                                                                                                                                      |

## Valid path_spec.include

```python
s3://my-bucket/foo/tests/bar.avro # single file table
s3://my-bucket/foo/tests/*.* # mulitple file level tables
s3://my-bucket/foo/tests/{table}/*.avro #table without partition
s3://my-bucket/foo/tests/{table}/*/*.avro #table where partitions are not specified
s3://my-bucket/foo/tests/{table}/*.* # table where no partitions as well as data type specified
s3://my-bucket/{dept}/tests/{table}/*.avro # specifying key wards to be used in display name
s3://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
s3://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
s3://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
s3://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in bucket
s3://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in bucket
```

## Valid path_spec.exclude

- **/tests/**
- s3://my-bucket/hr/\*\*
- \*_/tests/_.csv
- s3://my-bucket/foo/\*/my_table/\*\*
-

### Notes

- {table} represents folder for which dataset will be created.
- include path must end with (_._ or \*.[ext]) to represent leaf level.
- if \*.[ext] is provided then only files with specified type will be scanned.
- /\*/ represents single folder.
- {partition[i]} represents value of partition.
- {partition_key[i]} represents name of the partition.
- While extracting, “i” will be used to match partition_key to partition.
- all folder levels need to be specified in include. Only exclude path can have \*\* like matching.
- exclude path cannot have named variables ( {} ).
- Folder names should not contain {, }, \*, / in their names.
- {folder} is reserved for internal working. please do not use in named variables.

If you would like to write a more complicated function for resolving file names, then a [transformer](../transformers.md) would be a good fit.

:::caution

Specify as long fixed prefix ( with out /\*/ ) as possible in `path_spec.include`. This will reduce the scanning time and cost, specifically on AWS S3

:::

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

:::caution

If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

## Compatibility

Profiles are computed with PyDeequ, which relies on PySpark. Therefore, for computing profiles, we currently require Spark 3.0.3 with Hadoop 3.2 to be installed and the `SPARK_HOME` and `SPARK_VERSION` environment variables to be set. The Spark+Hadoop binary can be downloaded [here](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz).

For an example guide on setting up PyDeequ on AWS, see [this guide](https://aws.amazon.com/blogs/big-data/testing-data-quality-at-scale-with-pydeequ/).

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
