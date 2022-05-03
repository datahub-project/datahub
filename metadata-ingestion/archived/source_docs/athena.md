# Athena

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[athena]'`.

Athena source only works with python 3.7+.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)

| Capability | Status | Details | 
| -----------| ------ | ---- |
| Platform Instance | ✔️ | [link](../../docs/platform-instances.md) |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: athena
  config:
    # Coordinates
    aws_region: my_aws_region_name
    work_group: my_work_group

    # Credentials
    username: my_aws_access_key_id
    password: my_aws_secret_access_key
    database: my_database

    # Options
    s3_staging_dir: "s3://<bucket-name>/<folder>/"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

As a SQL-based service, the Athena integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                       | Required | Default      | Description                                                                                                                                                                                                |
| --------------------------- | -------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| `username`                  |          | Autodetected | Username credential. If not specified, detected with boto3 rules. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html                                                       |
| `password`                  |          | Autodetected | Same detection scheme as `username`                                                                                                                                                                        |
| `database`                  |          | Autodetected |                                                                                                                                                                                                            |
| `aws_region`                | ✅       |              | AWS region code.                                                                                                                                                                                           |
| `s3_staging_dir`            | ✅       |              | Of format `"s3://<bucket-name>/prefix/"`. The `s3_staging_dir` parameter is needed because Athena always writes query results to S3. <br />See https://docs.aws.amazon.com/athena/latest/ug/querying.html. |
| `work_group`                | ✅       |              | Name of Athena workgroup. <br />See https://docs.aws.amazon.com/athena/latest/ug/manage-queries-control-costs-with-workgroups.html.                                                                        |
| `env`                       |          | `"PROD"`     | Environment to use in namespace when constructing URNs.                                                                                                                                          |
| `platform_instance`         |          | None             | The Platform instance to use while constructing URNs.         |
| `options.<option>`          |          |              | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                    |
| `table_pattern.allow`       |          |              | List of regex patterns for tables to include in ingestion.                                                                                                                                                 |
| `table_pattern.deny`        |          |              | List of regex patterns for tables to exclude from ingestion.                                                                                                                                               |
| `table_pattern.ignoreCase`  |          | `True`       | Whether regex matching should ignore case or not                                                                                                                                                           |
| `schema_pattern.allow`      |          |              | List of regex patterns for schemas to include in ingestion.                                                                                                                                                |
| `schema_pattern.deny`       |          |              | List of regex patterns for schemas to exclude from ingestion.                                                                                                                                              |
| `schema_pattern.ignoreCase` |          | `True`       | Whether regex matching should ignore case or not                                                                                                                                                           |     |
| `view_pattern.allow`        |          |              | List of regex patterns for views to include in ingestion.                                                                                                                                                  |
| `view_pattern.deny`         |          |              | List of regex patterns for views to exclude from ingestion.                                                                                                                                                |
| `view_pattern.ignoreCase`   |          | `True`       | Whether regex matching should ignore case or not                                                                                                                                                           |     |
| `include_tables`            |          | `True`       | Whether tables should be ingested.                                                                                                                                                                         |
| `include_views`             |          | `False`      | Whether views should be ingested.                                                                                                                                                                         |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
