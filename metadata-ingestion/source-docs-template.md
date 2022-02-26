# Source Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Prerequisites

In order to ingest metadata from [Source Name], you will need:

* eg. Python version, source version, source access type

## Quickstart

### Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery]'.`


### Recipe

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

### Config Details

| Field | Required | Default | Description |
| -- | -- | -- | -- |
| `field1` | ✅ | `default_value` | A required field with a default value |
| `field2` | | `default_value` | Not required filed with a default value |
| `field3` | | | Not required filed without a default value |


| Field                                  | Required                                                                  | Default                                                                 | Description                                                                                                                                                                                                                                                                             |
|----------------------------------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `project_id`                           |                                                                           | Autodetected                                                            | Project ID to ingest from. If not specified, will infer from environment.                                                                                                                                                                                                               |
| `env`                                  |                                                                           | `"PROD"`                                                                | Environment to use in namespace when constructing URNs.                                                                                                                                                                                                                                 |
| `credential.project_id`                | Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set |                                                                         |                                                                                                                                                                                                                                                                                         |
| `credential.private_key_id`            | Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set |                                                                         | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                                                                                                 |
| `credential.private_key`               | Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set |                                                                         | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                                                                                                 |
| `credential.client_email`              | Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set |                                                                         | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                                                                                                 |
| `credential.client_id`                 | Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set |                                                                         | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                                                                                                 |
| `table_pattern.allow`                  |                                                                           |                                                                         | List of regex patterns for tables to include in ingestion.                                                                                                                                                                                                                              |
| `table_pattern.deny`                   |                                                                           |                                                                         | List of regex patterns for tables to exclude from ingestion.                                                                                                                                                                                                                            |
| `table_pattern.ignoreCase`             |                                                                           | `True`                                                                  | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                             |
| `schema_pattern.allow`                 |                                                                           |                                                                         | List of regex patterns for schemas to include in ingestion.                                                                                                                                                                                                                             |
| `schema_pattern.deny`                  |                                                                           |                                                                         | List of regex patterns for schemas to exclude from ingestion.                                                                                                                                                                                                                           |
| `schema_pattern.ignoreCase`            |                                                                           | `True`                                                                  | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                             |
| `view_pattern.allow`                   |                                                                           |                                                                         | List of regex patterns for views to include in ingestion.                                                                                                                                                                                                                               |
| `view_pattern.deny`                    |                                                                           |                                                                         | List of regex patterns for views to exclude from ingestion.                                                                                                                                                                                                                             |
| `view_pattern.ignoreCase`              |                                                                           | `True`                                                                  | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                             |
| `include_tables`                       |                                                                           | `True`                                                                  | Whether tables should be ingested.                                                                                                                                                                                                                                                      |
| `include_views`                        |                                                                           | `True`                                                                  | Whether views should be ingested.                                                                                                                                                                                                                                                       |
| `include_table_lineage`                |                                                                           | `True`                                                                  | Whether table level lineage should be ingested and processed.                                                                                                                                                                                                                           |
| `max_query_duration`                   |                                                                           | `15`                                                                    | A time buffer in minutes to adjust start_time and end_time while querying Bigquery audit logs.                                                                                                                                                                                          |
| `start_time`                           |                                                                           | Start of last full day in UTC (or hour, depending on `bucket_duration`) | Earliest time of lineage data to consider.                                                                                                                                                                                                                                              |
| `end_time`                             |                                                                           | End of last full day in UTC (or hour, depending on `bucket_duration`)   | Latest time of lineage data to consider.                                                                                                                                                                                                                                                |
| `extra_client_options`                 |                                                                           |                                                                         | Additional options to pass to `google.cloud.logging_v2.client.Client`.                                                                                                                                                                                                                  |
| `use_exported_bigquery_audit_metadata` |                                                                           | `False`                                                                 | When configured, use `BigQueryAuditMetadata` in `bigquery_audit_metadata_datasets` to compute lineage information.                                                                                                                                                                      |
| `use_date_sharded_audit_log_tables`    |                                                                           | `False`                                                                 | Whether to read date sharded tables or time partitioned tables when extracting lineage from exported audit logs.                                                                                                                                                                        |
| `bigquery_audit_metadata_datasets`     |                                                                           | None                                                                    | A list of datasets that contain a table named `cloudaudit_googleapis_com_data_access` which contain BigQuery audit logs, specifically, those containing `BigQueryAuditMetadata`. It is recommended that the project of the dataset is also specified, for example, `projectA.datasetB`. |
| `domain.domain_key.allow`              |                                                                           |                                                                         | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                                                                                                                          |
| `domain.domain_key.deny`               |                                                                           |                                                                         | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                                                                                                                         |
| `domain.domain_key.ignoreCase`         |                                                                           | `True`                                                                  | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                                                                                                                  |






