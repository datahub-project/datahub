# BigQuery

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery]'`.

## Prerequisites
### Create a datahub profile in GCP:
1. Create a custom role for datahub (https://cloud.google.com/iam/docs/creating-custom-roles#creating_a_custom_role)
2. Grant the following permissions to this role:
```   
   bigquery.datasets.get
   bigquery.datasets.getIamPolicy
   bigquery.jobs.create
   bigquery.jobs.list
   bigquery.jobs.listAll
   bigquery.models.getMetadata
   bigquery.models.list
   bigquery.routines.get
   bigquery.routines.list
   bigquery.tables.create # Needs for profiling
   bigquery.tables.get
   bigquery.tables.getData # Needs for profiling
   bigquery.tables.list
   logging.logEntries.list # Needs for lineage generation
   resourcemanager.projects.get
```
### Create a service account:

1. Setup a ServiceAccount (https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console)
and assign the previously created role to this service account.
2. Download a service account JSON keyfile.
   Example credential file:
```json
{
  "type": "service_account",
  "project_id": "project-id-1234567",
  "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
  "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
  "client_id": "113545814931671546333",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
}
```
3. To provide credentials to the source, you can either:
   Set an environment variable:
       $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"

   *or*

   Set credential config in your source based on the credential json file. For example: 

```yml
     credential:
       project_id: project-id-1234567
       private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
       private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
       client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
       client_id: "123456678890"
```

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



The following parameters are only relevant if include_table_lineage is set to true:

- max_query_duration 
- start_time 
- end_time 
- extra_client_options

When use_exported_bigquery_audit_metadata is set to true, lineage information will be computed using exported bigquery logs. On how to setup exported bigquery audit logs, refer to the following [docs](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud) on BigQuery audit logs. Note that only protoPayloads with "type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" are supported by the current ingestion version. The bigquery_audit_metadata_datasets parameter will be used only if use_exported_bigquery_audit_metadata is set to true.

Note: the bigquery_audit_metadata_datasets parameter receives a list of datasets, in the format $PROJECT.$DATASET. This way queries from a multiple number of projects can be used to compute lineage information.

Note: Since bigquery source also supports dataset level lineage, the auth client will require additional permissions to be able to access the google audit logs. Refer the permissions section in bigquery-usage section below which also accesses the audit logs.

## Compatibility

Coming soon!

# BigQuery Usage Stats

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[bigquery-usage]'`.

### Prerequisites

The Google Identity must have one of the following OAuth scopes granted to it: 

- https://www.googleapis.com/auth/logging.read
- https://www.googleapis.com/auth/logging.admin
- https://www.googleapis.com/auth/cloud-platform.read-only
- https://www.googleapis.com/auth/cloud-platform

And should be authorized on all projects you'd like to ingest usage stats from. 

## Capabilities

This plugin extracts the following:

- Statistics on queries issued and tables and columns accessed (excludes views)
- Aggregation of these statistics into buckets, by day or hour granularity

:::note

1. This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` source described above.
2. Depending on the compliance policies setup for the bigquery instance, sometimes logging.read permission is not sufficient. In that case, use either admin or private log viewer permission. 

:::

## Quickstart recipe

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

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

By default, we extract usage stats for the last day, with the recommendation that this source is executed every day.

| Field                       | Required | Default                                                        | Description                                                                                                                                                                                                                                                                                                                                                                            |
|-----------------------------|----------|----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `projects`                  |          |                                                                |                                                                                                                                                                                                                                                                                                                                                                                        |
| `extra_client_options`      |          |                                                                |                                                                                                                                                                                                                                                                                                                                                                                        |
| `env`                       |          | `"PROD"`                                                       | Environment to use in namespace when constructing URNs.                                                                                                                                                                                                                                                                                                                                |
| `start_time`                |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Earliest date of usage logs to consider.                                                                                                                                                                                                                                                                                                                                               |
| `end_time`                  |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Latest date of usage logs to consider.                                                                                                                                                                                                                                                                                                                                                 |
| `top_n_queries`             |          | `10`                                                           | Number of top queries to save to each table.                                                                                                                                                                                                                                                                                                                                           |
| `include_operational_stats` |          | `true`                                                         | Whether to display operational stats.                                                                                                                                                                                                                                                                                                                                                  |
| `extra_client_options`      |          |                                                                | Additional options to pass to `google.cloud.logging_v2.client.Client`.                                                                                                                                                                                                                                                                                                                 |
| `query_log_delay`           |          |                                                                | To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least `query_log_delay` additional events to be processed before attempting to resolve BigQuery job information from the logs. If `query_log_delay` is `None`, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage. |
| `max_query_duration`        |          | `15`                                                           | Correction to pad `start_time` and `end_time` with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.                                                                                                                                                                            |
| `table_pattern.allow`       |          |                                                                | List of regex patterns for tables to include in ingestion.                                                                                                                                                                                                                                                                                                                             |
| `table_pattern.deny`        |          |                                                                | List of regex patterns for tables to exclude in ingestion.                                                                                                                                                                                                                                                                                                                             |

## Compatibility

The source was last most recently confirmed compatible with the [December 16, 2021](https://cloud.google.com/bigquery/docs/release-notes#December_16_2021)
release of BigQuery. 

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
