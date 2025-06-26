### Prerequisites

To understand how BigQuery ingestion needs to be set up, first familiarize yourself with the concepts in the diagram below:

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/source-bigquery-setup.png"/>
</p>

There are two important concepts to understand and identify:

- _Extractor Project_: This is the project associated with a service-account, whose credentials you will be configuring in the connector. The connector uses this service-account to run jobs (including queries) within the project.
- _Bigquery Projects_ are the projects from which table metadata, lineage, usage, and profiling data need to be collected. By default, the extractor project is included in the list of projects that DataHub collects metadata from, but you can control that by passing in a specific list of project ids that you want to collect metadata from. Read the configuration section below to understand how to limit the list of projects that DataHub extracts metadata from.

#### Create a datahub profile in GCP

1. Create a custom role for datahub as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-custom-roles#creating_a_custom_role).
2. Follow the sections below to grant permissions to this role on this project and other projects.

##### Basic Requirements (needed for metadata ingestion)

1. Identify your Extractor Project where the service account will run queries to extract metadata.

| permission                       | Description                                                                                                                         | Capability                                                                                                    |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `bigquery.jobs.create`           | Run jobs (e.g. queries) within the project. _This only needs for the extractor project where the service account belongs_           |                                                                                                               |
| `bigquery.jobs.list`             | Manage the queries that the service account has sent. _This only needs for the extractor project where the service account belongs_ |                                                                                                               |
| `bigquery.readsessions.create`   | Create a session for streaming large results. _This only needs for the extractor project where the service account belongs_         |                                                                                                               |
| `bigquery.readsessions.getData`  | Get data from the read session. _This only needs for the extractor project where the service account belongs_                       |

2. Grant the following permissions to the Service Account on every project where you would like to extract metadata from

:::info

If you have multiple projects in your BigQuery setup, the role should be granted these permissions in each of the projects.

:::
| Permission | Description | Capability | Default GCP Role Which Contains This Permission |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------|-------------------------------------|---------------------------------------------------------------------------|
| `bigquery.datasets.get` | Retrieve metadata about a dataset. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.datasets.getIamPolicy` | Read a dataset's IAM permissions. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.tables.list` | List BigQuery tables. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.tables.get` | Retrieve metadata for a table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.routines.get` | Get Routines. Needs to retrieve metadata for a table from system table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.routines.list` | List Routines. Needs to retrieve metadata for a table from system table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `resourcemanager.projects.get` | Retrieve project names and metadata. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.jobs.listAll` | List all jobs (queries) submitted by any user. Needs for Lineage extraction. | Lineage Extraction/Usage Extraction | [roles/bigquery.resourceViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.resourceViewer) |
| `logging.logEntries.list` | Fetch log entries for lineage/usage data. Not required if `use_exported_bigquery_audit_metadata` is enabled. | Lineage Extraction/Usage Extraction | [roles/logging.privateLogViewer](https://cloud.google.com/logging/docs/access-control#logging.privateLogViewer) |
| `logging.privateLogEntries.list` | Fetch log entries for lineage/usage data. Not required if `use_exported_bigquery_audit_metadata` is enabled. | Lineage Extraction/Usage Extraction | [roles/logging.privateLogViewer](https://cloud.google.com/logging/docs/access-control#logging.privateLogViewer) |
| `bigquery.tables.getData` | Access table data to extract storage size, last updated at, data profiles etc. | Profiling | |
| `datacatalog.policyTags.get` | _Optional_ Get policy tags for columns with associated policy tags. This permission is required only if `extract_policy_tags_from_catalog` is enabled. | Policy Tag Extraction | [roles/datacatalog.viewer](https://cloud.google.com/data-catalog/docs/access-control#permissions-and-roles) |

#### Create a service account in the Extractor Project

1. Setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console)
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

   ```sh
   $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
   ```

   _or_

   Set credential config in your source based on the credential json file. For example:

   ```yml
   credential:
     project_id: project-id-1234567
     private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
     private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
     client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
     client_id: "123456678890"
   ```

##### Profiling Requirements

To profile BigQuery external tables backed by Google Drive document, you need to grant document's "Viewer" access to service account's email address (`client_email` in credentials json file). To find the Google Drive document linked to BigQuery table, open the BigQuery console, locate the needed table, select "Details" from the drop-down menu in the top-right corner and refer "Source" field . To share access of Google Drive document, open the document, click "Share" in the top-right corner, add the service account's email address that needs "Viewer" access. ![Google Drive Sharing Dialog](https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/google_drive_share.png)

### Lineage Computation Details

When `use_exported_bigquery_audit_metadata` is set to `true`, lineage information will be computed using exported bigquery logs. On how to setup exported bigquery audit logs, refer to the following [docs](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud) on BigQuery audit logs. Note that only protoPayloads with "type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" are supported by the current ingestion version. The `bigquery_audit_metadata_datasets` parameter will be used only if `use_exported_bigquery_audit_metadat` is set to `true`.

Note: the `bigquery_audit_metadata_datasets` parameter receives a list of datasets, in the format $PROJECT.$DATASET. This way queries from a multiple number of projects can be used to compute lineage information.

Note: Since bigquery source also supports dataset level lineage, the auth client will require additional permissions to be able to access the google audit logs. Refer the permissions section in bigquery-usage section below which also accesses the audit logs.

### Profiling Details

For performance reasons, we only profile the latest partition for partitioned tables and the latest shard for sharded tables.
You can set partition explicitly with `partition.partition_datetime` property if you want, though note that partition config will be applied to all partitioned tables.

### Caveats

- For materialized views, lineage is dependent on logs being retained. If your GCP logging is retained for 30 days (default) and 30 days have passed since the creation of the materialized view we won't be able to get lineage for them.
