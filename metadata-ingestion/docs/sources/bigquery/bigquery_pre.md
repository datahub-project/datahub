### Overview

The `bigquery` module ingests metadata from Bigquery into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Familiarize yourself with BigQuery ingestion architecture:

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/source-bigquery-setup.png"/>
</p>

Two key concepts:

- **Extractor Project**: Project containing the service account used to run metadata extraction queries
- **BigQuery Projects**: Projects from which DataHub collects metadata (tables, lineage, usage, profiling). By default includes the extractor project; configure `project_ids` to specify projects explicitly

#### Create a datahub profile in GCP

1. Create a custom role for DataHub following [BigQuery docs](https://cloud.google.com/iam/docs/creating-custom-roles#creating_a_custom_role)
2. Grant permissions to this role on the extractor project and all target projects (see below)

##### Basic Requirements (needed for metadata ingestion)

**1. Grant the following permissions on the Extractor Project:**

| permission                       | Description                                                                                                                         | Capability                                                                                                    |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `bigquery.jobs.create`           | Run jobs (e.g. queries) within the project. _This only needs for the extractor project where the service account belongs_           |                                                                                                               |
| `bigquery.jobs.list`             | Manage the queries that the service account has sent. _This only needs for the extractor project where the service account belongs_ |                                                                                                               |
| `bigquery.readsessions.create`   | Create a session for streaming large results. _This only needs for the extractor project where the service account belongs_         |                                                                                                               |
| `bigquery.readsessions.getData`  | Get data from the read session. _This only needs for the extractor project where the service account belongs_                       |

**2. Grant the following permissions on all target projects for metadata extraction:**

:::info

These permissions must be granted on **every project** you want to extract metadata from.

:::
| Permission | Description | Capability | Default GCP Role Which Contains This Permission |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------|-------------------------------------|---------------------------------------------------------------------------|
| `bigquery.datasets.get` | Retrieve metadata about a dataset. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.datasets.getIamPolicy` | Read a dataset's IAM permissions. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.tables.list` | List BigQuery tables. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.tables.get` | Retrieve metadata for a table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.routines.get` | Get Routines. Needs to retrieve metadata for a table from system table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.routines.list` | List Routines. Needs to retrieve metadata for a table from system table. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `resourcemanager.projects.get` | Get project metadata. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `resourcemanager.projects.list` | Search projects. Needed if not setting `project_ids`. | Table Metadata Extraction | [roles/bigquery.metadataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) |
| `bigquery.jobs.listAll` | List all jobs (queries) submitted by any user. Needs for Lineage extraction. | Lineage Extraction/Usage Extraction | [roles/bigquery.resourceViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.resourceViewer) |
| `logging.logEntries.list` | Fetch log entries for lineage/usage data. Not required if `use_exported_bigquery_audit_metadata` is enabled. | Lineage Extraction/Usage Extraction | [roles/logging.privateLogViewer](https://cloud.google.com/logging/docs/access-control#logging.privateLogViewer) |
| `logging.privateLogEntries.list` | Fetch log entries for lineage/usage data. Not required if `use_exported_bigquery_audit_metadata` is enabled. | Lineage Extraction/Usage Extraction | [roles/logging.privateLogViewer](https://cloud.google.com/logging/docs/access-control#logging.privateLogViewer) |
| `bigquery.tables.getData` | Access table data to extract storage size, last updated at, partition information, data profiles etc. **Required when profiling is enabled or when `use_tables_list_query_v2` is enabled.** This permission is needed to query BigQuery's `__TABLES__` pseudo-table. | Profiling/Enhanced Table Metadata | |
| `datacatalog.policyTags.get` | _Optional_ Get policy tags for columns with associated policy tags. This permission is required only if `extract_policy_tags_from_catalog` is enabled. | Policy Tag Extraction | [roles/datacatalog.viewer](https://cloud.google.com/data-catalog/docs/access-control#permissions-and-roles) |

:::warning Important: bigquery.tables.getData Permission

The `bigquery.tables.getData` permission is **required** in the following scenarios:

- When **profiling is enabled** (`profiling.enabled: true`)
- When **`use_tables_list_query_v2` is enabled** (for enhanced table metadata extraction)

Without this permission, you'll encounter errors when the connector tries to access BigQuery's `__TABLES__` pseudo-table for detailed table information including partition data, row counts, and storage metrics.

:::

#### Create a service account in the Extractor Project

1. Create a service account following [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and assign the custom role created above
2. Download a service account JSON keyfile
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

**For external tables backed by Google Drive:**

Grant "Viewer" access to the service account's email (`client_email` from credentials JSON) on the Google Drive documents:

1. Find the source document: BigQuery Console → Table → Details → "Source" field
2. Share the document: Open document → Share → Add service account email with "Viewer" access

![Google Drive Sharing Dialog](https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/google_drive_share.png)
