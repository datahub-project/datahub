### Overview

The `bigquery` module ingests metadata from Bigquery into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Lineage and Usage Computation Details

DataHub's BigQuery connector supports two approaches for extracting lineage and usage statistics:

##### Modern Approach (Default): `use_queries_v2: true`

**Recommended for most users** - Uses BigQuery's Information Schema for efficient metadata extraction.

- **Data Source**: BigQuery Information Schema (`INFORMATION_SCHEMA.JOBS*` tables)
- **Features**:
  - Advanced lineage extraction using SQL parsing
  - Query entities with full query text
  - Query popularity statistics and rankings
  - Multi-region support via `region_qualifiers`
  - Table and column-level usage statistics
  - User filtering pushdown for performance (see [User Email Filtering Pushdown](#user-email-filtering-pushdown-performance-optimization) section below)
- **Requirements**:
  - `bigquery.jobs.listAll` permission on target projects
  - No additional Cloud Logging permissions needed

**Configuration**:

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: true # Default
    include_queries: true # Enable query entities
    include_query_usage_statistics: true # Query popularity stats
    region_qualifiers: ["region-us", "region-eu"] # Multi-region support
```

##### User Email Filtering Pushdown (Performance Optimization)

The `pushdown_deny_usernames` and `pushdown_allow_usernames` options push user filtering directly to BigQuery's SQL query, reducing data transfer and improving performance for large query volumes.

**When to Use:**

- You have large query volumes (>10k queries in your time window)
- You want to exclude high-volume service accounts or bots
- You want to reduce BigQuery data transfer costs
- You want to reduce overall DataHub ingestion time

**Example Configuration:**

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: true # Required for pushdown
    pushdown_deny_usernames:
      - "bot_%"
      - "%@%.iam.gserviceaccount.com" # Exclude service accounts
    pushdown_allow_usernames:
      - "analyst_%@example.com"
      - "data_%@example.com"
```

**Behavior:**

- When patterns are configured: Filtering happens server-side with BigQuery SQL using case-insensitive `LIKE`
- When empty (default): No server-side filtering; use `usage.user_email_pattern` for client-side filtering
- Patterns use SQL LIKE syntax (`%` = any characters, `_` = single character)
- Matching is case-insensitive (e.g., `bot_%` matches `Bot_User@example.com`)
- If a user matches both allow AND deny patterns, deny takes precedence (user is excluded)

**Prerequisites:**

- `use_queries_v2: true` must be enabled (default)
- Patterns must be valid SQL LIKE patterns

**Note:** These configs are independent from `usage.user_email_pattern`. The pushdown filters are applied at the SQL query level for performance, while `user_email_pattern` is applied client-side during processing.

##### Legacy Approach: `use_queries_v2: false`

**Use when you need specific legacy features** - Processes BigQuery audit logs for metadata extraction.

- **Data Source**: BigQuery audit logs (two options below)
- **Features**:
  - Basic table-level lineage and usage statistics
  - `upstream_lineage_in_report` debugging feature
  - Works with existing audit log exports

**Two data source options**:

##### Option 1: Google Cloud Logging API (Default)

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: false
    use_exported_bigquery_audit_metadata: false # Default
```

- **Requirements**: `logging.logEntries.list` and `logging.privateLogEntries.list` permissions
- **Limitations**: API rate limits, potential costs for large volumes

##### Option 2: Pre-exported Audit Logs in BigQuery Tables

```yaml
source:
  type: bigquery
  config:
    use_queries_v2: false
    use_exported_bigquery_audit_metadata: true
    bigquery_audit_metadata_datasets:
      - "my-project.audit_dataset"
      - "another-project.audit_logs"
```

- **Requirements**:
  - Pre-exported audit logs in BigQuery tables
  - Tables must be named `cloudaudit_googleapis_com_data_access`
  - Only protoPayloads with `type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata` are supported
- **Benefits**: No Cloud Logging API limits, better for large-scale ingestion
- **Setup**: Follow [BigQuery audit logs export guide](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)
- **Note**: The `bigquery_audit_metadata_datasets` parameter accepts datasets in `$PROJECT.$DATASET` format, allowing lineage computation from multiple projects.

#### Profiling Details

:::note Profiling Permission Requirement

When profiling is enabled, the `bigquery.tables.getData` permission is **required**. This is needed to access detailed table metadata including partition information. See the permissions section above for details.

:::

For performance reasons, we only profile the latest partition for partitioned tables and the latest shard for sharded tables.
You can set partition explicitly with `partition.partition_datetime` property if you want, though note that partition config will be applied to all partitioned tables.

#### Caveats

- For materialized views, lineage is dependent on logs being retained. If your GCP logging is retained for 30 days (default) and 30 days have passed since the creation of the materialized view we won't be able to get lineage for them.

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
