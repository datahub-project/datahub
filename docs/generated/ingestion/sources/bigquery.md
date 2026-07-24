


# BigQuery

## Overview

BigQuery is a data platform used to store and query analytical or operational data. Learn more in the [official BigQuery documentation](https://cloud.google.com/bigquery).

The DataHub integration for BigQuery covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, and stateful deletion detection.

## Concept Mapping

| BigQuery Concept           | DataHub Entity (Subtype)               | Notes                                                                                                                        |
| -------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| GCP Project                | Platform Instance, Container (PROJECT) | Project ID is used as both the platform instance and the top-level container.                                                |
| Dataset                    | Container (DATASET)                    | Nested under its Project container. Includes location and labels.                                                            |
| Table                      | Dataset (TABLE)                        | Regular and partitioned tables. Schema, descriptions, PKs/FKs, partition keys, clustering columns, and labels are extracted. |
| Sharded Table              | Dataset (SHARDED TABLE)                | Tables with a `_yyyymmdd` suffix pattern; grouped under a shared URN prefix.                                                 |
| External Table             | Dataset (EXTERNAL TABLE)               | Includes source format, URIs, and compression properties.                                                                    |
| Table Snapshot             | Dataset (BIGQUERY TABLE SNAPSHOT)      | Identified by name pattern `{table}@{timestamp_ms}`.                                                                         |
| View                       | Dataset (VIEW)                         | SQL definition captured in ViewProperties. `materialized=false`.                                                             |
| Materialized View          | Dataset (VIEW)                         | SQL definition captured. `materialized=true` in ViewProperties.                                                              |
| Column / field             | SchemaField                            | Includes partition key, clustering key, PKs, FKs, and policy tags where available.                                           |
| Label                      | Tag                                    | Table, view, and dataset labels captured as tags when `capture_*_label_as_tag` is enabled.                                   |
| Policy tag (Data Catalog)  | Tag                                    | Column-level policy tags extracted when `extract_policy_tags_from_catalog` is enabled.                                       |
| Table / column lineage     | Lineage edges                          | From view definitions and audit log query history.                                                                           |
| Query operations and usage | DatasetUsageStatistics, Operation      | Per-dataset and per-column access counts extracted from audit logs.                                                          |


## Module `bigquery`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Project, Dataset. |
| Column-level Lineage | ✅ | Optionally enabled via configuration. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration. |
| Dataset Usage | ✅ | Enabled by default, can be disabled via configuration `include_usage_statistics`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Enabled by default via usage extraction, can be disabled via `usage.include_operational_stats`. |
| Partition Support | ✅ | Enabled by default, partition keys and clustering keys are supported. |
| [Platform Instance](../../../platform-instances.md) | ❌ | Platform instance is pre-set to the BigQuery project id. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Optionally enabled via configuration. |
| Test Connection | ✅ | Enabled by default. |

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
| `datacatalog.taxonomies.get` | _Optional_ Get policy tags for columns with associated policy tags. This permission is required only if `extract_policy_tags_from_catalog` is enabled. | Policy Tag Extraction | [roles/datacatalog.viewer](https://cloud.google.com/iam/docs/roles-permissions/datacatalog#datacatalog.viewer) |

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

#### Workload Identity Federation (keyless authentication)

As an alternative to long-lived service account keys, you can authenticate with [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) (WIF). WIF lets workloads running outside of Google Cloud (for example in AWS, Azure, on-prem Kubernetes, or self-hosted Docker) impersonate a Google service account using short-lived tokens minted from an external identity provider.

To use WIF, set `auth_type: workload_identity_federation` and supply the WIF configuration in one of three mutually exclusive ways:

| Field                               | Description                                                                                               |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `gcp_wif_configuration`             | Path to the WIF configuration JSON file on disk.                                                          |
| `gcp_wif_configuration_json`        | The WIF configuration as an inline YAML/JSON dict (or JSON string). Useful when storing the value inline. |
| `gcp_wif_configuration_json_string` | The WIF configuration as a single JSON string. Useful when injecting from a secret manager.               |

The WIF configuration is the standard `external_account` JSON produced by `gcloud iam workload-identity-pools create-cred-config` — for example:

```json
{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID",
  "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
  "token_url": "https://sts.googleapis.com/v1/token",
  "credential_source": { "file": "/var/run/secrets/tokens/gcp-ksa/token" },
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:generateAccessToken"
}
```

The impersonated service account must have the BigQuery permissions described above. When `auth_type` is `workload_identity_federation`, the `credential` field is not used and must be omitted.

##### Profiling Requirements

**For external tables backed by Google Drive:**

Grant "Viewer" access to the service account's email (`client_email` from credentials JSON) on the Google Drive documents:

1. Find the source document: BigQuery Console → Table → Details → "Source" field
2. Share the document: Open document → Share → Add service account email with "Viewer" access

![Google Drive Sharing Dialog](https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/google_drive_share.png)


### Install the Plugin
```shell
pip install 'acryl-datahub[bigquery]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Service account authentication (default)
source:
  type: bigquery
  config:
    dataset_pattern:
      allow:
        - finance_bq_dataset
    table_pattern:
      deny:
        # The exact name of the table is revenue_table_name
        # The reason we have this `.*` at the beginning is because the current implmenetation of table_pattern is testing
        # project_id.dataset_name.table_name
        # We will improve this in the future
        - .*revenue_table_name
    include_table_lineage: true
    include_usage_statistics: true
    profiling:
      enabled: true
      profile_table_level_only: true

sink:
  # sink configs

---
# Workload Identity Federation with configuration file
source:
  type: bigquery
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration: "/path/to/gcp_wif_configuration.json"
    include_table_lineage: true
    include_usage_statistics: true

---
# Workload Identity Federation with inline configuration (dict)
source:
  type: bigquery
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration_json:
      type: external_account
      audience: "//iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID"
      subject_token_type: "urn:ietf:params:oauth:token-type:jwt"
      token_url: "https://sts.googleapis.com/v1/token"
      credential_source:
        file: "/var/run/secrets/tokens/gcp-ksa/token"
      service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:generateAccessToken"

---
# Workload Identity Federation with JSON string (copy-paste from file)
source:
  type: bigquery
  config:
    auth_type: workload_identity_federation
    gcp_wif_configuration_json_string: |
      {
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
          "file": "/var/run/secrets/tokens/gcp-ksa/token"
        },
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:generateAccessToken"
      }

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">auth_type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "service_account", "workload_identity_federation"  |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">column_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of columns to process in a table. This is a low level config property which should be touched with care. This restriction is needed because excessively wide tables can result in failure to ingest the schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">convert_column_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">debug_include_full_payloads</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include full payload into events. It is only for debugging and internal use. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_legacy_sharded_table_support</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use the legacy sharded table urn suffix added. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_profiling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_time_window</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful time window tracking. This will store the time window after successful extraction and adjust the time window in subsequent runs to avoid reprocessing. NOTE: This is ONLY applicable when using queries v2 (use_queries_v2=True). This replaces enable_stateful_lineage_ingestion and enable_stateful_usage_ingestion for the queries v2 extraction path, since queries v2 extracts lineage, usage, operations, and queries together from a single audit log and uses a unified time window. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_usage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">exclude_empty_projects</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to exclude empty projects from being ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extra_client_options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional keyword arguments passed to GCP client constructors (bigquery.Client, GCPLoggingClient, etc.).  |
| <div className="path-line"><span className="path-main">extract_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate column level lineage. Requires lineage_use_sql_parser to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage_from_catalog</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | This flag enables the data lineage extraction from Data Lineage API exposed by Google Data Catalog. NOTE: This extractor can't build views lineage. It's recommended to enable the view's DDL parsing. Read the docs to have more information about: https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_policy_tags_from_catalog</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract policy tags from BigQuery tables using INFORMATION_SCHEMA and Data Catalog API. Policy tag display names are resolved by listing all tags per taxonomy (one API call per unique taxonomy, typically 3-10 calls per ingestion run). If the Data Catalog API is blocked by VPC Service Controls, policy tag resource names will be stored instead of display names (graceful degradation). Requires BigQuery API v2 (available since ~2020) and Data Catalog API access. For more information about policy tags and column-level security, refer to: https://cloud.google.com/bigquery/docs/column-level-security-intro <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the GCP Workload Identity Federation configuration JSON file. Mutually exclusive with gcp_wif_configuration_json and gcp_wif_configuration_json_string. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration_json</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | GCP Workload Identity Federation configuration as a dict or a JSON string. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json_string. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">gcp_wif_configuration_json_string</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP Workload Identity Federation configuration as a JSON string (contents of the configuration file). Useful for injecting configuration from secrets managers. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json. Note: WIF configuration typically contains public endpoint URLs rather than private keys, so SecretStr masking is not applied. If your WIF config contains sensitive material, ensure it is not logged at DEBUG level. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage_with_gcs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, column-level lineage will be extracted from the gcs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_data_platform_instance</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to create a DataPlatformInstance aspect, equal to the BigQuery project id. If enabled, will cause redundancy in the browse path for BigQuery entities in the UI, because the project id is represented as the top-level container. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to populate BigQuery Console url to Datasets/Tables <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query entities associated with lineage edges. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_query_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query popularity statistics. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_schema_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the BigQuery schema, i.e. projects, schemas, tables, and views. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_constraints</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest table constraints. If you know you don't use table constraints, you can disable it to save one extra query per dataset. In general it should be enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Option to enable/disable lineage generation. Is enabled by default. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_snapshots</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether table snapshots should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Generate usage statistic <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates view->view and table->view lineage using DataHub's sql parser. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">lineage_sql_parser_use_raw_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | This parameter ignores the lowercase pattern stipulated in the SQLParser. NOTE: Ignored if lineage_use_sql_parser is False. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">lineage_use_sql_parser</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use sql parser to resolve view/table lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">log_page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of log item will be queried per page for lineage collection <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">match_fully_qualified_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [deprecated] Whether `dataset_pattern` is matched against fully qualified dataset name `<project_id>.<dataset_name>`. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_query_duration</span></div> <div className="type-name-line"><span className="type-name">string(duration)</span></div> | Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time. <div className="default-line default-line-with-docs">Default: <span className="default-value">PT15M</span></div> |
| <div className="path-line"><span className="path-main">max_threads_dataset_parallelism</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use to parallelize BigQuery Dataset Metadata Extraction. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-main">number_of_datasets_process_in_batch_if_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of partitioned table queried in batch when getting metadata. This is a low level config property which should be touched with care. This restriction is needed because we query partitions system view which throws error if we try to touch too many tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.  |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">project_on_behalf</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">rate_limit</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Should we rate limit requests made to API. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">region_qualifiers_auto_discovery</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True, automatically extends `region_qualifiers` with any BigQuery regions detected from dataset locations during schema ingestion. Defaults to False to avoid unexpected query cost increases. Set to True if your project has datasets in regions beyond `region-us` and `region-eu`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">requests_per_min</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Used to control number of API calls made per min. Only used when `rate_limit` is set to `True`. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">scheme</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">bigquery</span></div> |
| <div className="path-line"><span className="path-main">sharded_table_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The regex pattern to match sharded tables and group as one table. This is a very low level config parameter, only change if you know what you are doing,  <div className="default-line default-line-with-docs">Default: <span className="default-value">((.+\D)&#91;&#95;$&#93;?)?(\d\d\d\d(?:0&#91;1-9&#93;&#124;1&#91;0-2&#93;)(?:0&#91;1-9&#93;&#124;...</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">temp_table_dataset_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#95;</span></div> |
| <div className="path-line"><span className="path-main">upstream_lineage_in_report</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Useful for debugging lineage information. Set to True to see the raw lineage created internally. Only works with legacy approach (`use_queries_v2: False`). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_date_sharded_audit_log_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_exported_bigquery_audit_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute lineage information. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_file_backed_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use a file backed cache for the view definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_legacy_table_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Source table row count, size, and last-altered time from the undocumented `__TABLES__` Legacy SQL construct instead of the supported `INFORMATION_SCHEMA.PARTITIONS` view. `__TABLES__` additionally covers views and snapshots: views keep their `lastModified` timestamp, and snapshots keep their `lastModified` timestamp along with the `rows_count` and `size_in_bytes` custom properties. `__TABLES__` is undocumented and unsupported by Google, so it may change or stop working without notice. Leave `False` to use `PARTITIONS`, which is dataset-scoped but covers base tables only. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_queries_v2</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, uses the new queries extractor to extract queries from bigquery. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_tables_list_query_v2</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | List tables using an improved query that extracts partitions and last modified timestamps more accurately. Requires the ability to read table data. Automatically enabled when profiling is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">bigquery_audit_metadata_datasets</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">bigquery_audit_metadata_datasets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">capture_dataset_label_as_tag</span></div> <div className="type-name-line"><span className="type-name">One of boolean, AllowDenyPattern</span></div> | Capture BigQuery dataset labels as DataHub tag <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">capture_dataset_label_as_tag.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">capture_dataset_label_as_tag.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_dataset_label_as_tag.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">capture_dataset_label_as_tag.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_dataset_label_as_tag.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">capture_table_label_as_tag</span></div> <div className="type-name-line"><span className="type-name">One of boolean, AllowDenyPattern</span></div> | Capture BigQuery table labels as DataHub tag <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">capture_table_label_as_tag.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">capture_table_label_as_tag.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_table_label_as_tag.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">capture_table_label_as_tag.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_table_label_as_tag.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">capture_view_label_as_tag</span></div> <div className="type-name-line"><span className="type-name">One of boolean, AllowDenyPattern</span></div> | Capture BigQuery view labels as DataHub tag <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">capture_view_label_as_tag.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">capture_view_label_as_tag.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_view_label_as_tag.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">capture_view_label_as_tag.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">capture_view_label_as_tag.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | BigQuery service account credential. Required when auth_type is 'service_account' unless Application Default Credentials are available (e.g. running on GCE/GKE). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_email</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client email  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client Id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_provider_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Auth provider x509 certificate url <div className="default-line default-line-with-docs">Default: <span className="default-value">https://www.googleapis.com/oauth2/v1/certs</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://accounts.google.com/o/oauth2/auth</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Project id to set the credentials <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">token_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Token uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://oauth2.googleapis.com/token</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication type <div className="default-line default-line-with-docs">Default: <span className="default-value">service&#95;account</span></div> |
| <div className="path-line"><span className="path-main">dataset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dataset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">gcs_lineage_config</span></div> <div className="type-name-line"><span className="type-name">GcsLineageProviderConfig</span></div> | Any source that produces gcs lineage from/to Datasets should inherit this class.  |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.</span><span className="path-main">ignore_non_path_spec_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ignore paths that are not match in path_specs. It only applies if path_specs are specified. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.</span><span className="path-main">strip_urls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Strip filename from gcs url. It only applies if path_specs are not specified. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.</span><span className="path-main">path_specs</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of PathSpec. See below the details about PathSpec <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.</span><span className="path-main">PathSpec</span></div> <div className="type-name-line"><span className="type-name">PathSpec</span></div> |   |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">include</span>&nbsp;<abbr title="Required if PathSpec is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.  |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">allow_double_stars</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Allow double stars in the include path. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">autodetect_partitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024` <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">default_extension</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">emit_folders_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If set, this path_spec emits each matching storage folder as a DataHub Container and creates no dataset entities. Folder depth is set by the number of wildcard levels in `include` (e.g. `.../*/` one level deep, `.../*/*/` two levels). `exclude` and `include_hidden_folders` still apply to the folder walk; file/dataset fields (`file_types`, `default_extension`, `table_name`, `tables_filter_pattern`) are rejected since no datasets are produced. `include` must end at a folder and must not contain `{table}` or `**`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">enable_compression</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable or disable processing compressed files. Currently .gz and .bz files are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">include_hidden_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include hidden folders in the traversal (folders starting with . or _ <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">sample_files</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">table_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Display name of the dataset.Combination of named variables from include path and strings <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">traversal_method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ALL", "MIN_MAX", "MAX"  |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">exclude</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | list of paths in glob pattern which will be excluded while scanning for the datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.exclude.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">file_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;csv&#x27;, &#x27;tsv&#x27;, &#x27;json&#x27;, &#x27;parquet&#x27;, &#x27;avro&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.file_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.</span><span className="path-main">tables_filter_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">gcs_lineage_config.path_specs.PathSpec.tables_filter_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Ingests specified project_ids. Use this property if you want to specify what projects to ingest or don't want to give project resourcemanager.projects.list to your service account. Overrides `project_id_pattern`.  |
| <div className="path-line"><span className="path-prefix">project_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">project_labels</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Ingests projects with the specified labels. Set value in the format of `key:value`. Use this property to define which projects to ingest basedon project-level labels. If project_ids or project_id is set, this configuration has no effect. The ingestion process filters projects by label first, and then applies the project_id_pattern.  |
| <div className="path-line"><span className="path-prefix">project_labels.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">pushdown_allow_usernames</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of user email patterns using SQL LIKE syntax (e.g., 'analyst_%@company.com') which WILL be considered for lineage/usage/queries extraction. Uses case-insensitive LIKE for server-side filtering (e.g., 'analyst_%' matches 'Analyst_John@company.com'). Only applicable if `use_queries_v2` is enabled. If not specified, all users not in deny list are included. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pushdown_allow_usernames.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">pushdown_deny_usernames</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of user email patterns using SQL LIKE syntax (e.g., 'bot_%', '%@%.iam.gserviceaccount.com') which will NOT be considered for lineage/usage/queries extraction. Uses case-insensitive LIKE for server-side filtering (e.g., 'bot_%' matches 'Bot_User@example.com'). This is primarily useful for improving performance by filtering out users with extremely high query volumes. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pushdown_deny_usernames.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">region_qualifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | BigQuery regions to be scanned for bigquery jobs when using `use_queries_v2`. See [this](https://cloud.google.com/bigquery/docs/information-schema-jobs#scope_and_syntax) for details.  |
| <div className="path-line"><span className="path-prefix">region_qualifiers.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_snapshot_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_snapshot_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">usage</span></div> <div className="type-name-line"><span className="type-name">BigQueryUsageConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">apply_view_usage_to_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to apply view's usage to its base tables. If set to False, uses sql parser and applies usage to views / tables mentioned in the query. If set to True, usage is applied to base tables only. Only applied with the legacy extraction path (`use_queries_v2: False`); ignored under queries-v2. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC **Deprecated**: set the top-level `end_time` instead - it governs lineage, usage, and operations together.  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. Only applied with the legacy extraction path (`use_queries_v2: False`); ignored under queries-v2. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">max_query_duration</span></div> <div className="type-name-line"><span className="type-name">string(duration)</span></div> | Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time. **Deprecated**: set the top-level `max_query_duration` instead. Note it only takes effect with the legacy extraction path (`use_queries_v2: False`). <div className="default-line default-line-with-docs">Default: <span className="default-value">PT15M</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. **Deprecated**: set the top-level `start_time` instead - it governs lineage, usage, and operations together. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">usage.user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes to use for classification. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">4</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#123;&#x27;type&#x27;: &#x27;datahub&#x27;, &#x27;config&#x27;: None&#125;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">DynamicTypedClassifierConfig</span></div> <div className="type-name-line"><span className="type-name">DynamicTypedClassifierConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if DynamicTypedClassifierConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.  |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">config</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | The configuration required for initializing the classifier. If not specified, uses defaults for classifer type. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GEProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ge", "sqlalchemy" <div className="default-line default-line-with-docs">Default: <span className="default-value">sqlalchemy</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">nested_field_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BigQueryAuthType": {
      "enum": [
        "service_account",
        "workload_identity_federation"
      ],
      "title": "BigQueryAuthType",
      "type": "string"
    },
    "BigQueryUsageConfig": {
      "additionalProperties": false,
      "properties": {
        "bucket_duration": {
          "$ref": "#/$defs/BucketDuration",
          "default": "DAY",
          "description": "Size of the time window to aggregate usage stats. **Deprecated**: set the top-level `bucket_duration` instead - it governs lineage, usage, and operations together."
        },
        "end_time": {
          "description": "Latest date of lineage/usage to consider. Default: Current time in UTC **Deprecated**: set the top-level `end_time` instead - it governs lineage, usage, and operations together.",
          "format": "date-time",
          "title": "End Time",
          "type": "string"
        },
        "start_time": {
          "default": null,
          "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. **Deprecated**: set the top-level `start_time` instead - it governs lineage, usage, and operations together.",
          "format": "date-time",
          "title": "Start Time",
          "type": "string"
        },
        "top_n_queries": {
          "default": 10,
          "description": "Number of top queries to save to each table.",
          "exclusiveMinimum": 0,
          "title": "Top N Queries",
          "type": "integer"
        },
        "user_email_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "regex patterns for user emails to filter in usage."
        },
        "include_operational_stats": {
          "default": true,
          "description": "Whether to display operational stats.",
          "title": "Include Operational Stats",
          "type": "boolean"
        },
        "include_read_operational_stats": {
          "default": false,
          "description": "Whether to report read operational stats. Experimental. Only applied with the legacy extraction path (`use_queries_v2: False`); ignored under queries-v2.",
          "title": "Include Read Operational Stats",
          "type": "boolean"
        },
        "format_sql_queries": {
          "default": false,
          "description": "Whether to format sql queries",
          "title": "Format Sql Queries",
          "type": "boolean"
        },
        "include_top_n_queries": {
          "default": true,
          "description": "Whether to ingest the top_n_queries.",
          "title": "Include Top N Queries",
          "type": "boolean"
        },
        "max_query_duration": {
          "default": "PT15M",
          "description": "Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time. **Deprecated**: set the top-level `max_query_duration` instead. Note it only takes effect with the legacy extraction path (`use_queries_v2: False`).",
          "format": "duration",
          "title": "Max Query Duration",
          "type": "string"
        },
        "apply_view_usage_to_tables": {
          "default": false,
          "description": "Whether to apply view's usage to its base tables. If set to False, uses sql parser and applies usage to views / tables mentioned in the query. If set to True, usage is applied to base tables only. Only applied with the legacy extraction path (`use_queries_v2: False`); ignored under queries-v2.",
          "title": "Apply View Usage To Tables",
          "type": "boolean"
        }
      },
      "title": "BigQueryUsageConfig",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "ClassificationConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether classification should be used to auto-detect glossary terms",
          "title": "Enabled",
          "type": "boolean"
        },
        "sample_size": {
          "default": 100,
          "description": "Number of sample values used for classification.",
          "title": "Sample Size",
          "type": "integer"
        },
        "max_workers": {
          "default": 4,
          "description": "Number of worker processes to use for classification. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "table_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
        },
        "column_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format."
        },
        "info_type_to_term": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Optional mapping to provide glossary term identifier for info type",
          "title": "Info Type To Term",
          "type": "object"
        },
        "classifiers": {
          "default": [
            {
              "type": "datahub",
              "config": null
            }
          ],
          "description": "Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
          "items": {
            "$ref": "#/$defs/DynamicTypedClassifierConfig"
          },
          "title": "Classifiers",
          "type": "array"
        }
      },
      "title": "ClassificationConfig",
      "type": "object"
    },
    "DynamicTypedClassifierConfig": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "description": "The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.",
          "title": "Type",
          "type": "string"
        },
        "config": {
          "anyOf": [
            {},
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.",
          "title": "Config"
        }
      },
      "required": [
        "type"
      ],
      "title": "DynamicTypedClassifierConfig",
      "type": "object"
    },
    "FolderTraversalMethod": {
      "enum": [
        "ALL",
        "MIN_MAX",
        "MAX"
      ],
      "title": "FolderTraversalMethod",
      "type": "string"
    },
    "GCPCredential": {
      "additionalProperties": false,
      "properties": {
        "project_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Project id to set the credentials",
          "title": "Project Id"
        },
        "private_key_id": {
          "description": "Private key id",
          "title": "Private Key Id",
          "type": "string"
        },
        "private_key": {
          "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'",
          "format": "password",
          "title": "Private Key",
          "type": "string",
          "writeOnly": true
        },
        "client_email": {
          "description": "Client email",
          "title": "Client Email",
          "type": "string"
        },
        "client_id": {
          "description": "Client Id",
          "title": "Client Id",
          "type": "string"
        },
        "auth_uri": {
          "default": "https://accounts.google.com/o/oauth2/auth",
          "description": "Authentication uri",
          "title": "Auth Uri",
          "type": "string"
        },
        "token_uri": {
          "default": "https://oauth2.googleapis.com/token",
          "description": "Token uri",
          "title": "Token Uri",
          "type": "string"
        },
        "auth_provider_x509_cert_url": {
          "default": "https://www.googleapis.com/oauth2/v1/certs",
          "description": "Auth provider x509 certificate url",
          "title": "Auth Provider X509 Cert Url",
          "type": "string"
        },
        "type": {
          "default": "service_account",
          "description": "Authentication type",
          "title": "Type",
          "type": "string"
        },
        "client_x509_cert_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
          "title": "Client X509 Cert Url"
        }
      },
      "required": [
        "private_key_id",
        "private_key",
        "client_email",
        "client_id"
      ],
      "title": "GCPCredential",
      "type": "object"
    },
    "GEProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "default": "sqlalchemy",
          "description": "Profiling method to use. `sqlalchemy` (default) runs profiling queries directly against your source's existing SQLAlchemy connection. `ge` selects the legacy Great Expectations profiler, which is deprecated and requires `pip install 'acryl-datahub[profiling-ge]'`.",
          "enum": [
            "ge",
            "sqlalchemy"
          ],
          "title": "Method",
          "type": "string"
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        }
      },
      "title": "GEProfilingConfig",
      "type": "object"
    },
    "GcsLineageProviderConfig": {
      "additionalProperties": false,
      "description": "Any source that produces gcs lineage from/to Datasets should inherit this class.",
      "properties": {
        "path_specs": {
          "default": [],
          "description": "List of PathSpec. See below the details about PathSpec",
          "items": {
            "$ref": "#/$defs/PathSpec"
          },
          "title": "Path Specs",
          "type": "array"
        },
        "strip_urls": {
          "default": true,
          "description": "Strip filename from gcs url. It only applies if path_specs are not specified.",
          "title": "Strip Urls",
          "type": "boolean"
        },
        "ignore_non_path_spec_path": {
          "default": false,
          "description": "Ignore paths that are not match in path_specs. It only applies if path_specs are specified.",
          "title": "Ignore Non Path Spec Path",
          "type": "boolean"
        }
      },
      "title": "GcsLineageProviderConfig",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "PathSpec": {
      "additionalProperties": false,
      "properties": {
        "include": {
          "description": "Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.",
          "title": "Include",
          "type": "string"
        },
        "exclude": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": [],
          "description": "list of paths in glob pattern which will be excluded while scanning for the datasets",
          "title": "Exclude"
        },
        "file_types": {
          "default": [
            "csv",
            "tsv",
            "json",
            "parquet",
            "avro"
          ],
          "description": "Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
          "items": {
            "type": "string"
          },
          "title": "File Types",
          "type": "array"
        },
        "default_extension": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
          "title": "Default Extension"
        },
        "table_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Display name of the dataset.Combination of named variables from include path and strings",
          "title": "Table Name"
        },
        "enable_compression": {
          "default": true,
          "description": "Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
          "title": "Enable Compression",
          "type": "boolean"
        },
        "sample_files": {
          "default": true,
          "description": "Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
          "title": "Sample Files",
          "type": "boolean"
        },
        "allow_double_stars": {
          "default": false,
          "description": "Allow double stars in the include path. This can affect performance significantly if enabled",
          "title": "Allow Double Stars",
          "type": "boolean"
        },
        "autodetect_partitions": {
          "default": true,
          "description": "Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024`",
          "title": "Autodetect Partitions",
          "type": "boolean"
        },
        "traversal_method": {
          "$ref": "#/$defs/FolderTraversalMethod",
          "default": "MAX",
          "description": "Method to traverse the folder. ALL: Traverse all the folders, MIN_MAX: Traverse the folders by finding min and max value, MAX: Traverse the folder with max value"
        },
        "include_hidden_folders": {
          "default": false,
          "description": "Include hidden folders in the traversal (folders starting with . or _",
          "title": "Include Hidden Folders",
          "type": "boolean"
        },
        "tables_filter_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "The tables_filter_pattern configuration field uses regular expressions to filter the tables part of the Pathspec for ingestion, allowing fine-grained control over which tables are included or excluded based on specified patterns. The default setting allows all tables."
        },
        "emit_folders_only": {
          "default": false,
          "description": "If set, this path_spec emits each matching storage folder as a DataHub Container and creates no dataset entities. Folder depth is set by the number of wildcard levels in `include` (e.g. `.../*/` one level deep, `.../*/*/` two levels). `exclude` and `include_hidden_folders` still apply to the folder walk; file/dataset fields (`file_types`, `default_extension`, `table_name`, `tables_filter_pattern`) are rejected since no datasets are produced. `include` must end at a folder and must not contain `{table}` or `**`.",
          "title": "Emit Folders Only",
          "type": "boolean"
        }
      },
      "required": [
        "include"
      ],
      "title": "PathSpec",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "classification": {
      "$ref": "#/$defs/ClassificationConfig",
      "default": {
        "enabled": false,
        "sample_size": 100,
        "max_workers": 4,
        "table_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "column_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "info_type_to_term": {},
        "classifiers": [
          {
            "config": null,
            "type": "datahub"
          }
        ]
      },
      "description": "For details, refer to [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md)."
    },
    "enable_stateful_profiling": {
      "default": true,
      "description": "Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated. ",
      "title": "Enable Stateful Profiling",
      "type": "boolean"
    },
    "bucket_duration": {
      "$ref": "#/$defs/BucketDuration",
      "default": "DAY",
      "description": "Size of the time window to aggregate usage stats."
    },
    "end_time": {
      "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
      "format": "date-time",
      "title": "End Time",
      "type": "string"
    },
    "start_time": {
      "default": null,
      "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
      "format": "date-time",
      "title": "Start Time",
      "type": "string"
    },
    "enable_stateful_time_window": {
      "default": false,
      "description": "Enable stateful time window tracking. This will store the time window after successful extraction and adjust the time window in subsequent runs to avoid reprocessing. NOTE: This is ONLY applicable when using queries v2 (use_queries_v2=True). This replaces enable_stateful_lineage_ingestion and enable_stateful_usage_ingestion for the queries v2 extraction path, since queries v2 extracts lineage, usage, operations, and queries together from a single audit log and uses a unified time window.",
      "title": "Enable Stateful Time Window",
      "type": "boolean"
    },
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Lineage Ingestion",
      "type": "boolean"
    },
    "enable_stateful_usage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Usage Ingestion",
      "type": "boolean"
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "options": {
      "additionalProperties": true,
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
      "title": "Options",
      "type": "object"
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered."
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "include_views": {
      "default": true,
      "description": "Whether views should be ingested.",
      "title": "Include Views",
      "type": "boolean"
    },
    "include_tables": {
      "default": true,
      "description": "Whether tables should be ingested.",
      "title": "Include Tables",
      "type": "boolean"
    },
    "include_table_location_lineage": {
      "default": true,
      "description": "If the source supports it, include table lineage to the underlying storage location.",
      "title": "Include Table Location Lineage",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "Populates view->view and table->view lineage using DataHub's sql parser.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "include_view_column_lineage": {
      "default": true,
      "description": "Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled.",
      "title": "Include View Column Lineage",
      "type": "boolean"
    },
    "use_file_backed_cache": {
      "default": true,
      "description": "Whether to use a file backed cache for the view definitions.",
      "title": "Use File Backed Cache",
      "type": "boolean"
    },
    "profiling": {
      "$ref": "#/$defs/GEProfilingConfig",
      "default": {
        "method": "sqlalchemy",
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "limit": null,
        "offset": null,
        "profile_table_level_only": false,
        "include_field_null_count": true,
        "include_field_distinct_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": true,
        "include_field_stddev_value": true,
        "include_field_quantiles": false,
        "include_field_distinct_value_frequencies": false,
        "include_field_histogram": false,
        "include_field_sample_values": true,
        "max_workers": 20,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null,
        "use_sampling": true,
        "sample_size": 10000,
        "profile_external_tables": false,
        "tags_to_ignore_sampling": null,
        "profile_nested_fields": false,
        "nested_field_max_depth": 10
      }
    },
    "include_data_platform_instance": {
      "default": false,
      "description": "Whether to create a DataPlatformInstance aspect, equal to the BigQuery project id. If enabled, will cause redundancy in the browse path for BigQuery entities in the UI, because the project id is represented as the top-level container.",
      "title": "Include Data Platform Instance",
      "type": "boolean"
    },
    "enable_legacy_sharded_table_support": {
      "default": true,
      "description": "Use the legacy sharded table urn suffix added.",
      "title": "Enable Legacy Sharded Table Support",
      "type": "boolean"
    },
    "project_ids": {
      "description": "Ingests specified project_ids. Use this property if you want to specify what projects to ingest or don't want to give project resourcemanager.projects.list to your service account. Overrides `project_id_pattern`.",
      "items": {
        "type": "string"
      },
      "title": "Project Ids",
      "type": "array"
    },
    "project_labels": {
      "description": "Ingests projects with the specified labels. Set value in the format of `key:value`. Use this property to define which projects to ingest basedon project-level labels. If project_ids or project_id is set, this configuration has no effect. The ingestion process filters projects by label first, and then applies the project_id_pattern.",
      "items": {
        "type": "string"
      },
      "title": "Project Labels",
      "type": "array"
    },
    "project_id_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for project_id to filter in ingestion."
    },
    "dataset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for dataset to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'"
    },
    "match_fully_qualified_names": {
      "default": true,
      "description": "[deprecated] Whether `dataset_pattern` is matched against fully qualified dataset name `<project_id>.<dataset_name>`.",
      "title": "Match Fully Qualified Names",
      "type": "boolean"
    },
    "table_snapshot_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for table snapshots to filter in ingestion. Specify regex to match the entire snapshot name in database.schema.snapshot format. e.g. to match all snapshots starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "rate_limit": {
      "default": false,
      "description": "Should we rate limit requests made to API.",
      "title": "Rate Limit",
      "type": "boolean"
    },
    "requests_per_min": {
      "default": 60,
      "description": "Used to control number of API calls made per min. Only used when `rate_limit` is set to `True`.",
      "title": "Requests Per Min",
      "type": "integer"
    },
    "temp_table_dataset_prefix": {
      "default": "_",
      "description": "If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming.",
      "title": "Temp Table Dataset Prefix",
      "type": "string"
    },
    "sharded_table_pattern": {
      "default": "((.+\\D)[_$]?)?(\\d\\d\\d\\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))$",
      "deprecated": true,
      "description": "The regex pattern to match sharded tables and group as one table. This is a very low level config parameter, only change if you know what you are doing, ",
      "title": "Sharded Table Pattern",
      "type": "string"
    },
    "gcp_wif_configuration": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the GCP Workload Identity Federation configuration JSON file. Mutually exclusive with gcp_wif_configuration_json and gcp_wif_configuration_json_string.",
      "title": "Gcp Wif Configuration"
    },
    "gcp_wif_configuration_json": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "GCP Workload Identity Federation configuration as a dict or a JSON string. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json_string.",
      "title": "Gcp Wif Configuration Json"
    },
    "gcp_wif_configuration_json_string": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "GCP Workload Identity Federation configuration as a JSON string (contents of the configuration file). Useful for injecting configuration from secrets managers. Mutually exclusive with gcp_wif_configuration and gcp_wif_configuration_json. Note: WIF configuration typically contains public endpoint URLs rather than private keys, so SecretStr masking is not applied. If your WIF config contains sensitive material, ensure it is not logged at DEBUG level.",
      "title": "Gcp Wif Configuration Json String"
    },
    "auth_type": {
      "$ref": "#/$defs/BigQueryAuthType",
      "default": "service_account",
      "description": "Authentication type to use. Defaults to 'service_account'. Set to 'workload_identity_federation' to authenticate via [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation), which avoids long-lived service account keys."
    },
    "credential": {
      "anyOf": [
        {
          "$ref": "#/$defs/GCPCredential"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "BigQuery service account credential. Required when auth_type is 'service_account' unless Application Default Credentials are available (e.g. running on GCE/GKE)."
    },
    "extra_client_options": {
      "additionalProperties": true,
      "description": "Additional keyword arguments passed to GCP client constructors (bigquery.Client, GCPLoggingClient, etc.).",
      "title": "Extra Client Options",
      "type": "object"
    },
    "project_on_behalf": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
      "title": "Project On Behalf"
    },
    "gcs_lineage_config": {
      "$ref": "#/$defs/GcsLineageProviderConfig",
      "default": {
        "path_specs": [],
        "strip_urls": true,
        "ignore_non_path_spec_path": false
      },
      "description": "Common config for gcs lineage generation"
    },
    "include_schema_metadata": {
      "default": true,
      "description": "Whether to ingest the BigQuery schema, i.e. projects, schemas, tables, and views.",
      "title": "Include Schema Metadata",
      "type": "boolean"
    },
    "usage": {
      "$ref": "#/$defs/BigQueryUsageConfig",
      "default": {
        "bucket_duration": "DAY",
        "end_time": "2026-07-24T10:05:58.253453Z",
        "start_time": "2026-07-23T00:00:00Z",
        "queries_character_limit": 24000,
        "top_n_queries": 10,
        "user_email_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "include_operational_stats": true,
        "include_read_operational_stats": false,
        "format_sql_queries": false,
        "include_top_n_queries": true,
        "max_query_duration": "PT15M",
        "apply_view_usage_to_tables": false
      },
      "description": "Usage related configs"
    },
    "include_usage_statistics": {
      "default": true,
      "description": "Generate usage statistic",
      "title": "Include Usage Statistics",
      "type": "boolean"
    },
    "capture_table_label_as_tag": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "$ref": "#/$defs/AllowDenyPattern"
        }
      ],
      "default": false,
      "description": "Capture BigQuery table labels as DataHub tag",
      "title": "Capture Table Label As Tag"
    },
    "capture_view_label_as_tag": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "$ref": "#/$defs/AllowDenyPattern"
        }
      ],
      "default": false,
      "description": "Capture BigQuery view labels as DataHub tag",
      "title": "Capture View Label As Tag"
    },
    "capture_dataset_label_as_tag": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "$ref": "#/$defs/AllowDenyPattern"
        }
      ],
      "default": false,
      "description": "Capture BigQuery dataset labels as DataHub tag",
      "title": "Capture Dataset Label As Tag"
    },
    "include_table_constraints": {
      "default": true,
      "description": "Whether to ingest table constraints. If you know you don't use table constraints, you can disable it to save one extra query per dataset. In general it should be enabled",
      "title": "Include Table Constraints",
      "type": "boolean"
    },
    "include_external_url": {
      "default": true,
      "description": "Whether to populate BigQuery Console url to Datasets/Tables",
      "title": "Include External Url",
      "type": "boolean"
    },
    "include_table_snapshots": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether table snapshots should be ingested.",
      "title": "Include Table Snapshots"
    },
    "use_legacy_table_stats": {
      "default": false,
      "description": "Source table row count, size, and last-altered time from the undocumented `__TABLES__` Legacy SQL construct instead of the supported `INFORMATION_SCHEMA.PARTITIONS` view. `__TABLES__` additionally covers views and snapshots: views keep their `lastModified` timestamp, and snapshots keep their `lastModified` timestamp along with the `rows_count` and `size_in_bytes` custom properties. `__TABLES__` is undocumented and unsupported by Google, so it may change or stop working without notice. Leave `False` to use `PARTITIONS`, which is dataset-scoped but covers base tables only.",
      "title": "Use Legacy Table Stats",
      "type": "boolean"
    },
    "debug_include_full_payloads": {
      "default": false,
      "description": "Include full payload into events. It is only for debugging and internal use.",
      "title": "Debug Include Full Payloads",
      "type": "boolean"
    },
    "number_of_datasets_process_in_batch_if_profiling_enabled": {
      "default": 1000,
      "description": "Number of partitioned table queried in batch when getting metadata. This is a low level config property which should be touched with care. This restriction is needed because we query partitions system view which throws error if we try to touch too many tables.",
      "title": "Number Of Datasets Process In Batch If Profiling Enabled",
      "type": "integer"
    },
    "use_tables_list_query_v2": {
      "default": false,
      "description": "List tables using an improved query that extracts partitions and last modified timestamps more accurately. Requires the ability to read table data. Automatically enabled when profiling is enabled.",
      "title": "Use Tables List Query V2",
      "type": "boolean"
    },
    "use_queries_v2": {
      "default": true,
      "description": "If enabled, uses the new queries extractor to extract queries from bigquery.",
      "title": "Use Queries V2",
      "type": "boolean"
    },
    "include_queries": {
      "default": true,
      "description": "If enabled, generate query entities associated with lineage edges. Only applicable if `use_queries_v2` is enabled.",
      "title": "Include Queries",
      "type": "boolean"
    },
    "include_query_usage_statistics": {
      "default": true,
      "description": "If enabled, generate query popularity statistics. Only applicable if `use_queries_v2` is enabled.",
      "title": "Include Query Usage Statistics",
      "type": "boolean"
    },
    "column_limit": {
      "default": 300,
      "description": "Maximum number of columns to process in a table. This is a low level config property which should be touched with care. This restriction is needed because excessively wide tables can result in failure to ingest the schema.",
      "title": "Column Limit",
      "type": "integer"
    },
    "lineage_use_sql_parser": {
      "default": true,
      "description": "Use sql parser to resolve view/table lineage.",
      "title": "Lineage Use Sql Parser",
      "type": "boolean"
    },
    "lineage_sql_parser_use_raw_names": {
      "default": false,
      "description": "This parameter ignores the lowercase pattern stipulated in the SQLParser. NOTE: Ignored if lineage_use_sql_parser is False.",
      "title": "Lineage Sql Parser Use Raw Names",
      "type": "boolean"
    },
    "extract_column_lineage": {
      "default": false,
      "description": "If enabled, generate column level lineage. Requires lineage_use_sql_parser to be enabled.",
      "title": "Extract Column Lineage",
      "type": "boolean"
    },
    "extract_lineage_from_catalog": {
      "default": false,
      "description": "This flag enables the data lineage extraction from Data Lineage API exposed by Google Data Catalog. NOTE: This extractor can't build views lineage. It's recommended to enable the view's DDL parsing. Read the docs to have more information about: https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage",
      "title": "Extract Lineage From Catalog",
      "type": "boolean"
    },
    "extract_policy_tags_from_catalog": {
      "default": false,
      "description": "Extract policy tags from BigQuery tables using INFORMATION_SCHEMA and Data Catalog API. Policy tag display names are resolved by listing all tags per taxonomy (one API call per unique taxonomy, typically 3-10 calls per ingestion run). If the Data Catalog API is blocked by VPC Service Controls, policy tag resource names will be stored instead of display names (graceful degradation). Requires BigQuery API v2 (available since ~2020) and Data Catalog API access. For more information about policy tags and column-level security, refer to: https://cloud.google.com/bigquery/docs/column-level-security-intro",
      "title": "Extract Policy Tags From Catalog",
      "type": "boolean"
    },
    "scheme": {
      "default": "bigquery",
      "title": "Scheme",
      "type": "string"
    },
    "log_page_size": {
      "default": 1000,
      "description": "The number of log item will be queried per page for lineage collection",
      "exclusiveMinimum": 0,
      "title": "Log Page Size",
      "type": "integer"
    },
    "include_table_lineage": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Option to enable/disable lineage generation. Is enabled by default.",
      "title": "Include Table Lineage"
    },
    "include_column_lineage_with_gcs": {
      "default": true,
      "description": "When enabled, column-level lineage will be extracted from the gcs.",
      "title": "Include Column Lineage With Gcs",
      "type": "boolean"
    },
    "max_query_duration": {
      "default": "PT15M",
      "description": "Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
      "format": "duration",
      "title": "Max Query Duration",
      "type": "string"
    },
    "bigquery_audit_metadata_datasets": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB.",
      "title": "Bigquery Audit Metadata Datasets"
    },
    "use_exported_bigquery_audit_metadata": {
      "default": false,
      "description": "When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute lineage information.",
      "title": "Use Exported Bigquery Audit Metadata",
      "type": "boolean"
    },
    "use_date_sharded_audit_log_tables": {
      "default": false,
      "description": "Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs.",
      "title": "Use Date Sharded Audit Log Tables",
      "type": "boolean"
    },
    "upstream_lineage_in_report": {
      "default": false,
      "description": "Useful for debugging lineage information. Set to True to see the raw lineage created internally. Only works with legacy approach (`use_queries_v2: False`).",
      "title": "Upstream Lineage In Report",
      "type": "boolean"
    },
    "convert_column_urns_to_lowercase": {
      "default": false,
      "description": "When enabled, converts column URNs to lowercase to ensure cross-platform compatibility.",
      "title": "Convert Column Urns To Lowercase",
      "type": "boolean"
    },
    "exclude_empty_projects": {
      "default": false,
      "description": "Option to exclude empty projects from being ingested.",
      "title": "Exclude Empty Projects",
      "type": "boolean"
    },
    "max_threads_dataset_parallelism": {
      "default": 20,
      "description": "Number of worker threads to use to parallelize BigQuery Dataset Metadata Extraction. Set to 1 to disable.",
      "title": "Max Threads Dataset Parallelism",
      "type": "integer"
    },
    "region_qualifiers": {
      "description": "BigQuery regions to be scanned for bigquery jobs when using `use_queries_v2`. See [this](https://cloud.google.com/bigquery/docs/information-schema-jobs#scope_and_syntax) for details.",
      "items": {
        "type": "string"
      },
      "title": "Region Qualifiers",
      "type": "array"
    },
    "region_qualifiers_auto_discovery": {
      "default": false,
      "description": "When True, automatically extends `region_qualifiers` with any BigQuery regions detected from dataset locations during schema ingestion. Defaults to False to avoid unexpected query cost increases. Set to True if your project has datasets in regions beyond `region-us` and `region-eu`.",
      "title": "Region Qualifiers Auto Discovery",
      "type": "boolean"
    },
    "pushdown_deny_usernames": {
      "default": [],
      "description": "List of user email patterns using SQL LIKE syntax (e.g., 'bot_%', '%@%.iam.gserviceaccount.com') which will NOT be considered for lineage/usage/queries extraction. Uses case-insensitive LIKE for server-side filtering (e.g., 'bot_%' matches 'Bot_User@example.com'). This is primarily useful for improving performance by filtering out users with extremely high query volumes. Only applicable if `use_queries_v2` is enabled.",
      "items": {
        "type": "string"
      },
      "title": "Pushdown Deny Usernames",
      "type": "array"
    },
    "pushdown_allow_usernames": {
      "default": [],
      "description": "List of user email patterns using SQL LIKE syntax (e.g., 'analyst_%@company.com') which WILL be considered for lineage/usage/queries extraction. Uses case-insensitive LIKE for server-side filtering (e.g., 'analyst_%' matches 'Analyst_John@company.com'). Only applicable if `use_queries_v2` is enabled. If not specified, all users not in deny list are included.",
      "items": {
        "type": "string"
      },
      "title": "Pushdown Allow Usernames",
      "type": "array"
    }
  },
  "title": "BigQueryV2Config",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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
    region_qualifiers: ["region-us", "region-eu"] # Regions to scan for INFORMATION_SCHEMA.JOBS
    region_qualifiers_auto_discovery: true # Set to true to auto-extend from discovered dataset locations (default: false)
```

##### Multi-Region Configuration

`INFORMATION_SCHEMA.JOBS` is scoped per region. By default DataHub scans `region-us` and `region-eu`. If your project has datasets in other regions (e.g. `europe-west1`, `asia-northeast1`), usage and lineage for those regions will be silently missing unless you configure additional regions.

Two options:

- **Auto-discovery** (recommended for multi-region projects): set `region_qualifiers_auto_discovery: true`. DataHub detects dataset locations during schema ingestion and merges any newly found regions into `region_qualifiers`. The configured `region_qualifiers` list is always used as the starting set — auto-discovery only adds to it, never removes from it.
- **Explicit list**: add regions directly to `region_qualifiers`, e.g. `["region-us", "region-eu", "region-asia-northeast1"]`.

:::info

`region_qualifiers_auto_discovery` defaults to `false` to avoid unexpected BigQuery query costs. Enable it only if you have datasets outside `region-us` / `region-eu`.

:::

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.bigquery_v2.bigquery.BigqueryV2Source`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/bigquery_v2/bigquery.py)


:::tip Questions?

If you've got any questions on configuring ingestion for BigQuery, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
