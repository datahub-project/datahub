


# Google Cloud Knowledge Catalog (Dataplex)

## Overview

Google Cloud Knowledge Catalog (Dataplex) is a is a fully managed service that automates the discovery and inventory of your distributed data and AI assets. Learn more in the [official Google Cloud Knowledge Catalog (Dataplex) documentation](https://cloud.google.com/dataplex).

The DataHub integration uses the Universal Catalog entries as the source of truth and maps them into DataHub datasets and containers with provider-native URNs (for example `bigquery`, `cloudsql`, `spanner`, `pubsub`, and `bigtable`). It also captures table-level lineage, Business Glossary ingestion and stateful deletion detection.

## Concept Mapping

The ingestion is entry-type driven: each Universal Catalog `entry_type` maps to a specific DataHub entity type and hierarchy behavior.

### Supported entry-type mapping

| Google Cloud Knowledge Catalog (Dataplex) entry type short name | DataHub platform     | Emitted entity                 | Parent relationship                    |
| :-------------------------------------------------------------- | :------------------- | :----------------------------- | :------------------------------------- |
| `bigquery-dataset`                                              | `bigquery`           | Container (`BigQuery Dataset`) | Parent is project container            |
| `bigquery-table`                                                | `bigquery`           | Dataset (`Table`)              | Parent is BigQuery dataset container   |
| `bigquery-view`                                                 | `bigquery`           | Dataset (`View`)               | Parent is BigQuery dataset container   |
| `cloudsql-mysql-instance`                                       | `cloudsql`           | Container (`Instance`)         | Parent is project container            |
| `cloudsql-mysql-database`                                       | `cloudsql`           | Container (`Database`)         | Parent is Cloud SQL instance container |
| `cloudsql-mysql-table`                                          | `cloudsql`           | Dataset (`Table`)              | Parent is Cloud SQL database container |
| `cloud-spanner-instance`                                        | `spanner`            | Container (`Instance`)         | Parent is project container            |
| `cloud-spanner-database`                                        | `spanner`            | Container (`Database`)         | Parent is Spanner instance container   |
| `cloud-spanner-table`                                           | `spanner`            | Dataset (`Table`)              | Parent is Spanner database container   |
| `cloud-spanner-graph`                                           | `spanner`            | Dataset (`Graph`)              | Parent is Spanner database container   |
| `cloud-bigtable-instance`                                       | `bigtable`           | Container (`Instance`)         | Parent is project container            |
| `cloud-bigtable-table`                                          | `bigtable`           | Dataset (`Table`)              | Parent is Bigtable instance container  |
| `pubsub-topic`                                                  | `pubsub`             | Dataset (`Topic`)              | Parent is project container            |
| `vertexai-dataset`                                              | `vertexai`           | Dataset (`Table`)              | Parent is project container            |
| `dataproc-metastore-service`                                    | `dataproc-metastore` | Container (`Service`)          | Parent is project container            |
| `dataproc-metastore-database`                                   | `dataproc-metastore` | Container (`Database`)         | Parent is Metastore service container  |
| `dataproc-metastore-table`                                      | `dataproc-metastore` | Dataset (`Table`)              | Parent is Metastore database container |

### Business Glossary mapping

Dataplex [Business Glossaries](https://cloud.google.com/dataplex/docs/glossaries-overview) are ingested as a three-level hierarchy of DataHub Glossary entities.

| Dataplex entity | DataHub entity | URN pattern                                                    |
| :-------------- | :------------- | :------------------------------------------------------------- |
| Glossary        | `GlossaryNode` | `dataplex.{project_id}.{location}.{glossary_id}`               |
| Category        | `GlossaryNode` | `dataplex.{project_id}.{location}.{glossary_id}.{category_id}` |
| Term            | `GlossaryTerm` | `dataplex.{project_id}.{location}.{glossary_id}.{term_id}`     |

Terms are marked as `EXTERNAL` with a `source_url` pointing to the Dataplex console entry. When `include_glossary_term_associations` is enabled (default), the connector also resolves term-to-asset links via the Dataplex `lookupEntryLinks` API and attaches the corresponding `GlossaryTerm` to each linked DataHub dataset.


## Module `dataplex`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Glossary Terms | ✅ | Optionally enabled via configuration `include_glossaries`. |
| [Platform Instance](../../../platform-instances.md) | ❌ | Project containers are generated instead. |
| Schema Metadata | ✅ | Enabled by default, can be disabled via configuration `include_schema`. |
| Table-Level Lineage | ✅ | Optionally enabled via configuration `include_lineage`. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `dataplex` module ingests metadata from Google Cloud Knowledge Catalog (Dataplex) into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The connector extracts metadata from Google Cloud Knowledge Catalog (Dataplex) using the **Universal Catalog Entries API**. This API extracts entries from system-managed entry groups for Google Cloud services and is the recommended approach for discovering resources across your GCP organization.

#### Spanner entry collection behavior

Spanner entries are collected through an additional `search_entries` workaround after the entry-group traversal phase. Because those entries are not discovered through `list_entry_groups`, `filter_config.entry_groups.pattern` does not apply to them. Use entry-level filters (`filter_config.entries.pattern` and `filter_config.entries.fqn_pattern`) to control Spanner inclusion.

### Prerequisites

Refer to [Google Cloud Knowledge Catalog (Dataplex) documentation](https://cloud.google.com/dataplex/docs) for the basics.

#### Project Selection

The connector supports three ways to select GCP projects, evaluated in this order of precedence:

1. **`project_ids`** — explicit list of project IDs. When set, this overrides the other two options and no project discovery is performed.
2. **`project_labels`** — list of `key:value` labels. Projects carrying any of these labels are discovered via the Cloud Resource Manager `search_projects` API and then filtered through `project_id_pattern`.
3. **`project_id_pattern`** — `AllowDenyPattern` of regexes. When `project_ids` is empty, all projects visible to the credentials are returned via the Cloud Resource Manager `search_projects` API and filtered through this pattern.

At least one of these must be set. Auto-discovery via `project_labels` or `project_id_pattern` requires the service account to have `resourcemanager.projects.get` (e.g. via `roles/browser`) on each candidate project so the Cloud Resource Manager `search_projects` API can return them; no folder/organization-level grant is needed. When `project_ids` is set explicitly, no Resource Manager permissions are needed.

#### API Enablement

Enable the following APIs on all target projects:

- **Dataplex API** (`dataplex.googleapis.com`) — see [Enable Knowledge Catalog](https://docs.cloud.google.com/dataplex/docs/enable-api)
- **Data Lineage API** (`datalineage.googleapis.com`) — required for lineage extraction (`include_lineage: true`), see [Enable Data Lineage API](https://docs.cloud.google.com/dataplex/docs/use-lineage#enable-apis)
- **Cloud Resource Manager API** (`cloudresourcemanager.googleapis.com`) — required for term-asset associations (`include_glossary_term_associations: true`)

#### Asset-Specific Configuration

Some asset types require additional setup for automatic metadata discovery:

**Cloud SQL** - Instances must be connected to Dataplex:

```sh
gcloud sql instances patch INSTANCE_NAME --enable-dataplex-integration --project=PROJECT_ID
```

**Dataproc Metastore** - Services must have Knowledge Catalog integration enabled:

```sh
# For new services
gcloud metastore services create SERVICE_NAME \
  --enable-dataplex-integration \
  --location=LOCATION

# For existing services
gcloud metastore services update SERVICE_NAME \
  --enable-dataplex-integration \
  --location=LOCATION
```

#### Authentication

Supports Application Default Credentials (ADC). See [GCP documentation](https://cloud.google.com/docs/authentication/provide-credentials-adc) for ADC setup.

For service account authentication, follow these instructions:

#### Create a service account and assign roles

1. Create a service account following [GCP docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and assign the required roles

2. Download the service account JSON keyfile

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
     project_id: "project-id-1234567"
     private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
     private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
     client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
     client_id: "123456678890"
   ```

#### Permissions

Grant the following roles to the service account on all target projects.

| Feature                                                              | Required Role                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Universal Catalog Entries API (core ingestion)                       | [`roles/dataplex.catalogViewer`](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)                                                                                                                                                                                                                           |
| Lineage extraction (`include_lineage: true`)                         | [`roles/datalineage.viewer`](https://cloud.google.com/dataplex/docs/iam-roles#datalineage.viewer)                                                                                                                                                                                                                                   |
| Business Glossary ingestion (`include_glossaries: true`)             | [`roles/dataplex.catalogViewer`](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)                                                                                                                                                                                                                           |
| Term-asset associations (`include_glossary_term_associations: true`) | [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser) on each candidate project (lighter-weight) or [`roles/resourcemanager.folderViewer`](https://cloud.google.com/resource-manager/docs/access-control-proj) — both provide `resourcemanager.projects.get`, required for resolving GCP project numbers |
| Project auto-discovery via `project_id_pattern` or `project_labels`  | [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser) on each candidate project — provides `resourcemanager.projects.get` needed for `search_projects` to return the project                                                                                                                             |

:::tip "Lineage requires the role on multiple projects"

Grant `roles/datalineage.viewer` on all projects where the corresponding process is actually executed. Note it may differ from the project containing the asset.
:::

Additional asset-specific viewer roles:

- `roles/aiplatform.viewer` (Vertex AI Viewer) is required when ingesting Vertex AI assets.
- `roles/spanner.viewer` (Cloud Spanner Viewer) is required when ingesting Cloud Spanner assets.


### Install the Plugin
```shell
pip install 'acryl-datahub[dataplex]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: dataplex
  config:
    # GCP project selection — set at least one of project_ids, project_id_pattern,
    # or project_labels. project_ids takes precedence and skips Resource Manager
    # API calls; the pattern/labels options auto-discover projects (require
    # roles/browser or any role granting resourcemanager.projects.get on the
    # candidate projects).
    project_ids:
      - "my-gcp-project"

    # Optional: regex allow/deny patterns to filter auto-discovered projects.
    # Ignored when project_ids is set explicitly.
    # project_id_pattern:
    #   allow:
    #     - "^prod-.*"
    #   deny:
    #     - "-sandbox$"

    # Optional: filter discovered projects by GCP labels in `key:value` form.
    # Ignored when project_ids is set explicitly.
    # project_labels:
    #   - "env:prod"

    # Optional: GCP location(s) for entries (Universal Catalog)
    # Use multi-region locations (us, eu, asia) to access system entry groups like @bigquery
    # Default: ["us", "eu", "asia", "global"]
    entries_locations:
      - "us"

    # Optional: Environment (default: PROD)
    env: "PROD"

    # Optional: GCP credentials (if not using Application Default Credentials)
    # credential:
    #   project_id: "my-gcp-project"
    #   private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
    #   private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
    #   client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
    #   client_id: "123456678890"

    # Optional: Metadata extraction
    # include_lineage: true   # Extract lineage (default: true)
    # include_schema: true    # Extract schema metadata (default: true)

    # Optional: Lineage retry settings
    # lineage_max_retries: 3                    # Max retry attempts (range: 1-10, default: 3)
    # lineage_retry_backoff_multiplier: 1.0     # Backoff delay multiplier (range: 0.1-10.0, default: 1.0)

    # Optional: Filtering patterns for entries
    # filter_config:
    #   entries:
    #     pattern:
    #       allow:
    #         - "bq_.*"      # Allow BigQuery entries
    #         - "pubsub_.*"  # Allow Pub/Sub entries
    #       deny:
    #         - ".*_test"    # Deny test entries
    #         - ".*_temp"    # Deny temporary entries

    # Optional: Performance tuning
    # batch_size: 1000 # Entries per batch for memory optimization (default: 1000)

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">dataplex_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL for Dataplex console (for generating external links). <div className="default-line default-line-with-docs">Default: <span className="default-value">https://console.cloud.google.com/dataplex</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_glossaries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest Dataplex Business Glossary entities as DataHub GlossaryNodes and GlossaryTerms. Glossaries, categories, and terms are emitted with correct parent hierarchy. Default: True. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_glossary_term_associations</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest term-to-asset associations via the Dataplex lookupEntryLinks API. For each ingested term, all entries_locations are queried per project to find linked assets. Requires a role granting resourcemanager.projects.get (e.g. roles/browser) on all configured projects to resolve GCP project numbers needed by the lookupEntryLinks API. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract lineage information using Dataplex Lineage API. Extracts table-level lineage relationships between entries. Lineage API calls automatically retry transient errors (timeouts, rate limits) with exponential backoff. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract and ingest schema metadata (columns, types, descriptions). Set to False to skip schema extraction for faster ingestion when only basic dataset metadata is needed. Disabling schema extraction can improve performance for large deployments. Default: True. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">lineage_max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retry attempts for lineage API calls when encountering transient errors (timeouts, rate limits, service unavailable). Each attempt uses exponential backoff. Higher values increase resilience but may slow down ingestion. Default: 3. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">lineage_retry_backoff_multiplier</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Multiplier for exponential backoff between lineage API retry attempts (in seconds). Wait time formula: multiplier * (2 ^ attempt_number), capped between 2-10 seconds. Higher values reduce API load but increase ingestion time. Default: 1.0. <div className="default-line default-line-with-docs">Default: <span className="default-value">1.0</span></div> |
| <div className="path-line"><span className="path-main">max_workers_entries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of parallel worker threads for fetching entry details (get_entry API calls). Entry detail fetching is the main bottleneck in the entries stage because each entry requires one blocking RPC. Increasing this value reduces wall-clock time proportionally up to the API quota limit. Increase for large deployments (>1k entries). Default: 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">max_workers_glossary</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of parallel worker threads for glossary ingestion (fetching terms and categories per glossary) and term-asset association traversal (lookupEntryLinks calls). Default: 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">max_workers_lineage</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of parallel worker threads for lineage lookups (search_links API calls). Lineage lookup volume scales with entries × lineage_locations, so parallelism here has a large impact on total ingestion time. Increase for large entry × location matrices. Default: 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | GCP credential information. If not specified, uses Application Default Credentials. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
| <div className="path-line"><span className="path-main">entries_locations</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of GCP regions to scan for Universal Catalog entries extraction. This list may include multi-regions (for example 'us', 'eu', 'asia') and single regions (for example 'us-central1'). Entries scanning runs across all configured entries_locations. Default: ['us', 'eu', 'asia', 'global'].  |
| <div className="path-line"><span className="path-prefix">entries_locations.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">filter_config</span></div> <div className="type-name-line"><span className="type-name">DataplexFilterConfig</span></div> | Filter configuration for Dataplex ingestion.  |
| <div className="path-line"><span className="path-prefix">filter_config.</span><span className="path-main">entries</span></div> <div className="type-name-line"><span className="type-name">EntriesFilterConfig</span></div> | Filter configuration specific to Dataplex Entries API (Universal Catalog).  |
| <div className="path-line"><span className="path-prefix">filter_config.entries.</span><span className="path-main">fqn_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">filter_config.entries.fqn_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">filter_config.entries.</span><span className="path-main">pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">filter_config.entries.pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">filter_config.</span><span className="path-main">entry_groups</span></div> <div className="type-name-line"><span className="type-name">EntryGroupFilterConfig</span></div> | Filter configuration for Dataplex entry groups.  |
| <div className="path-line"><span className="path-prefix">filter_config.entry_groups.</span><span className="path-main">pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">filter_config.entry_groups.pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">glossary_locations</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | GCP locations to scan for Dataplex Business Glossaries. Dataplex glossaries are typically created in 'global' but can exist in any location. Default: ['global'].  |
| <div className="path-line"><span className="path-prefix">glossary_locations.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">lineage_locations</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of GCP regions to scan for Dataplex lineage data. By default, includes all supported multi-regions and regions. Narrowing this list from the default is critical for better performance because lineage API calls scale with configured project/location pairs. This list may include multi-regions and single regions. In practice, lineage often resides in job regions while entries may be in multi-regions, so entries_locations and lineage_locations are configured separately. Example: ['eu', 'us-central1', 'europe-west1'].  |
| <div className="path-line"><span className="path-prefix">lineage_locations.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">project_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Explicit list of GCP project ids to ingest. Overrides project_id_pattern.  |
| <div className="path-line"><span className="path-prefix">project_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">project_labels</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Filter projects by labels in `key:value` format. Applied before project_id_pattern.  |
| <div className="path-line"><span className="path-prefix">project_labels.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for stale metadata removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "DataplexFilterConfig": {
      "additionalProperties": false,
      "description": "Filter configuration for Dataplex ingestion.",
      "properties": {
        "entry_groups": {
          "$ref": "#/$defs/EntryGroupFilterConfig",
          "description": "Filters for Dataplex entry group names."
        },
        "entries": {
          "$ref": "#/$defs/EntriesFilterConfig",
          "description": "Filters specific to Dataplex Entries API (Universal Catalog)."
        }
      },
      "title": "DataplexFilterConfig",
      "type": "object"
    },
    "EntriesFilterConfig": {
      "additionalProperties": false,
      "description": "Filter configuration specific to Dataplex Entries API (Universal Catalog).",
      "properties": {
        "pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns for Dataplex entry names to filter in ingestion."
        },
        "fqn_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns for Dataplex fully-qualified names to filter in ingestion."
        }
      },
      "title": "EntriesFilterConfig",
      "type": "object"
    },
    "EntryGroupFilterConfig": {
      "additionalProperties": false,
      "description": "Filter configuration for Dataplex entry groups.",
      "properties": {
        "pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns for entry group resource names to include/exclude."
        }
      },
      "title": "EntryGroupFilterConfig",
      "type": "object"
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
  "description": "Configuration for Google Dataplex source.\n\nProject selection (`project_ids`, `project_labels`, `project_id_pattern`) is\ninherited from `GcpProjectFilterConfig` and consumed by the shared\n`resolve_gcp_projects` helper. Auto-discovery (when `project_ids` is empty)\nuses Cloud Resource Manager `search_projects` and only requires\n`resourcemanager.projects.get` (e.g. `roles/browser`) on the candidate\nprojects \u2014 no folder/org-level grant is needed.",
  "properties": {
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Lineage Ingestion",
      "type": "boolean"
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
      "default": null,
      "description": "Stateful ingestion configuration for stale metadata removal."
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
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "project_ids": {
      "description": "Explicit list of GCP project ids to ingest. Overrides project_id_pattern.",
      "items": {
        "type": "string"
      },
      "title": "Project Ids",
      "type": "array"
    },
    "project_labels": {
      "description": "Filter projects by labels in `key:value` format. Applied before project_id_pattern.",
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
      "description": "Regex allow/deny pattern for GCP project ids."
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
      "description": "GCP credential information. If not specified, uses Application Default Credentials."
    },
    "entries_locations": {
      "description": "List of GCP regions to scan for Universal Catalog entries extraction. This list may include multi-regions (for example 'us', 'eu', 'asia') and single regions (for example 'us-central1'). Entries scanning runs across all configured entries_locations. Default: ['us', 'eu', 'asia', 'global'].",
      "items": {
        "type": "string"
      },
      "title": "Entries Locations",
      "type": "array"
    },
    "filter_config": {
      "$ref": "#/$defs/DataplexFilterConfig",
      "description": "Filters to control which Dataplex resources are ingested."
    },
    "include_schema": {
      "default": true,
      "description": "Whether to extract and ingest schema metadata (columns, types, descriptions). Set to False to skip schema extraction for faster ingestion when only basic dataset metadata is needed. Disabling schema extraction can improve performance for large deployments. Default: True.",
      "title": "Include Schema",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Whether to extract lineage information using Dataplex Lineage API. Extracts table-level lineage relationships between entries. Lineage API calls automatically retry transient errors (timeouts, rate limits) with exponential backoff.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "lineage_locations": {
      "description": "List of GCP regions to scan for Dataplex lineage data. By default, includes all supported multi-regions and regions. Narrowing this list from the default is critical for better performance because lineage API calls scale with configured project/location pairs. This list may include multi-regions and single regions. In practice, lineage often resides in job regions while entries may be in multi-regions, so entries_locations and lineage_locations are configured separately. Example: ['eu', 'us-central1', 'europe-west1'].",
      "items": {
        "type": "string"
      },
      "title": "Lineage Locations",
      "type": "array"
    },
    "lineage_max_retries": {
      "default": 3,
      "description": "Maximum number of retry attempts for lineage API calls when encountering transient errors (timeouts, rate limits, service unavailable). Each attempt uses exponential backoff. Higher values increase resilience but may slow down ingestion. Default: 3.",
      "maximum": 10,
      "minimum": 1,
      "title": "Lineage Max Retries",
      "type": "integer"
    },
    "lineage_retry_backoff_multiplier": {
      "default": 1.0,
      "description": "Multiplier for exponential backoff between lineage API retry attempts (in seconds). Wait time formula: multiplier * (2 ^ attempt_number), capped between 2-10 seconds. Higher values reduce API load but increase ingestion time. Default: 1.0.",
      "maximum": 10.0,
      "minimum": 0.1,
      "title": "Lineage Retry Backoff Multiplier",
      "type": "number"
    },
    "max_workers_entries": {
      "default": 10,
      "description": "Number of parallel worker threads for fetching entry details (get_entry API calls). Entry detail fetching is the main bottleneck in the entries stage because each entry requires one blocking RPC. Increasing this value reduces wall-clock time proportionally up to the API quota limit. Increase for large deployments (>1k entries). Default: 10.",
      "maximum": 100,
      "minimum": 1,
      "title": "Max Workers Entries",
      "type": "integer"
    },
    "max_workers_lineage": {
      "default": 10,
      "description": "Number of parallel worker threads for lineage lookups (search_links API calls). Lineage lookup volume scales with entries \u00d7 lineage_locations, so parallelism here has a large impact on total ingestion time. Increase for large entry \u00d7 location matrices. Default: 10.",
      "maximum": 100,
      "minimum": 1,
      "title": "Max Workers Lineage",
      "type": "integer"
    },
    "include_glossaries": {
      "default": true,
      "description": "Whether to ingest Dataplex Business Glossary entities as DataHub GlossaryNodes and GlossaryTerms. Glossaries, categories, and terms are emitted with correct parent hierarchy. Default: True.",
      "title": "Include Glossaries",
      "type": "boolean"
    },
    "include_glossary_term_associations": {
      "default": false,
      "description": "Whether to ingest term-to-asset associations via the Dataplex lookupEntryLinks API. For each ingested term, all entries_locations are queried per project to find linked assets. Requires a role granting resourcemanager.projects.get (e.g. roles/browser) on all configured projects to resolve GCP project numbers needed by the lookupEntryLinks API.",
      "title": "Include Glossary Term Associations",
      "type": "boolean"
    },
    "glossary_locations": {
      "description": "GCP locations to scan for Dataplex Business Glossaries. Dataplex glossaries are typically created in 'global' but can exist in any location. Default: ['global'].",
      "items": {
        "type": "string"
      },
      "title": "Glossary Locations",
      "type": "array"
    },
    "max_workers_glossary": {
      "default": 10,
      "description": "Number of parallel worker threads for glossary ingestion (fetching terms and categories per glossary) and term-asset association traversal (lookupEntryLinks calls). Default: 10.",
      "maximum": 100,
      "minimum": 1,
      "title": "Max Workers Glossary",
      "type": "integer"
    },
    "dataplex_url": {
      "default": "https://console.cloud.google.com/dataplex",
      "description": "Base URL for Dataplex console (for generating external links).",
      "title": "Dataplex Url",
      "type": "string"
    }
  },
  "title": "DataplexConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution
The Google Cloud Knowledge Catalog (Dataplex) connector will overwrite metadata from other Google Cloud source connectors (BigQuery, GCS, etc.) if they extract the same entities. If you're running multiple Google Cloud connectors, be aware that the last connector to run will determine the final metadata state for overlapping entities.
:::

#### Platform Alignment

Datasets discovered use the same URNs as native connectors (e.g., `bigquery`, `gcs`). This means:

- **No Duplication**: Google Cloud Knowledge Catalog (Dataplex) and native BigQuery/GCS connectors can run together - entities discovered by both will merge
- **Native Containers**: BigQuery tables appear in their native dataset containers
- **Unified View**: Users see a single view of all datasets regardless of discovery method

#### Custom Properties

The connector adds the following custom properties to datasets:

| Property                        | Always Present | Description                                                                              |
| ------------------------------- | -------------- | ---------------------------------------------------------------------------------------- |
| `dataplex_ingested`             | Yes            | Marker indicating the dataset was ingested via Google Cloud Knowledge Catalog (Dataplex) |
| `dataplex_entry_id`             | Yes            | The entry identifier in Google Cloud Knowledge Catalog (Dataplex)                        |
| `dataplex_entry_group`          | Yes            | The entry group containing this entry                                                    |
| `dataplex_fully_qualified_name` | Yes            | The fully qualified name of the entry                                                    |
| `dataplex_entry_type`           | No             | The Google Cloud Knowledge Catalog (Dataplex) entry type (e.g. `bigquery-table`)         |
| `dataplex_parent_entry`         | No             | The parent entry name, if set                                                            |
| `dataplex_source_resource`      | No             | The source resource identifier from the entry source                                     |
| `dataplex_source_system`        | No             | The source system from the entry source                                                  |
| `dataplex_source_platform`      | No             | The source platform from the entry source                                                |
| `dataplex_aspect_<aspect_type>` | No             | One property per aspect attached to the entry, named after the aspect type               |

#### Filtering Configuration

Filter which datasets to ingest using regex patterns with allow/deny lists:

**Example:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    filter_config:
      entries:
        pattern:
          allow:
            - "production_.*" # Only production datasets
          deny:
            - ".*_test" # Exclude test datasets
            - ".*_temp" # Exclude temporary datasets
```

#### Lineage

When `include_lineage` is enabled and proper permissions are granted, the connector extracts **table-level lineage** using the Dataplex Lineage API. The connector automatically tracks lineage from these Google Cloud systems:

**Supported Systems:**

- **BigQuery**: DDL (CREATE TABLE, CREATE TABLE AS SELECT, views, materialized views) and DML (SELECT, INSERT, MERGE, UPDATE, DELETE) operations
- **Cloud Data Fusion**: Pipeline executions
- **Cloud Composer**: Workflow orchestration
- **Dataflow**: Streaming and batch jobs
- **Dataproc**: Apache Spark and Apache Hive jobs (including Dataproc Serverless)
- **Vertex AI**: Models, datasets, feature store views, and feature groups

:::note
Only **BigQuery** lineage has been thoroughly tested with this connector. Lineage from other systems may work but has not been validated.
:::

**Not Supported:**

- **Column-level lineage**: The connector extracts only table-level lineage (column-level lineage is available in Dataplex Lineage API but not exposed through this connector)
- **Custom sources**: Only Google Cloud systems with automatic lineage tracking are supported
- **BigQuery Data Transfer Service**: Recurring loads are not automatically tracked

**Lineage Limitations:**

- Lineage data is retained for 30 days in Google Cloud Knowledge Catalog (Dataplex)
- Lineage may take up to 24 hours to appear after job completion
- Lineage is only available for entries with active lineage tracking enabled

For more details, see [Google Cloud Knowledge Catalog (Dataplex) Lineage Documentation](https://docs.cloud.google.com/dataplex/docs/about-data-lineage).

#### Configuration Options

**Metadata Extraction:**

- **`include_schema`** (default: `true`): Extract column metadata and types
- **`include_lineage`** (default: `true`): Extract table-level lineage (automatically retries transient errors)

#### Parallel Processing

Entry detail fetching and lineage lookups are parallelised using thread pools to significantly
reduce wall-clock ingestion time for large deployments.

**Entries stage** runs in three phases:

1. `list_entry_groups` + `list_entries` — sequential listing across all project × location pairs
   (fast; no parallelism needed)
2. `get_entry(ALL)` calls — parallel across a flat worker pool so entries are distributed evenly
   regardless of how they are spread across projects
3. Spanner entries via `search_entries` — sequential (already fully-fetched, nothing to parallelise)

**Lineage stage** dispatches one worker per entry to fetch `search_links` results across all
configured `lineage_locations`, so total API call time scales with
`max(entries / max_workers_lineage)` rather than `entries × lineage_locations`.

Two config fields control the thread pool sizes:

| Field                 | Default | Description                                      |
| --------------------- | ------- | ------------------------------------------------ |
| `max_workers_entries` | `10`    | Workers for `get_entry` calls (entries stage)    |
| `max_workers_lineage` | `10`    | Workers for `search_links` calls (lineage stage) |

Increase these values for large deployments, subject to your GCP API quota limits.

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Parallel processing (tune to your deployment size and API quota)
    max_workers_entries: 20 # default: 10
    max_workers_lineage: 40 # default: 20
```

**Lineage Retry Settings** (optional):

- **`lineage_max_retries`** (default: `3`, range: `1-10`): Retry attempts for transient errors
- **`lineage_retry_backoff_multiplier`** (default: `1.0`, range: `0.1-10.0`): Backoff delay multiplier

**Example Configuration:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    # Location for entries (Universal Catalog) - defaults to ["us", "eu", "asia", "global"]
    # Must be multi-region (us, eu, asia) for system entry groups like @bigquery
    entries_locations:
      - "us"

    # Metadata extraction settings
    include_schema: true # Enable schema metadata extraction (default: true)
    include_lineage: true # Enable lineage extraction with automatic retries

    # Lineage retry settings (optional, defaults shown)
    lineage_max_retries: 3 # Max retry attempts (range: 1-10)
    lineage_retry_backoff_multiplier: 1.0 # Exponential backoff multiplier (range: 0.1-10.0)
```

**Configuration for Large Deployments:**

For deployments with thousands of entries, memory optimization is important. The connector uses batched emission to keep memory bounded:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Performance tuning
    batch_size: 1000 # Process and emit 1000 entries at a time to optimize memory usage
```

#### Business Glossary

When `include_glossaries` is enabled (default), the connector ingests all [Dataplex Business Glossaries](https://cloud.google.com/dataplex/docs/glossaries-overview) from the configured `glossary_locations` (default: `global`) and emits the full Glossary → Category → Term hierarchy as DataHub Glossary entities.

Each term is emitted as a `GlossaryTerm` with:

- `term_source: EXTERNAL` and a `source_url` linking directly to the term in the Dataplex console
- `custom_properties` carrying `project_id`, `location`, `glossary_id`, and `term_id`

When `include_glossary_term_associations` is enabled (opt-in, default: `false`), the connector additionally resolves term-to-asset links using the Dataplex `lookupEntryLinks` API and attaches the corresponding terms to each linked DataHub dataset. This phase runs after entries are ingested, so only assets already discovered by the entries stage can be linked. It requires a role granting `resourcemanager.projects.get` (such as [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser)) on all configured projects. See the Permissions table in the [Prerequisites](#permissions) section above and the GCP [Resource Manager roles reference](https://cloud.google.com/iam/docs/understanding-roles#resource-manager-roles).

**Configuration:**

| Field                                | Default    | Description                                                                                                                                                                                                                |
| ------------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `include_glossaries`                 | `true`     | Ingest Dataplex Business Glossaries as `GlossaryNode`/`GlossaryTerm`                                                                                                                                                       |
| `include_glossary_term_associations` | `false`    | Attach glossary terms to linked datasets via `lookupEntryLinks`. Requires a role granting `resourcemanager.projects.get` such as [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser) (opt-in) |
| `glossary_locations`                 | `[global]` | GCP locations to scan for glossaries; most glossaries live in `global`                                                                                                                                                     |
| `max_workers_glossary`               | `10`       | Parallel workers for glossary ingestion and term-association lookups                                                                                                                                                       |

**Example:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_locations:
      - "us"

    # Business Glossary ingestion (enabled by default)
    include_glossaries: true
    glossary_locations:
      - "global"

    # Term-to-asset associations (opt-in; requires roles/browser or another
    # role granting resourcemanager.projects.get on each configured project)
    # include_glossary_term_associations: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Expected Knowledge Catalog (Dataplex) sync latency

Please be aware of the following documented delays when using this connector. These are standard Knowledge Catalog (Dataplex) behaviors and typically do not indicate an error:

- Metadata Sync: Updates to metadata or new entries can take up to 10 minutes to appear due to backend caching.
- Data Lineage: Lineage graphs are not real-time; updates typically take 30 minutes to 3 hours, but can take up to 24 hours to fully populate.
- Data Quality Results: Results from Auto Data Quality scans may have a slight processing delay before appearing in the UI.

If updates exceed these windows, check your [Cloud Logging](https://docs.cloud.google.com/dataplex/docs/logging) for specific job errors or permission issues.

#### Lineage Extraction Issues

**Automatic Retry Behavior:**

The connector automatically retries transient errors when extracting lineage:

- **Retried errors** (with exponential backoff): Timeouts (DeadlineExceeded), rate limiting (HTTP 429), service issues (HTTP 503, 500)
- **Non-retried errors** (logs warning and continues): Permission denied (HTTP 403), not found (HTTP 404), invalid argument (HTTP 400)

After exhausting retries, the connector logs a warning and continues processing other entries. You'll still get metadata even if lineage extraction fails for some entries.

**Common Issues:**

1. **Location scope**: Lineage API requests are scoped using each entry's own Dataplex location.
2. **Missing permissions**: Ensure service account has `roles/datalineage.viewer` role on all projects.
3. **No lineage data**: Some entries may not have lineage if they weren't created through supported systems (BigQuery DDL/DML, Cloud Data Fusion, etc.).
4. **Rate limiting**: If you encounter persistent rate limiting, increase `lineage_retry_backoff_multiplier` to add more delay between retries, or decrease `lineage_max_retries` if you prefer faster failure.

#### Others

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.dataplex.dataplex.DataplexSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dataplex/dataplex.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Google Cloud Knowledge Catalog (Dataplex), feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
