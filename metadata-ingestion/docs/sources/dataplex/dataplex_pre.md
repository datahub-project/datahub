### Overview

The `dataplex` module ingests metadata from Google Cloud Knowledge Catalog (Dataplex) into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The connector extracts metadata from Google Cloud Knowledge Catalog (Dataplex) using the **Universal Catalog Entries API**. This API extracts entries from system-managed entry groups for Google Cloud services and is the recommended approach for discovering resources across your GCP organization.

#### Extraction methods

The connector supports two ways of fetching entries, controlled by `extraction_method`:

- **`api`** (default) — iterates the configured projects and lists entries via the Catalog `list_entry_groups` / `list_entries` / `get_entry` APIs. This only returns entries **physically created** in each project's entry groups.
- **`export`** — submits one Dataplex [metadata export job](https://docs.cloud.google.com/dataplex/docs/export-metadata) per configured entries location (scoped to the configured projects and the entry types the connector supports), waits for the jobs to finish, and then reads the exported JSONL from a Cloud Storage bucket. Requires the `export_config` section.

Use `export` when your organization runs a **central catalog** architecture: tenant projects grant a central Dataplex project permission to read their metadata, and assets surface in the central project via Dataplex catalog linking/federation without being physically created there. Those linked entries are invisible to `list_entries` (the `api` method returns nothing), but a metadata export scoped to the central project includes them.

Notes on `export` mode:

- One export job runs per entry in `entries_locations`, and each location must resolve to a GCS bucket via `export_config.export_bucket_config` or `export_config.bucket_base_name` (bucket name `{bucket_base_name}-{location}`). The bucket for a `global`-location job must be in a compatible (multi-)region.
- `filter_config.entry_groups.pattern` does not apply (the export is scoped by entry type, not entry group); use the entry-level `filter_config.entries.pattern` / `fqn_pattern` filters instead.
- Lineage and Business Glossary extraction work identically in both methods — they use the live Data Lineage and Business Glossary APIs.
- If an export job fails or times out, or the exported output cannot be read completely, the run is reported as failed and stale-entity soft-deletion is skipped for that run, so temporarily missing entities are not tombstoned.

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

For `extraction_method: export`, the following additional grants are required (see [Export metadata](https://docs.cloud.google.com/dataplex/docs/export-metadata#required-roles)):

| Feature                                             | Required Role                                                                                                                                      |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Running metadata export jobs                        | [`roles/dataplex.metadataJobOwner`](https://cloud.google.com/dataplex/docs/iam-roles) on `export_config.export_job_runner_project`                 |
| Export scope access                                 | [`roles/dataplex.catalogEditor`](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogEditor) on each project in the export scope      |
| Writing/reading export output in the GCS bucket(s)  | [`roles/storage.objectUser`](https://cloud.google.com/storage/docs/access-control/iam-roles) on each configured export bucket                      |

:::tip "Lineage requires the role on multiple projects"

Grant `roles/datalineage.viewer` on all projects where the corresponding process is actually executed. Note it may differ from the project containing the asset.
:::

Additional asset-specific viewer roles:

- `roles/aiplatform.viewer` (Vertex AI Viewer) is required when ingesting Vertex AI assets.
- `roles/spanner.viewer` (Cloud Spanner Viewer) is required when ingesting Cloud Spanner assets.
