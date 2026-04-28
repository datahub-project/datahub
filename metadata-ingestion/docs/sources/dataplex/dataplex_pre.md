### Overview

The `dataplex` module ingests metadata from Google Cloud Knowledge Catalog (Dataplex) into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The connector extracts metadata from Google Cloud Knowledge Catalog (Dataplex) using the **Universal Catalog Entries API**. This API extracts entries from system-managed entry groups for Google Cloud services and is the recommended approach for discovering resources across your GCP organization.

#### Spanner entry collection behavior

Spanner entries are collected through an additional `search_entries` workaround after the entry-group traversal phase. Because those entries are not discovered through `list_entry_groups`, `filter_config.entry_groups.pattern` does not apply to them. Use entry-level filters (`filter_config.entries.pattern` and `filter_config.entries.fqn_pattern`) to control Spanner inclusion.

### Prerequisites

Refer to [Google Cloud Knowledge Catalog (Dataplex) documentation](https://cloud.google.com/dataplex/docs) for the basics.

#### API Enablement

Enable the following APIs on all target projects:

- **Dataplex API** (`dataplex.googleapis.com`) — see [Enable Knowledge Catalog](https://docs.cloud.google.com/dataplex/docs/enable-api)
- **Data Lineage API** (`datalineage.googleapis.com`) — required for lineage extraction (`include_lineage: true`), see [Enable Data Lineage API](https://docs.cloud.google.com/dataplex/docs/use-lineage#enable-apis)

Some asset types require additional setup. For example, Cloud SQL instances must be connected to Dataplex to enable automatic metadata harvesting (schemas, tables, and views):

```sh
gcloud sql instances patch my-cloud-sql-instance --enable-dataplex-integration --project=my-gcp-project
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

| Feature                                                              | Required Role                                                                                                                                            |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Universal Catalog Entries API (core ingestion)                       | [`roles/dataplex.catalogViewer`](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)                                                |
| Lineage extraction (`include_lineage: true`)                         | [`roles/datalineage.viewer`](https://cloud.google.com/dataplex/docs/iam-roles#datalineage.viewer)                                                        |
| Business Glossary ingestion (`include_glossaries: true`)             | [`roles/dataplex.catalogViewer`](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)                                                |
| Term-asset associations (`include_glossary_term_associations: true`) | [`roles/resourcemanager.projectViewer`](https://cloud.google.com/resource-manager/docs/access-control-proj) — required for resolving GCP project numbers |

:::tip "Lineage requires the role on multiple projects"

Grant `roles/datalineage.viewer` on all projects where the corresponding process is actually executed. Note it may differ from the project containing the asset.
:::

Additional asset-specific viewer roles:

- `roles/aiplatform.viewer` (Vertex AI Viewer) is required when ingesting Vertex AI assets.
- `roles/spanner.viewer` (Cloud Spanner Viewer) is required when ingesting Cloud Spanner assets.
