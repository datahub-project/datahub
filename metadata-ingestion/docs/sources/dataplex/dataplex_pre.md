### Overview

The `dataplex` module ingests metadata from Dataplex into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The Dataplex connector extracts metadata from Google Dataplex using the **Universal Catalog Entries API**. This API extracts entries from system-managed entry groups for Google Cloud services and is the recommended approach for discovering resources across your GCP organization.

#### Spanner entry collection behavior

Spanner entries are collected through an additional `search_entries` workaround after the entry-group traversal phase. Because those entries are not discovered through `list_entry_groups`, `filter_config.entry_groups.pattern` does not apply to them. Use entry-level filters (`filter_config.entries.pattern` and `filter_config.entries.fqn_pattern`) to control Spanner inclusion.

#### Supported services

- **BigQuery**: datasets, tables, models, routines, connections, and linked datasets
- **Cloud SQL**: instances
- **AlloyDB**: instances, databases, schemas, tables, and views
- **Spanner**: instances, databases, and tables
- **Pub/Sub**: topics and subscriptions
- **Cloud Storage**: buckets
- **Bigtable**: instances, clusters, and tables
- **Vertex AI**: models, datasets, and feature stores
- **Dataform**: repositories and workflows
- **Dataproc Metastore**: services and databases

:::note
Only **BigQuery** and **Cloud Storage (GCS)** have been thoroughly tested with this connector. Other services may work but have not been validated.
:::

### Prerequisites

Refer to [Dataplex documentation](https://cloud.google.com/dataplex/docs) for Dataplex basics.

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

Grant the following permissions to the service account on all target projects.

**Universal Catalog Entries API:**

**Default GCP Role:** [roles/dataplex.catalogViewer](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)

| Permission                  | Description                           |
| --------------------------- | ------------------------------------- |
| `dataplex.entryGroups.get`  | Retrieve specific entry group details |
| `dataplex.entryGroups.list` | View all entry groups in a location   |
| `dataplex.entries.get`      | Access entry metadata and details     |
| `dataplex.entries.getData`  | View data aspects within entries      |
| `dataplex.entries.list`     | Enumerate entries within groups       |

**Lineage extraction** (optional, `include_lineage: true`):

**Default GCP Role:** [roles/datalineage.viewer](https://docs.cloud.google.com/iam/docs/roles-permissions/datalineage#datalineage.viewer)

| Permission                 | Description                               |
| -------------------------- | ----------------------------------------- |
| `datalineage.links.get`    | Allows a user to view lineage links       |
| `datalineage.links.search` | Allows a user to search for lineage links |
