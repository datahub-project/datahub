### Overview

The `google-drive` module walks the folders and shared drives listed in `root_folder_ids`, exports every Google Doc it finds as Markdown, and ingests each one as a DataHub Document. Folders become Documents with the `Folder` subtype, so the Drive hierarchy is preserved for navigation.

This source ingests Google Docs only. Sheets, Slides, PDFs, and other file types in the same folders are ignored, as are shortcuts and files in the trash.

### Prerequisites

#### 1. Enable the Google Drive API

In the Google Cloud project that owns your service account, enable the **Google Drive API** (APIs & Services → Library → Google Drive API).

#### 2. Create a service account

Create a service account and a JSON key for it. The connector takes the key's fields under `credential`, the same shape the BigQuery source uses.

If you omit `credential`, the connector uses Application Default Credentials. Those credentials must carry the `https://www.googleapis.com/auth/drive.readonly` scope; the `cloud-platform` scope alone does not grant Drive access. On Google Cloud, attach the service account to the VM or workload and grant it folder access as described below.

To test locally with your own Google account instead of a service account key:

1. In your Google Cloud project, configure the OAuth consent screen (APIs & Services → OAuth consent screen). If your account is in a Google Workspace organization, choose **Internal**.
2. Create an OAuth client ID of type **Desktop app** (APIs & Services → Credentials) and download its JSON file. The gcloud CLI's built-in OAuth client cannot request the Drive scope, so you need your own.
3. Log in with that client:

   ```shell
   gcloud auth application-default login \
     --client-id-file=<path-to-oauth-client.json> \
     --scopes=https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform
   ```

The connector then reads Drive as you, so it sees the folders your account can open. `delegated_user` is not supported with these credentials.

#### 3. Grant access to the folders

Choose one of the following:

- **Share the folders with the service account.** Share each root folder (or shared drive) with the service account's `client_email` as a Viewer. The connector then sees exactly what is shared with it.
- **Use domain-wide delegation.** In the Google Workspace Admin console (Security → API controls → Domain-wide delegation), authorize the service account's client ID for the scope `https://www.googleapis.com/auth/drive.readonly`. Then set `delegated_user` to a Workspace user who can read the folders, and the connector reads Drive as that user.

The connector only requests the read-only Drive scope.

:::caution
DataHub does not mirror Google Drive permissions. Anyone who can read a Document in DataHub can read its text, so choose root folders whose audience matches your DataHub audience.
:::

#### 4. Find the folder IDs

Open the folder in Drive and copy the ID from the URL (`https://drive.google.com/drive/folders/<folder-id>`). You can also paste the full folder URL into `root_folder_ids`. For a shared drive, use the ID from the shared drive's URL.

#### 5. Embedding provider (optional)

By default the connector loads the embedding configuration from your DataHub server and generates semantic-search embeddings for every Doc. The machine that runs ingestion needs credentials for that embedding provider (for example, AWS credentials for Amazon Bedrock). Without them, Docs are ingested without embeddings and are exported again on every run until embedding succeeds. See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for details.

#### 6. DataHub permissions

The DataHub access token used by the ingestion recipe must be allowed to read, create, and edit Document entities. The source reads Documents it ingested earlier to skip Docs that have not changed; without read access, every Doc is exported again on every run.

#### 7. Python version

The `google-drive` plugin requires Python 3.11 or later.
