### Overview

The `google-drive` module walks the folders and shared drives listed in `root_folder_ids`, exports every Google Doc it finds as Markdown, and ingests each one as a DataHub Document. Folders become Documents with the `Folder` subtype, so the Drive hierarchy is preserved for navigation.

Version 1 ingests Google Docs only. Sheets, Slides, PDFs, and other file types in the same folders are ignored, as are shortcuts and files in the trash.

### Prerequisites

#### 1. Enable the Google Drive API

In the Google Cloud project that owns your service account, enable the **Google Drive API** (APIs & Services → Library → Google Drive API).

#### 2. Create a service account

Create a service account and a JSON key for it. The connector takes the key's fields under `credential`, the same shape the BigQuery source uses. If you omit `credential`, the connector uses Application Default Credentials.

#### 3. Grant access to the folders

Choose one of the following:

- **Share the folders with the service account.** Share each root folder (or shared drive) with the service account's `client_email` as a Viewer. The connector then sees exactly what is shared with it.
- **Use domain-wide delegation.** In the Google Workspace Admin console (Security → API controls → Domain-wide delegation), authorize the service account's client ID for the scope `https://www.googleapis.com/auth/drive.readonly`. Then set `delegated_user` to a Workspace user who can read the folders, and the connector reads Drive as that user.

The connector only requests the read-only Drive scope.

#### 4. Find the folder IDs

Open the folder in Drive and copy the ID from the URL (`https://drive.google.com/drive/folders/<folder-id>`). You can also paste the full folder URL into `root_folder_ids`. For a shared drive, use the ID from the shared drive's URL.

#### 5. Embedding provider (optional)

By default the connector loads the embedding configuration from your DataHub server and generates semantic-search embeddings for every Doc. See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for details.
