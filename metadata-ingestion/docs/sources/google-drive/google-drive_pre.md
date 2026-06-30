### Overview

:::caution Not Supported with Remote Executor
This source is available as a private beta feature on DataHub Cloud. Note that running the connector using the Remote Executor is not yet supported.
:::

The Google Drive source ingests Google Docs, and optionally Google Slides and Google Sheets, from Google Drive as DataHub Document entities with full-text markdown content and optional semantic embeddings for semantic search.

The source is named `google-drive` (not `google-docs`) because it can ingest all three Google Workspace file types — Docs, Slides, and Sheets — accessible via the Google Drive API.

#### Key Features

##### 1. Content Extraction

- **Google Docs**: Full text extracted and exported as markdown
- **Google Slides**: Exported as plain text (one block per slide); opt-in via `include_slides`
- **Google Sheets**: Exported as CSV-formatted text; opt-in via `include_sheets`
- **Folder Hierarchy**: Drive folders are materialised as Document entities to preserve parent-child relationships in the DataHub UI

##### 2. Flexible Scope

- **All-Drive mode**: When `folder_ids` is empty, ingests every accessible file
- **Folder mode**: Restrict ingestion to specific folders (supports full Drive URLs or bare IDs)
- **Recursive discovery**: Sub-folders are traversed automatically (controlled by `recursive`)
- **MIME-type toggles**: `include_docs`, `include_slides`, `include_sheets` select which file types to ingest

##### 3. Embedding Generation

Optional semantic search support:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Automatic chunking**: Documents are automatically chunked for optimal embedding generation
- **Automatic deduplication**: Prevents duplicate chunk embeddings

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed setup and advanced options.

##### 4. Stateful Ingestion

Supports incremental updates via stateful ingestion:

- **Deletion Detection**: Automatically removes stale entities from DataHub when files are deleted from Drive
- **Embedding Deduplication**: Skips embedding regeneration for documents whose content hash is unchanged between runs (every document is re-emitted each run; only the embedding step is skipped for unchanged content)
- **State Persistence**: Maintains ingestion state between runs

#### Related Documentation

- [Google Drive API](https://developers.google.com/drive/api/guides/about-sdk)
- [Google Service Accounts](https://cloud.google.com/iam/docs/service-accounts)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)
- [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)

### Prerequisites

#### 1. Enable the Google Drive API

1. Open the [Google Cloud Console](https://console.cloud.google.com/)
2. Select or create a project
3. Go to **APIs & Services → Library**
4. Search for **Google Drive API** and click **Enable**

#### 2. Create Credentials

Choose one of the following authentication methods.

##### Service Account (Recommended for Production)

1. Go to **IAM & Admin → Service Accounts** in the Cloud Console
2. Click **Create Service Account**, give it a name (e.g., `datahub-drive-reader`), and click **Done**
3. Open the service account, go to the **Keys** tab, click **Add Key → Create new key**, and download the JSON file
4. Share the Drive folders you want to ingest with the service account's email address (grant **Viewer** access)

Provide the key file path in the config:

```yaml
credentials:
  service_account_key_file: "/path/to/service-account.json"
```

Alternatively, pass the JSON contents directly (useful with secrets managers):

```yaml
credentials:
  service_account_key_json: "${GOOGLE_SA_KEY_JSON}"
```

##### Application Default Credentials (GKE / Cloud Run / Local)

If you are running on Google Cloud infrastructure or have run `gcloud auth application-default login` locally, no explicit credential config is needed — the source picks up credentials automatically.

#### 3. Grant Access to Folders

The authenticated account (service account or user) must have **Viewer** (or higher) access to every folder and file you want to ingest. **Share each folder directly with the service account's email** (e.g. `datahub-drive-reader@<project>.iam.gserviceaccount.com`).

> **Important:** A service account is a distinct identity in the `*.iam.gserviceaccount.com` domain — it is **not** a member of your Google Workspace organization. As a result, "anyone in your organization with the link" sharing does **not** grant the service account access; the folder must be shared with the service account's email address explicitly. This connector reads only what is shared with the authenticated account and does not currently support domain-wide delegation (user impersonation).

##### Troubleshooting: "Folder not accessible"

If ingestion reports a `Folder not accessible` failure (HTTP 404/403) and discovers no files:

- Confirm the `folder_id` is correct (copy it from the folder's URL: `https://drive.google.com/drive/folders/<FOLDER_ID>`).
- Confirm the folder is shared with the **service account's email** (not just with your own user), with at least **Viewer** access.
- Remember that organization-wide link sharing does not cover the service account (see the note above).

#### 4. Embedding Provider (Optional)

If you want semantic search capabilities, configure an embedding provider in your DataHub instance.

Supported providers include Cohere (API key) and AWS Bedrock (IAM roles). The connector will use sensible defaults for chunking and embedding configuration.

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed provider setup and configuration options.
