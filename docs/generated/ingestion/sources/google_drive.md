


# Google Drive

## Overview

Google Drive is Google Workspace's file storage and collaboration service, and Google Docs is its document editor. Learn more in the [official Google Drive documentation](https://support.google.com/drive).

The DataHub integration for Google Drive ingests Google Docs from the folders and shared drives you choose as DataHub Document entities. It keeps the folder hierarchy, owners, and links back to Drive, generates semantic-search embeddings inline, and supports stateful deletion detection.

## Concept Mapping

| Source Concept                 | DataHub Concept              | Notes                                                                                                                    |
| ------------------------------ | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Configured folder/shared drive | Document (subtype `Folder`)  | Emitted when `create_root_documents` is enabled (the default).                                                           |
| Folder                         | Document (subtype `Folder`)  | Empty body; parent of the documents and folders it contains.                                                             |
| Google Doc                     | Document                     | Body is the Doc exported as Markdown; with the default `EXTERNAL` import mode, links back to Drive via its external URL. |
| File owner                     | CorpUser (owner)             | Keyed by email address. Files in shared drives have no owners.                                                           |
| Folder path                    | Browse path, parent document | Every ancestor folder is a clickable browse-path entry; with `create_root_documents: false`, the root is a plain label.  |
| Google Workspace               | Platform instance (optional) | Set `platform_instance` to distinguish workspaces; URNs use globally unique file IDs.                                    |


## Module `google-drive`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default via `extract_owners`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| [Probe](../../../cli.md#recipe) | ✅ | Check what a recipe would ingest before running it, with `datahub recipe probe`. |
| Test Connection | ✅ | Enabled by default. |

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


### Install the Plugin
```shell
pip install 'acryl-datahub[google-drive]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: google-drive
  config:
    # Service account key fields. Omit `credential` to use Application Default Credentials.
    credential:
      project_id: my-project
      private_key_id: "${GDRIVE_PRIVATE_KEY_ID}"
      private_key: "${GDRIVE_PRIVATE_KEY}"
      client_email: datahub-ingest@my-project.iam.gserviceaccount.com
      client_id: "123456789012345678901"

    # Optional: read Drive as this Workspace user (requires domain-wide delegation).
    # delegated_user: docs-reader@example.com

    # Folder IDs, shared drive IDs, or folder URLs.
    root_folder_ids:
      - "https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUv"

    folder_pattern:
      deny:
        - ".*/Archive$"
    document_pattern:
      deny:
        - "^Copy of "

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">root_folder_ids</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | Google Drive folder IDs, shared drive IDs, or Drive folder URLs to ingest. Google Docs under each root are ingested, recursively by default.  |
| <div className="path-line"><span className="path-prefix">root_folder_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">continue_on_failure</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to keep ingesting other documents when one document fails. Set to false to abort the run on the first per-document error. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">create_root_documents</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit a folder document for each configured root. When false, items directly inside a root attach to `parent_document_urn` (or become top-level documents). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">delegated_user</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Email of a Google Workspace user to impersonate via domain-wide delegation. Without it, the service account only sees files that are explicitly shared with its client_email. Requires `credential`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">document_import_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL"  |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Map Google Drive file owners to DataHub owners (users keyed by email address). Files in shared drives have no owners. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_documents</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of documents to embed per ingestion run. The run fails once this limit is reached. Set to 0 or -1 to disable the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">max_text_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Skip Google Docs whose exported text (after images are replaced with placeholders) is longer than this many characters. A skipped document that was ingested earlier is soft-deleted by stale metadata removal. 0 disables the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000000</span></div> |
| <div className="path-line"><span className="path-main">min_text_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Skip Google Docs whose exported text is shorter than this many characters. 0 keeps every non-empty document. <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-main">parent_document_urn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional URN of an existing DataHub document to place the imported folder tree under. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional identifier for this Google Workspace, attached to every ingested document. Document URNs do not depend on it because Drive file IDs are globally unique. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">recursive</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to walk subfolders. When false, only Google Docs directly inside each root are ingested and no subfolder documents are emitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">skip_empty_documents</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Skip Google Docs whose exported text is empty. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">chunking</span></div> <div className="type-name-line"><span className="type-name">ChunkingConfig</span></div> | Chunking strategy configuration.  |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">combine_text_under_n_chars</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Combine chunks smaller than this size <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">max_characters</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum characters per chunk <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">max_chunks_per_document</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of chunks embedded per document. A document that produces more chunks is truncated to the first N so its semanticContent aspect stays under the Kafka producer size limit; the dropped chunks are reported as a warning, not a failure. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">overlap</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Character overlap between chunks <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "basic", "by_title" <div className="default-line default-line-with-docs">Default: <span className="default-value">by&#95;title</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | Service account key used to call the Google Drive API. If omitted, Application Default Credentials are used. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
| <div className="path-line"><span className="path-main">document_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">document_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">embedding</span></div> <div className="type-name-line"><span className="type-name">EmbeddingConfig</span></div> | Embedding generation configuration. <br />  <br /> Default behavior: Fetches configuration from DataHub server automatically. <br /> Override behavior: Validates local config against server when explicitly set.  |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">allow_local_embedding_config</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | BREAK-GLASS: Allow local config without server validation. NOT RECOMMENDED - may break semantic search. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | API key for Cohere (not needed for Bedrock with IAM roles) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region for Bedrock. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Batch size for embedding API calls <div className="default-line default-line-with-docs">Default: <span className="default-value">25</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">documents_per_minute</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of documents to embed per minute when rate_limit is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">endpoint</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Endpoint URL for local embedding server (e.g., http://localhost:11434/v1/embeddings). Only used when provider='local'. Falls back to LOCAL_EMBEDDING_ENDPOINT env var. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">input_type</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Input type for Cohere embeddings <div className="default-line default-line-with-docs">Default: <span className="default-value">search&#95;document</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">model</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Model name. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">model_embedding_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Storage key for embeddings (e.g., 'cohere_embed_v3'). Required if overriding server config. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">onnx_model_dir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Directory containing model.onnx (or model_quantized.onnx) and tokenizer.json. Only used when provider='onnx'. Falls back to ONNX_EMBEDDING_MODEL_DIR env var. Must be the same model the GMS query-side provider uses, or semantic search will not match. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">onnx_pooling</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pooling strategy for provider='onnx': 'cls' (first-token, default) or 'mean' (attention-masked mean). Must match the GMS provider's pooling for query/doc vector parity. When config is loaded from the server, falls back to the ONNX_EMBEDDING_POOLING env var (the same variable GMS reads), since the server API does not expose pooling. <div className="default-line default-line-with-docs">Default: <span className="default-value">cls</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">provider</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Embedding provider. 'local' calls a locally-running OpenAI-compatible server (e.g. Ollama). 'vertex_ai' uses GCP. 'onnx' runs a local ONNX model in-process (matches the GMS built-in provider). 'classical' is a deterministic hashed-feature embedding with no external service (matches the GMS built-in provider). If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">rate_limit</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable rate limiting for embedding API calls. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">request_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Per-call HTTP timeout in seconds for embedding providers. <div className="default-line default-line-with-docs">Default: <span className="default-value">60.0</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">vertex_location</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP region for the Vertex AI endpoint (e.g., 'us-east1', 'us-central1'). Matches the Java server default. <div className="default-line default-line-with-docs">Default: <span className="default-value">us-east1</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">vertex_project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP project ID for Vertex AI. Required when provider is 'vertex_ai' and using local config. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
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
    "ChunkingConfig": {
      "additionalProperties": false,
      "description": "Chunking strategy configuration.",
      "properties": {
        "strategy": {
          "default": "by_title",
          "description": "Chunking strategy to use",
          "enum": [
            "basic",
            "by_title"
          ],
          "title": "Strategy",
          "type": "string"
        },
        "max_characters": {
          "default": 500,
          "description": "Maximum characters per chunk",
          "title": "Max Characters",
          "type": "integer"
        },
        "overlap": {
          "default": 0,
          "description": "Character overlap between chunks",
          "title": "Overlap",
          "type": "integer"
        },
        "combine_text_under_n_chars": {
          "default": 100,
          "description": "Combine chunks smaller than this size",
          "title": "Combine Text Under N Chars",
          "type": "integer"
        },
        "max_chunks_per_document": {
          "default": 100,
          "description": "Maximum number of chunks embedded per document. A document that produces more chunks is truncated to the first N so its semanticContent aspect stays under the Kafka producer size limit; the dropped chunks are reported as a warning, not a failure.",
          "minimum": 1,
          "title": "Max Chunks Per Document",
          "type": "integer"
        }
      },
      "title": "ChunkingConfig",
      "type": "object"
    },
    "DocumentImportMode": {
      "description": "Whether ingested documents are native (editable) or external (read-only references).",
      "enum": [
        "NATIVE",
        "EXTERNAL"
      ],
      "title": "DocumentImportMode",
      "type": "string"
    },
    "EmbeddingConfig": {
      "additionalProperties": false,
      "description": "Embedding generation configuration.\n\nDefault behavior: Fetches configuration from DataHub server automatically.\nOverride behavior: Validates local config against server when explicitly set.",
      "properties": {
        "provider": {
          "anyOf": [
            {
              "enum": [
                "bedrock",
                "cohere",
                "openai",
                "local",
                "vertex_ai",
                "onnx",
                "classical"
              ],
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Embedding provider. 'local' calls a locally-running OpenAI-compatible server (e.g. Ollama). 'vertex_ai' uses GCP. 'onnx' runs a local ONNX model in-process (matches the GMS built-in provider). 'classical' is a deterministic hashed-feature embedding with no external service (matches the GMS built-in provider). If not set, loads from server.",
          "title": "Provider"
        },
        "endpoint": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Endpoint URL for local embedding server (e.g., http://localhost:11434/v1/embeddings). Only used when provider='local'. Falls back to LOCAL_EMBEDDING_ENDPOINT env var.",
          "title": "Endpoint"
        },
        "onnx_model_dir": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Directory containing model.onnx (or model_quantized.onnx) and tokenizer.json. Only used when provider='onnx'. Falls back to ONNX_EMBEDDING_MODEL_DIR env var. Must be the same model the GMS query-side provider uses, or semantic search will not match.",
          "title": "Onnx Model Dir"
        },
        "onnx_pooling": {
          "default": "cls",
          "description": "Pooling strategy for provider='onnx': 'cls' (first-token, default) or 'mean' (attention-masked mean). Must match the GMS provider's pooling for query/doc vector parity. When config is loaded from the server, falls back to the ONNX_EMBEDDING_POOLING env var (the same variable GMS reads), since the server API does not expose pooling.",
          "title": "Onnx Pooling",
          "type": "string"
        },
        "model": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Model name. If not set, loads from server.",
          "title": "Model"
        },
        "model_embedding_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Storage key for embeddings (e.g., 'cohere_embed_v3'). Required if overriding server config. If not set, loads from server.",
          "title": "Model Embedding Key"
        },
        "aws_region": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS region for Bedrock. If not set, loads from server.",
          "title": "Aws Region"
        },
        "vertex_project_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "GCP project ID for Vertex AI. Required when provider is 'vertex_ai' and using local config. If not set, loads from server.",
          "title": "Vertex Project Id"
        },
        "vertex_location": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": "us-east1",
          "description": "GCP region for the Vertex AI endpoint (e.g., 'us-east1', 'us-central1'). Matches the Java server default.",
          "title": "Vertex Location"
        },
        "api_key": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "API key for Cohere (not needed for Bedrock with IAM roles)",
          "title": "Api Key"
        },
        "batch_size": {
          "default": 25,
          "description": "Batch size for embedding API calls",
          "title": "Batch Size",
          "type": "integer"
        },
        "request_timeout": {
          "default": 60.0,
          "description": "Per-call HTTP timeout in seconds for embedding providers.",
          "exclusiveMinimum": 0,
          "title": "Request Timeout",
          "type": "number"
        },
        "input_type": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": "search_document",
          "description": "Input type for Cohere embeddings",
          "title": "Input Type"
        },
        "rate_limit": {
          "default": true,
          "description": "Enable rate limiting for embedding API calls.",
          "title": "Rate Limit",
          "type": "boolean"
        },
        "documents_per_minute": {
          "default": 300,
          "description": "Maximum number of documents to embed per minute when rate_limit is enabled.",
          "exclusiveMinimum": 0,
          "title": "Documents Per Minute",
          "type": "integer"
        },
        "allow_local_embedding_config": {
          "default": false,
          "description": "BREAK-GLASS: Allow local config without server validation. NOT RECOMMENDED - may break semantic search.",
          "title": "Allow Local Embedding Config",
          "type": "boolean"
        }
      },
      "title": "EmbeddingConfig",
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
  "properties": {
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
      "description": "Stateful Ingestion Config"
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
      "description": "Service account key used to call the Google Drive API. If omitted, Application Default Credentials are used."
    },
    "delegated_user": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Email of a Google Workspace user to impersonate via domain-wide delegation. Without it, the service account only sees files that are explicitly shared with its client_email. Requires `credential`.",
      "title": "Delegated User"
    },
    "root_folder_ids": {
      "description": "Google Drive folder IDs, shared drive IDs, or Drive folder URLs to ingest. Google Docs under each root are ingested, recursively by default.",
      "items": {
        "type": "string"
      },
      "title": "Root Folder Ids",
      "type": "array"
    },
    "recursive": {
      "default": true,
      "description": "Whether to walk subfolders. When false, only Google Docs directly inside each root are ingested and no subfolder documents are emitted.",
      "title": "Recursive",
      "type": "boolean"
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns matched against folder paths such as 'Root/Engineering/Archive' (the root folder's name comes first). A denied folder is skipped together with everything beneath it. Configured roots are never filtered."
    },
    "document_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns matched against Google Doc titles."
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
      "description": "Optional identifier for this Google Workspace, attached to every ingested document. Document URNs do not depend on it because Drive file IDs are globally unique.",
      "title": "Platform Instance"
    },
    "document_import_mode": {
      "$ref": "#/$defs/DocumentImportMode",
      "default": "EXTERNAL",
      "description": "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only references that link back to Google Drive."
    },
    "parent_document_urn": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional URN of an existing DataHub document to place the imported folder tree under.",
      "title": "Parent Document Urn"
    },
    "create_root_documents": {
      "default": true,
      "description": "Emit a folder document for each configured root. When false, items directly inside a root attach to `parent_document_urn` (or become top-level documents).",
      "title": "Create Root Documents",
      "type": "boolean"
    },
    "extract_owners": {
      "default": true,
      "description": "Map Google Drive file owners to DataHub owners (users keyed by email address). Files in shared drives have no owners.",
      "title": "Extract Owners",
      "type": "boolean"
    },
    "skip_empty_documents": {
      "default": true,
      "description": "Skip Google Docs whose exported text is empty.",
      "title": "Skip Empty Documents",
      "type": "boolean"
    },
    "min_text_length": {
      "default": 0,
      "description": "Skip Google Docs whose exported text is shorter than this many characters. 0 keeps every non-empty document.",
      "minimum": 0,
      "title": "Min Text Length",
      "type": "integer"
    },
    "max_text_length": {
      "default": 1000000,
      "description": "Skip Google Docs whose exported text (after images are replaced with placeholders) is longer than this many characters. A skipped document that was ingested earlier is soft-deleted by stale metadata removal. 0 disables the limit.",
      "minimum": 0,
      "title": "Max Text Length",
      "type": "integer"
    },
    "chunking": {
      "$ref": "#/$defs/ChunkingConfig",
      "description": "Configuration for document chunking (required for embeddings)."
    },
    "embedding": {
      "$ref": "#/$defs/EmbeddingConfig",
      "description": "Configuration for generating vector embeddings for semantic search. By default the configuration is loaded from the DataHub server."
    },
    "max_documents": {
      "default": 10000,
      "description": "Maximum number of documents to embed per ingestion run. The run fails once this limit is reached. Set to 0 or -1 to disable the limit.",
      "minimum": -1,
      "title": "Max Documents",
      "type": "integer"
    },
    "continue_on_failure": {
      "default": true,
      "description": "Whether to keep ingesting other documents when one document fails. Set to false to abort the run on the first per-document error.",
      "title": "Continue On Failure",
      "type": "boolean"
    }
  },
  "required": [
    "root_folder_ids"
  ],
  "title": "GoogleDriveSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Common Use Cases

##### Ingest a shared drive under an existing DataHub document

```yaml
source:
  type: google-drive
  config:
    root_folder_ids:
      - "0AXyZsharedDriveId00"
    parent_document_urn: "urn:li:document:team-handbook"
```

##### Ingest editable copies instead of read-only references

`document_import_mode: NATIVE` imports Docs as editable DataHub documents. The default, `EXTERNAL`, imports read-only references that link back to Google Drive.

##### Ingest only the top level of a folder

```yaml
source:
  type: google-drive
  config:
    root_folder_ids:
      - "1AbCdEfGhIjKlMnOpQrStUv"
    recursive: false
```

#### Filtering

- `folder_pattern` regexes are matched against the folder path, starting with the root folder's name (for example `Handbook/Engineering/Archive`). A denied folder is never listed, so nothing beneath it is ingested. Configured roots are never filtered.
- `document_pattern` regexes are matched against Doc titles.

#### How Change Detection Works

On each run the connector lists every configured folder, then compares each Doc's Drive version, title, parent folder, and the connector settings that shape its output with what DataHub already stores. Unchanged Docs are not exported again, which keeps re-runs cheap. Docs that were deleted, trashed, moved out of scope, or newly filtered out are soft-deleted when stateful ingestion is enabled.

#### Probe support

`datahub recipe probe` checks a recipe against Google Drive without running ingestion. It returns metadata only, never a Doc's text.

- `identity`: the account the recipe connects as and, when `delegated_user` is set, whether domain-wide delegation is in effect.
- `roots`: each configured root's name and kind (My Drive folder, shared drive, or a folder in a shared drive), or why it cannot be opened and how to fix it.
- `shared_drives`: the shared drives the account can see, with the ID to use in `root_folder_ids`.
- `scope`: what a run would pick up, counted the way ingestion counts it: folders and Docs found, how many each pattern skips, and whether `max_documents` would stop the run.
- `folders`: every folder under the roots, by the path `folder_pattern` is matched against, including folders the recipe skips.
- `documents --folder <path>`: the Google Docs in one folder, by title, including Docs `document_pattern` skips.

`datahub recipe probe filter` explains each verdict, and needs no connection. Folders are judged as `Folder` and Docs as `Google Doc`, together with the folders above them:

```shell
datahub recipe probe methods --recipe google_drive_recipe.yml
datahub recipe probe run roots --recipe google_drive_recipe.yml
datahub recipe probe run folders --recipe google_drive_recipe.yml --report-to folders.json
datahub recipe probe filter --recipe google_drive_recipe.yml --from-run folders.json
datahub recipe probe filter --recipe google_drive_recipe.yml --kind "Google Doc" \
  --parent "Handbook/Engineering" --name "Copy of Runbook"
```

- `explain --document <ID or URL>`: whether one file would be ingested, and if not, which rule decides. It applies the folder and title rules first, then exports the Doc to check the text-length rules (empty, shorter than `min_text_length`, or longer than `max_text_length`). It reports the text's length, never the text, and names the file only when it is under a configured root.

`explain` is the only command that exports a Doc. The listings and `probe filter` judge folders and titles only, so a Doc they keep can still be skipped for its text. None of them account for `max_documents`, which stops a run once that many Docs are embedded; `scope` says whether a run can reach it.

When Google refuses a token, for example because domain-wide delegation is not authorized, the probe exits with code 3 and says what to change.

### Limitations

- Only Google Docs are ingested. Sheets, Slides, PDFs, uploaded files, and shortcuts are skipped.
- Images are not ingested. Each embedded image is replaced with a text placeholder such as `[Image: <alt text>]`.
- Docs with multiple tabs are ingested as a single Document. Each tab begins with its title as a heading.
- Comments, suggestions, and some smart chips are not part of Google's Markdown export.
- Google Drive refuses to export Docs larger than 10 MB. Those Docs are skipped with a warning.
- Docs whose text is longer than `max_text_length` characters (default 1,000,000) are skipped with a warning. If such a Doc was ingested earlier and stateful ingestion is enabled, it is soft-deleted. Set `max_text_length: 0` to remove the limit.
- A run stops with a failure once `max_documents` Docs (default 10,000) have been embedded. Raise the limit, or set it to `0` to remove it, for larger folders.
- Permissions are not mirrored. Anyone who can read the Document in DataHub can read its text, so choose roots whose audience matches your DataHub audience.
- Files in shared drives have no owners, so they are ingested without ownership.
- Drive owners are added alongside any owners you assign in DataHub. When a Doc's Drive owner changes, only the owner this source added is removed.
- Each run lists every folder in scope. Unchanged Docs are not exported again, and unchanged folders are not written again.
- Re-ingesting a changed Doc or folder replaces its related assets and related documents, so links you add to them in DataHub are not kept.
- Do not point two recipes at overlapping folders. With stateful ingestion enabled, one recipe can soft-delete Documents that the other still ingests.

### Troubleshooting

#### A root folder is reported as not accessible

Either the ID is wrong or the source cannot see the folder.

- Check that the ID or URL in `root_folder_ids` points to a folder or shared drive, not a file.
- Share the folder with the account the source authenticates as: the service account's `client_email` when you set `credential`, or the Application Default Credentials account otherwise. Alternatively, set `delegated_user` to a user who can open it.

Run `datahub check source-connection` to see which roots fail, or `datahub recipe probe run roots --recipe <recipe>` to see why each one fails and how to fix it.

When any root or folder fails, the run reports a failure and stale-entity removal is skipped for that run, so nothing is soft-deleted because of a partial listing.

#### Docs are missing

- Check `documents_filtered`, `folders_filtered`, `documents_skipped_empty`, `documents_skipped_too_long`, `documents_export_failed`, `documents_unparseable`, `documents_failed`, and `shortcuts_skipped` in the ingestion report. `failed_documents` lists the Docs that could not be ingested; their existing copies in DataHub are kept.
- Without `delegated_user`, the service account only sees files shared with it. Files that a folder's owner can see are not automatically visible to it.
- Docs in the trash are never ingested.

#### Embeddings are missing for some Docs

If embedding generation fails, the Doc is still ingested and the failure is reported as a warning. The Doc is retried on the next run. `documents_embedding_deferred` counts these Docs, and `num_documents_truncated_oversized` and `num_documents_dropped_oversized` count Docs whose embeddings were truncated or dropped for size.

#### Every run exports every Doc again

Unchanged Docs are skipped by comparing them with what DataHub already stores. If `change_detection_lookup_failures` is non-zero, the ingestion token could not read Documents back from DataHub; grant it read access to Document entities.


### Code Coordinates
- Class Name: `datahub.ingestion.source.google_drive.google_drive_source.GoogleDriveSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/google_drive/google_drive_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Google Drive, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
