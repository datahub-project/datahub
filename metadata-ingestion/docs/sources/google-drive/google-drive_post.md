### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

:::note Shared Drives
Content in Shared Drives (Team Drives) is supported — the connector enumerates Shared Drive files automatically as long as the authenticated account (service account or impersonated user) has access to them.
:::

#### Common Use Cases

##### 1. Ingest All Accessible Google Docs (Default)

When `folder_ids` is empty the source ingests every Google Doc visible to the authenticated account:

```yaml
source:
  type: google-drive
  config:
    credentials:
      service_account_key_file: "/path/to/service-account.json"
```

##### 2. Ingest Specific Folders

Restrict ingestion to one or more Drive folders. You can provide a full folder URL or just the ID:

```yaml
source:
  type: google-drive
  config:
    credentials:
      service_account_key_file: "/path/to/service-account.json"
    folder_ids:
      - "1A2B3C4D5E6F7G8H9I0J" # Engineering Docs
      - "https://drive.google.com/drive/folders/9Z8Y7X6W5V4U3T2S1R0Q" # Product Specs
    recursive: true
```

##### 3. Include Slides and Sheets

Enable additional file types alongside Docs:

```yaml
source:
  type: google-drive
  config:
    credentials:
      service_account_key_file: "/path/to/service-account.json"
    include_docs: true
    include_slides: true
    include_sheets: true
```

##### 4. Semantic Search with Embeddings

Generate vector embeddings for semantic search in DataHub:

```yaml
source:
  type: google-drive
  config:
    credentials:
      service_account_key_file: "/path/to/service-account.json"
    embedding:
      provider: cohere
      cohere:
        api_key: "${COHERE_API_KEY}"
        model: embed-english-v3.0
```

##### 5. Production Setup with Stateful Ingestion

Enable incremental updates so that only new or changed documents are reprocessed:

```yaml
source:
  type: google-drive
  config:
    credentials:
      service_account_key_file: "/path/to/service-account.json"
    folder_ids:
      - "your-folder-id-here"
    stateful_ingestion:
      enabled: true
```

#### How It Works

##### Output Entities

Each ingested file is emitted as a DataHub **Document** entity (not a Dataset). Folders are optionally materialised as Document entities as well, so that the parent-child hierarchy is visible in the DataHub UI (controlled by `ingest_folders`).

The `document_import_mode` field controls how documents are stored:

- `EXTERNAL` (default): DataHub stores a read-only reference that links back to the original Google Drive file
- `NATIVE`: The document content is imported directly into DataHub for in-product editing

##### Processing Pipeline

1. **Discovery**: Google Drive API lists files matching the configured `folder_ids`, MIME-type filters, and `recursive` setting
2. **Export**: Each file is exported to markdown (Docs), plain text (Slides), or CSV (Sheets)
3. **Chunking**: Splits documents into semantic chunks (if embeddings enabled)
4. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled)
5. **Emission**: Emits Document entities with SemanticContent aspects to DataHub

##### Entity URNs & stability

Documents and folders are emitted as DataHub `Document` entities with URNs of the form:

- `urn:li:document:google-drive-{file_id}` — the default
- `urn:li:document:google-drive-{platform_instance}-{file_id}` — when `platform_instance` is set

The Google Drive **file ID is globally unique and permanent** (it survives renames, moves, and edits), so a given file always maps to the **same URN regardless of which service account, key path, or auth method ingests it**. Re-ingestion therefore updates the same entity (idempotent), and stale-entity removal works reliably.

Set `platform_instance` when you want to keep multiple logical Drive instances separate — but note it becomes part of the URN, so choose a stable value up front (changing it later re-creates entities under new URNs).

##### Stateful Ingestion Details

The source uses content-based change detection for embeddings and tracks URNs for deletion:

- Every document is re-emitted as a Document entity on each run
- A SHA-256 `content_hash` (based on `modifiedTime` and extraction algorithm version) is stored as a custom property on each Document entity
- The downstream embedding/chunking step reads this hash and skips regenerating embeddings when the hash is unchanged — only the embedding step is skipped for unchanged content, not the document emission itself
- All emitted URNs are tracked so that files deleted from Drive are automatically soft-deleted from DataHub

This means:

- **First run**: Processes all documents and generates embeddings
- **Subsequent runs**: Re-emits all Document entities; skips embedding regeneration for documents whose content hash is unchanged
- **Deleted files**: Automatically soft-deleted from DataHub

#### Performance Tuning

##### Rate Limiting

```yaml
requests_per_minute: 60 # Connector default — adjust based on your project's quota
```

The connector default is 60 requests per minute. Google's Drive API user quota is typically much higher, but organisational or project-level quotas may be stricter. If you hit `429 Too Many Requests` errors, lower this value.

##### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short documents (default: 50)
  skip_empty_documents: true # Skip files with no extractable text (default: true)
```

##### Testing / Limiting Run Size

```yaml
max_documents: 50 # Stop after ingesting 50 documents (useful for dry runs)
```

### Limitations

Module behaviour is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Google Drive API Limits

- **Rate Limits**: The connector default is 60 requests per minute (`requests_per_minute`); Google's actual Drive API quota varies by project and organisation
- **Export Limits**: Google imposes a 10 MB export limit per file; very large documents may be truncated
- **Attachments**: Binary file attachments (PDFs, images, Office files) are not ingested — only native Google Workspace files are supported

#### Content Extraction

- **Supported**: Google Docs (markdown), Google Slides (plain text per slide), Google Sheets (CSV)
- **Not Supported**: Binary files (.pdf, .docx, images), Google Forms, Google Drawings, Google Sites

### Troubleshooting

**"403 Forbidden" or "insufficientFilePermissions" errors:**

- Verify the service account has been granted **Viewer** access to the target folders
- Check that the **Google Drive API** is enabled in the Cloud project associated with the service account
- For shared drives (Team Drives), the service account may need to be added as a member of the drive

**"401 Unauthorized" or credential errors:**

- Verify the path in `credentials.service_account_key_file` is correct and the file is readable
- If using `credentials.service_account_key_json`, ensure the JSON string is valid (no truncation)
- For Application Default Credentials, confirm `gcloud auth application-default login` has been run, or that the workload has an attached service account

**Empty or missing content:**

- Check that the file type is enabled (`include_docs`, `include_slides`, `include_sheets`)
- Verify `filtering.min_text_length` is not filtering out short documents
- Ensure `recursive: true` if files are in sub-folders of the specified `folder_ids`

**Slow ingestion:**

- Restrict ingestion to specific folders via `folder_ids` instead of ingesting all of Drive
- Lower `requests_per_minute` if you are hitting quota errors
- Use `max_documents` to limit scope during initial testing

**Embedding generation failures:**

- Verify the provider API key is correct
- Check provider-specific rate limits (Cohere: 10 000 requests/min)
- Ensure the embedding model name is valid for your provider
- For Bedrock: verify IAM permissions and that model access is enabled in the AWS Console

**Stateful ingestion not working:**

- Ensure `stateful_ingestion.enabled: true` in the config
- Check the DataHub connection (the source needs to query previous state)
- Look for state persistence logs in the ingestion output

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
