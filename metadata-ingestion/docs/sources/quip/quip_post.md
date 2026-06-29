### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Common Use Cases

##### 1. Auto-Discover All Folders (Default)

By default, the connector discovers and ingests all folders accessible to the token's user:

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    base_url: "https://platform.quip.com"
```

##### 2. Ingest Specific Folders

Crawl only specific folder trees:

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    folder_ids:
      - "AbCdEfGhIjKl" # Engineering
      - "MnOpQrStUvWx" # Product
    recursive: true # Crawl sub-folders (default)
```

##### 3. Ingest Individual Threads

Ingest specific documents by thread ID:

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    thread_ids:
      - "1a2b3c4d5e6f"
      - "7g8h9i0j1k2l"
```

##### 4. Restrict to Certain Thread Types

Ingest only documents and spreadsheets (skip chats and slides):

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    thread_types:
      - "document"
      - "spreadsheet"
```

##### 5. Read-Only References vs. Native Documents

By default threads are imported as `EXTERNAL` references that link back to Quip. Set `NATIVE` to import editable copies into DataHub:

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    document_import_mode: NATIVE
```

##### 6. Production Setup with Stateful Ingestion

```yaml
source:
  type: quip
  config:
    access_token: "${QUIP_ACCESS_TOKEN}"
    stateful_ingestion:
      enabled: true
```

#### How It Works

##### Processing Pipeline

1. **Discovery**: Resolves root folders from `folder_ids` (or auto-discovers the user's folders) and walks the folder tree.
2. **Download**: Fetches each thread via the Quip Automation API, including its HTML body.
3. **Extraction**: Converts thread HTML to Markdown and extracts metadata and hierarchy.
4. **Chunking**: Splits documents into semantic chunks (if embeddings enabled).
5. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled).
6. **Emission**: Emits folder and thread Document entities (with `SemanticContent` aspects) to DataHub.

#### Performance Tuning

##### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short threads (default: 50)
max_threads: 5000 # Cap the number of threads ingested per run
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

#### Limitations and Considerations

- **Rate Limits**: Quip enforces per-user and per-company rate limits; the connector backs off and retries on HTTP 429, which can slow large ingestions.
- **Multi-folder Membership**: Quip folders behave like tags, so a thread can live in several folders. DataHub's `parentDocument` is single-valued, so each thread is parented to the first folder it is reached through.
- **Folders as Native Documents**: Folders have no stable external content URL in Quip, so they are always modeled as `NATIVE` Documents regardless of `document_import_mode` (which governs threads only).
- **Attachments**: File/image blobs attached to threads are not ingested (thread text only).

### Troubleshooting

#### Common Issues

**"401 Unauthorized" / authentication failed:**

- Verify the `access_token` is valid and not expired (regenerate at `<base_url>/dev/token`).
- Confirm `base_url` matches your Quip site (`https://platform.quip.com` vs. an enterprise endpoint).

**"403 Forbidden" / folder or thread not found:**

- Verify the token's user has read access to the configured `folder_ids` / `thread_ids`.

**Empty or missing content:**

- Empty/short threads are skipped; check the `filtering.min_text_length` setting (default: 50).

**Slow ingestion / frequent rate-limit waits:**

- Reduce scope with `folder_ids` or `max_threads`, or run during off-peak hours.

**Missing hierarchy / parent relationships:**

- Ensure `recursive: true` and that parent folders are within scope so their Document entities are emitted.
