### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

#### Common Use Cases

##### 1. Workspace-wide Documentation Search

Ingest entire workspace documentation with semantic search:

```yaml
source:
  type: notion
  config:
    api_key: "${NOTION_API_KEY}"

    # Start from workspace root page
    page_ids:
      - "workspace_root_page_id"
    recursive: true

    # Enable semantic embeddings
    embedding:
      provider: "cohere"
      model: "embed-english-v3.0"
      api_key: "${COHERE_API_KEY}"
```

##### 2. Specific Database Ingestion

Ingest a specific Notion database (e.g., "Product Requirements"):

```yaml
source:
  type: notion
  config:
    api_key: "${NOTION_API_KEY}"

    # Only this database
    database_ids:
      - "product_requirements_db_id"
    recursive: false # Only database entries, not child pages
```

##### 3. Multi-workspace Setup

Ingest from multiple workspaces (requires multiple integrations):

```yaml
source:
  type: notion
  config:
    api_key: "${NOTION_API_KEY}"

    # Multiple root pages from different workspaces
    page_ids:
      - "workspace_1_page_id"
      - "workspace_2_page_id"
    recursive: true
```

##### 4. Production Setup with AWS Bedrock

Enterprise setup using AWS Bedrock for embeddings:

```yaml
source:
  type: notion
  config:
    api_key: "${NOTION_API_KEY}"

    page_ids:
      - "company_wiki_root"
    recursive: true

    # Use AWS Bedrock (no API key needed, uses IAM roles)
    embedding:
      provider: "bedrock"
      aws_region: "us-west-2"
      model: "cohere.embed-english-v3"

    # Enable stateful ingestion for incremental updates
    stateful_ingestion:
      enabled: true
```

#### How It Works

##### Processing Pipeline

1. **Discovery**: Notion API discovers pages/databases
2. **Download**: Unstructured.io downloads and converts content to structured format
3. **Extraction**: Extracts text, metadata, and hierarchy from Notion pages
4. **Chunking**: Splits documents into semantic chunks (if embeddings enabled)
5. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled)
6. **Emission**: Emits Document entities with SemanticContent aspects to DataHub

##### Stateful Ingestion Details

The source uses content-based change detection:

- Calculates SHA-256 hash of document content + embedding configuration
- Compares hash with previous run to detect changes
- Only reprocesses documents when hash changes
- Tracks all emitted URNs to detect deletions

This means:

- **First run**: Processes all documents
- **Subsequent runs**: Only processes new/changed documents
- **Deleted pages**: Automatically soft-deleted from DataHub

#### Performance Tuning

##### Parallelism Settings

```yaml
processing:
  parallelism:
    num_processes: 4 # Increase for faster processing (default: 2)
    max_connections: 20 # Concurrent API connections (default: 10)
```

**Guidelines:**

- Small workspaces (<100 pages): `num_processes: 2`
- Medium workspaces (100-1000 pages): `num_processes: 4`
- Large workspaces (>1000 pages): `num_processes: 8`

##### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short pages (default: 50)
  skip_empty_documents: true # Skip empty pages (default: true)
```

##### Chunking Optimization

```yaml
chunking:
  strategy: "by_title" # Preserves document structure (recommended)
  max_characters: 500 # Chunk size (default: 500)
  combine_text_under_n_chars: 100 # Merge small chunks (default: 100)
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Notion API Limits

- **Rate Limits**: Notion enforces rate limits (3 requests/second for paid workspaces, 1/second for free)
- **Access Scope**: Integration only sees explicitly shared pages
- **Content Types**: Some Notion blocks may not extract perfectly (e.g., complex embeds, synced blocks)

#### Performance Considerations

- **Large Workspaces**: First run may take significant time for large workspaces
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Unstructured API and embedding providers may incur costs

#### Content Extraction

- **Supported Blocks**: Text, headings, lists, code blocks, tables, callouts, toggles, quotes
- **Limited Support**: Embeds, equations, files (extracted as links/references)
- **Not Supported**: Live charts, board/gallery/timeline views (database views)

### Troubleshooting

**"Integration not found" or "Unauthorized" errors:**

- Verify the `api_key` is correct (should start with `secret_`)
- Ensure pages are shared with the integration
- Check that the integration has "Read content" capability

**Empty or missing content:**

- Verify pages contain text (empty pages are skipped by default with `skip_empty_documents: true`)
- Check `min_text_length` filter setting (default: 50 characters)
- Ensure `recursive: true` if expecting child pages
- Check that child pages are not explicitly restricted

**Slow ingestion:**

- Increase `processing.parallelism.num_processes` (default: 2)
- Consider using `partition_by_api: false` for local processing (requires more memory)
- Filter specific pages instead of entire workspace using `page_ids`
- First run is always slower - subsequent runs use incremental updates

**Embedding generation failures:**

- Verify provider API key is correct
- Check provider-specific rate limits (Cohere: 10k requests/min)
- Ensure embedding model name is valid for your provider
- For Bedrock: verify IAM permissions and model access is enabled in AWS Console

**Stateful ingestion not working:**

- Ensure `stateful_ingestion.enabled: true` in config
- Check DataHub connection (source needs to query previous state)
- Verify state file path is writable (if using file-based state)
- Look for state persistence logs in ingestion output

**Missing hierarchy/parent relationships:**

- Verify `hierarchy.enabled: true` (default)
- Check that parent pages are being ingested
- Ensure `recursive: true` to discover parent-child relationships
- Parent pages must be accessible to the integration

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
