:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

## Overview

The Notion source ingests pages and databases from Notion workspaces as DataHub Document entities with optional semantic embeddings for semantic search.

### Key Features

#### 1. Content Extraction

- **Page Content**: Full text extraction from Notion pages including all supported block types
- **Database Rows**: Ingests database entries as individual documents
- **Hierarchical Structure**: Maintains parent-child relationships between pages
- **Metadata Extraction**: Captures creation/modification timestamps, authors, and custom properties

#### 2. Hierarchical Relationships

- **Parent-Child Links**: Preserves Notion's page hierarchy in DataHub
- **Automatic Discovery**: Recursively discovers nested pages starting from root pages
- **Flexible Navigation**: Browse documentation structure in DataHub UI

#### 3. Embedding Generation

Optional semantic search support:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Chunking strategies**: by_title, basic
- **Configurable chunk size**: Optimize for your embedding model (in characters)
- **Automatic deduplication**: Prevents duplicate chunk embeddings

#### 4. Stateful Ingestion

Supports smart incremental updates via stateful ingestion:

- **Content Change Detection**: Only reprocesses documents when content or embeddings config changes
- **Deletion Detection**: Automatically removes stale entities from DataHub
- **Recursive Discovery**: Start from root pages/databases, automatically discovers and ingests child pages
- **State Persistence**: Maintains processing state between runs to skip unchanged documents

## Prerequisites

### 1. Notion Integration

Create a Notion internal integration:

1. Go to https://www.notion.so/my-integrations
2. Click **"+ New integration"**
3. Give it a name (e.g., "DataHub Integration")
4. Select the workspace
5. Copy the **Internal Integration Token** (starts with `secret_`)

### 2. Share Pages with Integration

The integration can only access pages explicitly shared with it:

1. Open the page or database in Notion
2. Click **"Share"** in the top right
3. Search for your integration name
4. Click **"Invite"**

**Important**: For recursive ingestion, only share top-level pages. Child pages inherit access automatically.

### 3. Embedding Provider (Optional)

If you want semantic search capabilities, set up one of these providers:

#### Cohere

- Sign up at https://cohere.ai/
- Create an API key
- Supports: `embed-english-v3.0`, `embed-multilingual-v3.0`

#### AWS Bedrock

- AWS account with Bedrock access
- Enable Cohere models in AWS Console → Bedrock → Model access
- IAM permissions for `bedrock:InvokeModel`
- Recommended region: `us-west-2`

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed embedding setup.

## Common Use Cases

### 1. Workspace-wide Documentation Search

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

### 2. Specific Database Ingestion

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

### 3. Multi-workspace Setup

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

### 4. Production Setup with AWS Bedrock

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

## How It Works

### Processing Pipeline

1. **Discovery**: Notion API discovers pages/databases
2. **Download**: Unstructured.io downloads and converts content to structured format
3. **Extraction**: Extracts text, metadata, and hierarchy from Notion pages
4. **Chunking**: Splits documents into semantic chunks (if embeddings enabled)
5. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled)
6. **Emission**: Emits Document entities with SemanticContent aspects to DataHub

### Stateful Ingestion Details

The source uses content-based change detection:

- Calculates SHA-256 hash of document content + embedding configuration
- Compares hash with previous run to detect changes
- Only reprocesses documents when hash changes
- Tracks all emitted URNs to detect deletions

This means:

- **First run**: Processes all documents
- **Subsequent runs**: Only processes new/changed documents
- **Deleted pages**: Automatically soft-deleted from DataHub

## Limitations and Considerations

### Notion API Limits

- **Rate Limits**: Notion enforces rate limits (3 requests/second for paid workspaces, 1/second for free)
- **Access Scope**: Integration only sees explicitly shared pages
- **Content Types**: Some Notion blocks may not extract perfectly (e.g., complex embeds, synced blocks)

### Performance Considerations

- **Large Workspaces**: First run may take significant time for large workspaces
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Unstructured API and embedding providers may incur costs

### Content Extraction

- **Supported Blocks**: Text, headings, lists, code blocks, tables, callouts, toggles, quotes
- **Limited Support**: Embeds, equations, files (extracted as links/references)
- **Not Supported**: Live charts, board/gallery/timeline views (database views)

## Troubleshooting

### Common Issues

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

## Performance Tuning

### Parallelism Settings

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

### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short pages (default: 50)
  skip_empty_documents: true # Skip empty pages (default: true)
```

### Chunking Optimization

```yaml
chunking:
  strategy: "by_title" # Preserves document structure (recommended)
  max_characters: 500 # Chunk size (default: 500)
  combine_text_under_n_chars: 100 # Merge small chunks (default: 100)
```

## Related Documentation

- [Notion API Documentation](https://developers.notion.com/)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)
- [Unstructured.io Documentation](https://docs.unstructured.io/)
- [Cohere Embeddings API](https://docs.cohere.com/reference/embed)
- [AWS Bedrock Embeddings](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html)
- [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)
