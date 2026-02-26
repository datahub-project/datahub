:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

## Overview

The Confluence source ingests pages and spaces from Confluence workspaces (Cloud or Data Center) as DataHub Document entities with optional semantic embeddings for semantic search.

### Key Features

#### 1. Content Extraction

- **Page Content**: Full text extraction from Confluence pages including all content types
- **Space Discovery**: Automatic discovery of all pages within specified spaces
- **Hierarchical Structure**: Maintains parent-child relationships between pages
- **Metadata Extraction**: Captures creation/modification timestamps, authors, labels, and custom properties

#### 2. Hierarchical Relationships

- **Parent-Child Links**: Preserves Confluence page hierarchy in DataHub
- **Recursive Discovery**: Recursively discovers nested pages starting from root pages or entire spaces
- **Space Organization**: Maintains space context as custom properties
- **Flexible Navigation**: Browse documentation structure in DataHub UI

#### 3. Embedding Generation

Optional semantic search support with sensible defaults:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Automatic chunking**: Documents are automatically chunked for optimal embedding generation
- **Automatic deduplication**: Prevents duplicate chunk embeddings

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed setup and advanced options.

#### 4. Stateful Ingestion

Supports smart incremental updates via stateful ingestion:

- **Content Change Detection**: Only reprocesses documents when content or embeddings config changes
- **Deletion Detection**: Automatically removes stale entities from DataHub
- **Flexible Discovery**: Ingest entire spaces, specific pages, or page trees
- **State Persistence**: Maintains processing state between runs to skip unchanged documents

## Prerequisites

### 1. Confluence API Access

#### For Confluence Cloud

Create an API token:

1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Click **"Create API token"**
3. Give it a name (e.g., "DataHub Integration")
4. Copy the token (you won't be able to see it again)

You'll need:

- **Base URL**: Your Confluence Cloud URL (e.g., `https://your-domain.atlassian.net/wiki`)
- **Username**: Your Atlassian account email
- **API Token**: The token you just created

#### For Confluence Data Center / Server

Create a Personal Access Token:

1. Go to your Confluence → Profile → Personal Access Tokens
2. Click **"Create token"**
3. Give it a name and set expiration
4. Copy the token

You'll need:

- **Base URL**: Your Confluence server URL (e.g., `https://confluence.company.com`)
- **Personal Access Token**: The token you created

**Note**: For Data Center, you can also use username/password, but Personal Access Tokens are recommended.

### 2. Required Permissions

The API credentials must have:

- **Read access** to all spaces and pages you want to ingest
- For Cloud: User must be added to spaces or have site-wide read access
- For Data Center: User must have "View" permissions on spaces

### 3. Embedding Provider (Optional)

If you want semantic search capabilities, configure an embedding provider in your DataHub instance.

Supported providers include Cohere (API key) and AWS Bedrock (IAM roles). The connector will use sensible defaults for chunking and embedding configuration.

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed provider setup and configuration options.

## Common Use Cases

### 1. Auto-Discover All Spaces (Default)

By default, the connector discovers and ingests all accessible spaces:

```yaml
source:
  type: confluence
  config:
    # Confluence Cloud
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # No filtering - discovers all accessible spaces
    # Optional: limit number of spaces for large instances
    max_spaces: 100
```

### 2. Include Specific Spaces

Ingest only specific Confluence spaces:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # Include only these spaces
    spaces:
      allow:
        - "ENGINEERING"
        - "PRODUCT"
        - "DESIGN"
```

### 3. Exclude Personal and Archive Spaces

Ingest all spaces except specific ones:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # Exclude personal spaces and archived content
    spaces:
      deny:
        - "~john.doe"
        - "~jane.smith"
        - "ARCHIVE"
        - "OLD_DOCS"
```

### 4. Specific Page Trees Only

Ingest specific pages and their descendants:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # Start from specific pages
    pages:
      allow:
        - "123456789" # API Documentation page tree
        - "987654321" # User Guides page tree
    recursive: true # Include all child pages
```

### 5. Combined Space and Page Filtering

Combine space and page filters for fine-grained control:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # Include specific spaces
    spaces:
      allow:
        - "ENGINEERING"
        - "PRODUCT"
      # Exclude personal spaces even if in allow list
      deny:
        - "~admin"

    # Exclude specific pages (e.g., drafts, archived content)
    pages:
      deny:
        - "999999" # Draft page
        - "888888" # Archived page
```

### 6. Data Center / Server Setup

Connect to Confluence Data Center or Server:

```yaml
source:
  type: confluence
  config:
    # Data Center / Server
    cloud: false
    url: "https://confluence.company.com"
    personal_access_token: "${CONFLUENCE_PAT}"

    spaces:
      allow:
        - "WIKI"
        - "DOCS"
```

### 7. Production Setup with Stateful Ingestion

Enterprise setup with incremental updates:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    spaces:
      allow:
        - "COMPANY"
        - "PUBLIC"

    # Enable stateful ingestion for incremental updates
    stateful_ingestion:
      enabled: true
```

**Note**: Embedding configuration is managed by your DataHub instance. See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for setup.

### 8. Using URLs for Allow/Deny

You can specify spaces and pages using full URLs for both allow and deny lists:

```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@company.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

    # Use full URLs - connector extracts keys/IDs automatically
    spaces:
      allow:
        - "https://your-domain.atlassian.net/wiki/spaces/ENG"
        - "https://your-domain.atlassian.net/wiki/spaces/PRODUCT"
      deny:
        - "https://your-domain.atlassian.net/wiki/spaces/ARCHIVE"
        - "~john.doe" # Can mix URLs and keys

    pages:
      allow:
        - "https://your-domain.atlassian.net/wiki/spaces/ENG/pages/123456/Getting+Started"
      deny:
        - "https://your-domain.atlassian.net/wiki/spaces/ENG/pages/999999/Draft"
```

## Filtering Content

The connector provides flexible filtering options through allow and deny lists for both spaces and pages.

### Space Filtering

Control which Confluence spaces are ingested:

**`spaces.allow`**: Include only specific spaces (by default, all accessible spaces are discovered)

```yaml
spaces:
  allow:
    - "ENGINEERING" # Space key
    - "PRODUCT"
    - "https://your-domain.atlassian.net/wiki/spaces/DESIGN" # Or full URL
```

**`spaces.deny`**: Exclude specific spaces (applied after `spaces.allow`)

```yaml
spaces:
  deny:
    - "~john.doe" # Personal space
    - "ARCHIVE" # Archived content
    - "TEST" # Test space
```

### Page Filtering

Control which pages are ingested:

**`pages.allow`**: Include only specific pages (triggers page-based mode, bypasses space discovery)

```yaml
pages:
  allow:
    - "123456789" # Page ID
    - "987654321"
    - "https://your-domain.atlassian.net/wiki/spaces/ENG/pages/111111/API+Docs" # Or full URL
recursive: true # Include child pages
```

**`pages.deny`**: Exclude specific pages (works in both space-based and page-based modes)

```yaml
pages:
  deny:
    - "999999" # Draft page
    - "888888" # Archived page
```

### Filtering Rules

**Precedence**:

- Deny lists always take precedence over allow lists
- If a space/page is in both allow and deny lists, it will be excluded

**Modes**:

- **Space-based mode** (default): Discovers spaces, then ingests all pages within allowed spaces
- **Page-based mode**: When `page_allow` is specified, bypasses space discovery and fetches specific page trees

**Format Support**:

- Space keys: `"ENGINEERING"`, `"~username"` (for personal spaces)
- Page IDs: `"123456789"` (numeric string)
- Full URLs: Both space URLs and page URLs are automatically parsed

### Common Filtering Patterns

**Exclude all personal spaces:**

```yaml
spaces:
  deny:
    - "~*" # Note: Use explicit user IDs, wildcard not supported
    # Instead, list specific personal spaces:
    - "~john.doe"
    - "~jane.smith"
```

**Ingest only documentation spaces:**

```yaml
spaces:
  allow:
    - "DOCS"
    - "API_DOCS"
    - "USER_GUIDES"
```

**Focus on specific documentation trees:**

```yaml
pages:
  allow:
    - "123456" # API Documentation root page
    - "789012" # User Guides root page
recursive: true
```

**Exclude drafts and WIP pages:**

```yaml
pages:
  deny:
    - "999999" # Draft page ID
    - "888888" # WIP page ID
```

## How It Works

### Processing Pipeline

1. **Discovery**: Confluence API discovers spaces and pages
2. **Download**: Downloads page content via Confluence REST API
3. **Extraction**: Extracts text, metadata, and hierarchy from pages
4. **Chunking**: Splits documents into semantic chunks (if embeddings enabled)
5. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled)
6. **Emission**: Emits Document entities with SemanticContent aspects to DataHub

### URL Format Support

The connector supports multiple input formats for spaces and pages in allow/deny lists:

**Space Identifiers:**

- Space key: `"ENGINEERING"`, `"~username"` (for personal spaces)
- Full URL: `"https://your-domain.atlassian.net/wiki/spaces/ENGINEERING"`

**Page Identifiers:**

- Page ID: `"123456789"` (numeric string)
- Full URL (Cloud): `"https://your-domain.atlassian.net/wiki/spaces/ENG/pages/123456/Page+Title"`
- Full URL (Data Center): `"https://confluence.company.com/pages/viewpage.action?pageId=123456"`

The connector automatically extracts space keys and page IDs from URLs, so you can use either format interchangeably in `space_allow`, `space_deny`, `page_allow`, and `page_deny` lists.

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

### Confluence API Limits

- **Rate Limits**: Confluence enforces rate limits (Cloud: varies by plan, Data Center: configurable)
- **Content Types**: Complex macros may not extract perfectly (e.g., embedded content, custom macros)
- **Attachments**: File attachments are not ingested (only page content)

### Performance Considerations

- **Large Spaces**: First run may take significant time for large spaces (1000+ pages)
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Embedding providers may incur costs based on usage

### Content Extraction

- **Supported Content**: Text, headings, lists, code blocks, tables, panels
- **Limited Support**: Some macros extract as text/links
- **Not Supported**: Attachments, complex custom macros, embedded Jira issues (content only)

## Troubleshooting

### Common Issues

**"401 Unauthorized" or "Authentication failed" errors:**

- **Cloud**: Verify `username` (email) and `api_token` are correct
- **Data Center**: Verify `personal_access_token` is valid and not expired
- Check that `cloud: true/false` matches your Confluence type
- Ensure the URL includes `/wiki` suffix for Cloud (e.g., `https://domain.atlassian.net/wiki`)

**"403 Forbidden" or "Space not found" errors:**

- Verify the user has read access to the specified spaces
- Check that space keys are correct (case-sensitive)
- For Cloud, ensure user is added to private spaces
- For Data Center, verify "View Space" permissions

**Empty or missing content:**

- Verify pages contain text (empty pages are skipped by default with `skip_empty_documents: true`)
- Check `min_text_length` filter setting (default: 50 characters)
- Ensure `recursive: true` if expecting child pages
- Check that pages are not restricted or have special permissions

**Slow ingestion:**

- Increase `processing.parallelism.num_processes` (default: 2)
- Consider filtering specific spaces instead of all spaces
- First run is always slower - subsequent runs use incremental updates
- Large spaces with 1000+ pages may take several minutes

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
- Parent pages must be accessible to the API credentials

**Page IDs not working:**

- For Cloud, use the numeric page ID from the URL (after `/pages/`)
- For Data Center, page IDs may differ - use the ID from the page URL or query param `?pageId=`
- Alternatively, use full page URLs instead of IDs in `page_allow` or `page_deny`

**How to find space keys and page IDs:**

- **Space key**: Visible in the space URL: `https://domain.atlassian.net/wiki/spaces/ENGINEERING` → key is `ENGINEERING`
- **Page ID (Cloud)**: In the page URL after `/pages/`: `https://domain.atlassian.net/wiki/spaces/ENG/pages/123456/Title` → ID is `123456`
- **Page ID (Data Center)**: In the URL query parameter: `https://confluence.company.com/pages/viewpage.action?pageId=123456` → ID is `123456`
- **Personal space key**: Format is `~username` (e.g., `~john.doe` for user john.doe)

## Performance Tuning

### Parallelism Settings

```yaml
processing:
  parallelism:
    num_processes: 4 # Increase for faster processing (default: 2)
    max_connections: 20 # Concurrent API connections (default: 10)
```

**Guidelines:**

- Small spaces (<100 pages): `num_processes: 2`
- Medium spaces (100-500 pages): `num_processes: 4`
- Large spaces (>500 pages): `num_processes: 8`

### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short pages (default: 50)
  skip_empty_documents: true # Skip empty pages (default: true)
```

### Space Selection

Instead of ingesting all spaces, select specific ones:

```yaml
spaces:
  allow:
    - "ENGINEERING" # High-value documentation space
    - "PRODUCT" # Product requirements space
  deny:
    - "~*" # Exclude personal spaces (list specific users)
    - "ARCHIVE" # Exclude archived content
    - "TEST" # Exclude test spaces
```

## Related Documentation

- [Confluence Cloud REST API](https://developer.atlassian.com/cloud/confluence/rest/v1/intro/)
- [Confluence Data Center REST API](https://docs.atlassian.com/ConfluenceServer/rest/)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)
- [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)
