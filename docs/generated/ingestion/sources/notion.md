


# Notion

## Overview

Notion is a documentation or collaboration platform. Learn more in the [official Notion documentation](https://www.notion.so/).

The DataHub integration for Notion covers document/workspace entities and hierarchy context for knowledge assets. It also captures stateful deletion detection.

Ingest pages and databases from Notion workspaces as DataHub Document entities with optional semantic embeddings.

:::warning Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `notion`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Test Connection | ✅ | Enabled by default. |

### Overview

:::caution Not Supported with Remote Executor
This source is available as a private beta feature on DataHub Cloud. Note that running the connector using the Remote Executor is not yet supported.
:::

The Notion source ingests pages and databases from Notion workspaces as DataHub Document entities with optional semantic embeddings for semantic search.

#### Key Features

##### 1. Content Extraction

- **Page Content**: Full text extraction from Notion pages including all supported block types
- **Database Rows**: Ingests database entries as individual documents
- **Hierarchical Structure**: Maintains parent-child relationships between pages
- **Metadata Extraction**: Captures creation/modification timestamps, authors, and custom properties

##### 2. Hierarchical Relationships

- **Parent-Child Links**: Preserves Notion's page hierarchy in DataHub
- **Automatic Discovery**: Recursively discovers nested pages starting from root pages
- **Flexible Navigation**: Browse documentation structure in DataHub UI

##### 3. Embedding Generation

Optional semantic search support:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Chunking strategies**: by_title, basic
- **Configurable chunk size**: Optimize for your embedding model (in characters)
- **Automatic deduplication**: Prevents duplicate chunk embeddings

##### 4. Stateful Ingestion

Supports smart incremental updates via stateful ingestion:

- **Content Change Detection**: Only reprocesses documents when content or embeddings config changes
- **Deletion Detection**: Automatically removes stale entities from DataHub
- **Recursive Discovery**: Start from root pages/databases, automatically discovers and ingests child pages
- **State Persistence**: Maintains processing state between runs to skip unchanged documents

#### Related Documentation

- [Notion API Documentation](https://developers.notion.com/)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)
- [Unstructured.io Documentation](https://docs.unstructured.io/)
- [Cohere Embeddings API](https://docs.cohere.com/reference/embed)
- [AWS Bedrock Embeddings](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html)
- [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)

### Prerequisites

#### 1. Notion Integration

Create a Notion internal integration:

1. Go to https://www.notion.so/my-integrations
2. Click **"+ New integration"**
3. Give it a name (e.g., "DataHub Integration")
4. Select the workspace
5. Copy the **Internal Integration Token** (starts with `secret_`)

#### 2. Share Pages with Integration

The integration can only access pages explicitly shared with it:

1. Open the page or database in Notion
2. Click **"Share"** in the top right
3. Search for your integration name
4. Click **"Invite"**

**Important**: For recursive ingestion, only share top-level pages. Child pages inherit access automatically.

#### 3. Embedding Provider (Optional)

If you want semantic search capabilities, set up one of these providers:

##### Cohere

- Sign up at https://cohere.ai/
- Create an API key
- Supports: `embed-english-v3.0`, `embed-multilingual-v3.0`

##### AWS Bedrock

- AWS account with Bedrock access
- Enable Cohere models in AWS Console → Bedrock → Model access
- IAM permissions for `bedrock:InvokeModel`
- Recommended region: `us-west-2`

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed embedding setup.


### Install the Plugin
```shell
pip install 'acryl-datahub[notion]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: notion
  config:
    # Notion API token from your integration
    api_key: "${NOTION_API_KEY}"

    # Ingest specific pages (get IDs from page URLs)
    page_ids:
      - "your-page-id-here"

    # Or ingest all accessible content (leave page_ids and database_ids empty)
    # page_ids: []
    # database_ids: []

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Notion internal integration token. Create one at https://www.notion.so/my-integrations  |
| <div className="path-line"><span className="path-main">document_import_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL"  |
| <div className="path-line"><span className="path-main">max_documents</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of documents to process per ingestion run. The job will stop and fail with an error once this limit is reached. Set to 0 or -1 to disable the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">parent_document_urn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional parent document URN for top-level imported pages. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">recursive</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Recursively fetch child pages. When true, ingests all descendant pages of specified pages/databases. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">advanced</span></div> <div className="type-name-line"><span className="type-name">AdvancedConfig</span></div> | Advanced configuration options.  |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">continue_on_failure</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">max_errors</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> |  <div className="default-line ">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">output_format</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "json", "xml" <div className="default-line default-line-with-docs">Default: <span className="default-value">json</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">preserve_outputs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">raise_on_error</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">work_dir</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">/tmp/unstructured&#95;datahub</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">cache</span></div> <div className="type-name-line"><span className="type-name">CacheConfig</span></div> | Cache configuration.  |
| <div className="path-line"><span className="path-prefix">advanced.cache.</span><span className="path-main">cache_dir</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">~/.cache/unstructured&#95;datahub</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.cache.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.cache.</span><span className="path-main">ttl</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Cache TTL in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">86400</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.</span><span className="path-main">retry</span></div> <div className="type-name-line"><span className="type-name">RetryConfig</span></div> | Retry configuration.  |
| <div className="path-line"><span className="path-prefix">advanced.retry.</span><span className="path-main">backoff_factor</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> |  <div className="default-line ">Default: <span className="default-value">2</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.retry.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.retry.</span><span className="path-main">max_attempts</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> |  <div className="default-line ">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-prefix">advanced.retry.</span><span className="path-main">retry_on_timeout</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">chunking</span></div> <div className="type-name-line"><span className="type-name">ChunkingConfig</span></div> | Chunking strategy configuration.  |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">combine_text_under_n_chars</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Combine chunks smaller than this size <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">max_characters</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum characters per chunk <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">overlap</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Character overlap between chunks <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "basic", "by_title" <div className="default-line default-line-with-docs">Default: <span className="default-value">by&#95;title</span></div> |
| <div className="path-line"><span className="path-main">database_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of Notion database IDs to ingest. If both page_ids and database_ids are empty, the source will automatically discover and ingest ALL pages and databases accessible to the integration. IDs can be found in database URLs: https://www.notion.so/{DATABASE_ID}  |
| <div className="path-line"><span className="path-prefix">database_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">datahub</span></div> <div className="type-name-line"><span className="type-name">DataHubConnectionConfig</span></div> | DataHub connection configuration. <br />  <br /> Defaults are loaded in this priority: <br /> 1. Explicitly configured values in the recipe <br /> 2. Environment variables (DATAHUB_GMS_URL, DATAHUB_GMS_TOKEN) <br /> 3. ~/.datahubenv file (created by `datahub init`) <br /> 4. Hardcoded defaults (http://localhost:8080, no token)  |
| <div className="path-line"><span className="path-prefix">datahub.</span><span className="path-main">server</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub GMS server URL (defaults to DATAHUB_GMS_URL env var, ~/.datahubenv, or localhost:8080)  |
| <div className="path-line"><span className="path-prefix">datahub.</span><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | DataHub API token for authentication (defaults to DATAHUB_GMS_TOKEN env var or ~/.datahubenv)  |
| <div className="path-line"><span className="path-main">document_mapping</span></div> <div className="type-name-line"><span className="type-name">DocumentMappingConfig</span></div> | Document entity mapping configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">id_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for generating document IDs <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;source&#95;type&#125;-&#123;directory&#125;-&#123;basename&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">status</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "PUBLISHED", "UNPUBLISHED" <div className="default-line default-line-with-docs">Default: <span className="default-value">PUBLISHED</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">id_normalization</span></div> <div className="type-name-line"><span className="type-name">IdNormalizationConfig</span></div> | Document ID normalization rules.  |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Convert to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">max_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum ID length <div className="default-line default-line-with-docs">Default: <span className="default-value">200</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">remove_special_chars</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Remove special characters except _ and - <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">replace_spaces_with</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Replace spaces with this character <div className="default-line default-line-with-docs">Default: <span className="default-value">-</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">source</span></div> <div className="type-name-line"><span className="type-name">SourceConfig</span></div> | Document source configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">include_external_id</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include external ID in DocumentSource <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include external URL in DocumentSource <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL" <div className="default-line default-line-with-docs">Default: <span className="default-value">EXTERNAL</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">title</span></div> <div className="type-name-line"><span className="type-name">TitleExtractionConfig</span></div> | Title extraction configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">extract_from_content</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Try to extract title from document content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">fallback_to_filename</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use filename as title if not found in content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">max_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum title length <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
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
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">provider</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Embedding provider. 'local' calls a locally-running OpenAI-compatible server (e.g. Ollama). 'vertex_ai' uses GCP. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">rate_limit</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable rate limiting for embedding API calls. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">request_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Per-call HTTP timeout in seconds for embedding providers. <div className="default-line default-line-with-docs">Default: <span className="default-value">60.0</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">vertex_location</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP region for the Vertex AI endpoint (e.g., 'us-east1', 'us-central1'). Matches the Java server default. <div className="default-line default-line-with-docs">Default: <span className="default-value">us-east1</span></div> |
| <div className="path-line"><span className="path-prefix">embedding.</span><span className="path-main">vertex_project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | GCP project ID for Vertex AI. Required when provider is 'vertex_ai' and using local config. If not set, loads from server. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">filtering</span></div> <div className="type-name-line"><span className="type-name">FilteringConfig</span></div> | File filtering configuration.  |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">max_file_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum file size in bytes <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">min_file_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Minimum file size in bytes <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">min_text_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Minimum text length in characters <div className="default-line default-line-with-docs">Default: <span className="default-value">50</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">modified_after</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Only files modified after this date (ISO format) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">modified_before</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Only files modified before this date (ISO format) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">skip_empty_documents</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Skip documents with no text content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">exclude_patterns</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Glob patterns to exclude  |
| <div className="path-line"><span className="path-prefix">filtering.exclude_patterns.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">filtering.</span><span className="path-main">include_patterns</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Glob patterns to include  |
| <div className="path-line"><span className="path-prefix">filtering.include_patterns.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">hierarchy</span></div> <div className="type-name-line"><span className="type-name">HierarchyConfig</span></div> | Hierarchy configuration.  |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable parent-child relationships <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">parent_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "folder", "none", "custom", "notion", "confluence" <div className="default-line default-line-with-docs">Default: <span className="default-value">folder</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">custom_mapping</span></div> <div className="type-name-line"><span className="type-name">One of CustomMappingConfig, null</span></div> | Custom mapping configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.</span><span className="path-main">rules</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Custom parent mapping rules  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.</span><span className="path-main">CustomParentRule</span></div> <div className="type-name-line"><span className="type-name">CustomParentRule</span></div> | Custom parent mapping rule.  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.CustomParentRule.</span><span className="path-main">parent_id</span>&nbsp;<abbr title="Required if CustomParentRule is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Parent document ID for matching files  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.CustomParentRule.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if CustomParentRule is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Glob pattern to match file paths  |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">folder_mapping</span></div> <div className="type-name-line"><span className="type-name">FolderMappingConfig</span></div> | Folder hierarchy mapping configuration.  |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">create_parent_docs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Create Document entities for folders <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum hierarchy depth <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">parent_id_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for parent document IDs <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;source&#95;type&#125;-&#123;directory&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">root_parent</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional root document URN <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">page_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of Notion page IDs to ingest. IDs can be found in page URLs: https://www.notion.so/Page-Title-{PAGE_ID}. If both page_ids and database_ids are empty, the source will automatically discover and ingest ALL pages and databases accessible to the integration.  |
| <div className="path-line"><span className="path-prefix">page_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">processing</span></div> <div className="type-name-line"><span className="type-name">ProcessingConfig</span></div> | Processing configuration (partitioning only, no chunking).  |
| <div className="path-line"><span className="path-prefix">processing.</span><span className="path-main">parallelism</span></div> <div className="type-name-line"><span className="type-name">ParallelismConfig</span></div> | Parallelism configuration.  |
| <div className="path-line"><span className="path-prefix">processing.parallelism.</span><span className="path-main">disable_parallelism</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Disable all parallelism <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">processing.parallelism.</span><span className="path-main">max_connections</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Max concurrent connections for async operations <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">processing.parallelism.</span><span className="path-main">num_processes</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div> |
| <div className="path-line"><span className="path-prefix">processing.</span><span className="path-main">partition</span></div> <div className="type-name-line"><span className="type-name">PartitionConfig</span></div> | Unstructured partitioning configuration.  |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">additional_args</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional partition arguments  |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Unstructured API key <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">partition_by_api</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use Unstructured API for partitioning <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">split_pdf_concurrency_level</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of parallel requests for PDF pages <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">split_pdf_page</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable page-level splitting for large PDFs <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "auto", "hi_res", "fast", "ocr_only" <div className="default-line default-line-with-docs">Default: <span className="default-value">auto</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.</span><span className="path-main">ocr_languages</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Languages for OCR <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;eng&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">processing.partition.ocr_languages.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
    "AdvancedConfig": {
      "additionalProperties": false,
      "description": "Advanced configuration options.",
      "properties": {
        "work_dir": {
          "default": "/tmp/unstructured_datahub",
          "title": "Work Dir",
          "type": "string"
        },
        "preserve_outputs": {
          "default": false,
          "title": "Preserve Outputs",
          "type": "boolean"
        },
        "output_format": {
          "default": "json",
          "enum": [
            "json",
            "xml"
          ],
          "title": "Output Format",
          "type": "string"
        },
        "raise_on_error": {
          "default": false,
          "title": "Raise On Error",
          "type": "boolean"
        },
        "max_errors": {
          "default": 10,
          "title": "Max Errors",
          "type": "integer"
        },
        "continue_on_failure": {
          "default": true,
          "title": "Continue On Failure",
          "type": "boolean"
        },
        "retry": {
          "$ref": "#/$defs/RetryConfig"
        },
        "cache": {
          "$ref": "#/$defs/CacheConfig"
        }
      },
      "title": "AdvancedConfig",
      "type": "object"
    },
    "CacheConfig": {
      "additionalProperties": false,
      "description": "Cache configuration.",
      "properties": {
        "enabled": {
          "default": true,
          "title": "Enabled",
          "type": "boolean"
        },
        "cache_dir": {
          "default": "~/.cache/unstructured_datahub",
          "title": "Cache Dir",
          "type": "string"
        },
        "ttl": {
          "default": 86400,
          "description": "Cache TTL in seconds",
          "title": "Ttl",
          "type": "integer"
        }
      },
      "title": "CacheConfig",
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
        }
      },
      "title": "ChunkingConfig",
      "type": "object"
    },
    "CustomMappingConfig": {
      "additionalProperties": false,
      "description": "Custom parent mapping configuration.",
      "properties": {
        "rules": {
          "description": "Custom parent mapping rules",
          "items": {
            "$ref": "#/$defs/CustomParentRule"
          },
          "title": "Rules",
          "type": "array"
        }
      },
      "title": "CustomMappingConfig",
      "type": "object"
    },
    "CustomParentRule": {
      "additionalProperties": false,
      "description": "Custom parent mapping rule.",
      "properties": {
        "pattern": {
          "description": "Glob pattern to match file paths",
          "title": "Pattern",
          "type": "string"
        },
        "parent_id": {
          "description": "Parent document ID for matching files",
          "title": "Parent Id",
          "type": "string"
        }
      },
      "required": [
        "pattern",
        "parent_id"
      ],
      "title": "CustomParentRule",
      "type": "object"
    },
    "DataHubConnectionConfig": {
      "additionalProperties": false,
      "description": "DataHub connection configuration.\n\nDefaults are loaded in this priority:\n1. Explicitly configured values in the recipe\n2. Environment variables (DATAHUB_GMS_URL, DATAHUB_GMS_TOKEN)\n3. ~/.datahubenv file (created by `datahub init`)\n4. Hardcoded defaults (http://localhost:8080, no token)",
      "properties": {
        "server": {
          "description": "DataHub GMS server URL (defaults to DATAHUB_GMS_URL env var, ~/.datahubenv, or localhost:8080)",
          "title": "Server",
          "type": "string"
        },
        "token": {
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
          "description": "DataHub API token for authentication (defaults to DATAHUB_GMS_TOKEN env var or ~/.datahubenv)",
          "title": "Token"
        }
      },
      "title": "DataHubConnectionConfig",
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
    "DocumentMappingConfig": {
      "additionalProperties": false,
      "description": "Document entity mapping configuration.",
      "properties": {
        "id_pattern": {
          "default": "{source_type}-{directory}-{basename}",
          "description": "Pattern for generating document IDs",
          "title": "Id Pattern",
          "type": "string"
        },
        "id_normalization": {
          "$ref": "#/$defs/IdNormalizationConfig",
          "description": "ID normalization rules"
        },
        "title": {
          "$ref": "#/$defs/TitleExtractionConfig",
          "description": "Title extraction configuration"
        },
        "source": {
          "$ref": "#/$defs/SourceConfig",
          "description": "Source configuration"
        },
        "status": {
          "default": "PUBLISHED",
          "description": "Default publication status",
          "enum": [
            "PUBLISHED",
            "UNPUBLISHED"
          ],
          "title": "Status",
          "type": "string"
        }
      },
      "title": "DocumentMappingConfig",
      "type": "object"
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
                "vertex_ai"
              ],
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Embedding provider. 'local' calls a locally-running OpenAI-compatible server (e.g. Ollama). 'vertex_ai' uses GCP. If not set, loads from server.",
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
    "FilteringConfig": {
      "additionalProperties": false,
      "description": "File filtering configuration.",
      "properties": {
        "include_patterns": {
          "description": "Glob patterns to include",
          "items": {
            "type": "string"
          },
          "title": "Include Patterns",
          "type": "array"
        },
        "exclude_patterns": {
          "description": "Glob patterns to exclude",
          "items": {
            "type": "string"
          },
          "title": "Exclude Patterns",
          "type": "array"
        },
        "min_file_size": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Minimum file size in bytes",
          "title": "Min File Size"
        },
        "max_file_size": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Maximum file size in bytes",
          "title": "Max File Size"
        },
        "modified_after": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Only files modified after this date (ISO format)",
          "title": "Modified After"
        },
        "modified_before": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Only files modified before this date (ISO format)",
          "title": "Modified Before"
        },
        "skip_empty_documents": {
          "default": true,
          "description": "Skip documents with no text content",
          "title": "Skip Empty Documents",
          "type": "boolean"
        },
        "min_text_length": {
          "default": 50,
          "description": "Minimum text length in characters",
          "title": "Min Text Length",
          "type": "integer"
        }
      },
      "title": "FilteringConfig",
      "type": "object"
    },
    "FolderMappingConfig": {
      "additionalProperties": false,
      "description": "Folder hierarchy mapping configuration.",
      "properties": {
        "create_parent_docs": {
          "default": true,
          "description": "Create Document entities for folders",
          "title": "Create Parent Docs",
          "type": "boolean"
        },
        "parent_id_pattern": {
          "default": "{source_type}-{directory}",
          "description": "Pattern for parent document IDs",
          "title": "Parent Id Pattern",
          "type": "string"
        },
        "max_depth": {
          "default": 10,
          "description": "Maximum hierarchy depth",
          "maximum": 50,
          "minimum": 1,
          "title": "Max Depth",
          "type": "integer"
        },
        "root_parent": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Optional root document URN",
          "title": "Root Parent"
        }
      },
      "title": "FolderMappingConfig",
      "type": "object"
    },
    "HierarchyConfig": {
      "additionalProperties": false,
      "description": "Hierarchy configuration.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Enable parent-child relationships",
          "title": "Enabled",
          "type": "boolean"
        },
        "parent_strategy": {
          "default": "folder",
          "description": "Parent document creation strategy. 'notion' extracts parent from Notion API metadata. 'confluence' extracts parent from Confluence page ancestors.",
          "enum": [
            "folder",
            "none",
            "custom",
            "notion",
            "confluence"
          ],
          "title": "Parent Strategy",
          "type": "string"
        },
        "folder_mapping": {
          "$ref": "#/$defs/FolderMappingConfig",
          "description": "Folder mapping configuration"
        },
        "custom_mapping": {
          "anyOf": [
            {
              "$ref": "#/$defs/CustomMappingConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Custom mapping configuration"
        }
      },
      "title": "HierarchyConfig",
      "type": "object"
    },
    "IdNormalizationConfig": {
      "additionalProperties": false,
      "description": "Document ID normalization rules.",
      "properties": {
        "lowercase": {
          "default": true,
          "description": "Convert to lowercase",
          "title": "Lowercase",
          "type": "boolean"
        },
        "replace_spaces_with": {
          "default": "-",
          "description": "Replace spaces with this character",
          "title": "Replace Spaces With",
          "type": "string"
        },
        "remove_special_chars": {
          "default": true,
          "description": "Remove special characters except _ and -",
          "title": "Remove Special Chars",
          "type": "boolean"
        },
        "max_length": {
          "default": 200,
          "description": "Maximum ID length",
          "title": "Max Length",
          "type": "integer"
        }
      },
      "title": "IdNormalizationConfig",
      "type": "object"
    },
    "ParallelismConfig": {
      "additionalProperties": false,
      "description": "Parallelism configuration.",
      "properties": {
        "num_processes": {
          "default": 2,
          "description": "Number of worker processes",
          "maximum": 32,
          "minimum": 1,
          "title": "Num Processes",
          "type": "integer"
        },
        "disable_parallelism": {
          "default": false,
          "description": "Disable all parallelism",
          "title": "Disable Parallelism",
          "type": "boolean"
        },
        "max_connections": {
          "default": 10,
          "description": "Max concurrent connections for async operations",
          "title": "Max Connections",
          "type": "integer"
        }
      },
      "title": "ParallelismConfig",
      "type": "object"
    },
    "PartitionConfig": {
      "additionalProperties": false,
      "description": "Unstructured partitioning configuration.",
      "properties": {
        "strategy": {
          "default": "auto",
          "description": "Partitioning strategy",
          "enum": [
            "auto",
            "hi_res",
            "fast",
            "ocr_only"
          ],
          "title": "Strategy",
          "type": "string"
        },
        "partition_by_api": {
          "default": false,
          "description": "Use Unstructured API for partitioning",
          "title": "Partition By Api",
          "type": "boolean"
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
          "description": "Unstructured API key",
          "title": "Api Key"
        },
        "split_pdf_page": {
          "default": false,
          "description": "Enable page-level splitting for large PDFs",
          "title": "Split Pdf Page",
          "type": "boolean"
        },
        "split_pdf_concurrency_level": {
          "default": 5,
          "description": "Number of parallel requests for PDF pages",
          "title": "Split Pdf Concurrency Level",
          "type": "integer"
        },
        "ocr_languages": {
          "default": [
            "eng"
          ],
          "description": "Languages for OCR",
          "items": {
            "type": "string"
          },
          "title": "Ocr Languages",
          "type": "array"
        },
        "additional_args": {
          "additionalProperties": true,
          "description": "Additional partition arguments",
          "title": "Additional Args",
          "type": "object"
        }
      },
      "title": "PartitionConfig",
      "type": "object"
    },
    "ProcessingConfig": {
      "additionalProperties": false,
      "description": "Processing configuration (partitioning only, no chunking).",
      "properties": {
        "partition": {
          "$ref": "#/$defs/PartitionConfig",
          "description": "Partition configuration"
        },
        "parallelism": {
          "$ref": "#/$defs/ParallelismConfig",
          "description": "Parallelism configuration"
        }
      },
      "title": "ProcessingConfig",
      "type": "object"
    },
    "RetryConfig": {
      "additionalProperties": false,
      "description": "Retry configuration.",
      "properties": {
        "enabled": {
          "default": true,
          "title": "Enabled",
          "type": "boolean"
        },
        "max_attempts": {
          "default": 3,
          "title": "Max Attempts",
          "type": "integer"
        },
        "backoff_factor": {
          "default": 2,
          "title": "Backoff Factor",
          "type": "integer"
        },
        "retry_on_timeout": {
          "default": true,
          "title": "Retry On Timeout",
          "type": "boolean"
        }
      },
      "title": "RetryConfig",
      "type": "object"
    },
    "SourceConfig": {
      "additionalProperties": false,
      "description": "Document source configuration.",
      "properties": {
        "type": {
          "default": "EXTERNAL",
          "description": "Document source type: NATIVE for editable DataHub documents, EXTERNAL for read-only references.",
          "enum": [
            "NATIVE",
            "EXTERNAL"
          ],
          "title": "Type",
          "type": "string"
        },
        "include_external_url": {
          "default": true,
          "description": "Include external URL in DocumentSource",
          "title": "Include External Url",
          "type": "boolean"
        },
        "include_external_id": {
          "default": true,
          "description": "Include external ID in DocumentSource",
          "title": "Include External Id",
          "type": "boolean"
        }
      },
      "title": "SourceConfig",
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
    },
    "TitleExtractionConfig": {
      "additionalProperties": false,
      "description": "Title extraction configuration.",
      "properties": {
        "extract_from_content": {
          "default": true,
          "description": "Try to extract title from document content",
          "title": "Extract From Content",
          "type": "boolean"
        },
        "fallback_to_filename": {
          "default": true,
          "description": "Use filename as title if not found in content",
          "title": "Fallback To Filename",
          "type": "boolean"
        },
        "max_length": {
          "default": 500,
          "description": "Maximum title length",
          "title": "Max Length",
          "type": "integer"
        }
      },
      "title": "TitleExtractionConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Notion ingestion configuration.\n\nThis source extracts documents from Notion pages and databases\nusing the Notion API and Unstructured.io text extraction.",
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
    "api_key": {
      "description": "Notion internal integration token. Create one at https://www.notion.so/my-integrations",
      "format": "password",
      "title": "Api Key",
      "type": "string",
      "writeOnly": true
    },
    "page_ids": {
      "description": "List of Notion page IDs to ingest. IDs can be found in page URLs: https://www.notion.so/Page-Title-{PAGE_ID}. If both page_ids and database_ids are empty, the source will automatically discover and ingest ALL pages and databases accessible to the integration.",
      "items": {
        "type": "string"
      },
      "title": "Page Ids",
      "type": "array"
    },
    "database_ids": {
      "description": "List of Notion database IDs to ingest. If both page_ids and database_ids are empty, the source will automatically discover and ingest ALL pages and databases accessible to the integration. IDs can be found in database URLs: https://www.notion.so/{DATABASE_ID}",
      "items": {
        "type": "string"
      },
      "title": "Database Ids",
      "type": "array"
    },
    "recursive": {
      "default": true,
      "description": "Recursively fetch child pages. When true, ingests all descendant pages of specified pages/databases.",
      "title": "Recursive",
      "type": "boolean"
    },
    "document_import_mode": {
      "$ref": "#/$defs/DocumentImportMode",
      "default": "EXTERNAL",
      "description": "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only references that link back to Notion."
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
      "description": "Optional parent document URN for top-level imported pages.",
      "title": "Parent Document Urn"
    },
    "processing": {
      "$ref": "#/$defs/ProcessingConfig",
      "description": "Text extraction and partitioning configuration"
    },
    "document_mapping": {
      "$ref": "#/$defs/DocumentMappingConfig",
      "description": "Document entity mapping configuration (ID generation, title extraction)"
    },
    "hierarchy": {
      "$ref": "#/$defs/HierarchyConfig",
      "description": "Parent-child relationship configuration"
    },
    "filtering": {
      "$ref": "#/$defs/FilteringConfig",
      "description": "Document filtering configuration"
    },
    "datahub": {
      "$ref": "#/$defs/DataHubConnectionConfig",
      "description": "DataHub connection configuration (for querying server-side embedding config)"
    },
    "chunking": {
      "$ref": "#/$defs/ChunkingConfig",
      "description": "Chunking strategy configuration (for embeddings)"
    },
    "embedding": {
      "$ref": "#/$defs/EmbeddingConfig",
      "description": "Embedding generation configuration (Bedrock, Cohere, OpenAI, or Vertex AI)"
    },
    "max_documents": {
      "default": 10000,
      "description": "Maximum number of documents to process per ingestion run. The job will stop and fail with an error once this limit is reached. Set to 0 or -1 to disable the limit.",
      "minimum": -1,
      "title": "Max Documents",
      "type": "integer"
    },
    "advanced": {
      "$ref": "#/$defs/AdvancedConfig",
      "description": "Advanced configuration options (work directory, error handling)"
    }
  },
  "required": [
    "api_key"
  ],
  "title": "NotionSourceConfig",
  "type": "object"
}
```





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


### Code Coordinates
- Class Name: `datahub.ingestion.source.notion.notion_source.NotionSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/notion/notion_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Notion, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
