


# Confluence

## Overview

Confluence is a documentation or collaboration platform. Learn more in the [official Confluence documentation](https://www.atlassian.com/software/confluence).

The DataHub integration for Confluence covers document/workspace entities and hierarchy context for knowledge assets. It also captures stateful deletion detection.

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


## Module `confluence`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

:::caution Not Supported with Remote Executor
This source is available as a private beta feature on DataHub Cloud. Note that running the connector using the Remote Executor is not yet supported.
:::

The Confluence source ingests pages and spaces from Confluence workspaces (Cloud or Data Center) as DataHub Document entities with optional semantic embeddings for semantic search.

#### Key Features

##### 1. Content Extraction

- **Page Content**: Full text extraction from Confluence pages including all content types
- **Space Discovery**: Automatic discovery of all pages within specified spaces
- **Hierarchical Structure**: Maintains parent-child relationships between pages
- **Metadata Extraction**: Captures creation/modification timestamps, authors, labels, and custom properties

##### 2. Hierarchical Relationships

- **Parent-Child Links**: Preserves Confluence page hierarchy in DataHub
- **Recursive Discovery**: Recursively discovers nested pages starting from root pages or entire spaces
- **Space Organization**: Maintains space context as custom properties
- **Flexible Navigation**: Browse documentation structure in DataHub UI

##### 3. Embedding Generation

Optional semantic search support with sensible defaults:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Automatic chunking**: Documents are automatically chunked for optimal embedding generation
- **Automatic deduplication**: Prevents duplicate chunk embeddings

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed setup and advanced options.

##### 4. Stateful Ingestion

Supports smart incremental updates via stateful ingestion:

- **Content Change Detection**: Only reprocesses documents when content or embeddings config changes
- **Deletion Detection**: Automatically removes stale entities from DataHub
- **Flexible Discovery**: Ingest entire spaces, specific pages, or page trees
- **State Persistence**: Maintains processing state between runs to skip unchanged documents

#### Related Documentation

- [Confluence Cloud REST API](https://developer.atlassian.com/cloud/confluence/rest/v1/intro/)
- [Confluence Data Center REST API](https://docs.atlassian.com/ConfluenceServer/rest/)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)
- [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)

### Prerequisites

#### 1. Confluence API Access

##### For Confluence Cloud

Create an API token:

1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Click **"Create API token"**
3. Give it a name (e.g., "DataHub Integration")
4. Copy the token (you won't be able to see it again)

You'll need:

- **Base URL**: Your Confluence Cloud URL (e.g., `https://your-domain.atlassian.net/wiki`)
- **Username**: Your Atlassian account email
- **API Token**: The token you just created

##### For Confluence Data Center / Server

Create a Personal Access Token:

1. Go to your Confluence → Profile → Personal Access Tokens
2. Click **"Create token"**
3. Give it a name and set expiration
4. Copy the token

You'll need:

- **Base URL**: Your Confluence server URL (e.g., `https://confluence.company.com`)
- **Personal Access Token**: The token you created

**Note**: For Data Center, you can also use username/password, but Personal Access Tokens are recommended.

#### 2. Required Permissions

The API credentials must have:

- **Read access** to all spaces and pages you want to ingest
- For Cloud: User must be added to spaces or have site-wide read access
- For Data Center: User must have "View" permissions on spaces

#### 3. Embedding Provider (Optional)

If you want semantic search capabilities, configure an embedding provider in your DataHub instance.

Supported providers include Cohere (API key) and AWS Bedrock (IAM roles). The connector will use sensible defaults for chunking and embedding configuration.

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed provider setup and configuration options.


### Install the Plugin
```shell
pip install 'acryl-datahub[confluence]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: confluence
  config:
    cloud: true
    url: "https://your-domain.atlassian.net/wiki"
    username: "user@example.com"
    api_token: "${CONFLUENCE_API_TOKEN}"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL of your Confluence instance. Examples: 'https://your-domain.atlassian.net/wiki' (Cloud) or 'https://confluence.your-company.com' (Data Center)  |
| <div className="path-line"><span className="path-main">api_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | API token for Confluence Cloud authentication. Generate at: https://id.atlassian.com/manage-profile/security/api-tokens <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">cloud</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether this is a Confluence Cloud instance (True) or Data Center/Server (False). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">document_import_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL"  |
| <div className="path-line"><span className="path-main">ingest_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Confluence folders as document entities so that pages nested under folders keep their place in the hierarchy. Confluence folders are a separate content type that the page APIs never return, so when this is disabled a page whose immediate parent is a folder is emitted without a parent link and appears as a root document. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_documents</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of documents to process per ingestion run. The job will stop and fail with an error once this limit is reached. Set to 0 or -1 to disable the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">max_pages_per_space</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of pages to ingest per space. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">max_spaces</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of spaces to ingest when auto-discovering (applies when urls is not set). <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">parent_document_urn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional parent document URN for top-level imported pages. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">personal_access_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Personal Access Token for Confluence Data Center authentication. Generate from: User Profile > Settings > Personal Access Tokens <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional human-readable identifier for this Confluence instance (e.g., 'mycompany-prod', 'team-a-confluence'). If not provided, automatically generated by hashing the base URL, which guarantees global uniqueness across all Confluence installations (both Cloud and Data Center). Use explicit values for more readable URNs, but auto-generated hashes are perfectly fine and require no manual configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">recursive</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to recursively fetch child pages (applies to page URLs only). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Username for Confluence Cloud authentication (required for Cloud). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
| <div className="path-line"><span className="path-main">pages</span></div> <div className="type-name-line"><span className="type-name">PageFilterConfig</span></div> | Configuration for filtering Confluence pages.  |
| <div className="path-line"><span className="path-prefix">pages.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List of specific Confluence pages to include in ingestion. By default, all pages in discovered spaces are included. Specify page IDs or URLs to limit ingestion to specific pages and their children. <br />  <br /> Examples: <br />   - Page IDs: ['123456', '789012'] <br />   - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/123456/API-Docs'] <br />  <br /> When specified, only these page trees will be ingested (if recursive=true). This allows focusing on specific documentation sections. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">pages.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">pages.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List of specific Confluence pages to exclude from ingestion. Applies after allow filtering. <br />  <br /> Examples: <br />   - Exclude specific pages: ['123456', '789012'] <br />   - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/999999/Draft'] <br />  <br /> Useful for excluding specific pages within otherwise included spaces. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">pages.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
| <div className="path-line"><span className="path-main">spaces</span></div> <div className="type-name-line"><span className="type-name">SpaceFilterConfig</span></div> | Configuration for filtering Confluence spaces.  |
| <div className="path-line"><span className="path-prefix">spaces.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List of Confluence spaces to include in ingestion. By default, all accessible spaces are discovered. Specify space keys or URLs to limit ingestion to specific spaces. <br />  <br /> Examples: <br />   - Space keys: ['ENGINEERING', 'PRODUCT', 'DESIGN'] <br />   - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEAM'] <br />   - Mixed: ['ENGINEERING', 'https://domain.atlassian.net/wiki/spaces/PRODUCT'] <br />  <br /> If specified, only these spaces will be ingested. Use deny to exclude specific spaces from discovery. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">spaces.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">spaces.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List of Confluence spaces to exclude from ingestion. Applies after allow filtering. <br />  <br /> Examples: <br />   - Exclude personal spaces: ['~user1', '~user2'] <br />   - Exclude specific spaces: ['ARCHIVE', 'OLD_DOCS'] <br />   - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEST'] <br />  <br /> Useful for excluding personal spaces or archived content. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">spaces.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
    "PageFilterConfig": {
      "additionalProperties": false,
      "description": "Configuration for filtering Confluence pages.",
      "properties": {
        "allow": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "List of specific Confluence pages to include in ingestion. By default, all pages in discovered spaces are included. Specify page IDs or URLs to limit ingestion to specific pages and their children.\n\nExamples:\n  - Page IDs: ['123456', '789012']\n  - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/123456/API-Docs']\n\nWhen specified, only these page trees will be ingested (if recursive=true). This allows focusing on specific documentation sections.",
          "title": "Allow"
        },
        "deny": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "List of specific Confluence pages to exclude from ingestion. Applies after allow filtering.\n\nExamples:\n  - Exclude specific pages: ['123456', '789012']\n  - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/999999/Draft']\n\nUseful for excluding specific pages within otherwise included spaces.",
          "title": "Deny"
        }
      },
      "title": "PageFilterConfig",
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
    "SpaceFilterConfig": {
      "additionalProperties": false,
      "description": "Configuration for filtering Confluence spaces.",
      "properties": {
        "allow": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "List of Confluence spaces to include in ingestion. By default, all accessible spaces are discovered. Specify space keys or URLs to limit ingestion to specific spaces.\n\nExamples:\n  - Space keys: ['ENGINEERING', 'PRODUCT', 'DESIGN']\n  - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEAM']\n  - Mixed: ['ENGINEERING', 'https://domain.atlassian.net/wiki/spaces/PRODUCT']\n\nIf specified, only these spaces will be ingested. Use deny to exclude specific spaces from discovery.",
          "title": "Allow"
        },
        "deny": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "List of Confluence spaces to exclude from ingestion. Applies after allow filtering.\n\nExamples:\n  - Exclude personal spaces: ['~user1', '~user2']\n  - Exclude specific spaces: ['ARCHIVE', 'OLD_DOCS']\n  - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEST']\n\nUseful for excluding personal spaces or archived content.",
          "title": "Deny"
        }
      },
      "title": "SpaceFilterConfig",
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
  "description": "Configuration for Confluence source connector.",
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
    "url": {
      "description": "Base URL of your Confluence instance. Examples: 'https://your-domain.atlassian.net/wiki' (Cloud) or 'https://confluence.your-company.com' (Data Center)",
      "title": "Url",
      "type": "string"
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
      "description": "Optional human-readable identifier for this Confluence instance (e.g., 'mycompany-prod', 'team-a-confluence'). If not provided, automatically generated by hashing the base URL, which guarantees global uniqueness across all Confluence installations (both Cloud and Data Center). Use explicit values for more readable URNs, but auto-generated hashes are perfectly fine and require no manual configuration.",
      "title": "Platform Instance"
    },
    "cloud": {
      "default": true,
      "description": "Whether this is a Confluence Cloud instance (True) or Data Center/Server (False).",
      "title": "Cloud",
      "type": "boolean"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Username for Confluence Cloud authentication (required for Cloud).",
      "title": "Username"
    },
    "api_token": {
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
      "description": "API token for Confluence Cloud authentication. Generate at: https://id.atlassian.com/manage-profile/security/api-tokens",
      "title": "Api Token"
    },
    "personal_access_token": {
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
      "description": "Personal Access Token for Confluence Data Center authentication. Generate from: User Profile > Settings > Personal Access Tokens",
      "title": "Personal Access Token"
    },
    "spaces": {
      "$ref": "#/$defs/SpaceFilterConfig"
    },
    "pages": {
      "$ref": "#/$defs/PageFilterConfig"
    },
    "max_spaces": {
      "default": 100,
      "description": "Maximum number of spaces to ingest when auto-discovering (applies when urls is not set).",
      "title": "Max Spaces",
      "type": "integer"
    },
    "max_pages_per_space": {
      "default": 1000,
      "description": "Maximum number of pages to ingest per space.",
      "title": "Max Pages Per Space",
      "type": "integer"
    },
    "recursive": {
      "default": true,
      "description": "Whether to recursively fetch child pages (applies to page URLs only).",
      "title": "Recursive",
      "type": "boolean"
    },
    "document_import_mode": {
      "$ref": "#/$defs/DocumentImportMode",
      "default": "EXTERNAL",
      "description": "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only references that link back to Confluence."
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
    "ingest_folders": {
      "default": true,
      "description": "Ingest Confluence folders as document entities so that pages nested under folders keep their place in the hierarchy. Confluence folders are a separate content type that the page APIs never return, so when this is disabled a page whose immediate parent is a folder is emitted without a parent link and appears as a root document.",
      "title": "Ingest Folders",
      "type": "boolean"
    },
    "processing": {
      "$ref": "#/$defs/ProcessingConfig",
      "description": "Document processing configuration (partitioning strategy, OCR, etc.)."
    },
    "document_mapping": {
      "$ref": "#/$defs/DocumentMappingConfig",
      "description": "Configuration for mapping Confluence pages to DataHub documents."
    },
    "hierarchy": {
      "$ref": "#/$defs/HierarchyConfig",
      "description": "Parent-child relationship configuration."
    },
    "filtering": {
      "$ref": "#/$defs/FilteringConfig",
      "description": "Filtering options for document content."
    },
    "chunking": {
      "$ref": "#/$defs/ChunkingConfig",
      "description": "Configuration for document chunking (required for embeddings)."
    },
    "embedding": {
      "$ref": "#/$defs/EmbeddingConfig",
      "description": "Configuration for generating vector embeddings for semantic search."
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
      "description": "Advanced ingestion options."
    }
  },
  "required": [
    "url"
  ],
  "title": "ConfluenceSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Common Use Cases

##### 1. Auto-Discover All Spaces (Default)

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

##### 2. Include Specific Spaces

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

##### 3. Exclude Personal and Archive Spaces

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

##### 4. Specific Page Trees Only

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

##### 5. Combined Space and Page Filtering

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

##### 6. Data Center / Server Setup

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

##### 7. Production Setup with Stateful Ingestion

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

##### 8. Using URLs for Allow/Deny

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

#### Filtering Content

The connector provides flexible filtering options through allow and deny lists for both spaces and pages.

##### Space Filtering

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

##### Page Filtering

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

##### Filtering Rules

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

##### Common Filtering Patterns

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

#### How It Works

##### Processing Pipeline

1. **Discovery**: Confluence API discovers spaces and pages
2. **Download**: Downloads page content via Confluence REST API
3. **Extraction**: Extracts text, metadata, and hierarchy from pages
4. **Chunking**: Splits documents into semantic chunks (if embeddings enabled)
5. **Embedding**: Generates vector embeddings for each chunk (if embeddings enabled)
6. **Emission**: Emits Document entities with SemanticContent aspects to DataHub

##### URL Format Support

The connector supports multiple input formats for spaces and pages in allow/deny lists:

**Space Identifiers:**

- Space key: `"ENGINEERING"`, `"~username"` (for personal spaces)
- Full URL: `"https://your-domain.atlassian.net/wiki/spaces/ENGINEERING"`

**Page Identifiers:**

- Page ID: `"123456789"` (numeric string)
- Full URL (Cloud): `"https://your-domain.atlassian.net/wiki/spaces/ENG/pages/123456/Page+Title"`
- Full URL (Data Center): `"https://confluence.company.com/pages/viewpage.action?pageId=123456"`

The connector automatically extracts space keys and page IDs from URLs, so you can use either format interchangeably in `space_allow`, `space_deny`, `page_allow`, and `page_deny` lists.

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

- Small spaces (<100 pages): `num_processes: 2`
- Medium spaces (100-500 pages): `num_processes: 4`
- Large spaces (>500 pages): `num_processes: 8`

##### Filtering

```yaml
filtering:
  min_text_length: 100 # Skip short pages (default: 50)
  skip_empty_documents: true # Skip empty pages (default: true)
```

##### Space Selection

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

#### Limitations and Considerations

##### Confluence API Limits

- **Rate Limits**: Confluence enforces rate limits (Cloud: varies by plan, Data Center: configurable)
- **Content Types**: Complex macros may not extract perfectly (e.g., embedded content, custom macros)
- **Attachments**: File attachments are not ingested (only page content)

##### Performance Considerations

- **Large Spaces**: First run may take significant time for large spaces (1000+ pages)
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Embedding providers may incur costs based on usage

##### Content Extraction

- **Supported Content**: Text, headings, lists, code blocks, tables, panels
- **Limited Support**: Some macros extract as text/links
- **Not Supported**: Attachments, complex custom macros, embedded Jira issues (content only)

### Troubleshooting

#### Common Issues

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

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.confluence.confluence_source.ConfluenceSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/confluence/confluence_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Confluence, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
