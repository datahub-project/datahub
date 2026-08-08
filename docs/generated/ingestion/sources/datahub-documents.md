


# DataHubDocuments

## Overview

Datahub Documents is a documentation or collaboration platform. Learn more in the [official Datahub Documents documentation](https://datahub.com/docs/).

The DataHub integration for Datahub Documents covers document/workspace entities and hierarchy context for knowledge assets. It also captures stateful deletion detection.

Process Document entities from DataHub and generate semantic embeddings for semantic search.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `datahub-documents`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |

### Overview

The DataHub Documents source processes Document entities already stored in DataHub and enriches them with semantic embeddings for semantic search. This source is designed to work with DataHub's native Document entities that have been created via GraphQL, Python SDK, or other ingestion sources (like Notion, Confluence, etc.).

#### Key Features

##### 1. Smart Defaults with Server Configuration Sync

The source **automatically fetches embedding configuration from your DataHub server**, ensuring perfect alignment:

- Connects using environment variables (`DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`)
- Fetches embedding provider config (provider, model, region, dimensions)
- Validates local config against server (if provided)
- Works with minimal `config: {}` in your recipe

##### 2. Dual Operating Modes

**Event-Driven Mode (Recommended):**

- Processes documents in real-time from Kafka MCL events
- Only reprocesses when document content changes
- Efficient for continuous updates
- Automatically falls back to batch mode on first run

**Batch Mode:**

- Fetches all documents via GraphQL
- Good for initial setup or periodic full refreshes
- Processes all matching documents

##### 3. Incremental Processing

- Tracks document content hashes to skip unchanged documents
- Uses DataHub's stateful ingestion framework
- Reduces processing time and API costs
- Force reprocess option available

##### 4. Flexible Platform Filtering

- Process all NATIVE documents (default)
- Filter by specific platforms (e.g., `["notion", "confluence"]`)
- Process all documents regardless of source type

#### Related Documentation

- [Semantic Search Configuration](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/how-to/semantic-search-configuration) - **Start Here**
- [Notion Source](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/generated/ingestion/sources/notion) - Example document source with embeddings
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html) - Embedding models
- [AWS Bedrock Pricing](https://aws.amazon.com/bedrock/pricing/) - Cost estimation
- [DataHub Developer Guides](/docs/developers) - Additional developer documentation

### Prerequisites

#### 1. DataHub Server Configuration

**You MUST configure semantic search on your DataHub server before using this source.**

See the [Semantic Search Configuration Guide](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/how-to/semantic-search-configuration) for complete setup instructions.

Required server configuration:

- OpenSearch 2.17+ with k-NN plugin enabled
- AWS Bedrock or Cohere embedding provider configured
- Semantic search enabled in `application.yml`

#### 2. Document Entities in DataHub

This source processes existing Document entities. Documents can be created through:

- **GraphQL API**: Create documents programmatically
- **Python SDK**: Use `datahub.sdk.document.Document`
- **External Sources**: Notion, Confluence, SharePoint sources that emit Document entities

#### 3. AWS Credentials (for Bedrock)

If using AWS Bedrock for embeddings:

- IAM permissions for `bedrock:InvokeModel`
- AWS credentials configured (env vars, instance role, or ECS task role)
- Bedrock model access enabled in AWS Console

#### 4. Environment Variables

```bash
# Required for DataHub connection
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your-token-here"

# Optional: AWS credentials (if not using instance/task roles)
export AWS_PROFILE="datahub-dev"
# OR
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-west-2"
```


### Install the Plugin
```shell
pip install 'acryl-datahub[datahub-documents]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: datahub-documents
  config:
    # Minimal configuration - uses smart defaults
    # Automatically connects to DataHub using DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN environment variables
    # Fetches embedding configuration from server
    # Enables event-driven mode and incremental processing
    # No sink needed when deploying to DataHub (writes back to itself)

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">include_external_documents</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Index EXTERNAL documents (sourceType=EXTERNAL), not just NATIVE ones. When platform_filter is set, EXTERNAL documents are still restricted to those platforms; when platform_filter is empty, all EXTERNAL documents are included. Set to False to restore NATIVE-only behavior. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">index_delay_seconds</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Delay in seconds after each document that is actually indexed (embeddings emitted). Skipped documents (unchanged, externally owned, empty, etc.) are not delayed. Defaults to 0.025s to smooth write load during bootstrap without unduly slowing large backfills (e.g. ~42 min of added pacing for 100k new documents); set to 0 to disable. Sub-second values are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">0.025</span></div> |
| <div className="path-line"><span className="path-main">max_documents</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of documents to process per ingestion run. The job will stop and fail with an error once this limit is reached. This is a runaway-cost guardrail; it counts documents actually indexed, so incremental runs rarely approach it and it mainly bounds initial bootstraps/backfills. Set to 0 or -1 to disable the limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">100000</span></div> |
| <div className="path-line"><span className="path-main">min_text_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Minimum text length in characters to process (shorter documents are skipped) <div className="default-line default-line-with-docs">Default: <span className="default-value">50</span></div> |
| <div className="path-line"><span className="path-main">partition_strategy</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Text partitioning strategy. Currently only 'markdown' is supported. This field is included in the document hash to trigger reprocessing if the strategy changes. <div className="default-line default-line-with-docs">Default: <span className="default-value">markdown</span></div> |
| <div className="path-line"><span className="path-main">scroll_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of documents to fetch per scrollAcrossEntities page in batch mode. scrollAcrossEntities uses cursor-based pagination, so all documents are fetched across pages regardless of this value (no 10k Elasticsearch window limit). Tune for throughput vs. memory/load on GMS. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">scroll_delay_seconds</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Delay in seconds between consecutive scroll page requests in batch mode. Use a small delay to reduce read load on GMS/Elasticsearch when scrolling large document sets; set to 0 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">0.0</span></div> |
| <div className="path-line"><span className="path-main">skip_empty_text</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Skip documents with no text content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">skip_external_if_semantic_content_exists</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | For EXTERNAL documents only: before processing, check whether the document already has a semanticContent aspect for the active embedding model and, if so, skip it and leave it out of our incremental state. Some ingestion sources (e.g. Notion, Confluence) perform their own semantic indexing, so an existing aspect means that source owns the document. This source then only picks up EXTERNAL documents the source did NOT index and takes ownership of them. NATIVE documents are never skipped this way. Adds one metadata read per candidate EXTERNAL document not already in state. Incremental content-hash skipping still applies once a document is owned. Set to False to always re-embed EXTERNAL documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">chunking</span></div> <div className="type-name-line"><span className="type-name">ChunkingConfig</span></div> | Chunking strategy configuration.  |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">combine_text_under_n_chars</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Combine chunks smaller than this size <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">max_characters</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum characters per chunk <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">overlap</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Character overlap between chunks <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-prefix">chunking.</span><span className="path-main">strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "basic", "by_title" <div className="default-line default-line-with-docs">Default: <span className="default-value">by&#95;title</span></div> |
| <div className="path-line"><span className="path-main">datahub</span></div> <div className="type-name-line"><span className="type-name">DataHubConnectionConfig</span></div> | DataHub connection configuration. <br />  <br /> Defaults are loaded in this priority: <br /> 1. Explicitly configured values in the recipe <br /> 2. Environment variables (DATAHUB_GMS_URL, DATAHUB_GMS_TOKEN) <br /> 3. ~/.datahubenv file (created by `datahub init`) <br /> 4. Hardcoded defaults (http://localhost:8080, no token)  |
| <div className="path-line"><span className="path-prefix">datahub.</span><span className="path-main">server</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub GMS server URL (defaults to DATAHUB_GMS_URL env var, ~/.datahubenv, or localhost:8080)  |
| <div className="path-line"><span className="path-prefix">datahub.</span><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | DataHub API token for authentication (defaults to DATAHUB_GMS_TOKEN env var or ~/.datahubenv)  |
| <div className="path-line"><span className="path-main">document_urns</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Specific document URNs to process (if None, process all matching platforms) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">document_urns.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
| <div className="path-line"><span className="path-main">event_mode</span></div> <div className="type-name-line"><span className="type-name">EventModeConfig</span></div> | Event-driven mode configuration.  |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">consumer_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Consumer ID for offset tracking (defaults to 'datahub-documents-{pipeline_name}') <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable event-driven mode (polls MCL events instead of GraphQL batch) <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">idle_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Exit after this many seconds with no new events (incremental batch mode) <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">lookback_days</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of days to look back for events on first run (None means start from latest) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">poll_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of events to fetch per poll <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">poll_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for each poll request <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">reset_offsets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Reset consumer offsets to start from beginning <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.</span><span className="path-main">topics</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Topics to consume for document changes <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;MetadataChangeLog&#95;Versioned&#95;v1&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">event_mode.topics.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">incremental</span></div> <div className="type-name-line"><span className="type-name">IncrementalConfig</span></div> | Incremental processing configuration.  |
| <div className="path-line"><span className="path-prefix">incremental.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Only process documents whose text content has changed (tracks content hash). Uses stateful ingestion when enabled. The state_file_path option is deprecated and ignored when stateful ingestion is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">incremental.</span><span className="path-main">force_reprocess</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Force reprocess all documents regardless of content hash <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">incremental.</span><span className="path-main">state_file_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [DEPRECATED] Path to state file. This option is ignored when stateful ingestion is enabled. State is now managed through DataHub's stateful ingestion framework. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">locking</span></div> <div className="type-name-line"><span className="type-name">LockConfig</span></div> | Distributed lock configuration to prevent overlapping ingestion runs. <br />  <br /> This source is typically scheduled on a short interval (e.g. every 15 minutes). <br /> A full scroll + embedding pass can take longer than the schedule interval, so two <br /> runs could otherwise overlap and redundantly re-embed the same documents. The lock <br /> uses a ``dataHubStepState`` entity as a shared lease, written synchronously to the <br /> primary store so concurrent runners observe it immediately.  |
| <div className="path-line"><span className="path-prefix">locking.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Acquire a distributed lock before processing so overlapping scheduled runs do not duplicate scroll + indexing work. When the lock is held by another run, this run exits cleanly without processing. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">locking.</span><span className="path-main">lock_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Identifier for the lock's dataHubStepState entity. Defaults to 'datahub-documents-lock-{pipeline_name}'. Runs sharing a lock_id are mutually exclusive; give unrelated pipelines distinct ids. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">locking.</span><span className="path-main">lock_renewal_interval_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How often the lock holder renews (extends) its lease while running. Must be comfortably smaller than lock_ttl_seconds. This lets a job run for hours with a short TTL: the lease stays alive as long as the run is healthy. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">locking.</span><span className="path-main">lock_ttl_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Lease duration in seconds. The lock holder renews its lease periodically (see lock_renewal_interval_seconds) while it runs, so this only needs to exceed the renewal interval (with margin for transient write failures), NOT the total run duration. A lease not renewed within this window is treated as stale (e.g. from a crashed run) and may be taken over. After a crash, future runs are blocked for at most this long. <div className="default-line default-line-with-docs">Default: <span className="default-value">1800</span></div> |
| <div className="path-line"><span className="path-main">platform_filter</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Filter documents by platforms. Default (None): Process all NATIVE documents (sourceType=NATIVE) regardless of platform. EXTERNAL documents are also processed by default (see include_external_documents); set platform_filter to restrict EXTERNAL documents to specific platforms (e.g., ['notion', 'confluence']). Use ['*'] or ['ALL'] to process all documents regardless of source type or platform. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">platform_filter.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">DocumentChunkingStatefulIngestionConfig</span></div> | Configuration for document chunking stateful ingestion.  |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
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
    "DocumentChunkingStatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Configuration for document chunking stateful ingestion.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "DocumentChunkingStatefulIngestionConfig",
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
    "EventModeConfig": {
      "additionalProperties": false,
      "description": "Event-driven mode configuration.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Enable event-driven mode (polls MCL events instead of GraphQL batch)",
          "title": "Enabled",
          "type": "boolean"
        },
        "consumer_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Consumer ID for offset tracking (defaults to 'datahub-documents-{pipeline_name}')",
          "title": "Consumer Id"
        },
        "topics": {
          "default": [
            "MetadataChangeLog_Versioned_v1"
          ],
          "description": "Topics to consume for document changes",
          "items": {
            "type": "string"
          },
          "title": "Topics",
          "type": "array"
        },
        "lookback_days": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number of days to look back for events on first run (None means start from latest)",
          "title": "Lookback Days"
        },
        "reset_offsets": {
          "default": false,
          "description": "Reset consumer offsets to start from beginning",
          "title": "Reset Offsets",
          "type": "boolean"
        },
        "idle_timeout_seconds": {
          "default": 30,
          "description": "Exit after this many seconds with no new events (incremental batch mode)",
          "title": "Idle Timeout Seconds",
          "type": "integer"
        },
        "poll_timeout_seconds": {
          "default": 2,
          "description": "Timeout for each poll request",
          "title": "Poll Timeout Seconds",
          "type": "integer"
        },
        "poll_limit": {
          "default": 100,
          "description": "Maximum number of events to fetch per poll",
          "title": "Poll Limit",
          "type": "integer"
        }
      },
      "title": "EventModeConfig",
      "type": "object"
    },
    "IncrementalConfig": {
      "additionalProperties": false,
      "description": "Incremental processing configuration.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Only process documents whose text content has changed (tracks content hash). Uses stateful ingestion when enabled. The state_file_path option is deprecated and ignored when stateful ingestion is enabled.",
          "title": "Enabled",
          "type": "boolean"
        },
        "state_file_path": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "[DEPRECATED] Path to state file. This option is ignored when stateful ingestion is enabled. State is now managed through DataHub's stateful ingestion framework.",
          "title": "State File Path"
        },
        "force_reprocess": {
          "default": false,
          "description": "Force reprocess all documents regardless of content hash",
          "title": "Force Reprocess",
          "type": "boolean"
        }
      },
      "title": "IncrementalConfig",
      "type": "object"
    },
    "LockConfig": {
      "additionalProperties": false,
      "description": "Distributed lock configuration to prevent overlapping ingestion runs.\n\nThis source is typically scheduled on a short interval (e.g. every 15 minutes).\nA full scroll + embedding pass can take longer than the schedule interval, so two\nruns could otherwise overlap and redundantly re-embed the same documents. The lock\nuses a ``dataHubStepState`` entity as a shared lease, written synchronously to the\nprimary store so concurrent runners observe it immediately.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Acquire a distributed lock before processing so overlapping scheduled runs do not duplicate scroll + indexing work. When the lock is held by another run, this run exits cleanly without processing.",
          "title": "Enabled",
          "type": "boolean"
        },
        "lock_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Identifier for the lock's dataHubStepState entity. Defaults to 'datahub-documents-lock-{pipeline_name}'. Runs sharing a lock_id are mutually exclusive; give unrelated pipelines distinct ids.",
          "title": "Lock Id"
        },
        "lock_ttl_seconds": {
          "default": 1800,
          "description": "Lease duration in seconds. The lock holder renews its lease periodically (see lock_renewal_interval_seconds) while it runs, so this only needs to exceed the renewal interval (with margin for transient write failures), NOT the total run duration. A lease not renewed within this window is treated as stale (e.g. from a crashed run) and may be taken over. After a crash, future runs are blocked for at most this long.",
          "exclusiveMinimum": 0,
          "title": "Lock Ttl Seconds",
          "type": "integer"
        },
        "lock_renewal_interval_seconds": {
          "default": 300,
          "description": "How often the lock holder renews (extends) its lease while running. Must be comfortably smaller than lock_ttl_seconds. This lets a job run for hours with a short TTL: the lease stays alive as long as the run is healthy.",
          "exclusiveMinimum": 0,
          "title": "Lock Renewal Interval Seconds",
          "type": "integer"
        }
      },
      "title": "LockConfig",
      "type": "object"
    }
  },
  "description": "Configuration for DataHub Documents Source.",
  "properties": {
    "stateful_ingestion": {
      "$ref": "#/$defs/DocumentChunkingStatefulIngestionConfig",
      "description": "Stateful ingestion configuration. Enabled by default to support incremental mode (document hash tracking) and event mode (offset tracking)."
    },
    "datahub": {
      "$ref": "#/$defs/DataHubConnectionConfig",
      "description": "DataHub connection configuration. Only used when running standalone (e.g., CLI ingestion). In managed ingestion (deployed sources), the connection is automatically configured from the sink."
    },
    "platform_filter": {
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
      "description": "Filter documents by platforms. Default (None): Process all NATIVE documents (sourceType=NATIVE) regardless of platform. EXTERNAL documents are also processed by default (see include_external_documents); set platform_filter to restrict EXTERNAL documents to specific platforms (e.g., ['notion', 'confluence']). Use ['*'] or ['ALL'] to process all documents regardless of source type or platform.",
      "title": "Platform Filter"
    },
    "include_external_documents": {
      "default": true,
      "description": "Index EXTERNAL documents (sourceType=EXTERNAL), not just NATIVE ones. When platform_filter is set, EXTERNAL documents are still restricted to those platforms; when platform_filter is empty, all EXTERNAL documents are included. Set to False to restore NATIVE-only behavior.",
      "title": "Include External Documents",
      "type": "boolean"
    },
    "document_urns": {
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
      "description": "Specific document URNs to process (if None, process all matching platforms)",
      "title": "Document Urns"
    },
    "event_mode": {
      "$ref": "#/$defs/EventModeConfig",
      "description": "Event-driven mode configuration (polls Kafka MCL events)"
    },
    "incremental": {
      "$ref": "#/$defs/IncrementalConfig",
      "description": "Incremental processing configuration (skip unchanged documents)"
    },
    "chunking": {
      "$ref": "#/$defs/ChunkingConfig",
      "description": "Text chunking strategy configuration"
    },
    "embedding": {
      "$ref": "#/$defs/EmbeddingConfig",
      "description": "Embedding generation configuration (Bedrock, Cohere, OpenAI, or Vertex AI)"
    },
    "max_documents": {
      "default": 100000,
      "description": "Maximum number of documents to process per ingestion run. The job will stop and fail with an error once this limit is reached. This is a runaway-cost guardrail; it counts documents actually indexed, so incremental runs rarely approach it and it mainly bounds initial bootstraps/backfills. Set to 0 or -1 to disable the limit.",
      "minimum": -1,
      "title": "Max Documents",
      "type": "integer"
    },
    "scroll_batch_size": {
      "default": 1000,
      "description": "Number of documents to fetch per scrollAcrossEntities page in batch mode. scrollAcrossEntities uses cursor-based pagination, so all documents are fetched across pages regardless of this value (no 10k Elasticsearch window limit). Tune for throughput vs. memory/load on GMS.",
      "exclusiveMinimum": 0,
      "title": "Scroll Batch Size",
      "type": "integer"
    },
    "scroll_delay_seconds": {
      "default": 0.0,
      "description": "Delay in seconds between consecutive scroll page requests in batch mode. Use a small delay to reduce read load on GMS/Elasticsearch when scrolling large document sets; set to 0 to disable.",
      "minimum": 0,
      "title": "Scroll Delay Seconds",
      "type": "number"
    },
    "index_delay_seconds": {
      "default": 0.025,
      "description": "Delay in seconds after each document that is actually indexed (embeddings emitted). Skipped documents (unchanged, externally owned, empty, etc.) are not delayed. Defaults to 0.025s to smooth write load during bootstrap without unduly slowing large backfills (e.g. ~42 min of added pacing for 100k new documents); set to 0 to disable. Sub-second values are supported.",
      "minimum": 0,
      "title": "Index Delay Seconds",
      "type": "number"
    },
    "skip_external_if_semantic_content_exists": {
      "default": true,
      "description": "For EXTERNAL documents only: before processing, check whether the document already has a semanticContent aspect for the active embedding model and, if so, skip it and leave it out of our incremental state. Some ingestion sources (e.g. Notion, Confluence) perform their own semantic indexing, so an existing aspect means that source owns the document. This source then only picks up EXTERNAL documents the source did NOT index and takes ownership of them. NATIVE documents are never skipped this way. Adds one metadata read per candidate EXTERNAL document not already in state. Incremental content-hash skipping still applies once a document is owned. Set to False to always re-embed EXTERNAL documents.",
      "title": "Skip External If Semantic Content Exists",
      "type": "boolean"
    },
    "locking": {
      "$ref": "#/$defs/LockConfig",
      "description": "Distributed lock configuration to prevent overlapping scheduled runs from duplicating scroll + indexing work."
    },
    "partition_strategy": {
      "const": "markdown",
      "default": "markdown",
      "description": "Text partitioning strategy. Currently only 'markdown' is supported. This field is included in the document hash to trigger reprocessing if the strategy changes.",
      "title": "Partition Strategy",
      "type": "string"
    },
    "skip_empty_text": {
      "default": true,
      "description": "Skip documents with no text content",
      "title": "Skip Empty Text",
      "type": "boolean"
    },
    "min_text_length": {
      "default": 50,
      "description": "Minimum text length in characters to process (shorter documents are skipped)",
      "title": "Min Text Length",
      "type": "integer"
    }
  },
  "title": "DataHubDocumentsSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::tip Quick Start: Auto-Deploy for Semantic Search

To enable automatic semantic search indexing for your documents, deploy this source to DataHub with a simple command:

```bash
# Create minimal recipe and deploy with hourly schedule
cat > /tmp/datahub-docs.yml << 'EOF'
source:
  type: datahub-documents
  config: {}
EOF

datahub ingest deploy -c /tmp/datahub-docs.yml --name "document-embeddings" --schedule "0 * * * *"
```

This creates a managed ingestion source in DataHub that automatically processes documents every hour and generates embeddings for semantic search.

**What this does:**

- ✅ Deploys ingestion recipe to DataHub
- ✅ Runs hourly (cron: `0 * * * *`) to keep embeddings up-to-date
- ✅ Uses event-driven mode (only processes changed documents)
- ✅ Auto-configures from server (no manual embedding setup needed)

**Alternative schedules:**

```bash
# Every 15 minutes: "*/15 * * * *"
# Every 6 hours:    "0 */6 * * *"
# Daily at 2 AM:    "0 2 * * *"
```

> **Note:** In future DataHub versions, GMS will run this automatically. For now, manual deployment is required.

:::

#### Common Use Cases

##### 1. Event-Driven Processing (Production)

Process documents in real-time as they're created or updated:

```yaml
source:
  type: datahub-documents
  config:
    # Event mode enabled by default in recent versions
    event_mode:
      enabled: true
      idle_timeout_seconds: 60

sink:
  type: datahub-rest
  config: {}
```

**When to use:**

- Production deployments
- Continuous document updates
- Real-time semantic search needs

##### 2. Batch Processing (Initial Load)

Process all documents in a single run:

```yaml
source:
  type: datahub-documents
  config:
    event_mode:
      enabled: false

    # Optional: Process specific platforms
    platform_filter: ["notion", "confluence"]

sink:
  type: datahub-rest
  config: {}
```

**When to use:**

- Initial setup
- Periodic full refreshes
- Backfilling embeddings

##### 3. Platform-Specific Processing

Process documents from specific platforms only:

```yaml
source:
  type: datahub-documents
  config:
    # Process NATIVE documents + EXTERNAL from these platforms
    platform_filter: ["notion", "confluence"]

    incremental:
      enabled: true

sink:
  type: datahub-rest
  config: {}
```

##### 4. Force Reprocessing

Reprocess all documents regardless of content changes:

```yaml
source:
  type: datahub-documents
  config:
    incremental:
      enabled: true
      force_reprocess: true # Reprocess everything

    # Useful when:
    # - Changing chunking strategy
    # - Updating embedding model
    # - Fixing processing issues

sink:
  type: datahub-rest
  config: {}
```

##### 5. Custom Chunking and Embedding

Override server configuration with local settings:

```yaml
source:
  type: datahub-documents
  config:
    # Custom chunking
    chunking:
      strategy: by_title # or 'basic'
      max_characters: 1000 # Larger chunks
      combine_text_under_n_chars: 200

    # Override embedding config (validates against server)
    embedding:
      provider: bedrock
      model: cohere.embed-english-v3
      model_embedding_key: cohere_embed_v3
      aws_region: us-west-2
      batch_size: 50

sink:
  type: datahub-rest
  config: {}
```

**⚠️ Warning:** Custom embedding configs are validated against the server. Mismatches will cause errors.

#### How It Works

##### Processing Pipeline

```
1. Fetch Mode Selection
   ├─ Event Mode: Subscribe to Kafka MCL events
   └─ Batch Mode: GraphQL query for all documents

2. For Each Document:
   ├─ Check incremental state (skip if unchanged)
   ├─ Partition markdown → structured elements
   ├─ Chunk elements → semantic chunks
   │   ├─ by_title: Preserves document structure
   │   └─ basic: Fixed-size chunks with overlap
   ├─ Generate embeddings
   │   └─ Batches of 25 (configurable)
   └─ Emit SemanticContent aspect → DataHub

3. State Management
   ├─ Batch Mode: Track document content hashes
   └─ Event Mode: Track Kafka offsets
```

##### Event Mode Flow

**First Run (No State):**

1. Falls back to batch mode
2. Captures current Kafka offset BEFORE processing
3. Processes all documents
4. Saves offset to state
5. Next run continues from captured offset

**Subsequent Runs:**

1. Loads last committed offset from state
2. Consumes events from last position
3. Processes only changed documents
4. Updates offset after each batch
5. Exits after idle timeout (no new events)

##### Incremental Processing

**Content Hash Calculation:**

```python
hash_input = {
    "text": document.text,
    "partition_strategy": config.partition_strategy,
    "chunking_strategy": config.chunking.strategy,
    "max_characters": config.chunking.max_characters,
    # ... other chunking params
}
content_hash = sha256(json.dumps(hash_input))
```

**When Documents Are Reprocessed:**

- Text content changes
- Chunking configuration changes
- Partition strategy changes
- `force_reprocess: true` is set

#### Configuration Deep Dive

##### Platform Filtering

The `platform_filter` setting controls which documents are processed:

**`None` (default):**

```yaml
platform_filter: null # or omit the field
```

- Processes all NATIVE documents (sourceType=NATIVE)
- Ignores EXTERNAL documents from other platforms

**Specific Platforms:**

```yaml
platform_filter: ["notion", "confluence"]
```

- Processes NATIVE documents
- PLUS EXTERNAL documents from specified platforms

**All Documents:**

```yaml
platform_filter: ["*"] # or ["ALL"]
```

- Processes ALL documents regardless of source type or platform

##### Event Mode Configuration

```yaml
event_mode:
  enabled: true

  # Consumer ID for offset tracking
  consumer_id: "datahub-documents-{pipeline_name}" # Default

  # Kafka topics to consume
  topics:
    - "MetadataChangeLog_Versioned_v1"

  # Lookback window for first run
  lookback_days: null # null = start from latest, or specify days

  # Reset offsets to beginning (DANGEROUS - reprocesses everything)
  reset_offsets: false

  # Exit after N seconds with no new events
  idle_timeout_seconds: 30

  # Kafka poll settings
  poll_timeout_seconds: 2
  poll_limit: 100
```

##### Chunking Strategies

**by_title (Recommended):**

```yaml
chunking:
  strategy: by_title
  max_characters: 500
  combine_text_under_n_chars: 100
```

- Preserves document structure
- Groups text under section headers
- Combines small chunks intelligently
- Better semantic coherence

**basic:**

```yaml
chunking:
  strategy: basic
  max_characters: 500
  overlap: 50 # Character overlap between chunks
```

- Simple fixed-size chunks
- Configurable overlap
- No structure awareness

##### Embedding Configuration

**Default (Fetch from Server):**

```yaml
embedding: {} # or omit entirely
```

- Automatically fetches config from server
- Ensures alignment with server's semantic search
- **Recommended for production**

**Override (Validated Against Server):**

```yaml
embedding:
  provider: bedrock # bedrock, cohere, openai
  model: cohere.embed-english-v3
  model_embedding_key: cohere_embed_v3 # Must match server!
  aws_region: us-west-2
  batch_size: 25
  input_type: search_document # Cohere-specific
```

- Validates that config matches server
- Fails if mismatch detected
- Prevents broken semantic search

**Break-Glass Override (NOT RECOMMENDED):**

```yaml
embedding:
  allow_local_embedding_config: true
  provider: bedrock
  model: cohere.embed-english-v3
  # ... other settings
```

- Bypasses server validation
- **May break semantic search**
- Only use for debugging or special cases

##### Stateful Ingestion

```yaml
stateful_ingestion:
  enabled: true # Enabled by default

  # State backend configuration
  state_provider:
    type: datahub # Store state in DataHub
    config:
      datahub_api:
        server: "http://localhost:8080"
        token: "${DATAHUB_TOKEN}"

  # Ignore previous state (fresh start)
  ignore_old_state: false

  # Don't commit new state (dry run)
  ignore_new_state: false
```

#### Run Locking (Preventing Overlapping Runs)

This source is often scheduled on a short interval (e.g. every 15 minutes), but a full
scroll + embedding pass can take longer than the interval. Without coordination, a new run
could start while the previous one is still working, causing both to re-embed the same
documents and race on the `SemanticContent` aspect. To prevent this, the source acquires a
**distributed lock** before processing.

##### How It Works

- The lock is a lightweight lease backed by an internal `dataHubStepState` entity, which
  DataHub uses as a general-purpose key/value store. The lease payload (status, run id,
  expiry) lives in that entity's properties map.
- Lease writes are committed **synchronously to the primary store (MySQL)**, so concurrent
  runs observe each other immediately (no reliance on eventually-consistent search).
- When a run starts and the lock is already held by another active run, the new run **exits
  cleanly without processing** (this is reported as a warning, not a failure).
- While a run holds the lock, it periodically renews (heartbeats) the lease so long jobs
  keep their lock for the full duration of the run.

##### TTL and Lock Timeout

Each lease carries a **time-to-live (TTL)**. Because the lock holder renews the lease while
it runs, the TTL only needs to exceed the renewal interval — **not** the total run duration.
If a run crashes and stops renewing, its lease simply expires after the TTL and the next run
takes over automatically. After a crash, future runs are blocked for **at most** one TTL.

```yaml
source:
  type: datahub-documents
  config:
    locking:
      enabled: true # default
      # Optional explicit lock id; defaults to
      # "document-indexing-lock-<ingestion-source-id>"
      lock_id: null
      # Lease duration. A crashed run blocks the next run for at most this long.
      lock_ttl_seconds: 1800 # default: 30 minutes
      # How often the holder renews its lease while running.
      # Must be comfortably smaller than lock_ttl_seconds.
      lock_renewal_interval_seconds: 300 # default: 5 minutes
```

##### Manually Clearing a Lock

You normally never need to do this — a healthy run releases its lock on completion, and a
crashed run's lease expires after `lock_ttl_seconds`. If you want to clear a lock
**immediately** (e.g. you know a run died and don't want to wait out the TTL), delete the
backing `dataHubStepState` entity.

The lock URN is `urn:li:dataHubStepState:<lock_id>`, where `<lock_id>` is your configured
`locking.lock_id` or the auto-derived default `document-indexing-lock-<ingestion-source-id>`.
The exact URN is logged at startup and on every acquire/release:

```text
Document indexing lock enabled: urn=urn:li:dataHubStepState:document-indexing-lock-datahub-documents, ttl=1800s, ...
```

Delete it with the CLI:

```bash
datahub delete --urn "urn:li:dataHubStepState:<lock_id>" --hard -f
```

Deleting the entity is safe: the next run simply cold-starts a fresh lease.

#### Performance Tuning

##### Batch Size

```yaml
embedding:
  batch_size: 25 # Default
  # Increase for faster processing (if provider supports):
  # - Cohere: Up to 96
  # - Bedrock: Up to 100 (but rate-limited)
```

##### Event Mode Settings

```yaml
event_mode:
  poll_limit: 100  # Fetch up to 100 events per poll
  # Increase for high-volume scenarios:
  poll_limit: 500  # Process more events per batch
```

##### Filtering

```yaml
# Skip short or empty documents
skip_empty_text: true
min_text_length: 50 # Characters

# Process fewer documents
platform_filter: ["notion"] # Only one platform
document_urns: # Specific documents only
  - "urn:li:document:abc123"
```

#### Monitoring and Observability

##### Report Metrics

The source reports the following metrics:

```python
report = {
    "num_documents_fetched": 100,       # Total documents fetched
    "num_documents_processed": 85,      # Successfully processed
    "num_documents_skipped": 15,        # Skipped (various reasons)
    "num_documents_skipped_unchanged": 10,  # Unchanged content
    "num_documents_skipped_empty": 5,   # Empty or too short
    "num_chunks_created": 425,          # Total chunks generated
    "num_embeddings_generated": 425,    # Total embeddings
    "processing_errors": []             # List of errors
}
```

##### Logging

Enable debug logging for detailed insights:

```yaml
# In your ingestion recipe
source:
  type: datahub-documents
  config:
    # ... your config
# Set log level via environment variable
# export DATAHUB_DEBUG=true
```

Look for these log messages:

- `"Loading embedding configuration from DataHub server..."`
- `"✓ Loaded embedding configuration from server"`
- `"Incremental mode enabled, state file: ..."`
- `"Skipping document {urn} (unchanged content hash)"`

#### Cost Estimation

##### AWS Bedrock Pricing (Cohere Embed v3)

As of December 2024 in us-west-2:

- **$0.0001 per 1,000 input tokens** (~750 words)

**Example Costs:**

**One-time Processing:**

- 1,000 documents × 500 tokens each = 500,000 tokens = **$0.05**
- 10,000 documents × 500 tokens each = 5M tokens = **$0.50**
- 100,000 documents × 500 tokens each = 50M tokens = **$5.00**

**Incremental Updates (Event Mode):**

- 100 changed documents/day × 500 tokens = 50,000 tokens/day
- Monthly: 1.5M tokens = **$0.15/month**

**Query Embeddings (GMS):**

- Separate from this source (handled by GMS at search time)
- ~50 tokens per search query
- 10,000 queries = **$0.05**

### Limitations

#### Processing Limitations

- **Text Only:** Only processes `Document.text` field (markdown format expected)
- **No Binary Content:** Images, PDFs, etc. must be converted to text first
- **Markdown Partitioning:** Uses `unstructured.partition.md` which may not handle all markdown variants

#### Platform Filtering

- **Source Type Required:** Documents must have `sourceType` field (defaults to NATIVE if missing)
- **Platform Identification:** Relies on `dataPlatformInstance` or URL-based platform extraction

#### State Management

- **State Size:** State file grows with number of documents (includes hash for each)
- **State Backend:** Requires DataHub or file-based state provider

#### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Issue: "Semantic search is not enabled on the DataHub server"

**Cause:** Server does not have semantic search configured.

**Solution:**

1. Configure semantic search on your DataHub server first
2. See [Semantic Search Configuration Guide](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/how-to/semantic-search-configuration)
3. Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` in server config

#### Issue: "Server does not support semantic search configuration API"

**Cause:** Old DataHub server version (pre-v0.14.0).

**Solutions:**

**Option 1 (Recommended):** Upgrade DataHub server to v0.14.0+

**Option 2:** Provide local embedding config:

```yaml
embedding:
  provider: bedrock
  model: cohere.embed-english-v3
  model_embedding_key: cohere_embed_v3
  aws_region: us-west-2
```

#### Issue: Embedding Configuration Validation Fails

**Error:**

```
Embedding configuration mismatch with server:
- Model: local='cohere.embed-english-v3', server='amazon.titan-embed-text-v1'
```

**Cause:** Local config doesn't match server configuration.

**Solution:**

1. Either remove local embedding config (use server config)
2. Or update server config to match local settings
3. Or update local config to match server

#### Issue: No Documents Being Processed

**Possible Causes:**

1. **Platform Filter Too Restrictive:**

   ```yaml
   # If you have NATIVE documents but filter for external platforms:
   platform_filter: ["notion"]  # Won't process NATIVE documents!

   # Solution: Remove filter or use null
   platform_filter: null
   ```

2. **All Documents Unchanged:**

   - Check incremental mode is working correctly
   - Force reprocess if needed: `incremental.force_reprocess: true`

3. **Documents Have No Text:**
   - Verify documents have content in `Document.text` field
   - Check `min_text_length` threshold

#### Issue: Event Mode Not Working

**Symptoms:** Falls back to batch mode every run.

**Possible Causes:**

1. **Stateful Ingestion Disabled:**

   ```yaml
   stateful_ingestion:
     enabled: true # Must be enabled for event mode
   ```

2. **Kafka Connection Issues:**

   - Check DataHub Kafka is accessible
   - Verify network connectivity
   - Check Kafka broker configuration

3. **State Provider Misconfigured:**
   ```yaml
   stateful_ingestion:
     state_provider:
       type: datahub
       config:
         datahub_api:
           server: "http://correct-host:8080" # Correct URL
   ```

#### Issue: AWS Credentials Error

**Error:**

```
Unable to load credentials from any provider in the chain
```

**Solutions:**

1. **Verify AWS_PROFILE:**

   ```bash
   export AWS_PROFILE=datahub-dev
   cat ~/.aws/credentials  # Check profile exists
   ```

2. **For EC2 Instance Role:**

   ```bash
   # Check instance role is attached
   curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
   ```

3. **For ECS Task Role:**
   - Verify task definition has correct IAM role
   - Check ECS task logs for IAM-related errors

#### Issue: Slow Processing

**Optimization Strategies:**

1. **Increase Batch Size:**

   ```yaml
   embedding:
     batch_size: 50 # Up from default 25
   ```

2. **Use Event Mode:**

   - Only processes changed documents
   - Much faster than batch mode for updates

3. **Filter Documents:**

   ```yaml
   platform_filter: ["notion"] # Process fewer platforms
   min_text_length: 100 # Skip short documents
   ```

4. **Optimize Chunking:**
   ```yaml
   chunking:
     max_characters: 1000 # Larger chunks = fewer embeddings
   ```

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubDocumentsSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/datahub_documents/datahub_documents_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for DataHubDocuments, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
