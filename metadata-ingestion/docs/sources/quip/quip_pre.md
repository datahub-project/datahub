### Overview

:::caution Not Supported with Remote Executor
This source is available as a private beta feature on DataHub Cloud. Note that running the connector using the Remote Executor is not yet supported.
:::

The Quip source ingests threads (documents, spreadsheets, slides, and chats) and folders from a Quip site as DataHub Document entities, with optional semantic embeddings for semantic search.

#### Key Features

##### 1. Content Extraction

- **Thread Content**: Full text extraction from Quip threads, converting the thread HTML to Markdown (tables and slide content included)
- **All Thread Types**: Documents, spreadsheets, slides, and chats are all ingested (filterable via `thread_types`)
- **Folder Discovery**: Recursively crawls folders starting from configured folders, or auto-discovers the authenticated user's folders (private, desktop, shared, group)
- **Metadata Extraction**: Captures creation/modification timestamps, author, thread type, and source IDs as custom properties

##### 2. Hierarchical Relationships

- **Folder Hierarchy**: Each Quip folder is emitted as a Document, and each thread's `parentDocument` points at the folder that contains it
- **Recursive Discovery**: Recursively discovers nested folders up to a configurable depth
- **Flexible Navigation**: Browse the Quip folder structure in the DataHub UI

##### 3. Embedding Generation

Optional semantic search support with sensible defaults:

- **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
- **Automatic chunking**: Documents are automatically chunked for optimal embedding generation
- **Automatic deduplication**: Prevents duplicate chunk embeddings

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed setup and advanced options.

##### 4. Stateful Ingestion

- **Deletion Detection**: Automatically removes stale entities from DataHub when threads/folders disappear
- **Flexible Discovery**: Ingest entire folder trees, specific folders, or individual threads

#### Related Documentation

- [Quip Automation API Reference](https://quip.com/dev/automation/documentation/all)
- [Get a Personal Automation API Access Token](https://quip.com/dev/token)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)

### Prerequisites

#### 1. Quip API Access

Create a Personal Access Token:

1. Sign in to Quip and visit `<base_url>/dev/token` (for example, `https://quip.com/dev/token`).
2. Copy the generated token.

You'll need:

- **Base URL**: The Quip Automation API endpoint. Use `https://platform.quip.com` for quip.com, or your enterprise endpoint (for example `https://platform.quip-amazon.com`).
- **Access Token**: The personal access token you just created.

#### 2. Required Permissions

The token's user must have read access to every folder and thread you want to ingest. The connector only reads data; it never modifies Quip content.

#### 3. Embedding Provider (Optional)

If you want semantic search capabilities, configure an embedding provider in your DataHub instance. Supported providers include Cohere (API key) and AWS Bedrock (IAM roles).

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for detailed provider setup and configuration options.
