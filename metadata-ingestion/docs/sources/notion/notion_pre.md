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
