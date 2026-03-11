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
