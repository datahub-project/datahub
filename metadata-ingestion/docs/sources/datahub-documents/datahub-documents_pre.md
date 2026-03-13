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

- [Semantic Search Configuration](/docs/how-to/semantic-search-configuration.md) - **Start Here**
- [Notion Source](/docs/generated/ingestion/sources/notion) - Example document source with embeddings
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html) - Embedding models
- [AWS Bedrock Pricing](https://aws.amazon.com/bedrock/pricing/) - Cost estimation
- [DataHub Developer Guides](https://docs.datahub.com/docs/developers) - Additional developer documentation

### Prerequisites

#### 1. DataHub Server Configuration

**You MUST configure semantic search on your DataHub server before using this source.**

See the [Semantic Search Configuration Guide](/docs/how-to/semantic-search-configuration.md) for complete setup instructions.

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
