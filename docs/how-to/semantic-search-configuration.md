# Semantic Search Configuration

DataHub's semantic search uses vector embeddings to find entities using natural language queries like "customer churn analysis" — even when exact keywords differ. This guide shows you how to enable it.

## Prerequisites

### OpenSearch

- **OpenSearch 2.17.0 or higher** with k-NN plugin enabled (DataHub ships with `opensearchproject/opensearch:2.19.3`)
- **Note:** Elasticsearch is not supported for semantic search — OpenSearch's k-NN plugin is required

Verify k-NN plugin is enabled:

```bash
curl -X GET "localhost:9200/_cat/plugins?v&s=component&h=name,component,version"
```

You should see `opensearch-knn` in the output.

### Embedding Provider Credentials

| Provider             | What You Need                                                          |
| -------------------- | ---------------------------------------------------------------------- |
| **OpenAI** (default) | API key (`sk-...`) from [OpenAI](https://platform.openai.com/api-keys) |
| **AWS Bedrock**      | AWS credentials with Bedrock access                                    |
| **Cohere**           | API key from [Cohere](https://dashboard.cohere.com/api-keys)           |

## How to Configure Semantic Search

### DataHub Helm Charts (Recommended)

If you deploy DataHub on Kubernetes using the [DataHub Helm chart](https://github.com/acryldata/datahub-helm), configure semantic search in your `values.yaml`.

#### OpenAI (Default)

1. Create a Kubernetes secret with your API key:

```bash
kubectl create secret generic openai-secret --from-literal=api-key=sk-your-api-key-here
```

2. Add to your `values.yaml`:

```yaml
global:
  datahub:
    semantic_search:
      enabled: true
      vectorDimension: 3072
      provider:
        type: "openai"
        openai:
          apiKey:
            secretRef: "openai-secret"
            secretKey: "api-key"
          model: "text-embedding-3-large"
```

3. Upgrade the release:

```bash
helm upgrade datahub datahub/datahub -f values.yaml
```

#### AWS Bedrock

1. Add to your `values.yaml`:

```yaml
global:
  datahub:
    semantic_search:
      enabled: true
      vectorDimension: 1024
      provider:
        type: "aws-bedrock"
        bedrock:
          modelId: "cohere.embed-english-v3"
          awsRegion: "us-west-2"
```

> **Note:** Bedrock authenticates using the [AWS SDK default credential chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) — typically via [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html) on EKS, or EC2/ECS instance credentials.

2. Upgrade the release:

```bash
helm upgrade datahub datahub/datahub -f values.yaml
```

#### Cohere

1. Create a Kubernetes secret with your API key:

```bash
kubectl create secret generic cohere-secret --from-literal=api-key=your-cohere-api-key
```

2. Add to your `values.yaml`:

```yaml
global:
  datahub:
    semantic_search:
      enabled: true
      vectorDimension: 1024
      provider:
        type: "cohere"
        cohere:
          apiKey:
            secretRef: "cohere-secret"
            secretKey: "api-key"
          model: "embed-english-v3.0"
```

3. Upgrade the release:

```bash
helm upgrade datahub datahub/datahub -f values.yaml
```

### Environment Variables

For Docker Compose or non-Helm deployments, set these environment variables on the GMS service.

#### OpenAI (Default)

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
OPENAI_API_KEY=sk-your-api-key-here

# Optional — these are already the defaults:
# EMBEDDING_PROVIDER_TYPE=openai
# OPENAI_EMBEDDING_MODEL=text-embedding-3-large
# ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=3072
```

#### AWS Bedrock

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
EMBEDDING_PROVIDER_TYPE=aws-bedrock
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1024
EMBEDDING_PROVIDER_AWS_REGION=us-west-2

# Optional — default shown:
# EMBEDDING_PROVIDER_MODEL_ID=cohere.embed-english-v3

# Authentication uses the AWS SDK default credential chain:
# - EC2/ECS instance credentials (recommended for production)
# - AWS_PROFILE env var (for local development)
# - AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars (explicit keys)
```

#### Cohere

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
EMBEDDING_PROVIDER_TYPE=cohere
COHERE_API_KEY=your-cohere-api-key
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1024

# Optional — default shown:
# COHERE_EMBEDDING_MODEL=embed-english-v3.0
```

### Restart and Verify

After configuring, restart DataHub:

```bash
# Docker Compose
docker-compose restart datahub-gms

# Kubernetes (non-Helm)
kubectl rollout restart deployment datahub-gms
```

Check the logs for successful initialization:

```bash
# Docker Compose
docker-compose logs datahub-gms | grep -i "semantic\|embedding"

# Kubernetes
kubectl logs deployment/datahub-gms | grep -i "semantic\|embedding"
```

Expected output:

```
Creating embedding provider with type: openai
Initialized OpenAiEmbeddingProvider with model=text-embedding-3-large
```

## Generating Embeddings

Embeddings are generated by dedicated ingestion sources that connect to specific systems where your documents live (e.g., Notion, Confluence, SharePoint). Each source is responsible for:

1. Extracting document content from the source system
2. Chunking the text into manageable segments
3. Generating embeddings for each chunk using the configured provider
4. Emitting the embeddings to DataHub as `SemanticContent` aspects

### DataHub Documents Source

DataHub provides a native ingestion source for generating semantic embeddings for **DataHub's native Document entities**. This source processes documents that are already stored in DataHub (created via GraphQL, Python SDK, or other ingestion sources) and enriches them with embeddings.

**Key features:**

- Processes Document entities from DataHub
- Supports both batch mode (GraphQL) and event-driven mode (Kafka MCL)
- Incremental processing - only reprocesses documents when content changes
- Stateful ingestion for tracking progress
- Multiple chunking strategies (by_title, basic)

### Example Recipe

The DataHub Documents Source uses smart defaults to minimize configuration. Here's the minimal recipe:

```yaml
source:
  type: datahub-documents
  config: {}

sink:
  type: datahub-rest
  config: {}
```

This minimal configuration automatically:

- Connects to DataHub using `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` environment variables (defaults to `http://localhost:8080` if not set)
- Enables **event-driven mode** (processes documents in real-time from Kafka MCL)
- Enables **incremental processing** (only reprocesses documents when content changes)
- Uses **by_title chunking** with sensible defaults (500 characters, smart combining)
- **Fetches embedding configuration from the server** (matches your GMS semantic search config automatically)

#### Customization Options

If you need to override defaults, you can specify them explicitly:

```yaml
source:
  type: datahub-documents
  config:
    # DataHub connection (optional - defaults to env vars)
    datahub:
      server: "http://datahub-gms:8080"
      token: "${DATAHUB_TOKEN}"

    # Platform filtering (optional - defaults to all documents)
    platform_filter: ["notion", "confluence"]

    # Event mode (optional - enabled by default)
    event_mode:
      enabled: true
      idle_timeout_seconds: 60

    # Incremental processing (optional - enabled by default)
    incremental:
      enabled: true

    # Chunking strategy (optional - has sensible defaults)
    chunking:
      strategy: by_title
      max_characters: 500

    # Embedding (optional - fetches from server by default)
    embedding:
      # Leave empty to auto-fetch from server
      batch_size: 50 # Can override processing options

sink:
  type: datahub-rest
  config: {}
```

**Running the source:**

```bash
# Set environment variables
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your-token"

# Run ingestion with minimal recipe
datahub ingest -c recipe.yml

# Or inline for one-time use
DATAHUB_GMS_TOKEN=your-token datahub ingest -c recipe.yml
```

For detailed configuration options and advanced features (event-driven mode, platform filtering, chunking strategies), see the [DataHub Documents Source documentation](../generated/ingestion/sources/datahub-documents.md).

### External Sources

For documents from external systems like Notion or Confluence, use the respective ingestion sources that support semantic search:

- **Notion**: [Notion Source](../generated/ingestion/sources/notion.md) - Ingest pages, databases, and hierarchical content with embeddings
- **Confluence**: Coming soon
- **SharePoint**: Coming soon

Each external source handles fetching, chunking, and embedding generation specific to that platform's document format.

#### Notion Example

The Notion source automatically fetches embedding configuration from your DataHub server, so you only need to specify your Notion credentials:

```yaml
source:
  type: notion
  config:
    api_key: "${NOTION_API_KEY}"
    page_ids:
      - "your-page-id-here"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

For complete configuration options including custom embedding providers and chunking strategies, see the [Notion Source documentation](../generated/ingestion/sources/notion.md).

## Usage

### GraphQL API

#### Single Entity Type Search

```graphql
query {
  semanticSearch(
    input: {
      type: DATASET
      query: "customer churn prediction models"
      start: 0
      count: 10
    }
  ) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          description
        }
      }
    }
  }
}
```

#### Multi-Entity Search

```graphql
query {
  semanticSearchAcrossEntities(
    input: {
      types: [DATASET, DASHBOARD, CHART]
      query: "revenue analysis last quarter"
      start: 0
      count: 10
    }
  ) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}
```

### Python SDK (Coming Soon)

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter("http://localhost:8080")

# Semantic search
results = emitter.semantic_search(
    query="customer data pipeline",
    types=["dataset"],
    start=0,
    count=10
)
```

## Advanced Configuration

### Supported Models

| Provider    | Model                     | Dimensions | Notes                   |
| ----------- | ------------------------- | ---------- | ----------------------- |
| OpenAI      | `text-embedding-3-large`  | 3072       | Default, higher quality |
| OpenAI      | `text-embedding-3-small`  | 1536       | Fast, cost-effective    |
| AWS Bedrock | `cohere.embed-english-v3` | 1024       | AWS-managed             |
| Cohere      | `embed-english-v3.0`      | 1024       | English optimized       |
| Cohere      | `embed-multilingual-v3.0` | 1024       | 100+ languages          |

### application.yaml Reference

For fine-grained control, you can edit `application.yaml` directly:

```yaml
elasticsearch:
  index:
    semanticSearch:
      enabled: true
      enabledEntities: document # Comma-separated: dataset,dashboard,chart
      models:
        text_embedding_3_large:
          vectorDimension: 3072
          knnEngine: faiss
          spaceType: cosinesimil
          efConstruction: 128
          m: 16
      embeddingProvider:
        type: openai
        openai:
          apiKey: sk-your-api-key-here
          model: text-embedding-3-large
```

### Performance Tuning

For better search quality, tune the k-NN parameters in `application.yaml`:

```yaml
semanticSearch:
  models:
    text_embedding_3_large:
      efConstruction: 128 # Higher = better recall, slower indexing (default: 128)
      m: 16 # Higher = better recall, more memory (default: 16)
      spaceType: cosinesimil # cosinesimil, l2, innerproduct
      knnEngine: faiss # faiss, nmslib, lucene
```

**Recommendations:**

- **Small datasets (<10K docs)**: `efConstruction: 128, m: 16`
- **Medium datasets (10K-100K docs)**: `efConstruction: 256, m: 32`
- **Large datasets (>100K docs)**: `efConstruction: 512, m: 48`

### Cost Estimation

Check current pricing at [OpenAI pricing](https://openai.com/pricing).

### Security Best Practices

1. **Use secrets management**: Store API keys in Kubernetes secrets or vault, not in plain config files
2. **Principle of Least Privilege**: Grant only required permissions
3. **Rotate Credentials**: Rotate API keys regularly
4. **Monitor Usage**: Track API usage and costs through provider dashboards

## Troubleshooting

### Issue: "Semantic search is disabled or not configured"

**Solution**: Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` and restart GMS.

### Issue: API Key Error

```
Invalid API key provided
```

**Solution**: Verify your API key is set correctly in the environment where GMS runs.

### Issue: k-NN Index Creation Failed

```
Codec [zstd_no_dict] cannot be used with k-NN indices
```

**Solution**: This is a known issue with older DataHub versions. The semantic search port includes a fix. Ensure you have the latest code.

### Issue: Vector Dimension Mismatch

```
Dimension mismatch: expected 3072, got 1024
```

**Solution**: Your model's dimensions don't match the configuration. Update `ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION` to match your model (see [Supported Models](#supported-models) for dimensions).

## References

- [OpenAI Embeddings Guide](https://platform.openai.com/docs/guides/embeddings)
- [DataHub Helm Chart](https://github.com/acryldata/datahub-helm) — Kubernetes deployment
- [Switching Providers](../dev-guides/semantic-search/SWITCHING_PROVIDERS.md) — migrating between providers
- [Configuration Guide](../dev-guides/semantic-search/CONFIGURATION.md) — detailed configuration reference
- [OpenSearch k-NN Plugin](https://opensearch.org/docs/latest/search-plugins/knn/index/)
