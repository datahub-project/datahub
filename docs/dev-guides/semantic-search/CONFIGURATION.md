# Semantic Search Configuration Guide

This guide covers all configuration options for DataHub's semantic search, including embedding models, index settings, and environment variables.

## Enabling Semantic Search

### Environment Variables

Set these in your deployment configuration (e.g., `docker/profiles/empty2.env`):

```bash
# Enable semantic search feature
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true

# Entity types to enable (comma-separated)
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document

# Vector dimensions (must match embedding model)
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=3072
```

### Application Configuration

In `metadata-service/configuration/src/main/resources/application.yaml`, you need to configure:

1. **Semantic search settings** - Enable the feature and specify which entities support it
2. **Index model mappings** - Define the embedding model(s) and their vector dimensions
3. **Embedding provider** - Configure credentials for generating query embeddings

#### Index Model Mappings

The `models:` section defines the structure of the semantic index. **You must add a mapping for each embedding model you plan to use.** The model key (e.g., `text_embedding_3_large`) must match the key used when ingesting document embeddings.

```yaml
elasticsearch:
  entityIndex:
    semanticSearch:
      enabled: ${ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED:false}
      enabledEntities: ${ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES:document}

      # Define index mappings for each embedding model you use
      models:
        # Default: OpenAI text-embedding-3-large
        text_embedding_3_large:
          vectorDimension: 3072
          knnEngine: faiss
          spaceType: cosinesimil
          efConstruction: 128
          m: 16
```

**Common model configurations:**

| Model                         | Key                      | Dimensions |
| ----------------------------- | ------------------------ | ---------- |
| AWS Bedrock Cohere            | `cohere_embed_v3`        | 1024       |
| OpenAI text-embedding-3-small | `text_embedding_3_small` | 1536       |
| OpenAI text-embedding-3-large | `text_embedding_3_large` | 3072       |
| Cohere embed-english-v3.0     | `embed_english_v3_0`     | 1024       |
| Classical hash-v1-2048        | `hash_v1_2048`           | 2048       |

> **Important:** The model key is derived from the configured model ID using explicit mappings for known models, with a fallback that replaces dots, hyphens, and colons with underscores. For example:
>
> - `cohere.embed-english-v3` (AWS Bedrock, via `BEDROCK_EMBEDDING_MODEL`) → `cohere_embed_v3`
> - `embed-english-v3.0` (Cohere direct, via `COHERE_EMBEDDING_MODEL`) → `embed_english_v3_0`
> - `text-embedding-3-small` (OpenAI, via `OPENAI_EMBEDDING_MODEL`) → `text_embedding_3_small`
>
> The explicit mappings ensure AWS Bedrock Cohere models (`cohere.embed-english-v3`) and Cohere direct API models (`embed-english-v3.0`) produce distinct keys despite similar names.

> **Matching with Ingestion:** The model key in the index mapping must match the key used in the `semanticContent` aspect when documents are ingested. The ingestion connector (e.g., `datahub-documents`) uses the configured model to determine this key. For example, if your index has `text_embedding_3_small`, the ingested `semanticContent` aspect must have embeddings under `embeddings.text_embedding_3_small`.

#### Embedding Provider Configuration

Configure the provider used to generate query embeddings at search time:

```yaml
elasticsearch:
  entityIndex:
    semanticSearch:
      # ... models section above ...

      embeddingProvider:
        type: ${EMBEDDING_PROVIDER_TYPE:openai}
        maxCharacterLength: ${EMBEDDING_PROVIDER_MAX_CHAR_LENGTH:2048}
        bedrock:
          awsRegion: ${BEDROCK_EMBEDDING_AWS_REGION:us-west-2}
          model: ${BEDROCK_EMBEDDING_MODEL:cohere.embed-english-v3}

        # OpenAI configuration (used when type is "openai")
        openai:
          apiKey: ${OPENAI_API_KEY:}
          model: ${OPENAI_EMBEDDING_MODEL:text-embedding-3-large}
          endpoint: ${OPENAI_EMBEDDING_ENDPOINT:https://api.openai.com/v1/embeddings}

        # Cohere configuration (used when type is "cohere")
        cohere:
          apiKey: ${COHERE_API_KEY:}
          model: ${COHERE_EMBEDDING_MODEL:embed-english-v3.0}
          endpoint: ${COHERE_EMBEDDING_ENDPOINT:https://api.cohere.ai/v1/embed}
```

## Embedding Models

### Understanding Embedding Providers

There are **two separate embedding contexts** in DataHub's semantic search:

| Context                 | When              | Who Generates       | Configuration             |
| ----------------------- | ----------------- | ------------------- | ------------------------- |
| **Document Embeddings** | At ingestion time | Ingestion Connector | Configured in connector   |
| **Query Embeddings**    | At search time    | GMS                 | Configured in GMS (below) |

> **Important:** The GMS embedding provider (configured below) is used **only for query embedding**.
> Document embeddings are generated by the ingestion connector using its own embedding configuration.
> Both must use the **same embedding model** for semantic search to work correctly.

### Ingestion via MCP (Metadata Change Proposal)

Document embeddings are sent to DataHub via MCP using the `SemanticContent` aspect. This is the standard DataHub pattern for ingesting metadata.

**MCP Payload Structure:**

```json
{
  "entityType": "document",
  "entityUrn": "urn:li:document:my-doc-123",
  "aspectName": "semanticContent",
  "aspect": {
    "value": "{...JSON encoded SemanticContent...}",
    "contentType": "application/json"
  }
}
```

**SemanticContent Aspect:**

```json
{
  "embeddings": {
    "text_embedding_3_large": {
      "modelVersion": "openai/text-embedding-3-large",
      "generatedAt": 1702234567890,
      "chunkingStrategy": "sentence_boundary_400t",
      "totalChunks": 2,
      "totalTokens": 450,
      "chunks": [
        {
          "position": 0,
          "vector": [0.123, -0.456, ...],
          "characterOffset": 0,
          "characterLength": 512,
          "tokenCount": 230,
          "text": "First chunk of text..."
        }
      ]
    }
  }
}
```

**Privacy Option:** The `text` field in each chunk is optional. For sensitive data sources, you can omit the source text and send only the embedding vectors.

### GMS Query Embedding Provider (Java)

GMS uses an `EmbeddingProvider` implementation to generate query embeddings at search time.

**Interface:** `com.linkedin.metadata.search.embedding.EmbeddingProvider`

```java
public interface EmbeddingProvider {
  /**
   * Returns an embedding vector for the given text.
   * @param text The text to embed
   * @param model The model identifier (nullable, uses default if null)
   * @return The embedding vector
   */
  @Nonnull
  float[] embed(@Nonnull String text, @Nullable String model);
}
```

**Built-in Implementations:**

- `OpenAiEmbeddingProvider` - Uses OpenAI API (default)
- `AwsBedrockEmbeddingProvider` - Uses AWS Bedrock
- `AiGatewayEmbeddingProvider` - Uses an OAuth2-authenticated AI Gateway
- `ClassicalEmbeddingProvider` - Deterministic in-process lexical hashing (no external service); CI and smoke tests only, gated by `CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true`
- `NoOpEmbeddingProvider` - Throws exception if called (used when semantic search disabled)

The following providers can be configured:

#### AWS Bedrock

AWS Bedrock provides managed access to embedding models:

```bash
# Environment
EMBED_PROVIDER=bedrock
BEDROCK_EMBEDDING_MODEL=cohere.embed-english-v3
AWS_REGION=us-west-2

# For local development
AWS_PROFILE=your-profile

# For production (IAM role or explicit credentials)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

**Available Bedrock Models:**

| Model ID                       | Dimensions   | Max Tokens | Notes                   |
| ------------------------------ | ------------ | ---------- | ----------------------- |
| `cohere.embed-english-v3`      | 1024         | 512        | Best for English        |
| `cohere.embed-multilingual-v3` | 1024         | 512        | Multi-language support  |
| `amazon.titan-embed-text-v1`   | 1536         | 8192       | Amazon's model          |
| `amazon.titan-embed-text-v2:0` | 256/512/1024 | 8192       | Configurable dimensions |

#### OpenAI (Default)

OpenAI provides high-quality embedding models with simple API access:

```bash
# Required
EMBEDDING_PROVIDER_TYPE=openai
OPENAI_API_KEY=sk-your-api-key-here

# Optional - defaults shown
# OPENAI_EMBEDDING_MODEL: Model used to generate query embeddings via OpenAI API
OPENAI_EMBEDDING_MODEL=text-embedding-3-large
```

**Available OpenAI Models:**

| Model ID                 | Dimensions | Max Tokens | Notes                |
| ------------------------ | ---------- | ---------- | -------------------- |
| `text-embedding-3-small` | 1536       | 8191       | Fast, cost-effective |
| `text-embedding-3-large` | 3072       | 8191       | Higher quality       |

#### Cohere (Direct API)

Use Cohere's embedding API directly (without AWS Bedrock):

```bash
# Required
EMBEDDING_PROVIDER_TYPE=cohere
COHERE_API_KEY=your-cohere-api-key

# Optional - defaults shown
# COHERE_EMBEDDING_MODEL: Model used to generate query embeddings via Cohere API
COHERE_EMBEDDING_MODEL=embed-english-v3.0
```

**Available Cohere Models:**

| Model ID                  | Dimensions | Notes             |
| ------------------------- | ---------- | ----------------- |
| `embed-english-v3.0`      | 1024       | English optimized |
| `embed-multilingual-v3.0` | 1024       | 100+ languages    |

#### OAuth2-Authenticated AI Gateway

If your organization fronts Bedrock, Azure OpenAI, or Google Vertex AI behind a proxy gateway authenticated with an OAuth2 `client_credentials` JWT, use the `ai-gateway` provider. A single HTTP contract supports multiple upstream platforms via routing parameters.

```bash
# Required
EMBEDDING_PROVIDER_TYPE=ai-gateway
AI_GATEWAY_BASE_URL=https://gateway.example.com
AI_GATEWAY_PLATFORM=google-vertex # or azure-openai, amazon-bedrock
AI_GATEWAY_TOKEN_URL=https://auth.example.com/oauth2/token
AI_GATEWAY_CLIENT_ID=your-client-id
AI_GATEWAY_CLIENT_SECRET=your-client-secret

# Optional - defaults shown
AI_GATEWAY_EMBEDDING_MODEL=gemini-embedding-001
# Set to >0 to pass options.dimensions to the gateway; 0 uses model native dims
AI_GATEWAY_EMBEDDING_DIMENSIONS=0
```

#### Classical (Deterministic Hashing, CI and smoke tests only)

Compute deterministic lexical vectors in-process with no external service. This is a testing provider, not semantic search:

```bash
# Required
EMBEDDING_PROVIDER_TYPE=classical
# Required opt-in on every process that builds the provider (GMS, system-update, the MAE
# consumer): each refuses to start without it and logs a warning while it is active.
CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true

# Optional - default shown. Format hash-v1-<dims>; the width must match the
# semanticSearch.models entry (hash_v1_2048) and changing it requires a re-index.
CLASSICAL_EMBEDDING_MODEL=hash-v1-2048
```

The classical provider hashes word and character n-gram features with SHA-256 into a fixed-width vector (2048 dimensions by default). It needs no API key, endpoint, or model download, and the Python ingestion provider implements the identical algorithm, so document and query vectors are bit-identical. Ranking is lexical (shared words and character n-grams), not semantic: it does not match synonyms or paraphrases. It exists so CI, smoke tests and quickstarts can exercise the full embedding and kNN pipeline with zero external dependencies; a deployment that needs semantic quality without a cloud dependency should use the in-process `onnx` provider.

`EMBEDDING_PROVIDER_MAX_CHAR_LENGTH` does not apply to this provider: instead of truncating, it rejects any input over 16,384 code points, on both the query and the ingestion side. Keep the ingestion recipe's `chunking.max_characters` (default 500) below that limit, or those chunks fail to embed on every run.

#### Switching Between Providers

When switching embedding providers, you must delete and recreate the semantic index because different models produce vectors with different dimensions.

See **[SWITCHING_PROVIDERS.md](./SWITCHING_PROVIDERS.md)** for detailed step-by-step instructions.

#### Future Providers

Potential future built-in providers:

- Azure OpenAI
- Google Vertex AI

#### Custom/Self-Hosted Providers

To add a custom embedding provider, implement the `EmbeddingProvider` interface and register it in the factory.

**1. Implement the interface** (`metadata-io/src/main/java/com/linkedin/metadata/search/embedding/`):

```java
package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CustomEmbeddingProvider implements EmbeddingProvider {

  private final String endpoint;
  private final int dimensions;

  public CustomEmbeddingProvider(String endpoint, int dimensions) {
    this.endpoint = endpoint;
    this.dimensions = dimensions;
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    // Call your embedding service
    // Return float array with `dimensions` elements
    return callEmbeddingService(endpoint, text, model);
  }
}
```

**2. Register in the factory** (`metadata-service/factories/.../EmbeddingProviderFactory.java`):

```java
@Bean(name = "embeddingProvider")
@Nonnull
protected EmbeddingProvider getInstance() {
  // ... existing code ...

  String providerType = config.getType();

  if ("aws-bedrock".equalsIgnoreCase(providerType)) {
    return new AwsBedrockEmbeddingProvider(...);
  } else if ("custom".equalsIgnoreCase(providerType)) {
    return new CustomEmbeddingProvider(
        config.getCustomEndpoint(),
        config.getCustomDimensions()
    );
  } else {
    throw new IllegalStateException("Unsupported provider: " + providerType);
  }
}
```

**3. Add configuration** (`EmbeddingProviderConfiguration.java`):

```java
@Data
public class EmbeddingProviderConfiguration {
  private String type = "aws-bedrock";
  private String awsRegion = "us-west-2";
  private String modelId = "cohere.embed-english-v3";

  // Add custom provider fields
  private String customEndpoint;
  private int customDimensions = 1024;
}
```

**4. Configure in application.yaml**:

```yaml
elasticsearch:
  search:
    semanticSearch:
      embeddingProvider:
        type: custom
        customEndpoint: http://your-embedding-service:8080/embed
        customDimensions: 1024
```

## Index Configuration

### Adding a Model to the Index

Each embedding model you use must have a corresponding entry in the `models:` section of application.yaml. The index mapping is created when GMS starts, so you must configure this **before** ingesting documents.

**Example: Adding OpenAI text-embedding-3-small:**

```yaml
models:
  text_embedding_3_small: # Key derived from model name (dots/hyphens → underscores)
    vectorDimension: 1536 # Must match the model's output dimensions
    knnEngine: faiss
    spaceType: cosinesimil
    efConstruction: 128
    m: 16
```

### k-NN Settings

The semantic index uses OpenSearch's k-NN plugin. Key parameters:

```yaml
models:
  your_model_key:
    # Vector size (must match model output)
    vectorDimension: 1024

    # k-NN engine: faiss (recommended) or nmslib
    knnEngine: faiss

    # Similarity metric
    # - cosinesimil: Cosine similarity (recommended for text)
    # - l2: Euclidean distance
    # - innerproduct: Dot product
    spaceType: cosinesimil

    # HNSW graph parameters
    efConstruction: 128 # Build-time accuracy (32-512)
    m: 16 # Connections per node (4-64)
```

### Parameter Tuning

| Parameter        | Low | Medium | High | Trade-off                       |
| ---------------- | --- | ------ | ---- | ------------------------------- |
| `efConstruction` | 32  | 128    | 512  | Speed vs accuracy at build time |
| `m`              | 4   | 16     | 64   | Memory vs accuracy              |

**Recommendations:**

- **Development**: `efConstruction: 64, m: 8`
- **Production**: `efConstruction: 128, m: 16`
- **High Accuracy**: `efConstruction: 256, m: 32`

### Multiple Models (Advanced)

You can configure multiple embedding models in the index for migration scenarios or A/B testing. This allows you to have documents with embeddings from different models coexisting in the same index.

> **Note:** Most deployments only need a single model. Only add multiple models if you're migrating between providers or testing different models.

```yaml
models:
  # Current production model (OpenAI default)
  text_embedding_3_large:
    vectorDimension: 3072
    knnEngine: faiss
    spaceType: cosinesimil
    efConstruction: 128
    m: 16

  # Alternative model being tested
  cohere_embed_v3:
    vectorDimension: 1024
    knnEngine: faiss
    spaceType: cosinesimil
    efConstruction: 128
    m: 16
```

When using multiple models, the configured provider model (`BEDROCK_EMBEDDING_MODEL`, `OPENAI_EMBEDDING_MODEL`, `COHERE_EMBEDDING_MODEL`, or `AI_GATEWAY_EMBEDDING_MODEL`) determines which model is used for query embeddings at search time.

## Query Configuration

### GMS Query Settings

GMS needs credentials to generate query embeddings at search time:

```bash
# Mount AWS credentials in Docker
volumes:
  - ${HOME}/.aws:/home/datahub/.aws:ro

# Set profile
AWS_PROFILE=your-profile
```

### Query Parameters

In GraphQL queries:

```graphql
query SemanticSearch($input: SearchAcrossEntitiesInput!) {
  semanticSearchAcrossEntities(input: $input) {
    # ...
  }
}
```

Variables:

```json
{
  "input": {
    "query": "your natural language query",
    "types": ["DOCUMENT"], // Entity types to search
    "start": 0, // Pagination start
    "count": 10 // Results per page
  }
}
```

## Chunking Configuration

Chunking is handled by the **ingestion connector** when generating document embeddings. The typical strategy:

- **Chunk size**: ~400 tokens per chunk
- **Boundary**: Split at sentence boundaries when possible
- **Overlap**: Optional overlap between chunks for context continuity

The chunking parameters are configured in the ingestion connector, not in GMS. GMS receives pre-chunked embeddings from the connector.

## Monitoring

### Useful Queries

**Check embedding coverage:**

```bash
curl "http://localhost:9200/documentindex_v2_semantic/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "aggs": {
      "with_embeddings": {
        "filter": { "exists": { "field": "embeddings.text_embedding_3_large" } }
      },
      "without_embeddings": {
        "filter": { "bool": { "must_not": { "exists": { "field": "embeddings.text_embedding_3_large" } } } }
      }
    }
  }'
```

**Check index health:**

```bash
curl "http://localhost:9200/_cat/indices/*semantic*?v"
```

## Troubleshooting

### Common Issues

| Issue                                   | Cause                   | Solution                                                        |
| --------------------------------------- | ----------------------- | --------------------------------------------------------------- |
| "Unable to locate credentials"          | AWS creds not available | Mount `.aws` to `/home/datahub/.aws`                            |
| "Profile file contained no credentials" | SSO session expired     | Run `aws sso login --profile your-profile`                      |
| Empty search results                    | No embeddings in index  | Verify ingestion connector is generating embeddings             |
| Wrong results                           | Model mismatch          | Ensure ingestion connector and GMS use the same embedding model |

### Debug Logging

Enable debug logging in GMS:

```yaml
logging:
  level:
    com.linkedin.metadata.search: DEBUG
```

Check logs for:

```
[DEBUG-DUALWRITE] shouldWriteToSemanticIndex returned: true
Semantic dual-write enabled=true, enabledEntities=[document]
```
