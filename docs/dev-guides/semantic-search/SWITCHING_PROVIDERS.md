# Switching Embedding Providers

This guide explains how to switch between different embedding providers for semantic search, including the required steps to migrate your semantic index.

## Supported Providers

DataHub supports the following embedding providers out of the box:

> **Note:** OpenAI and Cohere (direct API) providers were added as example implementations to demonstrate how to integrate additional embedding providers. AWS Bedrock remains the recommended provider for production deployments.

| Provider    | Type Value | Models                                         | Use Case                           |
| ----------- | ---------- | ---------------------------------------------- | ---------------------------------- |
| AWS Bedrock | `bedrock`  | Cohere, Titan                                  | Production with AWS infrastructure |
| OpenAI      | `openai`   | text-embedding-3-small, text-embedding-3-large | General purpose, easy setup        |
| Cohere      | `cohere`   | embed-english-v3.0, embed-multilingual-v3.0    | Direct Cohere API access           |

## Configuration by Provider

### OpenAI

```bash
# Required
EMBEDDING_PROVIDER_TYPE=openai
OPENAI_API_KEY=sk-your-api-key-here

# Optional - defaults shown
# OPENAI_EMBEDDING_MODEL: Model used to generate query embeddings via OpenAI API
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
# EMBEDDING_PROVIDER_MODEL_ID: Model key used to look up embeddings in the semantic index
EMBEDDING_PROVIDER_MODEL_ID=text-embedding-3-small
```

> **Important:** Both values should match to ensure query embeddings are compared against the correct document embeddings in the index.

**Available Models:**

| Model                    | Dimensions | Max Tokens | Notes                |
| ------------------------ | ---------- | ---------- | -------------------- |
| `text-embedding-3-small` | 1536       | 8191       | Fast, cost-effective |
| `text-embedding-3-large` | 3072       | 8191       | Higher quality       |

### Cohere (Direct API)

```bash
# Required
EMBEDDING_PROVIDER_TYPE=cohere
COHERE_API_KEY=your-cohere-api-key

# Optional - defaults shown
# COHERE_EMBEDDING_MODEL: Model used to generate query embeddings via Cohere API
COHERE_EMBEDDING_MODEL=embed-english-v3.0
# EMBEDDING_PROVIDER_MODEL_ID: Model key used to look up embeddings in the semantic index
EMBEDDING_PROVIDER_MODEL_ID=embed-english-v3.0
```

> **Important:** Both values should match to ensure query embeddings are compared against the correct document embeddings in the index.

**Available Models:**

| Model                     | Dimensions | Notes             |
| ------------------------- | ---------- | ----------------- |
| `embed-english-v3.0`      | 1024       | English optimized |
| `embed-multilingual-v3.0` | 1024       | 100+ languages    |

### AWS Bedrock

```bash
# Required
EMBEDDING_PROVIDER_TYPE=bedrock
AWS_REGION=us-west-2

# Authentication (choose one)
AWS_PROFILE=your-profile          # For local development
# OR
AWS_ACCESS_KEY_ID=...             # For production
AWS_SECRET_ACCESS_KEY=...

# Optional - defaults shown
BEDROCK_EMBEDDING_MODEL=cohere.embed-english-v3
EMBEDDING_PROVIDER_MODEL_ID=cohere.embed-english-v3
```

## Switching Providers: Step-by-Step

When switching embedding providers, you **must** delete and recreate the semantic index because different models produce vectors with different dimensions that are incompatible.

### Step 1: Stop DataHub Services

Stop GMS and any ingestion jobs to prevent writes during migration:

```bash
# If using docker-compose
docker stop datahub-gms

# If using Kubernetes
kubectl scale deployment datahub-gms --replicas=0
```

### Step 2: Delete the Semantic Index

Delete the existing semantic index from OpenSearch/Elasticsearch:

```bash
# Check existing semantic indices
curl -s "http://localhost:9200/_cat/indices/*semantic*?v"

# Delete the semantic index (adjust index name as needed)
curl -X DELETE "http://localhost:9200/documentindex_v2_semantic"
```

### Step 3: Update Environment Configuration

Update your environment configuration with the new provider settings.

**For Docker deployments**, add to your environment file or docker-compose override:

```bash
# Example: Switching to OpenAI
EMBEDDING_PROVIDER_TYPE=openai
OPENAI_API_KEY=sk-your-api-key
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_PROVIDER_MODEL_ID=text-embedding-3-small
```

**For Kubernetes**, update your ConfigMap or secrets:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: datahub-embedding-provider
type: Opaque
stringData:
  EMBEDDING_PROVIDER_TYPE: "openai"
  OPENAI_API_KEY: "sk-your-api-key"
  OPENAI_EMBEDDING_MODEL: "text-embedding-3-small"
  EMBEDDING_PROVIDER_MODEL_ID: "text-embedding-3-small"
```

### Step 4: Update Index Configuration

Update `application.yaml` to include **only** the model you're switching to. Having only the model you use is recommended for cleaner index mappings.

```yaml
elasticsearch:
  entityIndex:
    semanticSearch:
      models:
        # Only include the model you're using
        # Use the model key that matches your EMBEDDING_PROVIDER_MODEL_ID
        text_embedding_3_small:
          vectorDimension: 1536 # Must match model output
          knnEngine: faiss
          spaceType: cosinesimil
          efConstruction: 128
          m: 16
```

**Model keys for each provider:**

| Provider    | Model ID                  | Model Key                | Dimensions |
| ----------- | ------------------------- | ------------------------ | ---------- |
| AWS Bedrock | `cohere.embed-english-v3` | `cohere_embed_v3`        | 1024       |
| OpenAI      | `text-embedding-3-small`  | `text_embedding_3_small` | 1536       |
| OpenAI      | `text-embedding-3-large`  | `text_embedding_3_large` | 3072       |
| Cohere      | `embed-english-v3.0`      | `embed_english_v3_0`     | 1024       |

Or via environment variable:

```bash
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1536
```

### Step 5: Start DataHub and Run System Update

Start GMS - the system update job will automatically recreate the semantic index:

```bash
# Docker
docker start datahub-gms

# Kubernetes
kubectl scale deployment datahub-gms --replicas=1
```

The system update job runs automatically on startup and will:

1. Detect the missing semantic index
2. Create it with the correct mapping for your embedding model
3. Log progress to the GMS logs

### Step 6: Re-ingest Documents

After the index is recreated, you need to re-ingest your documents to generate new embeddings with the new provider:

```bash
# Run your ingestion pipeline
datahub ingest -c your-recipe.yaml
```

**Important:** Make sure your ingestion recipe also uses the same embedding model. The ingestion connector generates document embeddings, while GMS generates query embeddings - both must use the same model.

### Step 7: Verify

Verify the new setup is working:

```bash
# Check the index exists with correct mapping
curl -s "http://localhost:9200/documentindex_v2_semantic/_mapping?pretty" | head -50

# Check documents have embeddings
curl -s "http://localhost:9200/documentindex_v2_semantic/_search" \
  -H "Content-Type: application/json" \
  -d '{"size": 1, "_source": ["urn", "embeddings"]}' | head -30

# Test semantic search via GraphQL or the UI
```

## Troubleshooting

### "No embeddings found" after switching

**Cause:** Documents were ingested before the provider switch and have embeddings from the old model.

**Solution:** Re-run ingestion to generate new embeddings with the new provider.

### "Dimension mismatch" errors

**Cause:** The index was created with a different vector dimension than the new model produces.

**Solution:** Delete the semantic index and let it be recreated (Steps 2-5 above).

### "Invalid API key" errors

**Cause:** API key not set or incorrect.

**Solution:** Verify your API key is correctly set in the environment:

```bash
# Check the environment variable is set (in the container)
docker exec datahub-gms env | grep -E 'OPENAI_API_KEY|COHERE_API_KEY'
```

### Query returns no results but documents exist

**Cause:** Model mismatch between ingestion and query time.

**Solution:** Ensure both the ingestion connector AND GMS use the same embedding model. Check:

- `EMBEDDING_PROVIDER_MODEL_ID` in GMS config
- Embedding model in your ingestion recipe

## Best Practices

1. **Use the same model everywhere**: Ensure ingestion connectors and GMS use identical embedding models.

2. **Test in development first**: Switch providers in a dev environment before production.

3. **Plan for re-ingestion**: Switching providers requires re-generating all embeddings, which can take time for large datasets.

4. **Monitor costs**: Different providers have different pricing. OpenAI and Cohere charge per token/request.

5. **Keep backups**: Before deleting indices, consider backing up if you might need to rollback.
