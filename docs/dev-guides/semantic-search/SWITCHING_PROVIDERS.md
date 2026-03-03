# Switching Embedding Providers

This guide explains how to migrate from one embedding provider to another. Switching providers requires deleting the semantic index and re-ingesting all documents because different models produce vectors with incompatible dimensions.

> For initial setup of semantic search (including all provider configurations), see [Semantic Search Configuration](../../how-to/semantic-search-configuration.md).

## When You Need This Guide

- Switching from OpenAI to AWS Bedrock (or vice versa)
- Switching from one model to another with different vector dimensions
- Changing from Cohere direct API to AWS Bedrock-managed Cohere

## Provider and Model Reference

| Provider    | Model                     | Model Key                | Dimensions |
| ----------- | ------------------------- | ------------------------ | ---------- |
| OpenAI      | `text-embedding-3-large`  | `text_embedding_3_large` | 3072       |
| OpenAI      | `text-embedding-3-small`  | `text_embedding_3_small` | 1536       |
| AWS Bedrock | `cohere.embed-english-v3` | `cohere_embed_v3`        | 1024       |
| Cohere      | `embed-english-v3.0`      | `embed_english_v3_0`     | 1024       |

> **Important:** The model key is derived from the model name by replacing `-` and `.` with `_`. Both the ingestion connector and GMS must use the same model to ensure query embeddings match document embeddings.

## Migration Steps

### Step 1: Stop DataHub Services

Stop GMS and any ingestion jobs to prevent writes during migration:

```bash
# Docker Compose
docker stop datahub-gms

# Kubernetes
kubectl scale deployment datahub-gms --replicas=0
```

### Step 2: Delete the Semantic Index

Delete the existing semantic index from OpenSearch:

```bash
# Check existing semantic indices
curl -s "http://localhost:9200/_cat/indices/*semantic*?v"

# Delete the semantic index (adjust index name as needed)
curl -X DELETE "http://localhost:9200/documentindex_v2_semantic"
```

### Step 3: Update Provider Configuration

Update your configuration with the new provider settings. See [Semantic Search Configuration](../../how-to/semantic-search-configuration.md) for the full configuration options for each provider (Helm charts and environment variables).

Make sure to update:

- **Provider type** (`EMBEDDING_PROVIDER_TYPE`)
- **API credentials** (API key or IAM role)
- **Vector dimension** (`ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION`) to match the new model

### Step 4: Update Index Configuration

If using `application.yaml`, update the model entry to match the new provider:

```yaml
elasticsearch:
  entityIndex:
    semanticSearch:
      models:
        # Use the model key that matches your new provider
        text_embedding_3_large:
          vectorDimension: 3072 # Must match model output
          knnEngine: faiss
          spaceType: cosinesimil
          efConstruction: 128
          m: 16
```

Or via environment variable:

```bash
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=3072
```

### Step 5: Start DataHub

Start GMS — the system update job will automatically recreate the semantic index:

```bash
# Docker Compose
docker start datahub-gms

# Kubernetes
kubectl scale deployment datahub-gms --replicas=1
```

The system update job runs automatically on startup and will:

1. Detect the missing semantic index
2. Create it with the correct mapping for your new embedding model
3. Log progress to the GMS logs

### Step 6: Re-ingest Documents

After the index is recreated, re-ingest your documents to generate new embeddings:

```bash
datahub ingest -c your-recipe.yaml
```

**Important:** Make sure your ingestion recipe also uses the same embedding model. The ingestion connector generates document embeddings, while GMS generates query embeddings — both must use the same model.

### Step 7: Verify

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

- The provider-specific model env var (`BEDROCK_EMBEDDING_MODEL`, `OPENAI_EMBEDDING_MODEL`, or `COHERE_EMBEDDING_MODEL`) in GMS config
- Embedding model in your ingestion recipe

## Best Practices

1. **Use the same model everywhere**: Ensure ingestion connectors and GMS use identical embedding models.
2. **Test in development first**: Switch providers in a dev environment before production.
3. **Plan for re-ingestion**: Switching providers requires re-generating all embeddings, which can take time for large datasets.
4. **Monitor costs**: Different providers have different pricing. OpenAI and Cohere charge per token/request.
5. **Keep backups**: Before deleting indices, consider backing up if you might need to rollback.
