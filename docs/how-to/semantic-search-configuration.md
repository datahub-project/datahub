# Semantic Search Configuration

Semantic search lets you find DataHub entities using natural language queries like "customer churn analysis" — even when exact keywords differ.

## Prerequisites

1. **OpenSearch 2.17.0+** with k-NN plugin (DataHub ships with `opensearchproject/opensearch:2.19.3`). Elasticsearch is **not** supported.
2. **An API key** for your chosen embedding provider (see table below). The in-process `onnx` provider needs a local model download instead of a key. The `classical` provider below needs neither, but it is a CI and smoke-test provider, not semantic search.

## How to Configure Semantic Search

### DataHub Helm Charts (Recommended)

If you deploy DataHub using the [DataHub Helm chart](https://github.com/acryldata/datahub-helm), add the following to your `values.yaml` and run `helm upgrade`.

#### OpenAI (Default)

Create a secret, then configure:

```bash
kubectl create secret generic openai-secret --from-literal=api-key=sk-your-api-key-here
```

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

#### AWS Bedrock

No API key needed — Bedrock authenticates via the [AWS SDK default credential chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) (IRSA, EC2/ECS instance credentials, etc).

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

#### Cohere

Create a secret, then configure:

```bash
kubectl create secret generic cohere-secret --from-literal=api-key=your-cohere-api-key
```

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

#### Apply Changes

```bash
helm upgrade datahub datahub/datahub -f values.yaml
```

### Environment Variables

For Docker Compose or non-Helm deployments, set these on the **datahub-gms** service and restart it.

#### OpenAI (Default)

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
OPENAI_API_KEY=sk-your-api-key-here

```

That's it — OpenAI is the default provider, so no other variables are needed.

#### AWS Bedrock

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
EMBEDDING_PROVIDER_TYPE=aws-bedrock
BEDROCK_EMBEDDING_AWS_REGION=us-west-2
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1024
```

Authentication uses the AWS SDK default credential chain (EC2/ECS instance credentials, `AWS_PROFILE`, or `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`).

#### Cohere

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
EMBEDDING_PROVIDER_TYPE=cohere
COHERE_API_KEY=your-cohere-api-key
ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1024
```

#### Classical (CI and smoke tests only)

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document
EMBEDDING_PROVIDER_TYPE=classical
CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true
```

The `classical` provider is not semantic search. It computes deterministic lexical vectors in-process: word and character n-gram features are hashed with SHA-256 into a 2048-dimensional vector, so results rank by shared words and character fragments, never by meaning. It exists so CI, smoke tests and quickstarts can run the full chunk, embed, kNN index and query pipeline with no API key, endpoint or model download, with bit-identical vectors on the Python ingestion side and the Java query side. GMS refuses to start with `EMBEDDING_PROVIDER_TYPE=classical` until `CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true` is also set, and logs a warning at startup while the provider is active. A deployment that needs semantic quality without a cloud dependency should use the in-process `onnx` provider instead. The default model is `hash-v1-2048`; `CLASSICAL_EMBEDDING_MODEL` selects another width (`hash-v1-<dims>`), which needs a matching `semanticSearch.models` entry and, like any model switch, a re-index (see [Switching Providers](../dev-guides/semantic-search/SWITCHING_PROVIDERS.md)). The `hash_v1_2048` field is added by the first system-update run on a version that includes it, and only when system-update itself runs with the semantic search variables above (Helm passes them to both; in Docker Compose give the `system-update` service and the MAE consumer the same variables as GMS, for example through a shared env file: every process that builds the embedding provider needs the opt-in too, or it refuses to start). Restarting only GMS does not add it. An instance already upgraded to such a version needs nothing more, while one that only restarted GMS with the new env var must run system-update once before the first classical ingestion. The `datahub-documents` source does not apply its per-minute document limiter (`embedding.rate_limit`, `documents_per_minute`) to in-process providers, this one and `onnx`, since there is no external API to protect.

### Verify It's Working

After restarting, check the GMS logs:

```bash
# Docker Compose
docker-compose logs datahub-gms | grep -i "embedding"

# Kubernetes
kubectl logs deployment/datahub-gms | grep -i "embedding"
```

You should see:

```
Creating embedding provider with type: openai
Initialized OpenAiEmbeddingProvider with model=text-embedding-3-large
```

## Generating Embeddings

Once semantic search is enabled, you need to run an ingestion source to generate embeddings for your documents.

### Minimal Recipe

```yaml
source:
  type: datahub-documents
  config: {}

sink:
  type: datahub-rest
  config: {}
```

This automatically connects to DataHub, fetches your embedding config from the server, and processes documents in real-time.

```bash
datahub ingest -c recipe.yml
```

For external document sources (Notion, Confluence, etc.), see the [Notion Source](../generated/ingestion/sources/notion.md) and [DataHub Documents Source](../generated/ingestion/sources/datahub-documents.md) documentation.

## Supported Models

| Provider    | Model                     | Dimensions | Notes                                          |
| ----------- | ------------------------- | ---------- | ---------------------------------------------- |
| OpenAI      | `text-embedding-3-large`  | 3072       | Default, higher quality                        |
| OpenAI      | `text-embedding-3-small`  | 1536       | Fast, cost-effective                           |
| AWS Bedrock | `cohere.embed-english-v3` | 1024       | AWS-managed                                    |
| Cohere      | `embed-english-v3.0`      | 1024       | English optimized                              |
| Cohere      | `embed-multilingual-v3.0` | 1024       | 100+ languages                                 |
| Classical   | `hash-v1-2048`            | 2048       | CI and smoke tests only: lexical, not semantic |

> To use a non-default model, set the model name in your Helm values or environment variable and update `vectorDimension` / `ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION` to match. The classical row is the exception: its width is fixed by the model name (`hash-v1-<dimensions>`) and its `hash_v1_2048` entry ships in the default `semanticSearch.models`, so there is nothing to update.

## Troubleshooting

| Symptom                                         | Fix                                                                                                                        |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| "Semantic search is disabled or not configured" | Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` and restart GMS                                                        |
| "Invalid API key provided"                      | Check your API key is set correctly in the GMS environment                                                                 |
| "Dimension mismatch: expected 3072, got 1024"   | Update `ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION` to match your model                                                       |
| "meant for CI, smoke tests and quickstarts"     | The `classical` provider needs `CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true`; for a local neural provider use `onnx` |

## Further Reading

- [Switching Providers](../dev-guides/semantic-search/SWITCHING_PROVIDERS.md) — how to migrate between providers (requires re-indexing)
- [Configuration Guide](../dev-guides/semantic-search/CONFIGURATION.md) — advanced `application.yaml` reference and performance tuning
- [DataHub Helm Chart](https://github.com/acryldata/datahub-helm)
- [OpenSearch k-NN Plugin](https://opensearch.org/docs/latest/search-plugins/knn/index/)
