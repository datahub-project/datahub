# Semantic Search Configuration

Semantic search lets you find DataHub entities using natural language queries like "customer churn analysis" — even when exact keywords differ.

Semantic search covers document entities (`ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES` defaults to `document`); keyword search still covers every entity type. The hosted providers below (OpenAI, AWS Bedrock, Cohere) call an external embedding API with your own credentials, while `onnx` runs a neural model in-process.

## Prerequisites

1. **OpenSearch 2.17.0+** with k-NN plugin (DataHub ships with `opensearchproject/opensearch:2.19.3`), or **Elasticsearch 8.18+**.
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

## Search V3

With Search V3 writes on (`ELASTICSEARCH_ENTITY_INDEX_V3_ENABLED=true`), document embeddings are also written to the V3 document index. Semantic search keeps reading the semantic indices until you set `ELASTICSEARCH_ENTITY_INDEX_V3_SEMANTIC_READ_ENABLED=true`, after which it reads the V3 document index instead. The flag is independent of `ELASTICSEARCH_ENTITY_INDEX_V3_KEYWORD_READ_ENABLED`, so keyword and semantic reads can move to V3 at different times. Turn it on only after the V3 document index holds your document embeddings: documents embedded before V3 writes were turned on get V3 vectors only once they are re-indexed into V3 (for example with the `RestoreIndices` upgrade job) or re-embedded. To check, count the documents that carry embeddings in each index, running `GET documentindex_v3/_count` and `GET documentindex_v2_semantic/_count` (named `<prefix>_documentindex_v3` and so on if you set an index prefix) with the body `{"query":{"nested":{"path":"embeddings.<model>.chunks","query":{"match_all":{}}}}}`, where `<model>` is your model key (for example `text_embedding_3_large`). The flag needs OpenSearch 3.5+ or Elasticsearch 8.18+ on the Search V3 cluster, and DataHub refuses to start with it on older OpenSearch, where V3 semantic reads have not been validated: before 3.5, OpenSearch k-NN pre-filters ignored fields under the V3 `_aspects` object, which facet and View filters used before they moved to top-level fields.

### Hybrid search

With V3 keyword reads and V3 semantic reads on, set `ELASTICSEARCH_ENTITY_INDEX_V3_HYBRID_READ_ENABLED=true` to rerank full-text keyword search with the V3 document vectors. For each full-text search sorted by relevance, DataHub embeds the query and runs a kNN query over the V3 indices that hold vectors, scoring only the documents among the reranked keyword results, which already passed the keyword query's filters: `documentindex_v3` with the default `ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`. Those are the documents among the first 100 keyword results, each scored by its chunk nearest the query, and they are reordered by a combined keyword and vector score, each into a position a document already held. A document without vectors, for example one not embedded yet, keeps its position. Datasets, dashboards and the other entity types without vectors keep their keyword positions, so keyword search still ranks across entity types. Results after the first 100, totals and facets are those of keyword search. A search whose first 100 results hold fewer than two rows of an entity type with vectors (documents by default), a query wrapped in quotes, and a URN or an `s3://`, `gs://` or `hdfs://` path stay keyword-only, with no embedding or kNN call. A repeated query reuses its embedding for a minute.

The embedding and kNN calls share a 2-second deadline. OpenAI, Cohere, Vertex AI and the local provider make one request bounded by the time left (Vertex AI's token refresh, about once an hour, is not), Bedrock bounds its call, retries included, and the kNN call's connection and read timeouts are set to the time left, so slow remote calls free their worker about when the search falls back. The in-process `onnx` and `classical` providers make no remote call and ignore the deadline: the search still falls back at the deadline, but an inference that runs past it keeps its worker until it finishes. When the calls take longer or fail, the keyword ranking is served and the `hybridReadTimeout` or `hybridReadFailed` metric is counted. A kNN response that reported a timed-out or failed shard may be missing hits, so it counts as a timeout or failure too, and `hybridReadPartial` counts it. `hybridReadRejected` counts searches served the keyword ranking because the hybrid workers and their queue were full, `hybridReadApplied` counts searches reranked with vector scores, and `hybridReadNoVectors` counts searches whose kNN response left fewer than two of those documents with a vector score. `hybridReadPartial` and `hybridReadNoVectors` are registered under `com.linkedin.metadata.search.hybrid.HybridSearchResultReranker`, the other metrics under `com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO`.

After three timeouts or failures in a row, hybrid read pauses for 30 seconds in that GMS process: searches get the keyword ranking without calling the provider, and `hybridReadSkipped` counts them. A failure counts only when its hybrid step started after the last counted one, so searches whose calls overlap, such as a results page and its facets that share one embedding call, count once, and a search still running when a pause begins does not count. A completed rerank ends the streak only if the provider has answered in time since both that rerank started and the last counted failure: a rerank on a cached embedding calls no provider, and its kNN failures still count. A provider that still answers some searches in time therefore keeps hybrid read on, since each such answer ends the streak: for a provider that is degraded rather than down, turn the flag off. Rejected searches do not wait, and an interrupted search says nothing about the provider, so neither counts toward the pause, although `hybridReadFailed` counts interrupted searches. A page served the keyword ranking is a slice of that ranking, so across pages served differently a document can show twice or not at all. With `SEARCH_SERVICE_ENABLE_CACHE=true`, such a page is cached like a reranked one, for `CACHE_TTL_SECONDS` (10 minutes by default), and with the Hazelcast cache every GMS process serves it, not only the paused one. With Search V3 on (`ELASTICSEARCH_ENTITY_INDEX_V3_ENABLED=true`), DataHub refuses to start with the flag on unless V3 keyword and semantic reads, semantic search, an embedding provider and the active model's mapping are all configured; with Search V3 off, the flag has no effect.

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

| Symptom                                                                                                                                                | Fix                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Elasticsearch 8.18+ required for semantic search"                                                                                                     | Upgrade the cluster to Elasticsearch 8.18 or newer, or turn semantic search off                                                                                                                                                                                                                                                                                                                                                                                                          |
| "Semantic search is disabled or not configured"                                                                                                        | Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` and restart GMS                                                                                                                                                                                                                                                                                                                                                                                                                      |
| "Invalid API key provided"                                                                                                                             | Check your API key is set correctly in the GMS environment                                                                                                                                                                                                                                                                                                                                                                                                                               |
| "Embedding provider returned 1024 dimensions for model '...'; configured mapping expects 3072", or a search engine error about query vector dimensions | Set the `vectorDimension` of the model named in the error to the size that model returns: `ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION` (Helm: `global.datahub.semantic_search.vectorDimension`) for `text_embedding_3_large`, `LOCAL_EMBEDDING_VECTOR_DIMENSION` for `nomic_embed_text`, and `VERTEX_AI_EMBEDDING_OUTPUT_DIMENSIONALITY=3072` for `gemini_embedding_001`, which sets both the model output and the mapping. Re-index only if the semantic index was built with another size |
| "meant for CI, smoke tests and quickstarts"                                                                                                            | The `classical` provider needs `CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true`; for a local neural provider use `onnx`                                                                                                                                                                                                                                                                                                                                                               |

## Further Reading

- [Switching Providers](../dev-guides/semantic-search/SWITCHING_PROVIDERS.md) — how to migrate between providers (requires re-indexing)
- [Configuration Guide](../dev-guides/semantic-search/CONFIGURATION.md) — advanced `application.yaml` reference and performance tuning
- [DataHub Helm Chart](https://github.com/acryldata/datahub-helm)
- [OpenSearch k-NN Plugin](https://opensearch.org/docs/latest/search-plugins/knn/index/)
