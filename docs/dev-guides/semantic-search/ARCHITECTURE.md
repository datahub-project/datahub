# Semantic Search Architecture

This document provides a detailed explanation of DataHub's semantic search architecture, design decisions, and implementation details.

## Design Philosophy

### Why Semantic Search?

Traditional keyword search has limitations:

1. **Vocabulary Mismatch**: Users may use different terms than those in documents
2. **Synonym Blindness**: "access request" won't match "permission request"
3. **Context Ignorance**: Keywords lack understanding of meaning

Semantic search addresses these by understanding the _meaning_ of text through vector embeddings—numerical representations that capture semantic similarity.

### Core Principles

1. **Non-invasive**: Semantic search is additive; it doesn't replace keyword search
2. **Configurable**: Organizations choose which entities and models to use
3. **Extensible**: New embedding models can be added without architectural changes
4. **Async Processing**: Embedding generation happens asynchronously to not block ingestion

## Index Architecture

### Dual-Index Strategy

For each entity type enabled for semantic search, two indices exist:

```
┌─────────────────────────────────┐  ┌─────────────────────────────────┐
│     documentindex_v2            │  │   documentindex_v2_semantic     │
├─────────────────────────────────┤  ├─────────────────────────────────┤
│ Standard OpenSearch index       │  │ OpenSearch index with k-NN     │
│                                 │  │                                 │
│ Fields:                         │  │ Fields:                         │
│ - urn                           │  │ - urn                           │
│ - title (text)                  │  │ - title (text)                  │
│ - text (text)                   │  │ - text (text)                   │
│ - browsePaths                   │  │ - browsePaths                   │
│ - tags                          │  │ - tags                          │
│ - ...                           │  │ - ...                           │
│                                 │  │                                 │
│                                 │  │ + embeddings (nested object):   │
│                                 │  │   - cohere_embed_v3:            │
│                                 │  │     - model_version             │
│                                 │  │     - generated_at              │
│                                 │  │     - chunks[] (nested):        │
│                                 │  │       - position                │
│                                 │  │       - text                    │
│                                 │  │       - vector (knn_vector)     │
└─────────────────────────────────┘  └─────────────────────────────────┘
```

### Why Separate Indices? (Transitional Architecture)

The dual-index approach is a **transitional architecture**. The long-term plan is to:

1. **Phase 1 (Current)**: Run both indices in parallel during transition
2. **Phase 2**: Migrate all search traffic to semantic indices
3. **Phase 3**: Retire `v2` indices entirely

**Benefits of the transitional approach:**

1. **Zero Downtime Migration**: Users continue using keyword search while semantic capabilities are built
2. **Gradual Validation**: Semantic search quality can be validated before full rollout
3. **Rollback Safety**: If issues arise, keyword search remains available
4. **Incremental Embedding Generation**: Embeddings can be backfilled without blocking operations

**Future State:**

Once the transition is complete, the `_semantic` indices will become the primary (and only) search indices. They will support both:

- **Keyword search**: Using standard OpenSearch text matching on the same index
- **Semantic search**: Using k-NN vector similarity

This unified index approach simplifies operations and reduces storage overhead.

### Embeddings Schema

The semantic index stores embeddings in a nested structure:

```json
{
  "urn": "urn:li:document:example-doc",
  "title": "Data Access Guide",
  "text": "How to request access to datasets...",
  "embeddings": {
    "cohere_embed_v3": {
      "model_version": "bedrock/cohere.embed-english-v3",
      "generated_at": "2024-01-15T10:30:00Z",
      "chunking_strategy": "sentence_boundary_400t",
      "total_chunks": 3,
      "total_tokens": 850,
      "chunks": [
        {
          "position": 0,
          "text": "How to request access to datasets...",
          "character_offset": 0,
          "character_length": 450,
          "token_count": 95,
          "vector": [0.023, -0.041, 0.087, ...]  // 1024 dimensions
        },
        {
          "position": 1,
          "text": "For sensitive data, additional approval...",
          "character_offset": 450,
          "character_length": 380,
          "token_count": 82,
          "vector": [0.019, -0.055, 0.091, ...]
        }
      ]
    }
  }
}
```

### Multi-Model Support

The embeddings structure supports multiple embedding models:

```json
{
  "embeddings": {
    "cohere_embed_v3": { ... },
    "openai_text_embedding_3": { ... },
    "custom_model": { ... }
  }
}
```

This allows:

- A/B testing different models
- Gradual migration between models
- Model-specific optimizations

## Data Flow

### Ingestion Flow

The ingestion connector generates document embeddings and sends them to GMS along with the document content:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Ingestion Flow                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐                                                        │
│  │   Source    │  1. Extract documents                                  │
│  │   System    │                                                        │
│  └──────┬──────┘                                                        │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────┐                                                        │
│  │  Ingestion  │  2. Generate embeddings for document content           │
│  │  Connector  │     (using connector's embedding provider)             │
│  └──────┬──────┘                                                        │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────┐  3. Send document + embeddings to GMS                  │
│  │     GMS     │                                                        │
│  └──────┬──────┘                                                        │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      OpenSearch                                  │   │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────┐   │   │
│  │  │ entityindex_v2      │  │ entityindex_v2_semantic         │   │   │
│  │  │ (keyword search)    │  │ (keyword + vector search)       │   │   │
│  │  │                     │  │                                 │   │   │
│  │  │ - urn               │  │ - urn                           │   │   │
│  │  │ - title             │  │ - title                         │   │   │
│  │  │ - text              │  │ - text                          │   │   │
│  │  │ - ...               │  │ - embeddings.model.chunks[].    │   │   │
│  │  │                     │  │     vector                      │   │   │
│  │  └─────────────────────┘  └─────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Embedding Generation

**Document Embeddings** are generated by the **ingestion connector** at ingestion time and sent to GMS via MCP (Metadata Change Proposal). This ensures:

1. **Consistency**: Every ingested document has embeddings from the start
2. **Simplicity**: No separate backfill job to manage
3. **Freshness**: Embeddings are always up-to-date with document content
4. **Audit Trail**: Embeddings are tracked in the Metadata Change Log (MCL)
5. **Privacy Support**: Sensitive sources can generate embeddings locally and share only vectors

#### MCP-Based Embedding Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Ingestion Pipeline                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │   Source     │───▶│  Ingestion   │───▶│    DataHub GMS       │   │
│  │   System     │    │  Connector   │    │                      │   │
│  └──────────────┘    └──────┬───────┘    └──────────┬───────────┘   │
│                             │                       │               │
│                             ▼                       ▼               │
│                    ┌──────────────────┐    ┌──────────────────┐    │
│                    │ Generate document│    │ Process MCP and  │    │
│                    │ embeddings       │    │ write to semantic │    │
│                    │ (in connector)   │    │ search index      │    │
│                    └────────┬─────────┘    └──────────────────┘    │
│                             │                       ▲               │
│                             │    MCP with          │               │
│                             └─────SemanticContent─┘               │
│                                    aspect                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### SemanticContent Aspect

Embeddings are stored as a proper DataHub aspect (`SemanticContent`), defined in PDL schema:

```json
{
  "entityType": "document",
  "entityUrn": "urn:li:document:my-doc",
  "aspectName": "semanticContent",
  "aspect": {
    "embeddings": {
      "cohere_embed_v3": {
        "modelVersion": "bedrock/cohere.embed-english-v3",
        "generatedAt": 1702234567890,
        "totalChunks": 2,
        "chunks": [
          { "position": 0, "vector": [...], "text": "..." },
          { "position": 1, "vector": [...], "text": "..." }
        ]
      }
    }
  }
}
```

#### Privacy-Sensitive Use Cases

The `text` field in each chunk is **optional**. This supports scenarios where:

- Source data contains sensitive information (PII, trade secrets)
- Customers want semantic search without storing source text in DataHub
- Embeddings are generated locally at the data source

**Note:** Embeddings are one-way—original text cannot be reconstructed from vectors.

**Query Embeddings** are generated by GMS at search time using the configured embedding provider (e.g., AWS Bedrock):

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   GraphQL   │───▶│     GMS     │───▶│  Embedding  │───▶│ OpenSearch  │
│   Client    │    │             │    │  Provider   │    │  k-NN Query │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                          │
                          ▼
                   Query embedding
                   generated here
                   (for search only)
```

**Key Point:** The GMS embedding provider is used **only for query embedding**, not for document embedding. The ingestion connector is responsible for document embeddings.

### Query Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   GraphQL   │───▶│     GMS     │───▶│  Embedding  │───▶│ OpenSearch  │
│   Client    │    │             │    │  Provider   │    │  k-NN Query │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
     semanticSearchAcrossEntities(                              │
       query: "how to access data"                              │
     )                                                          │
                                                                ▼
                                            ┌─────────────────────────────┐
                                            │  Nested k-NN Query:         │
                                            │                             │
                                            │  {                          │
                                            │    "nested": {              │
                                            │      "path": "embeddings    │
                                            │        .cohere_embed_v3     │
                                            │        .chunks",            │
                                            │      "query": {             │
                                            │        "knn": {             │
                                            │          "...chunks.vector":│
                                            │          { "vector": [...], │
                                            │            "k": 10 }        │
                                            │        }                    │
                                            │      }                      │
                                            │    }                        │
                                            │  }                          │
                                            └─────────────────────────────┘
```

## Chunking Strategy

### Why Chunk Documents?

Embedding models have token limits (512 tokens for cohere's embed-english-v3.0). Long documents must be split into chunks:

1. **Token Limits**: Models can't process unlimited text
2. **Precision**: Smaller chunks allow more precise matching
3. **Relevance**: A document may have one highly relevant section

### Chunking Algorithm

```python
def chunk_text(text, max_tokens=400):
    """
    Chunk text at sentence boundaries, respecting token limits.

    1. Split text into sentences
    2. Accumulate sentences until approaching limit
    3. Save chunk, start new accumulation
    4. Handle oversized sentences by character splitting
    """
```

**Parameters:**

- `max_tokens`: Target chunk size (default: 400)
- `chars_per_token`: Estimation ratio (default: 4 characters ≈ 1 token)

### Chunk Metadata

Each chunk stores metadata for debugging and analysis:

```json
{
  "position": 0,           // Order in document
  "text": "...",           // Chunk content
  "character_offset": 0,   // Start position in original
  "character_length": 450, // Length in characters
  "token_count": 95,       // Estimated tokens
  "vector": [...]          // Embedding vector
}
```

## k-NN Search Configuration

### OpenSearch k-NN Settings

The semantic index uses OpenSearch's k-NN plugin with FAISS engine:

```json
{
  "settings": {
    "index.knn": true
  },
  "mappings": {
    "properties": {
      "embeddings": {
        "type": "nested",
        "properties": {
          "cohere_embed_v3": {
            "type": "nested",
            "properties": {
              "chunks": {
                "type": "nested",
                "properties": {
                  "vector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                      "name": "hnsw",
                      "engine": "faiss",
                      "space_type": "cosinesimil",
                      "parameters": {
                        "ef_construction": 128,
                        "m": 16
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

### HNSW Parameters

| Parameter         | Value       | Description                                                          |
| ----------------- | ----------- | -------------------------------------------------------------------- |
| `ef_construction` | 128         | Build-time accuracy (higher = more accurate, slower build)           |
| `m`               | 16          | Number of connections per node (higher = more accurate, more memory) |
| `space_type`      | cosinesimil | Similarity metric (cosine similarity)                                |

## Security Considerations

### Data Privacy

1. **Embedding Storage**: Vectors are stored alongside documents; same access controls apply
2. **External API Calls**: Embedding providers receive document text; ensure compliance
3. **Credential Management**: API keys/AWS credentials must be secured

### Access Control

Semantic search respects DataHub's existing access controls:

- Users only see results they have permission to view
- Entity-level permissions are enforced before returning results

## Performance Considerations

### Indexing Performance

- **Dual-write Impact**: ~10-20% increase in write latency
- **Embedding Generation**: Async; doesn't block ingestion
- **Batch Processing**: Embeddings generated in batches for efficiency

### Query Performance

- **k-NN Overhead**: ~50-200ms per query (depends on index size)
- **Embedding Generation**: ~100-300ms for query embedding
- **Total Latency**: Typically 200-500ms end-to-end

### Scaling Recommendations

| Index Size     | Recommendation                    |
| -------------- | --------------------------------- |
| < 100K docs    | Single node sufficient            |
| 100K - 1M docs | Consider dedicated k-NN nodes     |
| > 1M docs      | Sharding and replicas recommended |

## Future Enhancements

1. **Hybrid Search**: Combine keyword and semantic scores for improved relevance
2. **Model Fine-tuning**: Domain-specific embedding models for better accuracy
