# Document kNN Index Setup

This guide explains how to create a kNN (k-Nearest Neighbors) vector index for semantic search on the **Document** entity in DataHub.

## Change Log

| Date       | Change                                                          | File                                                                            |
| ---------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 2025-12-03 | Set `document` as default `enabledEntities` for semantic search | `metadata-service/configuration/src/main/resources/application.yaml` (line 406) |
| 2025-12-03 | Added semantic search env vars for local dev                    | `~/.datahub/local.env`                                                          |

## Prerequisites

- OpenSearch 2.17.0+ (required for nested kNN pre-filtering)
- k-NN plugin installed and enabled
- DataHub running with Elasticsearch/OpenSearch

## Current State

The `document` entity is defined in `entity-registry.yml`. Important notes:

- **V2 index (`documentindex_v2`)**: Created automatically for ALL entities (no `searchGroup` needed)
- **V3 index**: NOT created (requires `searchGroup` which document doesn't have)
- **Semantic index (`documentindex_v2_semantic`)**: Only created if `document` is in `enabledEntities` config

> **Note**: `searchGroup` is only required for V3 multi-entity indices. V2 per-entity indices are created for all entities regardless of searchGroup.

## Step 1: Enable Semantic Search for Documents

> **✅ DONE**: As of 2025-12-03, `document` has been added to the default `enabledEntities` in `application.yaml`. No manual configuration is needed unless you want to override via environment variables.

The default configuration in `application.yaml` now includes:

```yaml
semanticSearch:
  enabled: ${ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED:false}
  enabledEntities: ${ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES:document} # document is now the default!
```

To override or customize, add configuration to `application.yaml` (or via environment variables):

```yaml
elasticsearch:
  entityIndex:
    v2:
      enabled: true
    semanticSearch:
      enabled: true
      enabledEntities:
        - document # Add document to the list
      models:
        cohere_embed_v3:
          vectorDimension: 1024
          spaceType: cosinesimil
          knnEngine: lucene
          m: 16
          efConstruction: 128
```

Or via environment variables:

```bash
export ELASTICSEARCH_ENTITY_INDEX_V2_ENABLED=true
export ELASTICSEARCH_ENTITY_INDEX_SEMANTIC_SEARCH_ENABLED=true
# Add 'document' to the enabled entities list in your configuration
```

## Step 2: Create the Index

### Option A: Via System Upgrade (Recommended for Production)

Run the BuildIndices system upgrade which will create all configured indices:

```bash
cd datahub-upgrade
./gradlew bootRun -Pargs="-u SystemUpdate"
```

Or run the specific upgrade:

```bash
./gradlew runLoadIndices
```

#### What does SystemUpdate do?

`SystemUpdate` is a comprehensive upgrade that runs both **blocking** and **non-blocking** system upgrades:

**Blocking Upgrades** (run first, require service availability):

- **BuildIndices** - Creates/updates all search indices including:
  - V2 per-entity indices (e.g., `datasetindex_v2`, `documentindex_v2`)
  - V3 multi-entity indices (if configured)
  - **Semantic search indices** (e.g., `documentindex_v2_semantic`) for entities in `enabledEntities`
- Handles index mapping changes and reindexing if needed
- **Idempotent**: If the index already exists with correct mappings, it's essentially a no-op

**Non-Blocking Upgrades** (run after blocking, can run while services are active):

- **CopyDocumentsToSemanticIndices** - Copies existing documents from base indices to their semantic counterparts
- Various data migration and cleanup tasks

**Flow for semantic indices:**

1. `BuildIndices` reads `semanticSearch.enabledEntities` from config
2. For each enabled entity (now including `document`), it creates `{entity}index_v2_semantic`
3. The semantic index includes the base V2 mappings + `embeddings` field for kNN vectors
4. `CopyDocumentsToSemanticIndices` then reindexes existing documents to the new semantic index (if enabled)

#### Important: CopyDocumentsToSemanticIndex Configuration

> ⚠️ **The document copy step is DISABLED by default!**

```yaml
# application.yaml (line 1020-1021)
systemUpdate:
  copyDocumentsToSemanticIndex:
    enabled: ${SYSTEM_UPDATE_COPY_DOCUMENTS_TO_SEMANTIC_INDEX_ENABLED:false} # Default: false
```

| Aspect                   | Behavior                                                           |
| ------------------------ | ------------------------------------------------------------------ |
| **Enabled by default?**  | ❌ No - must explicitly enable                                     |
| **Skip if already run?** | ❌ No - will re-run each time if enabled                           |
| **Conflict handling**    | Uses `abortOnVersionConflict: false` - won't fail on existing docs |
| **Re-run behavior**      | Will **upsert** documents (update existing, insert new)            |
| **Embeddings impact**    | Embeddings field is NOT overwritten during re-copy                 |

#### Recommended Workflow

**Option 1: Direct environment variables**

```bash
# First time: Create index + copy documents
export ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
export SYSTEM_UPDATE_COPY_DOCUMENTS_TO_SEMANTIC_INDEX_ENABLED=true
cd datahub-upgrade
./gradlew bootRun -Pargs="-u SystemUpdate"

# Subsequent runs: Just ensure indices exist (skip copy)
export SYSTEM_UPDATE_COPY_DOCUMENTS_TO_SEMANTIC_INDEX_ENABLED=false
./gradlew bootRun -Pargs="-u SystemUpdate"

# Generate embeddings (separate step, always safe to re-run)
./gradlew bootRun -Pargs="-u SearchEmbeddingsUpdate -a entityTypes=document"
```

**Option 2: Using `.env` file with Docker Compose (Recommended for Local Dev)**

> ✅ **Best for local development** - Add settings to `~/.datahub/local.env` and use `DATAHUB_LOCAL_COMMON_ENV` to load it.

1. Add settings to `~/.datahub/local.env`:

```bash
# ~/.datahub/local.env
ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
SYSTEM_UPDATE_COPY_DOCUMENTS_TO_SEMANTIC_INDEX_ENABLED=true
```

2. Apply changes using `reloadEnv`:

```bash
# If quickstartDebug is already running:
DATAHUB_LOCAL_COMMON_ENV=~/.datahub/local.env ./gradlew reloadEnv

# Or when starting fresh:
DATAHUB_LOCAL_COMMON_ENV=~/.datahub/local.env ./gradlew quickstartDebug
```

This approach:

- Keeps default env files clean (no modifications to `docker/datahub-upgrade/env/*.env`)
- Uses the standard DataHub local config location (`~/.datahub/`)
- Easy to toggle on/off by including or excluding the `DATAHUB_LOCAL_COMMON_ENV` variable
- Works for all services (GMS, datahub-upgrade, etc.) that reference `${DATAHUB_LOCAL_COMMON_ENV:-empty.env}`

#### What Gets Populated?

| What                                             | After SystemUpdate     | After SearchEmbeddingsUpdate |
| ------------------------------------------------ | ---------------------- | ---------------------------- |
| Index structure                                  | ✅ Created             | -                            |
| Base document fields (`urn`, `title`, etc.)      | ✅ Copied (if enabled) | -                            |
| Embeddings (`embeddings.cohere_embed_v3.chunks`) | ❌ Empty               | ✅ Populated                 |

### Option B: Manual Index Creation (Development/Testing)

Use the POC script to create the semantic index manually:

```bash
cd semantic-search-poc

# First, ensure the base document index exists
# (requires Step 1 to be complete and DataHub restarted)

# Then create the semantic index
uv run python create_semantic_indices.py \
  --host localhost \
  --port 9200 \
  --indices documentindex_v2 \
  --suffix _semantic \
  --models cohere_embed_v3:1024 \
  --copy-docs
```

### Option C: Direct OpenSearch API (Quick Testing)

Create the index directly via OpenSearch API:

```bash
# Create the semantic index with kNN enabled
curl -X PUT "localhost:9200/documentindex_v2_semantic" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 100
    }
  },
  "mappings": {
    "properties": {
      "urn": { "type": "keyword" },
      "title": { "type": "text" },
      "embeddings": {
        "properties": {
          "cohere_embed_v3": {
            "properties": {
              "chunks": {
                "type": "nested",
                "properties": {
                  "vector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                      "name": "hnsw",
                      "engine": "lucene",
                      "space_type": "cosinesimil",
                      "parameters": {
                        "m": 16,
                        "ef_construction": 128
                      }
                    }
                  },
                  "text": { "type": "text", "index": false },
                  "position": { "type": "integer" },
                  "character_offset": { "type": "integer" },
                  "character_length": { "type": "integer" },
                  "token_count": { "type": "integer" }
                }
              },
              "total_chunks": { "type": "integer" },
              "total_tokens": { "type": "integer" },
              "model_version": { "type": "keyword" },
              "generated_at": { "type": "date" }
            }
          }
        }
      }
    }
  }
}'
```

## Step 3: Populate Data

### Copy Base Documents (Non-Embedding Fields)

If the base `documentindex_v2` has data, copy it to the semantic index:

```bash
curl -X POST "localhost:9200/_reindex" -H 'Content-Type: application/json' -d '{
  "source": { "index": "documentindex_v2" },
  "dest": { "index": "documentindex_v2_semantic" }
}'
```

### Generate Embeddings

Embeddings must be generated separately. Options:

1. **Via System Upgrade** (when available):

   ```bash
   ./gradlew bootRun -Pargs="-u SearchEmbeddingsUpdate -a entityTypes=document"
   ```

2. **Via Custom Script** (adapt from `backfill_dataset_embeddings.py`):
   - Read documents from the index
   - Generate text representation for each document
   - Call embedding API (Cohere, Titan, etc.)
   - Update documents with embeddings

## Verification

Check the index was created:

```bash
curl -X GET "localhost:9200/documentindex_v2_semantic/_mapping?pretty"
```

Check document count:

```bash
curl -X GET "localhost:9200/documentindex_v2_semantic/_count"
```

Test a kNN query (requires embeddings to be populated):

```bash
curl -X POST "localhost:9200/documentindex_v2_semantic/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "score_mode": "max",
      "query": {
        "knn": {
          "embeddings.cohere_embed_v3.chunks.vector": {
            "vector": [0.1, 0.2, ...],
            "k": 10
          }
        }
      }
    }
  }
}'
```

## Index Structure

The semantic document index follows this structure:

```
documentindex_v2_semantic
├── (all fields from base documentindex_v2)
└── embeddings
    └── cohere_embed_v3 (or other model)
        ├── chunks[] (nested array)
        │   ├── vector (knn_vector, 1024 dims)
        │   ├── text (source text for this chunk)
        │   └── position (chunk order)
        ├── total_chunks
        ├── model_version
        └── generated_at
```

## Notes

- The `chunks` field is `nested` type to support multiple embedding chunks per document
- Queries use `score_mode: "max"` to score documents by their best-matching chunk
- Pre-filtering inside kNN requires OpenSearch 2.17.0+
- V2 indices are created for ALL entities regardless of `searchGroup`
- `searchGroup` is only required for V3 multi-entity indices (not used here)
