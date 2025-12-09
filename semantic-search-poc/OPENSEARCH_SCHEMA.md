# OpenSearch Schema Documentation

## Overview

This document describes the exact OpenSearch schema and data structure used in the DataHub semantic search implementation. It serves as the definitive technical reference for the index mappings, document structure, and query patterns.

### Requirements

- **OpenSearch Version**: 2.17.0 or higher (required for nested kNN filtering on parent document fields and for this POC tooling)
- **k-NN Plugin**: Must be installed and enabled
- **Memory**: Minimum 1GB heap for development, 4GB+ for production

## Index Configuration

### Index Names

- `datasetindex_v2_semantic` - Datasets with semantic embeddings
- `chartindex_v2_semantic` - Charts with semantic embeddings
- `dashboardindex_v2_semantic` - Dashboards with semantic embeddings

### Index Settings

```json
{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 100
    }
  }
}
```

## Field Mappings

### Core Document Structure

```json
{
  "mappings": {
    "properties": {
      "name": {
        "type": "text",
        "analyzer": "standard"
      },
      "platform": {
        "type": "keyword"
      },
      "qualifiedName": {
        "type": "keyword"
      },
      "description": {
        "type": "text"
      },
      "fieldPaths": {
        "type": "keyword"
      },
      "embeddings": {
        "type": "object",
        "properties": {
          "cohere_embed_v3": {
            "type": "object",
            "properties": {
              "model_version": {
                "type": "keyword"
              },
              "generated_at": {
                "type": "date",
                "format": "strict_date_time"
              },
              "chunking_strategy": {
                "type": "keyword"
              },
              "total_chunks": {
                "type": "integer"
              },
              "total_tokens": {
                "type": "integer"
              },
              "chunks": {
                "type": "nested",
                "properties": {
                  "position": {
                    "type": "integer"
                  },
                  "text": {
                    "type": "text",
                    "index": false
                  },
                  "character_offset": {
                    "type": "integer"
                  },
                  "character_length": {
                    "type": "integer"
                  },
                  "token_count": {
                    "type": "integer"
                  },
                  "vector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                      "engine": "lucene",
                      "space_type": "cosinesimil",
                      "name": "hnsw",
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

## Document Examples

### Complete Dataset Document

```json
{
  "_index": "datasetindex_v2_semantic",
  "_id": "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.analytics.customers%2CPROD%29",
  "_source": {
    "name": "customers",
    "platform": "urn:li:dataPlatform:dbt",
    "qualifiedName": "long_tail_companions.analytics.customers",
    "description": "This table has basic information about a customer, as well as some derived facts based on a customer's orders",
    "fieldPaths": [
      "customer_id",
      "first_name",
      "last_name",
      "first_order",
      "most_recent_order",
      "number_of_orders",
      "customer_lifetime_value"
    ],
    "browsePathV2": {
      "path": [
        {
          "id": "urn:li:container:dbt",
          "urn": "urn:li:container:dbt"
        }
      ]
    },
    "container": "urn:li:container:dbt",
    "customProperties": {
      "catalog_type": "table",
      "is_view": "False"
    },
    "embeddings": {
      "cohere_embed_v3": {
        "model_version": "cohere/embed-english-v3.0",
        "generated_at": "2024-01-15T10:30:45Z",
        "chunking_strategy": "single_chunk",
        "total_chunks": 1,
        "total_tokens": 47,
        "chunks": [
          {
            "position": 0,
            "text": "This table has basic information about a customer, as well as some derived facts based on a customer's orders. The \"customers\" table. Contains 7 fields including \"customer_id\", \"first_name\", \"last_name\". Maintained by @bi-engineering. Uses table materialization. Glossary terms: Bronze, 9afa9a59-93b2-47cb-9094-aa342eec24ad. Tagged as: \"contains_pii\", \"publisher\". Stored in dbt. From production environment",
            "token_count": 47,
            "vector": [
              0.023925781, -0.03640747, -0.0131073, 0.008430481, -0.022232056,
              0.0064468384, 0.0009493828, 0.0736084, 0.045135498, -0.0847168,
              0.026977539, 0.012207031, 0.002954483, -0.04385376, 0.0335083,
              -0.007019043
            ]
          }
        ]
      }
    },
    "hasDescription": true,
    "hasContainer": true,
    "origin": "PROD",
    "systemCreated": false,
    "typeNames": ["dataset"],
    "urn": "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.customers,PROD)"
  }
}
```

### Chart Document Example

```json
{
  "_index": "chartindex_v2_semantic",
  "_id": "urn%3Ali%3Achart%3A%28looker%2Cdashboard.revenue_analysis%29",
  "_source": {
    "name": "Revenue Analysis",
    "platform": "urn:li:dataPlatform:looker",
    "qualifiedName": "dashboard.revenue_analysis",
    "description": "Monthly revenue breakdown by region and product category",
    "chartType": "bar_chart",
    "embeddings": {
      "cohere_embed_v3": {
        "model_version": "cohere/embed-english-v3.0",
        "generated_at": "2024-01-15T11:15:22Z",
        "chunking_strategy": "single_chunk",
        "total_chunks": 1,
        "total_tokens": 23,
        "chunks": [
          {
            "position": 0,
            "text": "Revenue Analysis chart showing monthly revenue breakdown by region and product category. Bar chart visualization from Looker dashboard.",
            "token_count": 23,
            "vector": [
              0.042633057, -0.010513306, -0.017745972, 0.013504028, 0.009689331,
              -0.012954712, -0.044647217, 0.01663208, 0.054229736, 0.013473511,
              -0.03463745, 0.0077590942, 0.0049324036, -0.015342712,
              0.0026340485, 0.02204895
            ]
          }
        ]
      }
    }
  }
}
```

## Vector Field Configuration

### kNN Vector Settings

```json
{
  "vector": {
    "type": "knn_vector",
    "dimension": 1024,
    "method": {
      "engine": "lucene",
      "space_type": "cosinesimil",
      "name": "hnsw",
      "parameters": {
        "ef_construction": 128,
        "m": 16
      }
    }
  }
}
```

### Configuration Details

- **Dimension**: 1024 (matches Cohere embed-english-v3.0)
- **Engine**: Lucene (OpenSearch's native engine)
- **Algorithm**: HNSW (Hierarchical Navigable Small World)
- **Space Type**: `cosinesimil` (cosine similarity)
- **ef_construction**: 128 (build-time parameter for recall vs speed)
- **m**: 16 (number of bi-directional links for each node)

## Query Patterns

### Optimized Production Query (Recommended)

This query demonstrates best practices for semantic search with pre-filtering in OpenSearch 2.17+:

```json
{
  "size": 5,
  "track_total_hits": true,
  "_source": ["platform", "name", "qualifiedName", "description"],
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "score_mode": "max",
      "query": {
        "knn": {
          "embeddings.cohere_embed_v3.chunks.vector": {
            "vector": [
              /* 1024 dimensions */
            ],
            "k": 10, // With pre-filtering, k can be close to desired size
            "filter": {
              "bool": {
                "filter": [
                  {
                    "term": {
                      "platform.keyword": "urn:li:dataPlatform:snowflake"
                    }
                  },
                  { "terms": { "typeNames": ["dataset"] } },
                  {
                    "range": {
                      "embeddings.cohere_embed_v3.generated_at": {
                        "gte": "2024-01-01"
                      }
                    }
                  }
                ]
              }
            }
          }
        }
      }
    }
  }
}
```

**Key Features:**

- `track_total_hits: true` - Ensures accurate total count
- `k: 10` - With pre-filtering, k can be 1.2-2x the desired size (or even exact size)
- `filter` inside kNN - Pre-filters during vector search (requires OpenSearch 2.17+)
- `bool.filter` array - Combines multiple filter conditions efficiently
- `score_mode: max` - Uses the best matching chunk's score for the document

### Basic kNN Query Structure

```json
{
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "query": {
        "knn": {
          "embeddings.cohere_embed_v3.chunks.vector": {
            "vector": [0.1, 0.2, 0.3, ...], // 1024 dimensions
            "k": 5
          }
        }
      }
    }
  }
}
```

### Why Nested Queries Are Required

The `chunks` field is mapped as `"type": "nested"` because:

1. It's an array of objects containing vectors
2. Each chunk has its own vector, text, and metadata
3. OpenSearch requires nested queries to properly handle array-of-objects with kNN
4. This prevents field value mixing between different chunks

### Hybrid Search (Semantic + Keyword)

Combines vector similarity with keyword matching for improved relevance:

```json
{
  "size": 10,
  "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "embeddings.cohere_embed_v3.chunks",
            "score_mode": "max",
            "query": {
              "knn": {
                "embeddings.cohere_embed_v3.chunks.vector": {
                  "vector": [
                    /* 1024 dimensions */
                  ],
                  "k": 15, // Slight oversampling for hybrid scoring
                  "filter": {
                    "term": {
                      "platform.keyword": "urn:li:dataPlatform:snowflake"
                    }
                  }
                }
              }
            }
          }
        }
      ],
      "should": [
        {
          "match": {
            "name": {
              "query": "customer",
              "boost": 2.0
            }
          }
        },
        {
          "match": {
            "description": "user data"
          }
        }
      ],
      "minimum_should_match": 0
    }
  }
}
```

**Note**: This hybrid approach works best when the semantic and keyword scores are normalized. Consider using score normalization techniques for production use.

## Text Generation Schema

### Generated Text Format

The text used for embeddings follows this pattern:

```
{description}. The "{name}" {entity_type}. {field_info}. {ownership_info}. {properties_info}. {tags_info}. Stored in {platform}. From {environment} environment
```

### Example Generated Texts

```
"This table has basic information about a customer, as well as some derived facts based on a customer's orders. The \"customers\" table. Contains 7 fields including \"customer_id\", \"first_name\", \"last_name\". Maintained by @bi-engineering. Uses table materialization. Glossary terms: Bronze. Tagged as: \"contains_pii\", \"publisher\". Stored in dbt. From production environment"

"Revenue Analysis chart showing monthly revenue breakdown by region and product category. Bar chart visualization from Looker dashboard."

"Customer segmentation dashboard with user demographics and behavior metrics. Interactive dashboard from Looker platform."
```

## Index Statistics

### Current Implementation Stats

- **Total Documents**: ~1,460 (datasets), ~175 (charts), ~30 (dashboards)
- **Documents with Embeddings**: 1,000 datasets backfilled
- **Vector Storage**: ~4MB per 1,000 documents
- **Index Size**: ~25MB total including metadata

### Performance Metrics

- **Query Latency**: <100ms for kNN searches
- **Indexing Rate**: ~3 documents/second (with API rate limits)
- **Memory Usage**: ~8MB heap per 1,000 vectors
- **Disk Usage**: ~50KB per document with embeddings

## Field Mappings Reference

### Core DataHub Fields

```json
{
  "name": "keyword + text", // Asset name
  "platform": "keyword", // Data platform URN
  "qualifiedName": "keyword", // Full qualified name
  "description": "text", // Original description
  "fieldPaths": "keyword[]", // Column/field names
  "browsePathV2": "object", // Navigation hierarchy
  "container": "keyword", // Parent container URN
  "customProperties": "object", // Platform-specific metadata
  "hasDescription": "boolean", // Has description flag
  "hasContainer": "boolean", // Has container flag
  "origin": "keyword", // Environment (PROD, DEV, etc.)
  "systemCreated": "boolean", // System vs user created
  "typeNames": "keyword[]", // Entity type names
  "urn": "keyword" // Unique resource name
}
```

### Chart-Specific Fields

```json
{
  "chartType": "keyword", // Chart visualization type
  "dashboardUrn": "keyword", // Parent dashboard URN
  "chartUrl": "keyword" // Direct chart URL
}
```

### Dashboard-Specific Fields

```json
{
  "dashboardUrl": "keyword", // Dashboard URL
  "charts": "keyword[]", // Child chart URNs
  "lastModified": "date" // Last modification time
}
```

## Migration and Versioning

### Index Versioning Strategy

- Base indices: `datasetindex_v2`, `chartindex_v2`, `dashboardindex_v2`
- Semantic indices: `{base}_semantic` suffix
- Future versions: `{base}_semantic_v2`, etc.

### Schema Evolution

1. **Backward Compatible Changes**: Add new fields without breaking existing queries
2. **Breaking Changes**: Create new versioned indices and migrate data
3. **Field Deprecation**: Mark fields as deprecated before removal
4. **Vector Dimension Changes**: Requires full reindexing

### Reindexing Process

```bash
# Create new index with updated mapping
PUT /datasetindex_v2_semantic_v2

# Reindex data with transformations
POST /_reindex
{
  "source": {"index": "datasetindex_v2_semantic"},
  "dest": {"index": "datasetindex_v2_semantic_v2"}
}

# Update application to use new index
# Drop old index after validation
```

## Version Compatibility Notes

### OpenSearch 2.11 vs 2.17+ Behavior

- **OpenSearch 2.11**: Filters inside nested kNN queries on parent document fields do NOT work (returns 0 results)
- **OpenSearch 2.17+**: Full support for pre-filtering on parent document fields within nested kNN queries

### Filter Placement Strategies

#### Pre-filtering (OpenSearch 2.17+ only)

```json
{
  "nested": {
    "query": {
      "knn": {
        "filter": {
          /* Filters applied DURING kNN search */
        }
      }
    }
  }
}
```

#### Post-filtering (All versions, less efficient)

```json
{
  "bool": {
    "filter": [
      /* Filters applied AFTER kNN search */
    ],
    "must": [
      {
        "nested": {
          "query": {
            "knn": {
              /* No filter here */
            }
          }
        }
      }
    ]
  }
}
```

## Troubleshooting Schema Issues

### Common Mapping Problems

1. **Vector Dimension Mismatch**: Ensure all vectors are exactly 1024 dimensions
2. **Nested Field Access**: Use proper nested query syntax for chunks array
3. **Field Type Conflicts**: Check mapping conflicts during index updates
4. **Memory Issues**: Monitor JVM heap with large vector datasets

### Diagnostic Queries

```bash
# Check index mapping
GET /datasetindex_v2_semantic/_mapping

# Verify vector field configuration
GET /datasetindex_v2_semantic/_mapping/field/embeddings.cohere_embed_v3.chunks.vector

# Check document structure
GET /datasetindex_v2_semantic/_search?size=1&pretty

# Verify nested field access
GET /datasetindex_v2_semantic/_search
{
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "query": {"match_all": {}}
    }
  }
}
```

### Performance Tuning

```json
{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 100, // Higher = better recall, slower
      "number_of_shards": 1, // Single shard for small datasets
      "number_of_replicas": 0, // No replicas for development
      "refresh_interval": "30s", // Slower refresh for bulk indexing
      "max_result_window": 10000 // Increase for deep pagination
    }
  }
}
```

### kNN Query Optimization Tips

1. **k value sizing**:
   - With pre-filtering (2.17+): k = desired_results × 1.2-2
   - With post-filtering (2.11): k = desired_results × 5-10
2. **ef_search tuning**: Start with 100, increase for better recall
3. **Filter placement**: Use pre-filtering (inside kNN) in OpenSearch 2.17+ for better performance
4. **Batch queries**: Use multi-search API for multiple concurrent searches
5. **Cache warming**: Pre-load frequently accessed vectors into memory

---

This schema documentation provides the complete technical specification for the OpenSearch semantic search implementation. It should be referenced for all development, troubleshooting, and maintenance activities.
