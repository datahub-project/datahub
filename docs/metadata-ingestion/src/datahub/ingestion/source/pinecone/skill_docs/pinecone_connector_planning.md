# DataHub Pinecone Connector - Implementation Plan

## Overview

This document outlines the implementation plan for a DataHub connector to ingest metadata from Pinecone, a vector database platform. The connector extracts information about indexes, namespaces, and vector metadata to provide visibility into vector data assets within DataHub.

---

## Pinecone Architecture

### Key Concepts

**1. Indexes**: Top-level organizational units that store vector data

- Dense indexes: Store dense vectors for semantic search
- Sparse indexes: Store sparse vectors for lexical/keyword search
- Each index has a defined dimension and similarity metric

**2. Namespaces**: Logical partitions within an index

- Used for multitenancy and data isolation
- All operations (upsert, query, fetch) target a specific namespace
- Created automatically during upsert operations

**3. Vectors**: Basic units of data

- Each vector has an ID, vector values, and optional metadata
- Metadata: Flat JSON key-value pairs (max 40KB per record)
- Metadata supports filtering during queries

**4. Hosting Environments**:

- Serverless indexes
- Pod-based indexes

---

## DataHub Entity Mapping

### Proposed Hierarchy

```
Platform: pinecone
├── Container (Index)
│   ├── Properties: dimension, metric, index_type, host, pod_type, replicas
│   └── Container (Namespace)
│       ├── Properties: vector_count, metadata_config
│       └── Dataset (Virtual collection representing vectors)
│           ├── Schema: Inferred from metadata fields
│           └── Properties: sample_metadata, vector_dimension
```

### Entity Types

**1. Platform**: `pinecone`

**2. Container (Index Level)**

- URN: `urn:li:container:<guid-from-index-name>`
- SubType: `Index`
- Properties: name, dimension, metric, index type, host URL, pod config, status

**3. Container (Namespace Level)**

- URN: `urn:li:container:<guid-from-index-namespace>`
- SubType: `Namespace`
- Properties: namespace name, vector count, indexed metadata fields
- Parent: Index container

**4. Dataset (Vector Collection)**

- URN: `urn:li:dataset:(urn:li:dataPlatform:pinecone,<index>.<namespace>,PROD)`
- SubType: `Vector Collection`
- Schema: Inferred from metadata fields across sampled vectors

---

## Implementation Structure

### File Organization

```
metadata-ingestion/src/datahub/ingestion/source/pinecone/
├── __init__.py
├── pinecone_source.py     # Main source class
├── config.py              # Configuration model
├── report.py              # Reporting and statistics
└── pinecone_client.py     # API client wrapper
```

### Configuration Schema

```python
class PineconeConfig(
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    StatefulIngestionConfigBase
):
    api_key: TransparentSecretStr
    environment: Optional[str] = None
    index_host_mapping: Optional[Dict[str, str]] = None
    index_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    namespace_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    enable_schema_inference: bool = True
    schema_sampling_size: int = 100
    max_metadata_fields: int = 100
    max_workers: int = 5
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
```

---

## API Integration

### Pinecone SDK Usage

```python
from pinecone import Pinecone, ServerlessSpec

pc = Pinecone(api_key="YOUR_API_KEY")
indexes = pc.list_indexes()
index_description = pc.describe_index("my-index")
index = pc.Index("my-index")
stats = index.describe_index_stats()
results = index.query(
    vector=[0.1] * dimension,
    top_k=100,
    namespace="my-namespace",
    include_metadata=True
)
```

---

## Metadata Extraction Flow

### Phase 1: Index Discovery

```
1. Call pc.list_indexes()
2. For each index:
   - Apply index_pattern filter
   - Call describe_index() for details
   - Emit Container workunit (Index level)
```

### Phase 2: Namespace Discovery

```
1. For each index:
   - Call describe_index_stats() to get namespaces
   - Apply namespace_pattern filter
   - Emit Container workunit (Namespace level)
   - Set parent container to Index
```

### Phase 3: Schema Inference

```
1. For each namespace:
   - Sample vectors using query() or fetch()
   - Collect metadata from sampled vectors
   - Aggregate metadata keys and infer types
   - Build SchemaMetadata with SchemaFields
   - Emit Dataset workunit with schema
```

---

## Implementation Phases

### Phase 1: Core Functionality (MVP)

- Basic configuration and authentication
- Index discovery and metadata extraction
- Container workunits for indexes
- Basic reporting

### Phase 2: Namespace Support

- Namespace discovery via describe_index_stats()
- Namespace containers
- Namespace filtering

### Phase 3: Schema Inference

- Vector sampling logic
- Metadata aggregation
- Type inference
- Schema generation
- Dataset workunits

### Phase 4: Advanced Features

- Stateful ingestion
- Stale entity removal
- Performance optimization (parallel processing)
- Pod-based index support

### Phase 5: Polish

- Comprehensive testing
- Documentation
- Example recipes
- Error handling improvements

---

## Challenges and Considerations

### Namespace Listing

**Challenge:** `describe_namespace()` only works for serverless indexes

**Solution:** Use `describe_index_stats()` which returns namespace names and counts for both serverless and pod-based

### Vector Sampling

**Challenge:** No direct "list all vectors" API

**Recommendation:** Use `list()` + `fetch()` for deterministic sampling

### Schema Inference Performance

**Challenge:** Sampling many vectors can be slow

**Solution:** Configurable sampling size, parallel processing, caching

### Rate Limiting

**Challenge:** API rate limits may affect large-scale ingestion

**Solution:** Implement exponential backoff, configurable delays

---

## Success Criteria

1. Successfully extract metadata from Pinecone indexes
2. Create proper container hierarchy (Index -> Namespace -> Dataset)
3. Infer meaningful schemas from vector metadata
4. Support both serverless and pod-based indexes
5. Handle errors gracefully with clear reporting
6. Provide comprehensive documentation
7. Pass all unit and integration tests

---

## References

- [Pinecone Documentation](https://docs.pinecone.io/)
- [Pinecone Python SDK](https://github.com/pinecone-io/pinecone-python-client)
- [DataHub Source Development Guide](https://datahubproject.io/docs/metadata-ingestion/developing)

---

**Status:** Planning Complete | **Version:** 1.0.0 | **Last Updated:** 2026
