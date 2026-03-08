# DataHub Pinecone Connector - Implementation Plan

## Overview

This document outlines the implementation plan for a DataHub connector to ingest metadata from Pinecone, a vector database platform. The connector will extract information about indexes, namespaces, and vector metadata to provide visibility into vector data assets within DataHub.

## Pinecone Architecture

### Key Concepts

1. **Indexes**: Top-level organizational units that store vector data
   - Dense indexes: Store dense vectors for semantic search
   - Sparse indexes: Store sparse vectors for lexical/keyword search
   - Each index has a defined dimension and similarity metric

2. **Namespaces**: Logical partitions within an index
   - Used for multitenancy and data isolation
   - All operations (upsert, query, fetch) target a specific namespace
   - Created automatically during upsert operations

3. **Vectors**: Basic units of data
   - Each vector has an ID, vector values, and optional metadata
   - Metadata: Flat JSON key-value pairs (max 40KB per record)
   - Metadata supports filtering during queries

4. **Hosting Environments**:
   - Serverless indexes
   - Pod-based indexes

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

1. **Platform**: `pinecone`
   - Platform instance: Environment/project identifier

2. **Container (Index Level)**
   - URN: `urn:li:container:<guid-from-index-name>`
   - SubType: `Index`
   - Properties:
     - Index name
     - Dimension
     - Metric (cosine, euclidean, dotproduct)
     - Index type (dense/sparse)
     - Host URL
     - Pod configuration (for pod-based)
     - Total vector count
     - Status

3. **Container (Namespace Level)**
   - URN: `urn:li:container:<guid-from-index-namespace>`
   - SubType: `Namespace`
   - Properties:
     - Namespace name
     - Vector count
     - Indexed metadata fields
   - Parent: Index container

4. **Dataset (Vector Collection)**
   - URN: `urn:li:dataset:(urn:li:dataPlatform:pinecone,<index>.<namespace>,PROD)`
   - SubType: `Vector Collection`
   - Schema: Inferred from metadata fields across sampled vectors
   - Properties:
     - Vector dimension
     - Sample metadata
     - Record count

## Implementation Structure

### File Organization

```
metadata-ingestion/src/datahub/ingestion/source/pinecone/
├── __init__.py
├── pinecone_source.py          # Main source class
├── config.py                    # Configuration model
├── report.py                    # Reporting and statistics
└── pinecone_client.py           # API client wrapper
```

### Configuration Schema

```python
class PineconeConfig(
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    StatefulIngestionConfigBase
):
    # Authentication
    api_key: TransparentSecretStr
    environment: Optional[str] = None  # For pod-based indexes

    # Connection
    index_host_mapping: Optional[Dict[str, str]] = None  # Manual host mapping

    # Filtering
    index_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    namespace_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    # Schema inference
    enable_schema_inference: bool = True
    schema_sampling_size: int = 100  # Vectors to sample per namespace
    max_metadata_fields: int = 100   # Max fields in inferred schema

    # Performance
    max_workers: int = 5  # Parallel processing

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
```

### Core Classes

#### 1. PineconeSource

```python
@platform_name("Pinecone")
@config_class(PineconeConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class PineconeSource(StatefulIngestionSourceBase):
    """
    DataHub source for Pinecone vector database.

    Extracts:
    - Index metadata (dimension, metric, configuration)
    - Namespace information (vector counts)
    - Inferred schemas from vector metadata
    """

    def __init__(self, config: PineconeConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.client = PineconeClient(config)
        self.report = PineconeSourceReport()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # 1. List all indexes
        # 2. For each index, emit container workunit
        # 3. List namespaces in each index
        # 4. For each namespace, emit container + dataset workunits
        # 5. Sample vectors and infer schema
        pass
```

#### 2. PineconeClient

```python
class PineconeClient:
    """Wrapper around Pinecone SDK for metadata extraction."""

    def __init__(self, config: PineconeConfig):
        self.config = config
        self.pc = Pinecone(api_key=config.api_key.get_secret_value())

    def list_indexes(self) -> List[IndexInfo]:
        """List all indexes in the account."""
        pass

    def describe_index(self, index_name: str) -> IndexDescription:
        """Get detailed information about an index."""
        pass

    def list_namespaces(self, index_name: str) -> List[str]:
        """List all namespaces in an index."""
        pass

    def describe_namespace(self, index_name: str, namespace: str) -> NamespaceStats:
        """Get statistics for a namespace (serverless only)."""
        pass

    def sample_vectors(
        self,
        index_name: str,
        namespace: str,
        limit: int
    ) -> List[VectorRecord]:
        """Sample vectors from a namespace for schema inference."""
        pass
```

#### 3. Schema Inference

```python
class MetadataSchemaInferrer:
    """Infers DataHub schema from Pinecone vector metadata."""

    def infer_schema(
        self,
        vectors: List[VectorRecord],
        max_fields: int
    ) -> SchemaMetadata:
        """
        Analyze metadata from sampled vectors and create schema.

        - Aggregate all metadata keys
        - Infer types (string, number, boolean, array)
        - Track field frequency
        - Generate SchemaField objects
        """
        pass
```

## API Integration

### Pinecone SDK Usage

```python
from pinecone import Pinecone, ServerlessSpec

# Initialize client
pc = Pinecone(api_key="YOUR_API_KEY")

# List indexes
indexes = pc.list_indexes()

# Describe index
index_description = pc.describe_index("my-index")

# Connect to index
index = pc.Index("my-index")

# Get index stats
stats = index.describe_index_stats()

# List namespaces (serverless only)
# Via describe_namespace API endpoint

# Sample vectors
# Use query with random vectors or fetch by IDs
results = index.query(
    vector=[0.1] * dimension,
    top_k=100,
    namespace="my-namespace",
    include_metadata=True
)
```

### API Endpoints

1. **Control Plane** (via SDK):
   - `list_indexes()` - Get all indexes
   - `describe_index(name)` - Get index configuration

2. **Data Plane** (via Index object):
   - `describe_index_stats()` - Get vector counts per namespace
   - `query()` - Sample vectors with metadata
   - `fetch()` - Get specific vectors by ID

3. **Serverless-specific**:
   - `describe_namespace()` - Get namespace details (REST API)

## Metadata Extraction Flow

### Phase 1: Index Discovery

```
1. Call pc.list_indexes()
2. For each index:
   - Apply index_pattern filter
   - Call describe_index() for details
   - Emit Container workunit (Index level)
   - Extract: name, dimension, metric, host, pod_type, replicas
```

### Phase 2: Namespace Discovery

```
1. For each index:
   - Connect to index: pc.Index(name)
   - Call describe_index_stats() to get namespaces
   - For serverless: Call describe_namespace() for each namespace
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

### Phase 4: Lineage & Relationships

```
1. Set container relationships:
   - Namespace → parent Index
   - Dataset → parent Namespace
2. Add platform instance information
3. Add browse paths
```

## Dependencies

### Required Python Packages

```python
# setup.py additions
pinecone_requires = {
    "pinecone-client>=3.0.0",
}

entry_points = {
    "datahub.ingestion.source.plugins": [
        "pinecone = datahub.ingestion.source.pinecone.pinecone_source:PineconeSource",
    ],
}
```

## Testing Strategy

### Unit Tests

```
tests/unit/test_pinecone_source.py
- Test configuration validation
- Test URN generation
- Test schema inference logic
- Test filtering patterns
- Mock Pinecone API responses
```

### Integration Tests

```
tests/integration/pinecone/
- Test against real Pinecone instance
- Requires API key in environment
- Test index discovery
- Test namespace listing
- Test vector sampling
- Test end-to-end ingestion
```

### Test Data Setup

```python
# Create test index
pc.create_index(
    name="test-index",
    dimension=384,
    metric="cosine",
    spec=ServerlessSpec(cloud="aws", region="us-east-1")
)

# Upsert test vectors with metadata
index.upsert(vectors=[
    {
        "id": "vec1",
        "values": [0.1] * 384,
        "metadata": {
            "source": "test",
            "category": "example",
            "score": 0.95,
            "tags": ["tag1", "tag2"]
        }
    }
], namespace="test-namespace")
```

## Documentation

### Integration Documentation

Create `metadata-ingestion/integration_docs/sources/pinecone/pinecone.md`:

```markdown
# Pinecone

## Overview

Ingest metadata from Pinecone vector database into DataHub.

## Capabilities

- Extract index configurations
- Discover namespaces
- Infer schemas from vector metadata
- Support for both serverless and pod-based indexes

## Configuration

...

## Compatibility

- Pinecone API version: 3.0+
- Supports serverless and pod-based indexes

## Prerequisites

- Pinecone API key
- Network access to Pinecone API
```

### Recipe Examples

```yaml
# Basic recipe
source:
  type: pinecone
  config:
    api_key: "${PINECONE_API_KEY}"
    enable_schema_inference: true
    schema_sampling_size: 100

# Advanced recipe with filtering
source:
  type: pinecone
  config:
    api_key: "${PINECONE_API_KEY}"
    platform_instance: "production"
    index_pattern:
      allow:
        - "prod-.*"
      deny:
        - ".*-test"
    namespace_pattern:
      allow:
        - "customer-.*"
    enable_schema_inference: true
    schema_sampling_size: 200
    max_metadata_fields: 150
    stateful_ingestion:
      enabled: true
```

## Implementation Phases

### Phase 1: Core Functionality (MVP)

- [ ] Basic configuration and authentication
- [ ] Index discovery and metadata extraction
- [ ] Container workunits for indexes
- [ ] Basic reporting

### Phase 2: Namespace Support

- [ ] Namespace discovery via describe_index_stats()
- [ ] Namespace containers
- [ ] Serverless namespace details
- [ ] Namespace filtering

### Phase 3: Schema Inference

- [ ] Vector sampling logic
- [ ] Metadata aggregation
- [ ] Type inference
- [ ] Schema generation
- [ ] Dataset workunits

### Phase 4: Advanced Features

- [ ] Stateful ingestion
- [ ] Stale entity removal
- [ ] Performance optimization (parallel processing)
- [ ] Pod-based index support
- [ ] Browse paths v2

### Phase 5: Polish

- [ ] Comprehensive testing
- [ ] Documentation
- [ ] Example recipes
- [ ] Error handling improvements
- [ ] Logging and debugging

## Challenges & Considerations

### 1. Namespace Listing

- **Challenge**: `describe_namespace()` only works for serverless indexes
- **Solution**: Use `describe_index_stats()` which returns namespace names and counts for both serverless and pod-based

### 2. Vector Sampling

- **Challenge**: No direct "list all vectors" API
- **Options**:
  - Use `query()` with random/dummy vectors to get samples
  - Use `fetch()` if vector IDs are known
  - Use `list()` to get vector IDs, then fetch
- **Recommendation**: Use `list()` + `fetch()` for deterministic sampling

### 3. Schema Inference Performance

- **Challenge**: Sampling many vectors can be slow
- **Solution**:
  - Configurable sampling size
  - Parallel processing across namespaces
  - Cache results for incremental runs

### 4. Metadata Size Limits

- **Challenge**: Pinecone supports 40KB metadata per vector
- **Solution**:
  - Limit number of fields extracted
  - Truncate large string values in samples
  - Configurable max_metadata_fields

### 5. Authentication

- **Challenge**: Different auth for serverless vs pod-based
- **Solution**: Pinecone SDK v3+ handles this automatically with API key

### 6. Rate Limiting

- **Challenge**: API rate limits may affect large-scale ingestion
- **Solution**:
  - Implement exponential backoff
  - Configurable delays between requests
  - Batch operations where possible

## Success Criteria

1. Successfully extract metadata from Pinecone indexes
2. Create proper container hierarchy (Index → Namespace → Dataset)
3. Infer meaningful schemas from vector metadata
4. Support both serverless and pod-based indexes
5. Handle errors gracefully with clear reporting
6. Provide comprehensive documentation
7. Pass all unit and integration tests

## Future Enhancements

1. **Usage Statistics**: Track query patterns if API provides access
2. **Lineage**: Connect to embedding model sources if detectable
3. **Data Quality**: Validate vector dimensions, detect anomalies
4. **Cost Tracking**: Extract pod/serverless cost information
5. **Performance Metrics**: Index query latency, throughput stats
6. **Embedding Model Metadata**: Track which models generated vectors
7. **Integration with DataHub AI**: Connect to semantic search features

## References

- [Pinecone Documentation](https://docs.pinecone.io/)
- [Pinecone Python SDK](https://github.com/pinecone-io/pinecone-python-client)
- [DataHub Source Development Guide](https://datahubproject.io/docs/metadata-ingestion/developing)
- [MongoDB Source](metadata-ingestion/src/datahub/ingestion/source/mongodb.py) - Similar NoSQL pattern
- [Elasticsearch Source](metadata-ingestion/src/datahub/ingestion/source/elastic_search.py) - Similar search/index pattern
