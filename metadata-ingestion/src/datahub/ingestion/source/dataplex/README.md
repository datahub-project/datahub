# Google Dataplex Source - Developer Guide

This directory contains the DataHub connector for Google Dataplex.

**For user documentation, setup instructions, and configuration examples, see the Dataplex connector documentation at docs.datahub.com.**

## Implementation Overview

This connector extracts metadata from Google Dataplex entities (discovered tables/filesets) and maps them to DataHub datasets using **source platform URNs** (BigQuery, GCS, etc.) to align with native source connectors.

### Architecture

The connector follows the pattern established by the `bigquery_v2` source:

- Uses Google Cloud client libraries (`google-cloud-dataplex`, `google-cloud-datacatalog-lineage`)
- Supports both service account credentials and Application Default Credentials
- **Generates datasets with source platform URNs** (no Dataplex-specific containers)
- Links BigQuery entities to BigQuery dataset containers for consistent navigation
- Preserves Dataplex context (lake, zone, asset) as custom properties
- Supports parallel zone processing with `ThreadedIteratorExecutor`

### Entity Mapping

| Dataplex Resource | DataHub Entity Type | URN Platform | Container                           |
| ----------------- | ------------------- | ------------ | ----------------------------------- |
| Entity (BigQuery) | Dataset             | `bigquery`   | BigQuery dataset container          |
| Entity (GCS)      | Dataset             | `gcs`        | None (matches GCS connector)        |
| Lake/Zone/Asset   | N/A                 | N/A          | Preserved as custom properties only |

**Key Design Decision**: Dataplex entities use their source platform URNs instead of "dataplex" platform URNs. This ensures:

- Entities discovered by Dataplex appear in the same containers as entities from native BigQuery/GCS connectors
- No duplication when running multiple connectors (same URN = same entity)
- Consistent user experience regardless of discovery method
- Dataplex context is preserved via custom properties for traceability

### Dual API Architecture

The connector supports extracting metadata from two complementary Google Cloud APIs:

#### 1. **Entries API (Universal Catalog)** - Primary, Default Enabled

The Entries API accesses Google Cloud's Universal Catalog, which provides a centralized view of metadata across Google Cloud resources.

**Key Characteristics:**

- **System-managed entry groups**: `@bigquery`, `@pubsub`, `@datacatalog`, etc.
- **Comprehensive coverage**: Automatically discovers BigQuery tables, Pub/Sub topics, and other resources
- **Multi-region access**: Requires multi-region locations (`us`, `eu`, `asia`) to access system entry groups
- **Richer metadata**: Provides schema information and detailed aspects

**When to use:**

- ✅ Discovering all BigQuery tables across projects
- ✅ Accessing system-managed resources
- ✅ Getting the most complete metadata available
- ✅ **Recommended for most users**

**Configuration:**

```yaml
include_entries: true # Default
entries_location: "us" # Multi-region required for system entry groups
```

#### 2. **Entities API (Lakes/Zones)** - Optional, Default Disabled

The Entities API accesses Dataplex's lake and zone structure, providing entity-level metadata for resources managed through Dataplex.

**Key Characteristics:**

- **Lake/zone hierarchy**: Organized by Dataplex lakes and zones
- **Discovered assets**: Tables and filesets explicitly added to Dataplex
- **Regional access**: Uses specific regional locations (`us-central1`, etc.)
- **Dataplex context**: Direct lake/zone/asset association

**When to use:**

- Use if you need entity-level details specific to lakes/zones not available in Entries API
- Can be used alongside Entries API (duplicates are automatically handled)

**Configuration:**

```yaml
include_entities: true # Optional
location: "us-central1" # Regional location for entities
```

#### API Coordination

When both APIs are enabled:

1. **Processing Order**: Entities API → Entries API
2. **Aspect Replacement**: Entries completely replace entity aspects for the same resource (same URN)
3. **Source of Truth**: Universal Catalog (Entries API) takes precedence
4. **Data Loss**: Entity custom properties (lake, zone, asset) are LOST when entries overwrite

**⚠️ Important Limitation:**

When the same table is discovered by both APIs, DataHub's aspect-level replacement means:

- ✅ Entry metadata (schema, entry custom properties) is preserved
- ❌ Entity metadata (lake, zone, asset custom properties) is **completely lost**

This is DataHub's standard behavior (not a bug). Aspects are replaced atomically.

**Recommendation:**

- Use **Entries API only** (default) for most use cases
- Use **Entities API only** if you need lake/zone organizational context
- Only use both if APIs discover **non-overlapping datasets**

**Example with both APIs (not recommended for overlapping data):**

```yaml
source:
  type: dataplex
  config:
    project_ids: ["my-project"]

    # Entries API (primary)
    include_entries: true
    entries_location: "us"

    # Entities API (optional - WARNING: overlapping tables lose entity context)
    include_entities: true
    location: "us-central1"
```

### Key Components

- **[dataplex.py](dataplex.py)**: Main source implementation with dual API extraction logic
- **[dataplex_config.py](dataplex_config.py)**: Configuration models using Pydantic
- **[dataplex_report.py](dataplex_report.py)**: Reporting and metrics tracking
- **[dataplex_helpers.py](dataplex_helpers.py)**: Helper functions for URN generation and type mapping
- **[dataplex_lineage.py](dataplex_lineage.py)**: Lineage extraction using Dataplex Lineage API

### Capabilities

The connector implements the following DataHub capabilities:

- `SCHEMA_METADATA`: Schema information from discovered entities
- `LINEAGE_COARSE`: Table-level lineage extraction via Dataplex Lineage API (when enabled)
- **Platform Alignment**: Entities use source platform URNs (BigQuery, GCS) and containers

## Development Setup

### Prerequisites

1. Python 3.8+
2. DataHub development environment set up
3. Access to a GCP project with Dataplex enabled

### Install Development Dependencies

```bash
cd metadata-ingestion
./scripts/install_deps.sh
```

### Run Linting

```bash
./gradlew :metadata-ingestion:lintFix
```

### Run Tests

```bash
./gradlew :metadata-ingestion:testQuick
```

## Project Structure

```
dataplex/
├── __init__.py                   # Package exports
├── dataplex.py                   # Main source implementation
├── dataplex_config.py            # Configuration classes
├── dataplex_report.py            # Reporting and metrics
├── dataplex_helpers.py           # Helper functions and utilities
├── dataplex_lineage.py           # Lineage extraction
├── README.md                     # This file (developer guide)
├── TEST_GUIDE.md                 # Testing documentation
├── TESTING.md                    # Test implementation details
└── example_code/                 # Reference examples and experiments
    ├── README.md
    ├── dataplex_client.py
    ├── generate_sample_lineage.py  # Script to create sample lineage
    └── dataplex_implementation.md
```

## Implementation Notes

### URN Generation

Entities are generated using **source platform URNs** for consistency with native connectors:

- **BigQuery Entities**: `urn:li:dataset:(urn:li:dataPlatform:bigquery,{project}.{dataset}.{table},PROD)`
- **GCS Entities**: `urn:li:dataset:(urn:li:dataPlatform:gcs,{bucket}/{path},PROD)`

The connector uses `make_dataset_urn_with_platform_instance()` with the source platform determined by querying the Dataplex asset.

### Container Linking

- **BigQuery entities**: Linked to BigQuery dataset containers using `BigQueryDatasetKey`
- **GCS entities**: No container (matches GCS connector behavior)
- **Implementation**: Self-contained `make_bigquery_dataset_container_key()` helper in `dataplex_helpers.py`

### Custom Properties

Dataplex context is preserved on each dataset via custom properties:

```python
custom_properties = {
    "dataplex_ingested": "true",
    "dataplex_lake": lake_id,
    "dataplex_zone": zone_id,
    "dataplex_entity_id": entity_id,
    "dataplex_zone_type": zone.type_.name,
    "data_path": entity.data_path,
    "system": entity.system.name,
    "format": entity.format.format_.name,
}
```

### Parallel Processing

Entity extraction is parallelized at the zone level using `ThreadedIteratorExecutor`:

- Configurable via `max_workers` config option (default: 10)
- Each zone's entities are processed by a worker thread
- Thread-safe locks protect shared data structures (asset metadata, zone metadata, entity tracking)

### Entries API Implementation

The Entries API (Universal Catalog) extraction provides comprehensive metadata discovery:

#### Entry Group Discovery

The connector discovers entry groups using the Universal Catalog API:

```python
# List entry groups in project/location
parent = f"projects/{project_id}/locations/{entries_location}"
entry_groups_request = ListEntryGroupsRequest(parent=parent)
entry_groups = catalog_client.list_entry_groups(request=entry_groups_request)
```

**System-managed entry groups** (like `@bigquery`, `@pubsub`) contain automatically discovered resources and require multi-region locations (`us`, `eu`, `asia`).

#### Entry Processing

For each entry group, the connector:

1. **Lists entries** in the group
2. **Fetches full details** with `EntryView.ALL` to get schema and aspects
3. **Extracts fully qualified name (FQN)** from entry aspects
4. **Determines source platform** from FQN prefix (e.g., `bigquery:` → platform=`bigquery`)
5. **Generates source platform URN** for consistency with native connectors

```python
# Extract FQN from entry aspects
fqn = entry.fully_qualified_name  # e.g., "bigquery:project.dataset.table"

# Parse platform and resource path
platform, resource_path = fqn.split(":", 1)  # "bigquery", "project.dataset.table"

# Generate URN using source platform
dataset_urn = make_dataset_urn_with_platform_instance(
    platform=platform,
    name=resource_path,
    platform_instance=None,
    env=env,
)
```

#### Schema Extraction from Entries

The Entries API provides rich schema metadata through entry aspects:

```python
# Extract schema from entry aspects
for aspect in entry.aspects:
    if aspect.type_ == "schema":
        # Parse schema aspect data
        schema_aspect = aspect.value
        # Convert to DataHub SchemaMetadata
        schema_metadata = extract_schema_from_entry_aspects(entry, ...)
```

**Schema fields extracted:**

- Column names and types
- Column descriptions
- Nullability constraints
- Data type mappings (BigQuery → DataHub standard types)

#### Custom Properties from Entry Metadata

Entry metadata is preserved as custom properties:

```python
custom_properties = {
    "dataplex_ingested": "true",
    "dataplex_entry_id": entry_id,
    "dataplex_entry_group": entry_group_id,
    "entry_source_system": entry.entry_source.system,
    "entry_type": entry.entry_type,
    "fully_qualified_name": fqn,
}
```

#### Location Requirements

**Critical**: The `entries_location` config must be a **multi-region location** (`us`, `eu`, `asia`) to access system-managed entry groups:

```yaml
# ✅ Correct - multi-region for system entry groups
entries_location: "us"

# ❌ Incorrect - regional locations only have placeholder entries
entries_location: "us-central1"  # Will miss BigQuery tables!
```

The connector validates this and warns if a regional location is detected.

#### Memory Optimization

Entries are processed with batched emission to prevent memory issues:

```python
# Batch entries emission
if len(cached_entries_mcps) >= batch_size:
    # Emit batch to DataHub
    for mcp in cached_entries_mcps:
        yield mcp.as_workunit()
    # Clear cache
    cached_entries_mcps.clear()
```

This ensures memory usage stays bounded even with 50k+ entries.

### Lineage Extraction

The connector extracts lineage using Google's Data Lineage API:

#### Architecture

The lineage extraction follows the pattern from `bigquery_v2`:

- **[dataplex_lineage.py](dataplex_lineage.py)**: `DataplexLineageExtractor` class
- Queries Dataplex Lineage API using `LineageClient`
- Builds internal lineage maps with `LineageEdge` data structures
- Generates DataHub `UpstreamLineageClass` aspects

#### How It Works

1. **Entity Tracking**: As entities are processed, their IDs are tracked
2. **Lineage Query**: After entity extraction, queries `search_links` API
3. **Map Building**: Constructs lineage maps from upstream/downstream relationships
4. **Workunit Generation**: Creates DataHub lineage workunits
5. **Emission**: Yields workunits to be ingested into DataHub

#### Configuration Options

```python
# Enable/disable lineage extraction
extract_lineage: bool = True  # Default: True
```

#### Lineage API Integration

The connector uses Google's `LineageClient` to query lineage:

```python
# Search for upstream lineage (entity is target)
request = SearchLinksRequest(parent=parent, target=entity_reference)
upstream_links = lineage_client.search_links(request=request)

# Search for downstream lineage (entity is source)
request = SearchLinksRequest(parent=parent, source=entity_reference)
downstream_links = lineage_client.search_links(request=request)
```

#### Data Structures

```python
@dataclass
class LineageEdge:
    entity_id: str  # Upstream entity ID
    audit_stamp: datetime
    lineage_type: str  # TRANSFORMED, COPY, etc.
```

#### Limitations

- Dataplex does not support column-level lineage extraction
- Lineage is only available for entities with active lineage tracking
- Retention period: 30 days (Dataplex limitation)
- Cross-region lineage is not supported by Dataplex

## Contributing

When contributing to this connector:

1. Follow patterns from `vertexai` and `bigquery_v2` sources
2. Add type hints to all functions
3. Update both developer README (this file) and user docs
4. Add unit tests for new functionality
5. Run linting: `./gradlew :metadata-ingestion:lintFix`
6. Follow the DataHub code standards (see CLAUDE.md in the repo root)

## References

- [Dataplex API Documentation](https://cloud.google.com/dataplex/docs)
- [DataHub Developer Guide](https://datahubproject.io/docs/developers)
