# Google Dataplex Source - Developer Guide

This directory contains the DataHub connector for Google Dataplex.

**For user documentation, setup instructions, and configuration examples, see:**

- [Dataplex Connector Documentation](../../../docs/sources/dataplex/dataplex_pre.md)

## Implementation Overview

This connector extracts metadata from Google Dataplex and maps it to DataHub's metadata model.

### Architecture

The connector follows the pattern established by the `vertexai` and `bigquery_v2` sources:

- Uses Google Cloud client libraries (`google-cloud-dataplex`)
- Supports both service account credentials and Application Default Credentials
- Implements hierarchical container relationships
- Supports schema metadata extraction and sibling relationships

### Entity Mapping

| Dataplex Resource | DataHub Entity Type | Container Hierarchy Level |
| ----------------- | ------------------- | ------------------------- |
| Project           | Container           | Level 1 (root)            |
| Lake              | Container           | Level 2                   |
| Zone              | Container           | Level 3                   |
| Asset             | Container           | Level 4                   |
| Entity            | Dataset             | Leaf node                 |

### Key Components

- **[dataplex.py](dataplex.py)**: Main source implementation with extraction logic
- **[dataplex_config.py](dataplex_config.py)**: Configuration models using Pydantic
- **[dataplex_report.py](dataplex_report.py)**: Reporting and metrics tracking
- **[dataplex_helpers.py](dataplex_helpers.py)**: Helper functions for URN generation and type mapping

### Capabilities

The connector implements the following DataHub capabilities:

- `CONTAINERS`: Hierarchical container extraction for Projects, Lakes, Zones, and Assets
- `SCHEMA_METADATA`: Schema information from discovered entities
- `LINEAGE_COARSE`: Lineage extraction via Dataplex Lineage API (when enabled)

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
├── README.md                     # This file (developer guide)
├── TEST_GUIDE.md                 # Testing documentation
├── TESTING.md                    # Test implementation details
└── example_code/                 # Reference examples and experiments
    ├── README.md
    ├── dataplex_client.py
    └── dataplex_implementation.md
```

## Implementation Notes

### Container Key Hierarchy

The connector uses custom `ContainerKey` classes to represent the hierarchical structure:

```python
ProjectIdKey                    # Base container for GCP project
└── DataplexLakeKey            # Lake within project
    └── DataplexZoneKey        # Zone within lake
        └── DataplexAssetKey   # Asset within zone
```

### URN Generation

- **Dataplex Entities**: `urn:li:dataset:(urn:li:dataPlatform:dataplex,{project_id}.{entity_id},{env})`
- **Source Platform Entities**: `urn:li:dataset:(urn:li:dataPlatform:{platform},{project_id}.{entity_id},{env})`
- **Containers**: Generated using `gen_containers()` with hierarchical relationships

### Sibling Relationship Logic

When `create_sibling_relationships=True`:

- Dataplex entity determines its source platform (BigQuery/GCS) by querying the asset
- Creates bidirectional sibling links between Dataplex and source platform URNs
- Primary sibling designation controlled by `dataplex_is_primary_sibling` config

## Contributing

When contributing to this connector:

1. Follow patterns from `vertexai` and `bigquery_v2` sources
2. Add type hints to all functions
3. Update both developer README (this file) and user docs (`../../../docs/sources/dataplex/`)
4. Add unit tests for new functionality
5. Run linting: `./gradlew :metadata-ingestion:lintFix`
6. Follow the [DataHub code standards](../../../../CLAUDE.md)

## References

- [User Documentation](../../../docs/sources/dataplex/dataplex_pre.md)
- [Dataplex API Documentation](https://cloud.google.com/dataplex/docs)
- [DataHub Developer Guide](https://datahubproject.io/docs/developers)
