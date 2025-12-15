# Event Specifications and Tracking Scenarios Implementation

## Overview

This document describes the implementation of Event Specifications and Tracking Scenarios extraction for the Snowplow DataHub connector, including lineage relationships between these entities and existing data structures.

## Features Implemented

### 1. Event Specifications Extraction
- **What**: Event specifications are high-level tracking plans in Snowplow BDP that define which events should be tracked
- **DataHub Entity**: Represented as Datasets with subtype "Event Specification"
- **API Endpoint**: `GET /organizations/{orgId}/event-specs/v1`
- **API Documentation**: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/

### 2. Tracking Scenarios Extraction
- **What**: Tracking scenarios are containers that group related event specifications
- **DataHub Entity**: Represented as Containers with subtype "Tracking Scenario"
- **API Endpoint**: `GET /organizations/{orgId}/tracking-scenarios/v1`
- **API Documentation**: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/

### 3. Lineage Relationships

#### Event Spec → Schema Lineage
- Event specifications reference one or more event schemas
- Implemented as UpstreamLineage relationships
- Schema references extracted from `event_spec.event_schemas` array
- Lineage type: `TRANSFORMED` (event specs transform/define schemas)

#### Tracking Scenario → Event Spec Relationships
- Tracking scenarios contain multiple event specifications
- Implemented as Container relationships
- Event specs become children of tracking scenario containers
- Extracted from `scenario.event_specs` array

## Architecture

### API Response Format Differences

**Important**: Different Snowplow API endpoints use different response formats:

1. **Data Structures API** (per Swagger spec):
   - Returns: Direct array `[...]`
   - Example: `[{schema1}, {schema2}]`

2. **Event Specifications API** (per official docs):
   - Returns: Wrapped format `{"data": [...], "includes": [...], "errors": [...]}`
   - Example: `{"data": [{spec1}], "includes": [], "errors": []}`

3. **Tracking Scenarios API** (per official docs):
   - Returns: Wrapped format `{"data": [...], "includes": [...], "errors": [...]}`
   - Example: `{"data": [{scenario1}], "includes": [], "errors": []}`

### Code Structure

#### Models (`snowplow_models.py`)

**EventSpecification** (lines 157-174)
```python
class EventSpecification(BaseModel):
    id: str
    name: str
    description: Optional[str]
    event_schemas: List[EventSchemaReference]  # References to schemas
    status: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
```

**EventSpecificationsResponse** (lines 177-195)
- Wrapper model for API response
- Contains `data`, `includes`, and `errors` fields
- Includes API documentation URL in docstring

**TrackingScenario** (lines 203-220)
```python
class TrackingScenario(BaseModel):
    id: str
    name: str
    description: Optional[str]
    event_specs: List[str]  # Event spec IDs
    status: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
```

**TrackingScenariosResponse** (lines 223-240)
- Wrapper model for API response
- Contains `data`, `includes`, and `errors` fields
- Includes API documentation URL in docstring

#### Client (`snowplow_client.py`)

**get_event_specifications()** (lines 321-364)
- Fetches event specifications from BDP API
- Parses wrapped response format
- Logs API errors/warnings
- Returns list of EventSpecification objects

**get_tracking_scenarios()** (lines 401-444)
- Fetches tracking scenarios from BDP API
- Parses wrapped response format
- Logs API errors/warnings
- Returns list of TrackingScenario objects

#### Source (`snowplow.py`)

**_extract_event_specifications()** (lines 721-795)
- Extracts event specifications as datasets
- Creates lineage from event specs to referenced schemas
- Emits DatasetProperties, SubTypes, Container, Status, UpstreamLineage, and BrowsePaths aspects

**_extract_tracking_scenarios()** (lines 797-891)
- Extracts tracking scenarios as containers
- Creates container relationships from scenarios to event specs
- Emits Container, ContainerProperties, Status, DataPlatformInstance, SubTypes, and BrowsePaths aspects

### Lineage Implementation

#### Event Spec → Schema Lineage (lines 765-787)

```python
# Add lineage from event spec to referenced schemas
if event_spec.event_schemas:
    upstream_urns = []
    for schema_ref in event_spec.event_schemas:
        # Create schema URN from vendor/name/version
        schema_urn = self._make_schema_dataset_urn(
            vendor=schema_ref.vendor,
            name=schema_ref.name,
            version=schema_ref.version,
        )
        upstream_urns.append(
            UpstreamClass(
                dataset=schema_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        )

    if upstream_urns:
        upstream_lineage = UpstreamLineageClass(upstreams=upstream_urns)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=upstream_lineage,
        ).as_workunit()
```

#### Tracking Scenario → Event Spec Relationships (lines 865-882)

```python
# Add container relationships to event specs referenced in this scenario
if scenario.event_specs:
    scenario_container_urn = str(
        make_container_urn(
            guid=scenario_key.guid(),
        )
    )

    for event_spec_id in scenario.event_specs:
        # Create event spec dataset URN
        event_spec_urn = self._make_event_spec_dataset_urn(event_spec_id)

        # Link event spec to tracking scenario container
        container_aspect = ContainerClass(container=scenario_container_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=event_spec_urn,
            aspect=container_aspect,
        ).as_workunit()
```

## Configuration

Enable event specifications and tracking scenarios in your recipe:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "your-org-uuid"
      api_key_id: "your-key-id"
      api_key: "${SNOWPLOW_API_KEY}"

    # Enable event specifications extraction
    extract_event_specifications: true

    # Enable tracking scenarios extraction
    extract_tracking_scenarios: true
```

## Testing

### Integration Tests

**test_snowplow_event_specs_and_tracking_scenarios** (lines 106-213 in `test_snowplow.py`)
- Tests event specification extraction
- Tests tracking scenario extraction
- Verifies lineage from event specs to schemas
- Verifies container relationships from scenarios to event specs
- Uses golden file comparison: `snowplow_event_specs_golden.json`

### Test Fixtures

**event_specifications_response.json**
- Contains 2 event specifications
- Uses wrapped response format per official API
- Includes `evt-spec-checkout` and `evt-spec-product-interaction`

**tracking_scenarios_response.json**
- Contains 2 tracking scenarios
- Uses wrapped response format per official API
- Includes `scenario-ecommerce` and `scenario-mobile-app`

### Mock Server Support

**mock_bdp_server.py** includes endpoints for:
- `/organizations/{orgId}/event-specs/v1` (lines 243-268)
- `/organizations/{orgId}/tracking-scenarios/v1` (lines 271-296)

Both endpoints:
- Return wrapped format responses
- Include API documentation URLs in docstrings
- Load data from fixture files

## Golden File

**snowplow_event_specs_golden.json** (38 KB)
- Contains complete output from ingestion with event specs and tracking scenarios
- Includes all aspects (properties, subtypes, container, status, lineage, browse paths)
- Verified against test fixtures to ensure correct relationships

## Validation

All features validated against official Snowplow API specifications:
- ✅ Event Specifications API: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/
- ✅ Tracking Scenarios API: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/
- ✅ Wrapped response format correctly implemented
- ✅ API documentation URLs added to code
- ✅ Error/warning logging from API responses

## Code Quality

- ✅ All tests passing (3/3)
- ✅ ruff format: All files formatted correctly
- ✅ mypy: No type errors in 9 source files
- ✅ Golden files created and validated

## Entity Hierarchy

```
Organization (Container)
└── Tracking Scenario (Container)
    └── Event Specification (Dataset)
        └── Schema (Dataset) - via UpstreamLineage
```

## Future Enhancements

Potential additions for future work:
- Data Products extraction (separate entity type)
- Event spec versioning history
- Scenario dependency graph
- Schema evolution tracking
