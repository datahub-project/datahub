# Data Products Implementation

## Overview

This document describes the implementation of Data Products extraction for the Snowplow DataHub connector. Data Products are high-level groupings of event specifications with ownership, business domain, and access information. They help organize tracking design at a business domain level.

## Features Implemented

### 1. Data Products Extraction
- **What**: Data products are business-level groupings of event specifications in Snowplow BDP
- **DataHub Entity**: Represented as Containers with subtype "Data Product"
- **API Endpoint**: `GET /organizations/{orgId}/data-products/v2`
- **API Documentation**: https://docs.snowplow.io/docs/data-product-studio/data-products/api/

### 2. Ownership Tracking
- Data products include owner field (email or team)
- Mapped to DataHub OwnerClass with DATAOWNER type
- Owner information emitted as Ownership aspect

### 3. Custom Properties
- Access instructions
- Source application ID
- Status (draft, published, deprecated)
- Created/updated timestamps
- All stored in ContainerProperties custom properties

### 4. Container Relationships
- Event specifications are linked to data products via container relationships
- Creates hierarchy: Data Product (container) → Event Specification (dataset)

## Architecture

### API Response Format

Data Products API uses v2 endpoint with wrapped response format:

```json
{
  "data": [
    {
      "id": "dp-ecommerce-analytics",
      "name": "E-commerce Analytics",
      "description": "Product analytics for e-commerce events",
      "domain": "Analytics",
      "owner": "analytics-team@company.com",
      "accessInstructions": "Query the atomic.events table...",
      "status": "published",
      "sourceApplicationId": "app-test-ecommerce",
      "eventSpecs": ["evt-spec-checkout", "evt-spec-product-interaction"],
      "createdAt": "2024-01-15T10:00:00Z",
      "updatedAt": "2024-02-20T14:30:00Z"
    }
  ],
  "includes": [],
  "errors": []
}
```

### Code Structure

#### Models (`snowplow_models.py`)

**DataProduct** (lines 267-292)
```python
class DataProduct(BaseModel):
    id: str
    name: str
    description: Optional[str]
    domain: Optional[str]  # Business domain
    owner: Optional[str]  # Owner email or team
    access_instructions: Optional[str]
    status: Optional[str]  # draft, published, deprecated
    event_specs: List[str]  # Event specification IDs
    source_application_id: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
```

**DataProductsResponse** (lines 295-313)
- Wrapper model for API response
- Contains `data`, `includes`, and `errors` fields
- Includes API documentation URL in docstring

#### Client (`snowplow_client.py`)

**get_data_products()** (lines 481-526)
- Fetches data products from BDP API v2 endpoint
- Parses wrapped response format
- Logs API errors/warnings
- Returns list of DataProduct objects

**get_data_product(product_id)** (lines 528-552)
- Fetches specific data product by ID
- Returns single DataProduct or None if not found

#### Source (`snowplow.py`)

**SnowplowDataProductKey** (lines 102-105)
- Container key for data products
- Extends SnowplowOrganizationKey
- Includes product_id field

**_extract_data_products()** (lines 891-1009)
- Extracts data products as containers
- Emits Container, ContainerProperties, Status, DataPlatformInstance, SubTypes, BrowsePaths aspects
- Adds ownership if owner specified
- Adds custom properties for additional metadata
- Links event specifications to data product containers

### Data Product Container Structure

```
Organization (Container)
└── Data Product (Container)
    └── Event Specification (Dataset)
        └── Schema (Dataset) - via UpstreamLineage
```

## Configuration

Enable data products in your recipe:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "your-org-uuid"
      api_key_id: "your-key-id"
      api_key: "${SNOWPLOW_API_KEY}"

    # Enable data products extraction (experimental)
    extract_data_products: true

    # Also enable event specifications (data products reference them)
    extract_event_specifications: true
```

### Filtering

Filter data products using patterns:

```yaml
source:
  type: snowplow
  config:
    # ... connection config ...

    extract_data_products: true

    # Filter patterns
    data_product_pattern:
      allow:
        - "dp-ecommerce.*"
        - "dp-mobile.*"
      deny:
        - ".*-deprecated"
```

## Testing

### Integration Tests

**test_snowplow_data_products** (lines 216-333 in `test_snowplow.py`)
- Tests data product extraction
- Tests ownership tracking
- Tests custom properties
- Verifies container relationships from data products to event specs
- Uses golden file comparison: `snowplow_data_products_golden.json`

### Test Fixtures

**data_products_response.json**
- Contains 2 data products
- Uses wrapped response format per official API
- Includes:
  - `dp-ecommerce-analytics` (published, with 2 event specs)
  - `dp-mobile-engagement` (draft, with 1 event spec)

### Mock Server Support

**mock_bdp_server.py** includes endpoint for:
- `/organizations/{orgId}/data-products/v2` (lines 161-186)

Endpoint:
- Returns wrapped format responses
- Includes API documentation URL in docstring
- Loads data from fixture file

## Golden File

**snowplow_data_products_golden.json**
- Contains complete output from ingestion with data products
- Includes:
  - All data structures (schemas)
  - Event specifications
  - Tracking scenarios
  - Data products with ownership and custom properties
  - All container relationships and lineage

## Validation

All features validated against official Snowplow API specifications:
- ✅ Data Products API: https://docs.snowplow.io/docs/data-product-studio/data-products/api/
- ✅ API v2 endpoint correctly implemented
- ✅ Wrapped response format correctly handled
- ✅ All fields from API included in model
- ✅ Ownership and custom properties extracted
- ✅ Container relationships created

## Code Quality

- ✅ All tests passing (4/4)
- ✅ ruff format: All files formatted correctly
- ✅ mypy: No type errors in 9 source files
- ✅ Golden files created and validated

## Feature Status

**Status**: Experimental (as marked in config)

**Why Experimental**:
- Data Products API is relatively new in Snowplow BDP
- May evolve as Snowplow refines the feature
- Domain and custom properties mapping may need adjustments

**Production Readiness**:
- Core functionality is stable and tested
- Safe to use in production with understanding it may need updates as API evolves
- All error handling and logging in place

## Future Enhancements

Potential additions for future work:
- Data product versioning history
- Data product subscriptions tracking
- Richer domain mapping (if Snowplow adds domain URNs)
- Data product metrics and KPIs
- Data product SLA tracking

## References

- [Managing Data Products via the API | Snowplow Documentation](https://docs.snowplow.io/docs/data-product-studio/data-products/api/)
- [Introduction to Data Products | Snowplow Documentation](https://docs.snowplow.io/docs/fundamentals/data-products/)
- [Data Product Studio | Snowplow Documentation](https://docs.snowplow.io/docs/data-product-studio/)
