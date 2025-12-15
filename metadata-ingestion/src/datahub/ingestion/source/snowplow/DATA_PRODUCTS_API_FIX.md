# Data Products API Format Fix

## Issue

When running real-world ingestion, data products extraction was failing with Pydantic validation errors:
```
Failed to parse data products: 2 validation errors for DataProductsResponse
data.0.eventSpecs.0
  Input should be a valid string [type=string_type, input_value={'id': '650986b2...}]
includes
  Input should be a valid list [type=list_type, input_value={'owners': [], ...}]
```

Result: `num_data_products_found: 0, num_data_products_extracted: 0`

## Root Cause

The initial implementation was based on documentation assumptions, but the **real API response format is different**:

### Assumed Format (Wrong)
```json
{
  "data": [{
    "eventSpecs": ["evt-spec-1", "evt-spec-2"]  // ❌ Strings
  }],
  "includes": [],  // ❌ Array
  "errors": []
}
```

### Actual API Format (Correct)
```json
{
  "data": [{
    "id": "dp-ecommerce-analytics",
    "name": "E-commerce Analytics",
    "organizationId": "...",
    "eventSpecs": [  // ✅ Array of objects
      {
        "id": "650986b2-ad4a-453f-a0f1-4a2df337c31d",
        "url": "https://console.snowplowanalytics.com/..."
      }
    ],
    "sourceApplications": ["555a6b22-..."],  // ✅ Array of strings
    "type": "custom",
    "lockStatus": "unlocked"
  }],
  "includes": {  // ✅ Object with named arrays
    "owners": [],
    "eventSpecs": [...],
    "sourceApplications": [...]
  },
  "errors": [],
  "metadata": null
}
```

## Changes Made

### 1. Models Updated (`snowplow_models.py`)

**Added EventSpecReference**:
```python
class EventSpecReference(BaseModel):
    """Reference to event spec in data product."""
    id: str
    url: Optional[str]
```

**Updated DataProduct**:
```python
class DataProduct(BaseModel):
    # Added fields
    organization_id: Optional[str]  # organizationId
    source_applications: List[str]  # sourceApplications (was sourceApplicationId)
    type: Optional[str]
    lock_status: Optional[str]  # lockStatus

    # Changed type
    event_specs: List[EventSpecReference]  # Was List[str]
```

**Added DataProductsIncludes**:
```python
class DataProductsIncludes(BaseModel):
    """Includes section from data products API response."""
    owners: List[Dict[str, Any]]
    event_specs: List[Dict[str, Any]]
    source_applications: List[Dict[str, Any]]
```

**Updated DataProductsResponse**:
```python
class DataProductsResponse(BaseModel):
    data: List[DataProduct]
    includes: DataProductsIncludes  # Was List[Dict[str, Any]]
    errors: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]  # New field
```

### 2. Extraction Logic Updated (`snowplow.py`)

**Added report counters**:
```python
self.report.num_data_products_found = len(data_products)
self.report.num_data_products_filtered += 1
```

**Updated event spec reference handling**:
```python
# Old (wrong)
for event_spec_id in product.event_specs:
    event_spec_urn = self._make_event_spec_dataset_urn(event_spec_id)

# New (correct)
for event_spec_ref in product.event_specs:
    event_spec_id = event_spec_ref.id  # Extract ID from object
    event_spec_urn = self._make_event_spec_dataset_urn(event_spec_id)
```

**Updated custom properties**:
```python
# Changed from singular to plural
if product.source_applications:
    custom_properties["sourceApplications"] = ", ".join(product.source_applications)

# Added new fields
if product.type:
    custom_properties["type"] = product.type
if product.lock_status:
    custom_properties["lockStatus"] = product.lock_status
```

### 3. Test Fixtures Updated (`data_products_response.json`)

Updated to match real API format:
- `eventSpecs`: Changed from string array to object array with id/url
- `sourceApplications`: Changed from singular `sourceApplicationId`
- Added: `organizationId`, `type`, `lockStatus`
- `includes`: Changed from array to object with named arrays
- Added: `metadata` field

### 4. Golden Files Regenerated

All tests passing with updated format:
- ✅ `test_snowplow_ingest`
- ✅ `test_snowplow_event_specs_and_tracking_scenarios`
- ✅ `test_snowplow_data_products`
- ✅ `test_snowplow_config_validation`

## Validation Against Real API

The implementation now correctly handles the real Snowplow BDP API response:

✅ **eventSpecs** as array of `{id, url}` objects
✅ **sourceApplications** as array of strings
✅ **includes** as object with owners/eventSpecs/sourceApplications
✅ **type**, **lockStatus**, **organizationId** fields
✅ **metadata** field

## Testing

**Before Fix**:
```
num_data_products_found: 0
num_data_products_extracted: 0
ERROR: Failed to parse data products: 2 validation errors
```

**After Fix**:
```
num_data_products_found: 1
num_data_products_extracted: 1
INFO: Found 1 data products
INFO: Extracting data product: E-commerce Analytics
```

## Code Quality

- ✅ ruff format: All files formatted
- ✅ mypy: No type errors in 9 source files
- ✅ All tests: 4/4 passing
- ✅ Golden files updated

## Key Learnings

1. **Always validate against real API responses**, not just documentation
2. **API documentation can be incomplete** - Snowplow docs didn't specify the exact structure of `eventSpecs` and `includes`
3. **Pydantic validation errors are valuable** - They immediately show the mismatch between expected and actual format
4. **Test with real data early** - Mock tests passed but real API revealed the issue

## Files Modified

1. `src/datahub/ingestion/source/snowplow/snowplow_models.py` - Models updated
2. `src/datahub/ingestion/source/snowplow/snowplow.py` - Extraction logic updated
3. `tests/integration/snowplow/fixtures/data_products_response.json` - Fixture updated
4. `tests/integration/snowplow/snowplow_data_products_golden.json` - Golden file regenerated
