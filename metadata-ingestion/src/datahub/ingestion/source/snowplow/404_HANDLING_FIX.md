# 404 Endpoint Handling Fix

## Issue

When running ingestion against Snowplow organizations that don't have certain features enabled, the connector was failing with Pydantic validation errors:

```
WARNING: Resource not found: https://console.snowplowanalytics.com/.../tracking-scenarios/v1
ERROR: Failed to parse tracking scenarios: 1 validation error for TrackingScenariosResponse
data
  Field required [type=missing, input_value={}, input_type=dict]
```

This occurred because:
1. Some Snowplow organizations don't have tracking scenarios/event specs/data products endpoints available (404)
2. The client returns empty dict `{}` on 404
3. The code tried to parse the empty dict as a response model
4. Validation failed because the model expects a `data` field

## Root Cause

The `_request` method in `snowplow_client.py` handles 404 errors by returning an empty dict:

```python
elif e.response.status_code == 404:
    logger.warning(f"Resource not found: {url}")
    return {}  # Return empty dict for not found
```

However, the API methods (`get_event_specifications`, `get_tracking_scenarios`, `get_data_products`) immediately tried to validate this empty dict against their response models, which expect a `data` field.

## Solution

Added 404 checks **before** validation in all three methods:

### Before (Wrong)
```python
def get_tracking_scenarios(self) -> List[TrackingScenario]:
    response_data = self._request("GET", endpoint)

    # Immediately tries to validate - FAILS on 404
    response = TrackingScenariosResponse.model_validate(response_data)
    return response.data
```

### After (Correct)
```python
def get_tracking_scenarios(self) -> List[TrackingScenario]:
    response_data = self._request("GET", endpoint)

    # Handle 404 (resource not found) - endpoint may not be available
    if not response_data or response_data == {}:
        logger.info("Tracking scenarios endpoint not available (404) - this is normal for some organizations")
        return []

    # Only validate if we have actual data
    response = TrackingScenariosResponse.model_validate(response_data)
    return response.data
```

## Changes Made

### 1. `snowplow_client.py` - get_event_specifications()

**Lines 340-345**: Added 404 check
```python
# Handle 404 (resource not found) - endpoint may not be available
if not response_data or response_data == {}:
    logger.info("Event specifications endpoint not available (404) - this is normal for some organizations")
    return []
```

### 2. `snowplow_client.py` - get_tracking_scenarios()

**Lines 425-430**: Added 404 check
```python
# Handle 404 (resource not found) - endpoint may not be available
if not response_data or response_data == {}:
    logger.info("Tracking scenarios endpoint not available (404) - this is normal for some organizations")
    return []
```

### 3. `snowplow_client.py` - get_data_products()

**Lines 510-515**: Added 404 check
```python
# Handle 404 (resource not found) - endpoint may not be available
if not response_data or response_data == {}:
    logger.info("Data products endpoint not available (404) - this is normal for some organizations")
    return []
```

## Behavior After Fix

### When Endpoint is Available
```
INFO: Fetching tracking scenarios from Snowplow
INFO: Found 2 tracking scenarios
```

### When Endpoint Returns 404
```
INFO: Fetching tracking scenarios from Snowplow
WARNING: Resource not found: https://.../tracking-scenarios/v1
INFO: Tracking scenarios endpoint not available (404) - this is normal for some organizations
```

**No errors**, connector continues gracefully with empty list.

## Why This is Normal

Not all Snowplow organizations have all features enabled:

- **Event Specifications** - Only available in newer BDP plans
- **Tracking Scenarios** - Optional feature, not enabled by default
- **Data Products** - Newer feature (v2 API), may not be available in all organizations

The connector should **gracefully handle** missing endpoints and continue with available data.

## Testing

**All tests passing**:
- ✅ `test_snowplow_ingest` - Data structures extraction
- ✅ `test_snowplow_event_specs_and_tracking_scenarios` - Event specs with lineage
- ✅ `test_snowplow_data_products` - Data products with ownership
- ✅ `test_snowplow_config_validation` - Configuration validation

**Code Quality**:
- ✅ ruff format: All files formatted
- ✅ mypy: No type errors

## Expected Output

When running against an organization without tracking scenarios:

**Before Fix**:
```
ERROR: Failed to parse tracking scenarios: 1 validation error
num_tracking_scenarios_found: 0
num_tracking_scenarios_extracted: 0
```

**After Fix**:
```
INFO: Tracking scenarios endpoint not available (404) - this is normal
num_tracking_scenarios_found: 0
num_tracking_scenarios_extracted: 0
```

No error, clean INFO log message, ingestion continues successfully.

## Related Files

- `src/datahub/ingestion/source/snowplow/snowplow_client.py` - Client with 404 handling
- All tests continue to pass with this change
- No impact on organizations that DO have these endpoints available
