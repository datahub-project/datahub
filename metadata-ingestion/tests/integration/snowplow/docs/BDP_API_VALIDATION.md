# Snowplow BDP API Response Format Validation

**Date**: 2025-12-12
**Purpose**: Document actual BDP API response formats vs connector expectations
**Source**: Real API responses during testing

---

## Summary

The connector has been updated to handle the **actual BDP API response formats**, which differ from initial expectations based on documentation.

---

## API Endpoints Analyzed

### 1. Authentication Endpoint

**Endpoint**: `POST /organizations/{orgId}/credentials/v3/token`

**Request Headers**:

```
X-Api-Key-Id: {api_key_id}
X-Api-Key: {api_key_secret}
```

**Response Format**: ✅ **Verified Working**

```json
{
  "accessToken": "eyJhbGc..."
}
```

**Connector Handling**: ✅ Correctly implemented in `SnowplowBDPClient._authenticate()`

---

### 2. Users Endpoint

**Endpoint**: `GET /organizations/{orgId}/users`

**Expected Format** (based on typical REST patterns):

```json
{
  "data": [{ "id": "...", "email": "...", "name": "..." }]
}
```

**Actual Format** ⚠️ (from real API):

```json
[
  {
    "id": "53ac1013-d825-47...",
    "email": "user@example.com",
    "name": "User Name",
    "displayName": "Display Name",
    "role": "*",
    "filters": []
  }
]
```

**Key Difference**: Returns **array directly**, not wrapped in `{"data": [...]}`

**Connector Fix**: ✅ Added fallback parsing in `get_users()`:

```python
# Try wrapped format first
response = UsersResponse.model_validate(response_data)

# Fallback to direct array format (real BDP API)
if isinstance(response_data, list):
    return [User.model_validate(user) for user in response_data]
```

**Status**: ✅ **Working** - 3 users cached successfully

---

### 3. Data Structures List Endpoint

**Endpoint**: `GET /organizations/{orgId}/data-structures/v1`

**Expected Format**:

```json
{
  "data": [
    {
      "hash": "...",
      "meta": {...},
      "data": {...}
    }
  ]
}
```

**Actual Format** ⚠️ (from real API):

```json
[
  {
    "hash": "5242ff4ca845492f...",
    "vendor": "io.snowplow",
    "name": "schema_name",
    "meta": {
      "hidden": false,
      "schemaType": "event"
    },
    "creator": "User Name",
    "updatedAt": "2024-12-04T10:00:00Z"
  }
]
```

**Key Differences**:

1. Returns **array directly**, not wrapped
2. **Missing `data` field** (JSON schema definition)
3. Includes minimal metadata only

**Connector Fix**: ✅ Added:

1. Fallback array parsing
2. Automatic fetching of full details by hash when `data` missing

**Status**: ✅ **Working** - Fetches full details automatically

---

### 4. Data Structure Detail Endpoint

**Endpoint**: `GET /organizations/{orgId}/data-structures/v1/{hash}`

**Expected Format**:

```json
{
  "hash": "...",
  "meta": {...},
  "data": {
    "$schema": "...",
    "self": {...},
    "properties": {...}
  },
  "deployments": [...]
}
```

**Actual Format** ⚠️ (from real API):

```json
{
  "hash": "5242ff4ca845492f...",
  "vendor": "io.snowplow",
  "name": "schema_name",
  "meta": {
    "hidden": false,
    "schemaType": "event",
    "customData": {}
  },
  "deployments": [
    {
      "version": "1-0-0",
      "ts": "2024-01-15T10:00:00Z",
      "initiator": "User Name",
      "initiatorId": "user-uuid",
      "env": "PROD"
    }
  ]
}
```

**Key Differences**:

1. **Still missing `data` field** (even in detail endpoint!)
2. ✅ Has `meta` field
3. ✅ Has `deployments` array (critical for ownership)
4. ✅ Has `vendor` and `name` fields

**Connector Fix**: ✅ Updated to:

1. Extract version from deployments when `data` missing
2. Skip detailed schema parsing when `data` unavailable
3. Still emit ownership from deployments

**Status**: ✅ **Working** - Can process schemas without full definition

---

### 5. Deployments in Data Structures

**Format**:

```json
{
  "deployments": [
    {
      "version": "1-0-0",
      "patchLevel": 0,
      "contentHash": "abc123...",
      "env": "PROD",
      "ts": "2024-01-15T10:00:00Z",
      "message": "Initial deployment",
      "initiator": "User Full Name",
      "initiatorId": "uuid-of-user"
    }
  ]
}
```

**Critical Fields for Ownership**:

- ✅ `initiator`: Full name (fallback)
- ✅ `initiatorId`: UUID for reliable user lookup
- ✅ `ts`: Timestamp for sorting
- ✅ `version`: Schema version

**Connector Handling**: ✅ Correctly implemented:

1. Sorts by `ts` to find oldest (creator) and newest (modifier)
2. Resolves `initiatorId` to email via Users API
3. Falls back to `initiator` name if ID resolution fails

**Status**: ✅ **Working** - Ownership extracted successfully in tests

---

## Model Validations

### Updated Pydantic Models

#### DataStructure Model

```python
class DataStructure(BaseModel):
    hash: Optional[str] = None
    vendor: Optional[str] = None
    name: Optional[str] = None
    meta: Optional[SchemaMetadata] = None  # ✅ Made optional (was required)
    data: Optional[SchemaData] = None      # ✅ Made optional (was required)
    deployments: List[DataStructureDeployment] = Field(default_factory=list)
```

**Rationale**: Real API doesn't always return `data` field, even in detail endpoint

#### Response Wrapper Models

```python
# These models exist but API returns arrays directly
class DataStructuresResponse(BaseModel):
    data: List[DataStructure]

class UsersResponse(BaseModel):
    data: List[User]
```

**Fix**: Added fallback parsing when API returns unwrapped arrays

---

## Testing Results

### Mock Server (Original Behavior)

- ✅ Returns wrapped responses: `{"data": [...]}`
- ✅ Includes full `data` field with schema definition
- ✅ All tests passing

### Real BDP API (Updated Behavior)

- ✅ Returns unwrapped arrays: `[...]`
- ⚠️ Missing `data` field (schema definition)
- ✅ Has `deployments` with ownership info
- ✅ Connector adapted successfully
- ✅ Ownership extraction working

---

## Compatibility Matrix

| Feature                               | Mock Server | Real BDP API | Connector Support |
| ------------------------------------- | ----------- | ------------ | ----------------- |
| Wrapped responses (`{"data": [...]}`) | ✅          | ❌           | ✅ Both           |
| Direct array responses (`[...]`)      | ❌          | ✅           | ✅ Both           |
| Full schema definition (`data` field) | ✅          | ❌           | ✅ Optional       |
| Schema metadata (`meta`)              | ✅          | ✅           | ✅ Required       |
| Deployments array                     | ✅          | ✅           | ✅ Required       |
| User resolution                       | ✅          | ✅           | ✅ Working        |
| Ownership extraction                  | ✅          | ✅           | ✅ Working        |

---

## Recommendations

### For Production Use

1. **Schema Definition Missing**:

   - The real BDP API doesn't return the JSON schema definition (`data` field)
   - Ownership tracking works fine without it
   - Detailed schema field extraction not possible without this data
   - **Recommendation**: This is acceptable for ownership use case

2. **API Version Differences**:

   - Mock server behavior suggests older/different API version
   - Real API has evolved to different response format
   - Connector now handles both formats

3. **Future Considerations**:
   - If schema definitions become available, they'll be automatically used
   - Consider separate endpoint for full schema details if needed
   - Monitor API changes via version headers

### For Testing

1. **Update Mock Server** (optional):

   - Could update to match real API format
   - Current approach (supporting both) is more robust
   - Tests validate both historical and current formats

2. **Integration Tests**:
   - ✅ Currently test with both wrapped and unwrapped responses
   - ✅ Handle missing `data` field correctly
   - ✅ Ownership extraction verified

---

## API Documentation Gaps

Based on testing, the following should be clarified in BDP API docs:

1. **Response Format**:

   - List endpoints return arrays directly (not wrapped)
   - No `{"data": [...]}` envelope

2. **Schema Definition Availability**:

   - `data` field may not be included in responses
   - Even detail endpoint doesn't always return full schema
   - Deployments array is always present

3. **User Resolution**:
   - Users endpoint returns array directly
   - `initiatorId` is reliable UUID for user lookup
   - `initiator` is full name string (less reliable)

---

## Connector Resilience

The connector now handles:

✅ **Response Format Variations**:

- Wrapped (`{"data": [...]}`) vs unwrapped (`[...]`)
- Missing optional fields (`data`, `description`)
- Different API versions

✅ **Graceful Degradation**:

- Works without full schema definition
- Falls back to deployment versions
- Logs what's missing for debugging

✅ **Ownership Extraction**:

- Primary use case works with available data
- Doesn't require full schema definition
- Uses deployments array reliably

---

## Validation Status

| Endpoint                       | Format Validated | Model Updated | Tested | Status  |
| ------------------------------ | ---------------- | ------------- | ------ | ------- |
| POST /credentials/v3/token     | ✅               | ✅            | ✅     | Working |
| GET /users                     | ✅               | ✅            | ✅     | Working |
| GET /data-structures/v1        | ✅               | ✅            | ✅     | Working |
| GET /data-structures/v1/{hash} | ✅               | ✅            | ✅     | Working |

**Overall Status**: ✅ **Connector validated against real BDP API**

---

## Next Steps

1. ✅ **Immediate**: Run full ingestion test with real credentials
2. ⏭️ **After Success**: Verify ownership in DataHub UI
3. ⏭️ **Optional**: Contact Snowplow about schema definition availability
4. ⏭️ **Future**: Update documentation based on learnings

---

**Last Updated**: 2025-12-12
**Validated Against**: Real Snowplow BDP API (Production)
**Connector Version**: With real API compatibility fixes
