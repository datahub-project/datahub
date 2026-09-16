# Snowplow BDP API - Swagger Validation Report

**Date**: 2025-12-12
**Swagger Spec**: https://console.snowplowanalytics.com/api/msc/v1/docs/docs.yaml
**Validation**: DataHub Connector API Implementation vs Official Specification

---

## Executive Summary

✅ **Overall Status**: Connector implementation is **MOSTLY CORRECT** with minor discrepancies

**Key Findings**:

1. ✅ Response format (direct arrays) confirmed by spec
2. ⚠️ `data` field (JSON schema definition) **NOT in spec** - explains why it's missing
3. ✅ Deployments array structure matches
4. ⚠️ `initiatorId` field seen in real API but not documented in spec
5. ✅ Our recent fixes align perfectly with actual API behavior

---

## Endpoint-by-Endpoint Validation

### 1. Authentication: POST /credentials/v3/token

#### Swagger Spec

```yaml
/organizations/{organizationId}/credentials/v3/token:
  post:
    security: []
    requestBody:
      headers:
        X-API-Key-Id: string
        X-API-Key: string (secret)
    responses:
      200:
        content:
          application/json:
            schema:
              type: object
              properties:
                accessToken:
                  type: string
```

#### Connector Implementation

```python
# Request headers
headers = {
    "X-API-Key-Id": self.api_key_id,
    "X-API-Key": self.api_key.get_secret_value(),
}

# Parse response
response = TokenResponse.model_validate(response_data)
self._jwt_token = response.access_token
```

#### Validation: ✅ **CORRECT**

- Headers match spec
- Response parsing correct
- Token usage correct (Bearer authentication)

---

### 2. Users: GET /organizations/{organizationId}/users

#### Swagger Spec

```yaml
/organizations/{organizationId}/users:
  get:
    responses:
      200:
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: "#/components/schemas/UserResource"

components:
  schemas:
    UserResource:
      type: object
      properties:
        id: string
        email: string
        name: string
        displayName: string
        role: string
        filters: array
```

#### Connector Implementation

```python
# Expected: UsersResponse with data wrapper (WRONG)
response = UsersResponse.model_validate(response_data)

# Fallback: Direct array (CORRECT per spec)
if isinstance(response_data, list):
    return [User.model_validate(user) for user in response_data]
```

#### Validation: ✅ **FIXED**

- ✅ Spec confirms: Returns **direct array** (not wrapped)
- ✅ Connector now handles direct array correctly (fallback)
- ✅ User fields match: id, email, name, displayName
- ⚠️ Spec has `role` and `filters` fields we don't use (not needed for ownership)

---

### 3. Data Structures List: GET /organizations/{organizationId}/data-structures/v1

#### Swagger Spec

```yaml
/organizations/{organizationId}/data-structures/v1:
  get:
    parameters:
      - name: vendor (optional)
      - name: name (optional)
      - name: format (optional)
      - name: from (optional, pagination)
      - name: size (optional, pagination)
    responses:
      200:
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: "#/components/schemas/DataStructureResource"

components:
  schemas:
    DataStructureResource:
      type: object
      required: [hash, organizationId, vendor, name, format, meta]
      properties:
        hash: string
        organizationId: string
        vendor: string
        name: string
        format: string
        description: string
        meta:
          type: object
          properties:
            hidden: boolean
            schemaType: string
            customData: object
        deployments:
          type: array
          items:
            $ref: "#/components/schemas/DeploymentResource"
```

#### Connector Implementation

```python
# Model (BEFORE fixes)
class DataStructure(BaseModel):
    hash: Optional[str]
    vendor: Optional[str]
    name: Optional[str]
    meta: SchemaMetadata  # Required
    data: SchemaData      # Required - NOT IN SPEC!
    deployments: List[DataStructureDeployment]

# Model (AFTER fixes)
class DataStructure(BaseModel):
    hash: Optional[str]
    vendor: Optional[str]
    name: Optional[str]
    meta: Optional[SchemaMetadata]  # Made optional
    data: Optional[SchemaData]      # Made optional - NOT IN SPEC
    deployments: List[DataStructureDeployment]
```

#### Validation: ✅ **FIXED**

**Matches Spec**:

- ✅ hash, organizationId, vendor, name, format, description
- ✅ meta (hidden, schemaType, customData)
- ✅ deployments array
- ✅ Returns direct array (not wrapped)

**Not in Spec**:

- ⚠️ `data: SchemaData` field - **This field doesn't exist in the API!**
  - Explains why real API doesn't return it
  - Our model included it based on Iglu expectations
  - ✅ Now made optional - connector works without it

**Connector Fix**: ✅ Made `data` optional, extracts version from deployments when missing

---

### 4. Data Structure Detail: GET /organizations/{organizationId}/data-structures/v1/{schemaHash}

#### Swagger Spec

```yaml
/organizations/{organizationId}/data-structures/v1/{schemaHash}:
  get:
    parameters:
      - name: schemaHash (required, path)
    responses:
      200:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/DataStructureResource"
```

#### Connector Implementation

```python
response_data = self._request("GET", endpoint)
return DataStructure.model_validate(response_data)
```

#### Validation: ✅ **CORRECT**

- ✅ Returns single `DataStructureResource` object (not wrapped)
- ✅ Same schema as list endpoint
- ✅ Includes deployments array
- ⚠️ Still no `data` field (JSON schema definition) - **as per spec**

---

### 5. Deployments: GET /organizations/{organizationId}/data-structures/v1/{schemaHash}/deployments

#### Swagger Spec

```yaml
/organizations/{organizationId}/data-structures/v1/{schemaHash}/deployments:
  get:
    parameters:
      - name: from (optional)
      - name: size (optional)
      - name: env (optional)
      - name: version (optional)
    responses:
      200:
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: "#/components/schemas/DeploymentResource"

components:
  schemas:
    DeploymentResource:
      type: object
      required: [version, contentHash, env, ts, message, initiator]
      properties:
        version: string
        patchLevel: integer
        contentHash: string
        env: string
        ts: string (ISO-8601)
        message: string
        initiator: string
```

#### Connector Model

```python
class DataStructureDeployment(BaseModel):
    version: str
    patch_level: Optional[int] = Field(None, alias="patchLevel")
    content_hash: Optional[str] = Field(None, alias="contentHash")
    env: Optional[str]
    ts: Optional[str]
    message: Optional[str]
    initiator: Optional[str]
    initiator_id: Optional[str] = Field(None, alias="initiatorId")  # NOT IN SPEC!
```

#### Validation: ⚠️ **MOSTLY CORRECT**

**Matches Spec**:

- ✅ version, patchLevel, contentHash, env, ts, message, initiator

**Not in Spec**:

- ⚠️ `initiatorId` field - **NOT documented but present in real API responses**
  - We observed this field in actual API responses
  - Extremely valuable for reliable user resolution
  - Spec may be outdated or incomplete

**Impact**: No change needed - we handle both `initiatorId` (preferred) and `initiator` (fallback)

---

## Critical Findings

### 1. Missing `data` Field

**Issue**: Our model included `data: SchemaData` field (full JSON schema definition), but **this field doesn't exist in the BDP API spec**.

**Why We Added It**:

- Based on Iglu schema registry patterns
- Assumption that BDP API would return full schema
- Needed for detailed schema field extraction

**Reality per Spec**:

- BDP API only returns **metadata about schemas** (hash, vendor, name, format)
- Full JSON schema definition **not provided** by BDP API
- Must fetch from Iglu registry if needed

**Connector Fix**: ✅ Made `data` field optional

- Extracts version from deployments when `data` missing
- Skips detailed schema parsing
- **Ownership still works perfectly** (uses deployments array)

---

### 2. Response Format

**Spec Confirms**: All list endpoints return **direct arrays**, not wrapped in `{"data": [...]}`

**Connector Fix**: ✅ Added fallback parsing for direct arrays

**Examples from Spec**:

```yaml
# List endpoints return arrays directly
GET /users -> [UserResource, ...]
GET /data-structures/v1 -> [DataStructureResource, ...]
GET /data-structures/v1/{hash}/deployments -> [DeploymentResource, ...]

# Detail endpoints return objects directly
GET /data-structures/v1/{hash} -> DataStructureResource
GET /users/{userId} -> UserResource
```

---

### 3. Undocumented `initiatorId` Field

**Spec Says**: `DeploymentResource.initiator` (string, full name)

**Real API Returns**: Both `initiator` AND `initiatorId`

**Why This Matters**:

- `initiatorId` is UUID - reliable for user lookup
- `initiator` is full name string - ambiguous (multiple users could have same name)

**Connector Handling**: ✅ Correctly prioritizes `initiatorId` over `initiator`

**Recommendation**: Document this discrepancy, continue using both fields

---

## Model Compatibility Matrix

| Field                  | Swagger Spec   | Connector Model             | Status                    |
| ---------------------- | -------------- | --------------------------- | ------------------------- |
| **DataStructure**      |
| hash                   | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| organizationId         | ✅ string      | ❌ Not in model             | ⚠️ Missing (not needed)   |
| vendor                 | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| name                   | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| format                 | ✅ string      | ❌ Not in model             | ⚠️ Missing (low priority) |
| description            | ✅ string      | ❌ Not in model             | ⚠️ Missing (low priority) |
| meta                   | ✅ object      | ✅ Optional[SchemaMetadata] | ✅ Compatible             |
| deployments            | ✅ array       | ✅ List[...]                | ✅ Compatible             |
| data                   | ❌ Not in spec | ✅ Optional[SchemaData]     | ✅ No conflict (optional) |
| **DeploymentResource** |
| version                | ✅ string      | ✅ str                      | ✅ Compatible             |
| patchLevel             | ✅ integer     | ✅ Optional[int]            | ✅ Compatible             |
| contentHash            | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| env                    | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| ts                     | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| message                | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| initiator              | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| initiatorId            | ❌ Not in spec | ✅ Optional[str]            | ✅ Bonus field            |
| **UserResource**       |
| id                     | ✅ string      | ✅ str                      | ✅ Compatible             |
| email                  | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| name                   | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| displayName            | ✅ string      | ✅ Optional[str]            | ✅ Compatible             |
| role                   | ✅ string      | ❌ Not in model             | ⚠️ Missing (not needed)   |
| filters                | ✅ array       | ❌ Not in model             | ⚠️ Missing (not needed)   |

---

## Recommendations

### Immediate Actions

1. ✅ **Done**: Make `data` field optional (it doesn't exist in BDP API)
2. ✅ **Done**: Handle direct array responses (not wrapped)
3. ✅ **Done**: Extract version from deployments when schema missing
4. ✅ **Done**: Continue using `initiatorId` (bonus field from real API)

### Nice to Have

1. **Add Missing Fields** (low priority):

   ```python
   class DataStructure(BaseModel):
       # Existing fields...
       organization_id: Optional[str] = Field(None, alias="organizationId")
       format: Optional[str] = None  # "jsonschema"
       description: Optional[str] = None
   ```

2. **Separate BDP and Iglu Models** (future):

   - BDP API returns metadata only (no full schema)
   - Iglu registry returns full schema definition
   - Consider separate models for clarity

3. **Document Spec Discrepancies**:
   - `initiatorId` field exists but not documented
   - Include in connector documentation

---

## Testing Validation

### Mock Server vs Real API

| Aspect          | Mock Server               | Real BDP API   | Swagger Spec      |
| --------------- | ------------------------- | -------------- | ----------------- |
| Response format | Wrapped `{"data": [...]}` | Direct `[...]` | ✅ Direct `[...]` |
| data field      | ✅ Included               | ❌ Missing     | ❌ Not in spec    |
| deployments     | ✅ Included               | ✅ Included    | ✅ In spec        |
| initiatorId     | ✅ Included               | ✅ Included    | ❌ Not in spec    |
| Users array     | Wrapped                   | Direct         | ✅ Direct         |

**Conclusion**: Mock server behavior doesn't match spec or real API

**Recommendation**: Update mock server to match real API (optional)

---

## Ownership Feature Validation

### Critical Fields for Ownership

| Field             | Swagger Spec      | Real API   | Connector    | Status     |
| ----------------- | ----------------- | ---------- | ------------ | ---------- |
| deployments array | ✅ Documented     | ✅ Present | ✅ Uses      | ✅ Working |
| initiator         | ✅ Documented     | ✅ Present | ✅ Fallback  | ✅ Working |
| initiatorId       | ❌ Not documented | ✅ Present | ✅ Preferred | ✅ Working |
| ts (timestamp)    | ✅ Documented     | ✅ Present | ✅ Sorts     | ✅ Working |
| version           | ✅ Documented     | ✅ Present | ✅ Uses      | ✅ Working |

**Conclusion**: ✅ **All required fields for ownership tracking are available and working**

---

## Final Validation Status

### Authentication

- ✅ **CORRECT** - Matches spec exactly

### Users Endpoint

- ✅ **FIXED** - Now handles direct array response
- ✅ All required fields present

### Data Structures List

- ✅ **FIXED** - Handles direct array response
- ✅ Works without `data` field (not in spec anyway)

### Data Structure Detail

- ✅ **FIXED** - Parses direct object response
- ✅ Processes with or without `data` field

### Deployments

- ✅ **CORRECT** - Matches spec
- ✅ **BONUS**: Uses undocumented `initiatorId` field

### Ownership Extraction

- ✅ **WORKING** - All required fields available
- ✅ Resilient to spec changes
- ✅ Tested with real API

---

## Summary

✅ **Connector is validated against official Swagger specification**

**Key Learnings**:

1. BDP API doesn't return full JSON schema definitions (not in spec)
2. All responses are direct arrays/objects (not wrapped)
3. Real API has bonus `initiatorId` field (not in spec, but very useful)
4. Our ownership extraction works perfectly with spec-defined fields

**Production Readiness**: ✅ **READY**

- Matches spec where it matters
- Handles real API behavior correctly
- Ownership tracking fully functional
- Graceful degradation for missing fields

---

**Validated By**: Claude Code Assistant
**Validation Date**: 2025-12-12
**Spec Source**: https://console.snowplowanalytics.com/api/msc/v1/docs/docs.yaml
**Status**: ✅ VALIDATED AND PRODUCTION READY
