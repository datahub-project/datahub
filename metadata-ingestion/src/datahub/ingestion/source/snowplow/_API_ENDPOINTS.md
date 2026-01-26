# Snowplow API Endpoints Reference

This document tracks all API endpoints used by the Snowplow connector with links to official documentation.

## Snowplow BDP Console API

**Base URL**: `https://console.snowplowanalytics.com/api/msc/v1`

### Authentication

| Endpoint                                               | Method  | Purpose                                | Docs Link                                                                                     |
| ------------------------------------------------------ | ------- | -------------------------------------- | --------------------------------------------------------------------------------------------- |
| `/organizations/{organizationId}/credentials/v3/token` | **GET** | Exchange API credentials for JWT token | https://docs.snowplow.io/docs/using-the-snowplow-console/managing-console-api-authentication/ |

**Headers Required**:

- `X-API-Key-ID`: API key identifier
- `X-API-Key`: API key secret

**Response**: `{"accessToken": "<JWT>"}`

**Usage**: All subsequent requests use `Authorization: Bearer <JWT>` header

---

### Data Structures API

| Endpoint                                                                                          | Method | Purpose                                 | Docs Link                                                                                      |
| ------------------------------------------------------------------------------------------------- | ------ | --------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `/organizations/{organizationId}/data-structures/v1`                                              | GET    | List all data structures (schemas)      | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/ |
| `/organizations/{organizationId}/data-structures/v1/{dataStructureHash}`                          | GET    | Get specific data structure by hash     | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/ |
| `/organizations/{organizationId}/data-structures/v1/{dataStructureHash}/versions/{versionNumber}` | GET    | Get specific version of data structure  | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/ |
| `/organizations/{organizationId}/data-structures/v1/{dataStructureHash}/deployments`              | GET    | Get deployment status of data structure | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/api/ |

**Query Parameters**:

- `vendor` (optional): Filter by schema vendor
- `name` (optional): Filter by schema name

**Response Format**:

```json
{
  "data": [
    {
      "meta": {
        "hidden": false,
        "schemaType": "event",
        "customData": {}
      },
      "data": {
        "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "self": {
          "vendor": "com.example",
          "name": "event_name",
          "format": "jsonschema",
          "version": "1-0-0"
        },
        "type": "object",
        "properties": {...}
      }
    }
  ]
}
```

---

### Event Specifications API

| Endpoint                                                       | Method | Purpose                          | Docs Link                                                                   |
| -------------------------------------------------------------- | ------ | -------------------------------- | --------------------------------------------------------------------------- |
| `/organizations/{organizationId}/event-specs/v1`               | GET    | List all event specifications    | https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/ |
| `/organizations/{organizationId}/event-specs/v1/{eventSpecId}` | GET    | Get specific event specification | https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/ |

**Response Format**:

```json
{
  "data": [
    {
      "id": "event_spec_id",
      "name": "Event Name",
      "description": "Event description",
      "eventSchemas": [
        {
          "vendor": "com.example",
          "name": "event_name",
          "version": "1-0-0"
        }
      ]
    }
  ]
}
```

---

### Tracking Scenarios API

| Endpoint                                                             | Method | Purpose                        | Docs Link                                                                                         |
| -------------------------------------------------------------------- | ------ | ------------------------------ | ------------------------------------------------------------------------------------------------- |
| `/organizations/{organizationId}/tracking-scenarios/v1`              | GET    | List all tracking scenarios    | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/ |
| `/organizations/{organizationId}/tracking-scenarios/v1/{scenarioId}` | GET    | Get specific tracking scenario | https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/ |

**Response Format**:

```json
{
  "data": [
    {
      "id": "scenario_id",
      "name": "Scenario Name",
      "description": "Scenario description",
      "eventSpecs": ["event_spec_id_1", "event_spec_id_2"]
    }
  ]
}
```

---

### Users API

| Endpoint                                | Method | Purpose                        | Docs Link                                                                                         |
| --------------------------------------- | ------ | ------------------------------ | ------------------------------------------------------------------------------------------------- |
| `/organizations/{organizationId}/users` | GET    | List all users in organization | https://console.snowplowanalytics.com/api/msc/v1/docs/#/Users/getOrganizationsOrganizationidUsers |

**Response Format**:

```json
{
  "data": [
    {
      "id": "user-uuid-123",
      "email": "tamas.nemeth@acryl.io",
      "name": "Tamás Németh",
      "displayName": "Tamás Németh"
    }
  ]
}
```

**Usage**: Cache users at ingestion start to map `initiatorId` → `email` for ownership extraction.

**Note**: Currently, `initiatorId` is not available in deployments array, so this is used for future-proofing and fallback name matching.

---

### Data Products API (Optional)

| Endpoint                                           | Method | Purpose                | Docs Link                                                                                                        |
| -------------------------------------------------- | ------ | ---------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `/organizations/{organizationId}/data-products/v1` | GET    | List all data products | https://docs.snowplow.io/docs/understanding-tracking-design/defining-the-data-to-collect-with-data-products/api/ |

---

## Iglu Schema Registry API

**Base URL**: Configurable (e.g., `https://iglu.acme.com` or `http://iglucentral.com`)

### Schema Service

| Endpoint                                          | Method | Purpose                                                   | Docs Link                                                                       |
| ------------------------------------------------- | ------ | --------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `/api/schemas`                                    | GET    | List all schemas (may not be available on all registries) | https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/ |
| `/api/schemas/{vendor}/{name}/{format}/{version}` | GET    | Get specific schema by vendor/name/version                | https://docs.snowplow.io/docs/api-reference/iglu/iglu-repositories/iglu-server/ |

**Example**:

- `/api/schemas/com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0`

**Authentication** (for private Iglu servers):

- `apikey`: UUID passed as query parameter or header

**Response Format**:

```json
{
  "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
  "self": {
    "vendor": "com.example",
    "name": "event_name",
    "format": "jsonschema",
    "version": "1-0-0"
  },
  "type": "object",
  "properties": {
    "field1": { "type": "string" },
    "field2": { "type": "integer" }
  },
  "required": ["field1"]
}
```

---

## Error Handling

### Common HTTP Status Codes

| Code | Meaning      | Action                                         |
| ---- | ------------ | ---------------------------------------------- |
| 200  | Success      | Process response                               |
| 401  | Unauthorized | Re-authenticate (JWT expired) or check API key |
| 403  | Forbidden    | Check permissions for API key                  |
| 404  | Not Found    | Resource doesn't exist (skip or warn)          |
| 429  | Rate Limit   | Exponential backoff, retry after delay         |
| 500  | Server Error | Retry with exponential backoff                 |

---

## Pagination

**BDP Console API Pagination**:

- Documentation doesn't specify pagination format
- Need to test with API to determine if cursor-based, offset-based, or link-based
- Likely returns all results in single response for most endpoints

**Iglu Server Pagination**:

- Not typically paginated (registries are relatively small)

---

## Rate Limits

- Not explicitly documented for BDP Console API
- Recommended: Implement exponential backoff for 429 responses
- Respect `Retry-After` header if present

---

## Notes

1. **Organization ID**: Found in BDP Console URL (UUID in first segment after host)
2. **Data Structure Hash**: Unique identifier for each schema (not the same as SchemaVer version)
3. **SchemaVer Format**: `MODEL-REVISION-ADDITION` (e.g., `1-0-0`)
4. **JWT Token Lifespan**: Not documented - implement re-authentication on 401 errors
5. **Self-Describing JSON**: All schemas follow the self-describing JSON pattern with `self` descriptor

---

## References

- [BDP Console API Documentation](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Snowplow Documentation](https://docs.snowplow.io/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)
- [Managing Data Structures](https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/)
