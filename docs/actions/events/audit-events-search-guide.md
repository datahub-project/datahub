import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Audit Events Search API V1

<FeatureAvailability />

## Endpoint

`/openapi/v1/events/audit/search`

## Overview

This API allows you to search for audit events that occur within DataHub. Audit events track various actions performed by users and systems, providing a comprehensive history of activities and changes within your DataHub instance.

## Request Structure

The Audit Events Search API accepts POST requests with optional query parameters and a required JSON body.

### Query Parameters

| Name       | Type    | Description                                                             | Required | Default |
| ---------- | ------- | ----------------------------------------------------------------------- | -------- | ------- |
| startTime  | int64   | The timestamp (in ms) to start the search from, defaults to one day ago | No       | -1      |
| endTime    | int64   | The timestamp (in ms) to end the search at, defaults to current time    | No       | -1      |
| size       | int32   | The maximum number of events to return in one response                  | No       | 10      |
| scrollId   | string  | The scroll ID used for pagination when fetching subsequent results      | No       | null    |
| includeRaw | boolean | Whether to include the raw event data in the response                   | No       | true    |

### Request Body

The request body must be a JSON object with the following structure:

```json
{
  "eventTypes": ["string"],
  "entityTypes": ["string"],
  "aspectTypes": ["string"],
  "actorUrns": ["string"]
}
```

| Field       | Type     | Description                                                    | Required |
| ----------- | -------- | -------------------------------------------------------------- | -------- |
| eventTypes  | string[] | List of event types to filter by (empty means all event types) | No       |
| entityTypes | string[] | List of entity types to filter by (empty means all entities)   | No       |
| aspectTypes | string[] | List of aspect types to filter by (empty means all aspects)    | No       |
| actorUrns   | string[] | List of actor URNs to filter by (empty means all actors)       | No       |

These filters work as `and` filters between each other and `or` filters for elements in the list so:

```json
{
  "eventTypes": ["CreateAccessTokenEvent", "RevokeAccessTokenEvent"],
  "actorUrns": ["urn:li:corpuser:datahub"]
}
```

Filters for events that are either `CreateAccessTokenEvent` or `RevokeAccessTokenEvent` AND had `urn:li:corpuser:datahub` as the actor.

## Response Structure

The API returns a JSON object with the following structure:

```json
{
  "nextScrollId": "string",
  "count": 0,
  "total": 0,
  "usageEvents": [
    {
      // Event data varies based on event type
    }
  ]
}
```

| Field        | Type   | Description                                                        |
| ------------ | ------ | ------------------------------------------------------------------ |
| nextScrollId | string | ID for retrieving the next page of results (if more are available) |
| count        | int32  | Number of events returned in this response                         |
| total        | int32  | Total count of matching events (calculated up to 10,000)           |
| usageEvents  | array  | Array of usage events matching the search criteria                 |

## Event Types

The API supports various event types that track different actions within DataHub. Each event type has its own specific structure, but all share common properties defined in the `UsageEventResult` base type.

### Common Fields (UsageEventResult)

All event types include these base fields:

| Field            | Type   | Description                                                  |
| ---------------- | ------ | ------------------------------------------------------------ |
| eventType        | string | Type of the event                                            |
| timestamp        | int64  | Timestamp when the event occurred (in milliseconds)          |
| actorUrn         | string | URN of the actor who performed the action                    |
| sourceIP         | string | IP address from which the action was performed               |
| eventSource      | enum   | Source API of the event (RESTLI, OPENAPI, GRAPHQL, SSO_SCIM) |
| userAgent        | string | User agent string from the HTTP request (if applicable)      |
| telemetryTraceId | string | Trace ID from system telemetry                               |
| rawUsageEvent    | object | Full raw event contents if includeRaw=true                   |

### Specific Event Types

The API returns different event types, each with its own specific structure in addition to the common fields:

#### EntityEvent

Tracks general entity operations.

```json
{
  "eventType": "EntityEvent",
  "timestamp": 1649953100653,
  "actorUrn": "urn:li:corpuser:jdoe",
  "sourceIP": "192.168.1.1",
  "eventSource": "GRAPHQL",
  "userAgent": "Mozilla/5.0...",
  "telemetryTraceId": "abc123",
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "aspectName": "ownership"
}
```

#### Create/Update/Delete Event Types

Several event types track specific creation, update, and deletion actions:

- **CreateUserEvent**: Tracks user creation
- **UpdateUserEvent**: Tracks user updates
- **CreateAccessTokenEvent**: Tracks access token creation
- **RevokeAccessTokenEvent**: Tracks access token revocation
- **CreatePolicyEvent**: Tracks policy creation
- **UpdatePolicyEvent**: Tracks policy updates
- **DeletePolicyEvent**: Tracks policy deletes
- **CreateIngestionSourceEvent**: Tracks ingestion source creation
- **UpdateIngestionSourceEvent**: Tracks ingestion source updates
- **DeleteEntityEvent**: Tracks entity deletion
- **UpdateAspectEvent**: Tracks aspect updates

All these event types share the same structure:

```json
{
  "eventType": "[Event Type Name]",
  "timestamp": 1649953100653,
  "actorUrn": "urn:li:corpuser:jdoe",
  "sourceIP": "192.168.1.1",
  "eventSource": "GRAPHQL",
  "userAgent": "Mozilla/5.0...",
  "telemetryTraceId": "abc123",
  "entityUrn": "urn:li:dataset:abc",
  "entityType": "dataset",
  "aspectName": "ownership"
}
```

#### LogInEvent & FailedLogInEvent

Tracks user login events with a specific login source.

```json
{
  "eventType": "LogInEvent",
  "timestamp": 1649953100653,
  "actorUrn": "urn:li:corpuser:jdoe",
  "sourceIP": "192.168.1.1",
  "eventSource": "GRAPHQL",
  "userAgent": "Mozilla/5.0...",
  "telemetryTraceId": "abc123",
  "loginSource": "PASSWORD_LOGIN"
}
```

`loginSource` can be one of:

- PASSWORD_RESET
- PASSWORD_LOGIN
- FALLBACK_LOGIN
- SIGN_UP_LINK_LOGIN
- GUEST_LOGIN
- SSO_LOGIN
- OIDC_IMPLICIT_LOGIN

## Usage Examples

### Basic Search for All Events

To search for all audit events with default parameters:

```json
// POST /openapi/v1/events/audit/search
{
  "eventTypes": [],
  "entityTypes": [],
  "aspectTypes": [],
  "actorUrns": []
}
```

### Search for Events by a Specific User

To search for all events performed by a specific user:

```json
// POST /openapi/v1/events/audit/search
{
  "eventTypes": [],
  "entityTypes": [],
  "aspectTypes": [],
  "actorUrns": ["urn:li:corpuser:jdoe"]
}
```

### Search for Specific Event Types with Time Range

To search for specific event types within a time range:

```json
// POST /openapi/v1/events/audit/search?startTime=1649953000000&endTime=1649954000000
{
  "eventTypes": ["LogInEvent", "CreateUserEvent"],
  "entityTypes": [],
  "aspectTypes": [],
  "actorUrns": []
}
```

### Search for Events on Specific Entity Types

To search for events related to specific entity types:

```json
// POST /openapi/v1/events/audit/search
{
  "eventTypes": [],
  "entityTypes": ["dataset", "dashboard"],
  "aspectTypes": [],
  "actorUrns": []
}
```

### Paginating through Results

To retrieve the first page of results:

```json
// POST /openapi/v1/events/audit/search?size=25
{
  "eventTypes": [],
  "entityTypes": [],
  "aspectTypes": [],
  "actorUrns": []
}
```

To retrieve subsequent pages, use the `nextScrollId` from the previous response:

```json
// POST /openapi/v1/events/audit/search?scrollId=abcdef123456&size=25
{
  "eventTypes": [],
  "entityTypes": [],
  "aspectTypes": [],
  "actorUrns": []
}
```

## Best Practices

1. **Use Time Ranges**: Always specify start and end times when searching for events to limit the result set and improve performance.

2. **Filter Appropriately**: Use the filtering options (eventTypes, entityTypes, etc.) to narrow down your search to only the events you're interested in.

3. **Paginate Results**: Use the size parameter and scrollId to paginate through large result sets rather than trying to retrieve all events at once.

4. **Monitor User Activity**: Use the actorUrns filter to track actions by specific users, which is useful for security auditing.
