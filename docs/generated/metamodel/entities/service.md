# Service

A Service is a catalog entry for an external service that exposes callable APIs — an MCP server, a
REST API, a GraphQL API, a gRPC service, and so on. A Service groups the [APIs](./api.md) it exposes,
carries the full interface definition, and sits between the [Repository](./repository.md) that produces
it and the [Applications](./application.md)/agents that consume it in the
`repo → service → api → app → dataset` chain.

The specific kind of service is recorded with the standard `subTypes` aspect (e.g. `MCP`, `REST_API`,
`OPEN_API`, `GRPC`). Note that a Service holds only catalog information (identity, description,
interface); operational configuration such as authentication for DataHub's Ask DataHub integration
lives separately in `GlobalSettings.aiPlugins`, not on the entity.

## Identity

Services are identified by a single field:

- **id**: A unique identifier for the service, such as `glean-search`, `internal-tools`, or
  `weather-api`.

An example URN is `urn:li:service:glean-search`.

## Important Capabilities

### Service Properties

The `serviceProperties` aspect holds catalog identity and the service's place in the graph:

- **displayName**: Name shown in the UI and search results, searchable with autocomplete.
- **description**: What the service provides.
- **lifecycle**: Lifecycle stage — `EXPERIMENTAL`, `PRODUCTION`, or `DEPRECATED` — rendered as a badge
  so consumers can see at a glance whether the service is safe to depend on.
- **apis**: The [APIs](./api.md) this service composes/exposes (`ServiceComposesApi`). This is a lineage
  edge with the API downstream of the service, so endpoints nest correctly under their service.
- **sourceRepository**: The [Repository](./repository.md) the service is produced from (`SourcedFrom`).

### Service Definition

The `serviceDefinition` aspect carries the full interface document — the source of truth from which
the operation-level API entities are parsed. It records:

- **format**: How to parse `rawSpec` — `OPENAPI`, `GRAPHQL_SDL`, `GRPC_PROTO`, `ASYNCAPI`,
  `JSON_SCHEMA`, or `OTHER`.
- **rawSpec**: The entire spec document, verbatim. Stored as a `LargeString` (optionally compressed)
  so large specs fit under the aspect-size limit; readers receive the decompressed text. Rendered
  read-only on the service profile's Definition tab.
- **version** / **externalUrl**: Optional spec version and a link to the canonical hosted source.

It is kept as its own aspect (rather than a field on `serviceProperties`) because the spec blob is
large and only needed on the profile, whereas `serviceProperties` is fetched on every search card and
header.

### MCP Server Properties

For services of subtype `MCP`, the `mcpServerProperties` aspect holds connection details: the server
`url`, the `transport` (`HTTP`, `SSE`, or `WEBSOCKET`), an optional connection `timeout`, and any
non-auth `customHeaders` (e.g. `X-Tenant-ID`). Authentication headers are configured in
`GlobalSettings.aiPlugins`, not here.

### Health and Governance

Services surface operational health through the shared incidents subsystem (`incidentsSummary`) and
support the standard ownership, tags, and status aspects.



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### serviceProperties
Common properties for all Service types.

A Service is a catalog entry for an external service (MCP server, REST API, etc.).
This aspect contains identity and descriptive information.
Subtype-specific properties are in separate aspects (e.g., McpServerProperties).

Note: This contains only catalog information (identity, description).
Ask DataHub configuration (auth, instructions) is in GlobalSettings.aiPlugins.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| displayName | string | ✓ | Display name shown in UI and search results. | Searchable |
| description | string |  | Description of what this service provides. | Searchable |
| lifecycle | ServiceLifecycle |  | Lifecycle stage of the service, from experimental through production to deprecation. Rendered as ... | Searchable |
| apis | string[] |  | The APIs (callables) this service composes / exposes — e.g. the tools an MCP server serves, or th... | Searchable, → ServiceComposesApi |
| sourceRepository | string |  | The source-code repository this service is produced from (the SourcedFrom provenance edge). Light... | Searchable, → SourcedFrom |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "serviceProperties",
    "schemaVersion": 2
  },
  "name": "ServiceProperties",
  "namespace": "com.linkedin.service",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "displayName",
      "doc": "Display name shown in UI and search results."
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of what this service provides."
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Lifecycle"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "DEPRECATED": " Being retired; consumers should migrate off. ",
            "EXPERIMENTAL": " Early / unstable; may change or disappear without notice. ",
            "PRODUCTION": " Stable and supported for production use. "
          },
          "name": "ServiceLifecycle",
          "namespace": "com.linkedin.service",
          "symbols": [
            "EXPERIMENTAL",
            "PRODUCTION",
            "DEPRECATED"
          ]
        }
      ],
      "name": "lifecycle",
      "default": null,
      "doc": "Lifecycle stage of the service, from experimental through production to\ndeprecation. Rendered as a small badge on the profile so consumers can see\nat a glance whether the service is safe to depend on."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "api"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "ServiceComposesApi"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "apis",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "apis",
      "default": null,
      "doc": "The APIs (callables) this service composes / exposes \u2014 e.g. the tools an\nMCP server serves, or the endpoints a REST API exposes. The reverse edge\n(an API \"served by\" its service) is the incoming ServiceComposesApi\nrelationship on the API.\n\nThis is a lineage edge: the API is downstream of the service that exposes\nit, so the software-to-data chain nests correctly as\nrepo -> service -> api -> app -> dataset (the endpoints hang off their\nservice, not off the repository directly)."
    },
    {
      "Relationship": {
        "entityTypes": [
          "repository"
        ],
        "isLineage": true,
        "isUpstream": true,
        "name": "SourcedFrom"
      },
      "Searchable": {
        "fieldName": "sourceRepository",
        "fieldType": "URN",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "sourceRepository",
      "default": null,
      "doc": "The source-code repository this service is produced from (the SourcedFrom\nprovenance edge). Lights up the repo -> service -> api -> app -> dataset chain:\nthe Repository profile shows incoming SourcedFrom edges as \"what it produces\"."
    }
  ],
  "doc": "Common properties for all Service types.\n\nA Service is a catalog entry for an external service (MCP server, REST API, etc.).\nThis aspect contains identity and descriptive information.\nSubtype-specific properties are in separate aspects (e.g., McpServerProperties).\n\nNote: This contains only catalog information (identity, description).\nAsk DataHub configuration (auth, instructions) is in GlobalSettings.aiPlugins."
}
```





#### mcpServerProperties
MCP-specific properties for Services of subtype MCP.

Only attached to Service entities where subType = MCP.
Contains connection details for the MCP server.

Note: This contains only connection information (URL, transport, timeout).
Authentication and Ask DataHub configuration are in GlobalSettings.aiPlugins.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| url | string | ✓ | MCP server endpoint URL. Example: https://mcp.glean.com/v1 | Searchable |
| transport | McpTransport | ✓ | Transport protocol for MCP communication. |  |
| timeout | float |  | Connection timeout in seconds. When absent, the integrations service applies its own default (cur... |  |
| customHeaders | map |  | Custom headers to send with every request. These are non-auth headers (e.g., X-Tenant-ID, X-Clien... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mcpServerProperties",
    "schemaVersion": 2
  },
  "name": "McpServerProperties",
  "namespace": "com.linkedin.service",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT",
        "queryByDefault": false
      },
      "type": "string",
      "name": "url",
      "doc": "MCP server endpoint URL.\nExample: https://mcp.glean.com/v1"
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "HTTP": "Standard HTTP/HTTPS transport.",
          "SSE": "Server-Sent Events for streaming responses.",
          "WEBSOCKET": "WebSocket for bidirectional communication."
        },
        "name": "McpTransport",
        "namespace": "com.linkedin.service",
        "symbols": [
          "HTTP",
          "SSE",
          "WEBSOCKET"
        ]
      },
      "name": "transport",
      "default": "HTTP",
      "doc": "Transport protocol for MCP communication."
    },
    {
      "type": [
        "null",
        "float"
      ],
      "name": "timeout",
      "default": null,
      "doc": "Connection timeout in seconds.\nWhen absent, the integrations service applies its own default (currently 300s)."
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "customHeaders",
      "default": null,
      "doc": "Custom headers to send with every request.\nThese are non-auth headers (e.g., X-Tenant-ID, X-Client-Version).\nAuth headers are configured in GlobalSettings.aiPlugins."
    }
  ],
  "doc": "MCP-specific properties for Services of subtype MCP.\n\nOnly attached to Service entities where subType = MCP.\nContains connection details for the MCP server.\n\nNote: This contains only connection information (URL, transport, timeout).\nAuthentication and Ask DataHub configuration are in GlobalSettings.aiPlugins."
}
```





#### serviceDefinition
The definition document for a Service — the full OpenAPI YAML, GraphQL
SDL, gRPC .proto, or AsyncAPI document that describes everything the service
exposes.

This is the source of truth for the service's interface. The operation-level
`api` entities the service composes (the ServiceComposesApi relationship) are
the parsed projection of this document.

Kept as a separate aspect (not a field on ServiceProperties) because the
rawSpec blob is large and only needed on the profile — ServiceProperties
is fetched on every search card and header, this is not.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| format | ServiceDefinitionFormat | ✓ | The spec format, so consumers know how to parse and render `rawSpec`. |  |
| rawSpec | LargeString | ✓ | The entire spec document, verbatim (the source of truth). Rendered read-only on the Service profi... |  |
| version | string |  | Spec version, e.g. the OpenAPI `info.version`. Optional. |  |
| externalUrl | string |  | Link to the canonical source of the spec, if hosted elsewhere. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "serviceDefinition"
  },
  "name": "ServiceDefinition",
  "namespace": "com.linkedin.service",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ASYNCAPI": " An AsyncAPI (event / streaming) document. ",
          "GRAPHQL_SDL": " A GraphQL Schema Definition Language document. ",
          "GRPC_PROTO": " A gRPC Protocol Buffers (.proto) service definition. ",
          "JSON_SCHEMA": " A raw JSON Schema document (e.g. an MCP server's tools/list payload). ",
          "OPENAPI": " An OpenAPI (v2/Swagger or v3) document. ",
          "OTHER": " Any other spec format not enumerated above. "
        },
        "name": "ServiceDefinitionFormat",
        "namespace": "com.linkedin.service",
        "symbols": [
          "OPENAPI",
          "GRAPHQL_SDL",
          "GRPC_PROTO",
          "ASYNCAPI",
          "JSON_SCHEMA",
          "OTHER"
        ]
      },
      "name": "format",
      "doc": "The spec format, so consumers know how to parse and render `rawSpec`."
    },
    {
      "type": {
        "type": "record",
        "name": "LargeString",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "string",
            "name": "blob",
            "doc": " When compression = NONE, the raw UTF-8 text. Otherwise base64(codec(utf8(text))) \u2014 base64 keeps it JSON-safe. "
          },
          {
            "type": {
              "type": "enum",
              "name": "CompressionType",
              "namespace": "com.linkedin.common",
              "symbols": [
                "NONE",
                "GZIP"
              ]
            },
            "name": "compression",
            "default": "NONE",
            "doc": " Codec applied to `blob`. "
          },
          {
            "type": [
              "null",
              "long"
            ],
            "name": "uncompressedSize",
            "default": null,
            "doc": " Byte length of the original (decompressed) UTF-8 text \u2014 for display/budgeting without decoding. "
          }
        ],
        "doc": "A string whose stored form may be compressed, so large text (API specs,\nschemas, docs) can live in an aspect without breaching the aspect-size limit.\nThe logical value is always the decompressed UTF-8 text; `compression` tells\nconsumers how to decode `blob`. NOTE: never mark a LargeString field\n@Searchable \u2014 a compressed blob cannot be indexed (enforced at model load)."
      },
      "name": "rawSpec",
      "doc": "The entire spec document, verbatim (the source of truth). Rendered\nread-only on the Service profile's Definition tab. Stored as a LargeString so\nlarge specs (full OpenAPI docs, schemas) compress under the aspect-size\nlimit; readers get the decompressed text (decoded server-side in the\nGraphQL mapper)."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "version",
      "default": null,
      "doc": "Spec version, e.g. the OpenAPI `info.version`. Optional."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "Link to the canonical source of the spec, if hosted elsewhere."
    }
  ],
  "doc": "The definition document for a Service \u2014 the full OpenAPI YAML, GraphQL\nSDL, gRPC .proto, or AsyncAPI document that describes everything the service\nexposes.\n\nThis is the source of truth for the service's interface. The operation-level\n`api` entities the service composes (the ServiceComposesApi relationship) are\nthe parsed projection of this document.\n\nKept as a separate aspect (not a field on ServiceProperties) because the\nrawSpec blob is large and only needed on the profile \u2014 ServiceProperties\nis fetched on every search card and header, this is not."
}
```





#### incidentsSummary
Summary related incidents on an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| resolvedIncidents | string[] | ✓ | Resolved incidents for an asset Deprecated! Use the richer resolvedIncidentsDetails instead. | ⚠️ Deprecated |
| activeIncidents | string[] | ✓ | Active incidents for an asset Deprecated! Use the richer activeIncidentsDetails instead. | ⚠️ Deprecated |
| resolvedIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of resolved incidents | Searchable, → ResolvedIncidents |
| activeIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of active incidents | Searchable, → ActiveIncidents |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### semanticContent
Semantic content for enabling vector similarity search.

This aspect stores chunked text and embedding vectors for any entity that supports semantic search. Generation of the data for this aspect should be built in tight collaboration with the embedding generator during semantic search query processing. Chunk determination and generation can happen somewhat independently
The data in this aspect is directly passed along to the semantic search index.

Design notes:
- Supports multiple embedding models (e.g., different providers or versions)
- Supports chunked content for long documents
- Text field is optional to support privacy-sensitive use cases where
  only embeddings (not source text) are shared with DataHub



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| embeddings | map | ✓ | Map of embedding model name to embedding data. Key is the model identifier (e.g., cohere_embed_v3... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "semanticContent"
  },
  "name": "SemanticContent",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "map",
        "values": {
          "type": "record",
          "name": "EmbeddingModelData",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "modelVersion",
              "doc": "Full model identifier including provider.\nExamples: bedrock/cohere.embed-english-v3, openai/text-embedding-ada-002"
            },
            {
              "type": "long",
              "name": "generatedAt",
              "doc": "Timestamp when embeddings were generated (milliseconds since epoch)."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "chunkingStrategy",
              "default": null,
              "doc": "Description of the chunking strategy used.\nExamples: sentence_boundary_400t, fixed_512_chars, paragraph"
            },
            {
              "type": "int",
              "name": "totalChunks",
              "doc": "Total number of chunks."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "totalTokens",
              "default": null,
              "doc": "Estimated total token count across all chunks."
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "EmbeddingChunk",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "int",
                      "name": "position",
                      "doc": "Zero-based position/index of this chunk within the document."
                    },
                    {
                      "type": {
                        "type": "array",
                        "items": "float"
                      },
                      "name": "vector",
                      "doc": "The embedding vector for this chunk.\nDimensionality depends on the model (e.g., 1024 for Cohere v3)."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "characterOffset",
                      "default": null,
                      "doc": "Character offset from start of source text.\nUsed for highlighting and navigation. Optional for privacy."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "characterLength",
                      "default": null,
                      "doc": "Character length of the chunk in source text.\nOptional for privacy-sensitive use cases."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "tokenCount",
                      "default": null,
                      "doc": "Estimated token count for this chunk."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "text",
                      "default": null,
                      "doc": "Original text of this chunk.\nOPTIONAL: May be omitted for privacy-sensitive data sources\nwhere only embeddings should be stored, not source text."
                    }
                  ],
                  "doc": "A single chunk of text with its embedding vector.\n\nChunks enable semantic search over long documents by breaking them\ninto smaller, semantically coherent pieces that fit within model\ncontext windows."
                }
              },
              "name": "chunks",
              "doc": "Individual chunks with their embedding vectors."
            }
          ],
          "doc": "Embedding data for a specific model.\nContains metadata about the embedding generation and the chunked vectors."
        }
      },
      "name": "embeddings",
      "doc": "Map of embedding model name to embedding data.\nKey is the model identifier (e.g., cohere_embed_v3, openai_ada_002).\nAllows storing embeddings from multiple models simultaneously."
    }
  ],
  "doc": "Semantic content for enabling vector similarity search.\n\nThis aspect stores chunked text and embedding vectors for any entity that supports semantic search. Generation of the data for this aspect should be built in tight collaboration with the embedding generator during semantic search query processing. Chunk determination and generation can happen somewhat independently\nThe data in this aspect is directly passed along to the semantic search index.\n\nDesign notes:\n- Supports multiple embedding models (e.g., different providers or versions)\n- Supports chunked content for long documents\n- Text field is optional to support privacy-sensitive use cases where\n  only embeddings (not source text) are shared with DataHub"
}
```





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### IncidentSummaryDetails

Summary statistics about incidents on an entity.

**Fields:**

- `urn` (string): The urn of the incident
- `type` (string): The type of an incident
- `createdAt` (long): The time at which the incident was raised in milliseconds since epoch.
- `resolvedAt` (long?): The time at which the incident was marked as resolved in milliseconds since e...
- `priority` (int?): The priority of the incident


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- ServiceComposesApi

   - Api via `serviceProperties.apis`
- SourcedFrom

   - Repository via `serviceProperties.sourceRepository`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
#### Incoming
These are the relationships stored in other entity's aspects
- IncidentOn

   - Incident via `incidentInfo.entities`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
