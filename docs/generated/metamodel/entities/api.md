# API

An API is a named callable with a typed input and output schema — an MCP tool, a REST endpoint, a
gRPC method, a GraphQL operation, a function, and so on. Cataloging APIs as first-class entities lets
both humans and agents discover existing callables before building new ones, and makes
caller → API dependencies (the services that compose an API, the agents that invoke it) visible in
the graph.

The kind of API is captured with the standard `subTypes` aspect. The canonical subtypes are
`MCP_TOOL`, `REST_ENDPOINT`, `GRPC_METHOD`, `GRAPHQL_OPERATION`, and `FUNCTION` — each grounded in a
concrete callable kind rather than in how the API happens to be used ("being an agent's tool" is a
relationship, not an intrinsic kind — an MCP tool is just an API).

## Identity

APIs are identified by a single field:

- **id**: A unique identifier for the API, such as `order-lookup-mcp` or a generated UUID.

An example URN is `urn:li:api:order-lookup-mcp`. For REST endpoints the id is minted from the owning
service, HTTP method, and path (e.g. `order-entry-api/GET/orders/{orderId}`) so the same operation
resolves to the same URN whether it is registered by hand or scraped from an OpenAPI spec. Because a
`(method, path)` pair identifies one REST operation, an endpoint served under multiple methods
(GET vs POST on the same path) is modeled as a distinct API entity per method.

## Important Capabilities

### API Properties

The `apiProperties` aspect holds catalog identity:

- **name**: Display name, searchable with autocomplete.
- **description**: What the API does and when to use it.
- **externalUrl**: Optional link to the API's registry entry, documentation, or source.
- **sourceRepository**: The [Repository](./repository.md) the API is produced from
  (`SourcedFrom`), lighting up the `repo → service → api → app → dataset` chain.
- **created** / **lastModified**: Audit stamps.

### Signature

The input/output contract lives on a separate `apiSignature` aspect, kept apart from `apiProperties`
because the signature is scraped from the endpoint (an OpenAPI/JSON-Schema doc, an MCP tool manifest,
a function definition) and evolves on its own cadence — when the contract changes, only this aspect is
re-ingested, leaving catalog identity untouched. It carries:

- **schemaDefinition**: The full input/output schema as an opaque string (typically JSON Schema) — the
  source-of-truth representation that round-trips even constructs the structured fields can't capture.
- **inputFields** / **outputFields**: A structured, typed view reusing DataHub's schema-field model,
  so nested/struct/array types, nullability, and field descriptions render with the standard schema
  components. Each field's `fieldPath` is the parameter name; `nullable=false` means a required argument.

### REST Properties

For APIs of subtype `REST_ENDPOINT`, the `restApiProperties` aspect records the `method` (HTTP verb)
and `path` (route template relative to the owning service's base URL, e.g. `/orders/{orderId}`). These
are kept on a subtype-specific aspect so callers can filter by method and group endpoints that share
a path.

### Relationships

An API is connected to the rest of the software-to-data graph through incoming edges:

- A [Service](./service.md) composes it (`ServiceComposesApi`).
- [AI Agents](./aiAgent.md) invoke it as a tool (`AgentUsesTool`).
- [Agent Skills](./agentSkill.md) require it (`SkillRequiresTool`).

### Governance and Versioning

APIs support the standard governance aspects — ownership, tags, glossary terms, domains, structured
properties, and institutional memory — plus native versioning (`versionProperties`).



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

#### apiProperties
Properties of an API — a named callable with a typed input and output schema
(an MCP tool, REST endpoint, gRPC method, GraphQL operation, function, etc.).
APIs are cataloged as first-class entities so both humans and agents can
discover them before building new ones, and so caller -> API dependencies
(services that compose APIs, agents that invoke them) are visible in the
graph. The kind of API is captured via the standard subTypes aspect, and the
input/output schema lives on the separate apiSignature aspect.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the API. | Searchable |
| description | string |  | Description of what the API does and when to use it. | Searchable |
| externalUrl | string |  | Optional link to the API's registry entry, documentation, or source. |  |
| sourceRepository | string |  | The source-code repository this API is produced from (the SourcedFrom provenance edge). Lights up... | Searchable, → SourcedFrom |
| created | [AuditStamp](#auditstamp) |  | When this API was registered. | Searchable |
| lastModified | [AuditStamp](#auditstamp) |  | When this API was last modified. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "apiProperties"
  },
  "name": "ApiProperties",
  "namespace": "com.linkedin.api",
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
      "name": "name",
      "doc": "Display name of the API."
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
      "doc": "Description of what the API does and when to use it."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "Optional link to the API's registry entry, documentation, or source."
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
      "doc": "The source-code repository this API is produced from (the SourcedFrom\nprovenance edge). Lights up the repo -> service -> api -> app -> dataset chain."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME",
          "queryByDefault": false
        }
      },
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
      "name": "created",
      "default": null,
      "doc": "When this API was registered."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "When this API was last modified."
    }
  ],
  "doc": "Properties of an API \u2014 a named callable with a typed input and output schema\n(an MCP tool, REST endpoint, gRPC method, GraphQL operation, function, etc.).\nAPIs are cataloged as first-class entities so both humans and agents can\ndiscover them before building new ones, and so caller -> API dependencies\n(services that compose APIs, agents that invoke them) are visible in the\ngraph. The kind of API is captured via the standard subTypes aspect, and the\ninput/output schema lives on the separate apiSignature aspect."
}
```





#### apiSignature
The input/output signature of an API — the schema that defines what the
callable accepts and returns. Kept separate from apiProperties because the
signature is scraped from the endpoint (an OpenAPI/JSON-Schema doc, an MCP
tool manifest, a function definition) and evolves on its own cadence: when
the endpoint's contract changes, only this aspect is re-ingested, leaving the
catalog identity (name, ownership, docs) on apiProperties untouched.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| schemaDefinition | string |  | The input/output schema stored as an opaque string (typically JSON Schema). The source-of-truth r... |  |
| inputFields | [SchemaField](#schemafield)[] |  | Typed input parameters of the API's signature (the input schema), reusing DataHub's schema-field ... |  |
| outputFields | [SchemaField](#schemafield)[] |  | The output shape the API returns when invoked, modeled as schema fields (a scalar return is a sin... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "apiSignature"
  },
  "name": "ApiSignature",
  "namespace": "com.linkedin.api",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "schemaDefinition",
      "default": null,
      "doc": "The input/output schema stored as an opaque string (typically JSON Schema).\nThe source-of-truth representation of the signature; round-trips the full\ncontract even when it uses constructs the structured fields below cannot\ncapture. For a structured, typed view use `inputFields` + `outputFields`."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "SchemaField",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "Searchable": {
                  "boostScore": 1.0,
                  "fieldName": "fieldPaths",
                  "fieldType": "TEXT",
                  "queryByDefault": "true"
                },
                "type": "string",
                "name": "fieldPath",
                "doc": "Flattened name of the field. Field is computed from jsonPath field."
              },
              {
                "Deprecated": true,
                "type": [
                  "null",
                  "string"
                ],
                "name": "jsonPath",
                "default": null,
                "doc": "Flattened name of a field in JSON Path notation."
              },
              {
                "type": "boolean",
                "name": "nullable",
                "default": false,
                "doc": "Indicates if this field is optional or nullable"
              },
              {
                "Searchable": {
                  "boostScore": 0.1,
                  "fieldName": "fieldDescriptions",
                  "fieldType": "TEXT",
                  "sanitizeRichText": true
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Description"
              },
              {
                "Deprecated": true,
                "Searchable": {
                  "boostScore": 0.2,
                  "fieldName": "fieldLabels",
                  "fieldType": "TEXT"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "label",
                "default": null,
                "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description.\n\nNote that this field is deprecated and is not surfaced in the UI."
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
                "name": "created",
                "default": null,
                "doc": "An AuditStamp corresponding to the creation of this schema field."
              },
              {
                "type": [
                  "null",
                  "com.linkedin.common.AuditStamp"
                ],
                "name": "lastModified",
                "default": null,
                "doc": "An AuditStamp corresponding to the last modification of this schema field."
              },
              {
                "type": {
                  "type": "record",
                  "name": "SchemaFieldDataType",
                  "namespace": "com.linkedin.schema",
                  "fields": [
                    {
                      "type": [
                        {
                          "type": "record",
                          "name": "BooleanType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Boolean field type."
                        },
                        {
                          "type": "record",
                          "name": "FixedType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Fixed field type."
                        },
                        {
                          "type": "record",
                          "name": "StringType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "String field type."
                        },
                        {
                          "type": "record",
                          "name": "BytesType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Bytes field type."
                        },
                        {
                          "type": "record",
                          "name": "NumberType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Number data type: long, integer, short, etc.."
                        },
                        {
                          "type": "record",
                          "name": "DateType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Date field type."
                        },
                        {
                          "type": "record",
                          "name": "TimeType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Time field type. This should also be used for datetimes."
                        },
                        {
                          "type": "record",
                          "name": "EnumType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Enum field type."
                        },
                        {
                          "type": "record",
                          "name": "NullType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Null field type."
                        },
                        {
                          "type": "record",
                          "name": "MapType",
                          "namespace": "com.linkedin.schema",
                          "fields": [
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "keyType",
                              "default": null,
                              "doc": "Key type in a map"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "valueType",
                              "default": null,
                              "doc": "Type of the value in a map"
                            }
                          ],
                          "doc": "Map field type."
                        },
                        {
                          "type": "record",
                          "name": "ArrayType",
                          "namespace": "com.linkedin.schema",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "string"
                                }
                              ],
                              "name": "nestedType",
                              "default": null,
                              "doc": "List of types this array holds."
                            }
                          ],
                          "doc": "Array field type."
                        },
                        {
                          "type": "record",
                          "name": "UnionType",
                          "namespace": "com.linkedin.schema",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "string"
                                }
                              ],
                              "name": "nestedTypes",
                              "default": null,
                              "doc": "List of types in union type."
                            }
                          ],
                          "doc": "Union field type."
                        },
                        {
                          "type": "record",
                          "name": "RecordType",
                          "namespace": "com.linkedin.schema",
                          "fields": [],
                          "doc": "Record field type."
                        }
                      ],
                      "name": "type",
                      "doc": "Data platform specific types"
                    }
                  ],
                  "doc": "Schema field data types"
                },
                "name": "type",
                "doc": "Platform independent field type of the field."
              },
              {
                "type": "string",
                "name": "nativeDataType",
                "doc": "The native type of the field in the dataset's platform as declared by platform schema."
              },
              {
                "type": "boolean",
                "name": "recursive",
                "default": false,
                "doc": "There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."
              },
              {
                "Relationship": {
                  "/tags/*/tag": {
                    "entityTypes": [
                      "tag"
                    ],
                    "name": "SchemaFieldTaggedWith"
                  }
                },
                "Searchable": {
                  "/tags/*/attribution/actor": {
                    "fieldName": "fieldTagAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/tags/*/attribution/source": {
                    "fieldName": "fieldTagAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/tags/*/attribution/time": {
                    "fieldName": "fieldTagAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  },
                  "/tags/*/tag": {
                    "boostScore": 0.5,
                    "fieldName": "fieldTags",
                    "fieldType": "URN"
                  }
                },
                "type": [
                  "null",
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
                ],
                "name": "globalTags",
                "default": null,
                "doc": "Tags associated with the field"
              },
              {
                "Relationship": {
                  "/terms/*/urn": {
                    "entityTypes": [
                      "glossaryTerm"
                    ],
                    "name": "SchemaFieldWithGlossaryTerm"
                  }
                },
                "Searchable": {
                  "/terms/*/attribution/actor": {
                    "fieldName": "fieldTermAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/terms/*/attribution/source": {
                    "fieldName": "fieldTermAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/terms/*/attribution/time": {
                    "fieldName": "fieldTermAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  },
                  "/terms/*/urn": {
                    "boostScore": 0.5,
                    "fieldName": "fieldGlossaryTerms",
                    "fieldType": "URN"
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "record",
                    "Aspect": {
                      "name": "glossaryTerms"
                    },
                    "name": "GlossaryTerms",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": {
                          "type": "array",
                          "items": {
                            "type": "record",
                            "name": "GlossaryTermAssociation",
                            "namespace": "com.linkedin.common",
                            "fields": [
                              {
                                "Relationship": {
                                  "entityTypes": [
                                    "glossaryTerm"
                                  ],
                                  "name": "TermedWith"
                                },
                                "Searchable": {
                                  "addToFilters": true,
                                  "fieldName": "glossaryTerms",
                                  "fieldType": "URN",
                                  "filterNameOverride": "Glossary Term",
                                  "hasValuesFieldName": "hasGlossaryTerms",
                                  "includeSystemModifiedAt": true,
                                  "systemModifiedAtFieldName": "termsModifiedAt"
                                },
                                "java": {
                                  "class": "com.linkedin.common.urn.GlossaryTermUrn"
                                },
                                "type": "string",
                                "name": "urn",
                                "doc": "Urn of the applied glossary term"
                              },
                              {
                                "java": {
                                  "class": "com.linkedin.common.urn.Urn"
                                },
                                "type": [
                                  "null",
                                  "string"
                                ],
                                "name": "actor",
                                "default": null,
                                "doc": "The user URN which will be credited for adding associating this term to the entity"
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
                                    "fieldName": "termAttributionActors",
                                    "fieldType": "URN",
                                    "queryByDefault": false
                                  },
                                  "/source": {
                                    "fieldName": "termAttributionSources",
                                    "fieldType": "URN",
                                    "queryByDefault": false
                                  },
                                  "/time": {
                                    "fieldName": "termAttributionDates",
                                    "fieldType": "DATETIME",
                                    "queryByDefault": false
                                  }
                                },
                                "type": [
                                  "null",
                                  "com.linkedin.common.MetadataAttribution"
                                ],
                                "name": "attribution",
                                "default": null,
                                "doc": "Information about who, why, and how this metadata was applied"
                              }
                            ],
                            "doc": "Properties of an applied glossary term."
                          }
                        },
                        "name": "terms",
                        "doc": "The related business terms"
                      },
                      {
                        "type": "com.linkedin.common.AuditStamp",
                        "name": "auditStamp",
                        "doc": "Audit stamp containing who reported the related business term"
                      }
                    ],
                    "doc": "Related business terms information"
                  }
                ],
                "name": "glossaryTerms",
                "default": null,
                "doc": "Glossary terms associated with the field"
              },
              {
                "type": "boolean",
                "name": "isPartOfKey",
                "default": false,
                "doc": "For schema fields that are part of complex keys, set this field to true\nWe do this to easily distinguish between value and key fields"
              },
              {
                "type": [
                  "null",
                  "boolean"
                ],
                "name": "isPartitioningKey",
                "default": null,
                "doc": "For Datasets which are partitioned, this determines the partitioning key.\nNote that multiple columns can be part of a partitioning key, but currently we do not support\nrendering the ordered partitioning key."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "jsonProps",
                "default": null,
                "doc": "For schema fields that have other properties that are not modeled explicitly,\nuse this field to serialize those properties into a JSON string"
              }
            ],
            "doc": "SchemaField to describe metadata related to dataset schema."
          }
        }
      ],
      "name": "inputFields",
      "default": null,
      "doc": "Typed input parameters of the API's signature (the input schema), reusing\nDataHub's schema-field model so nested/struct/array types, nullability, and\nfield descriptions render with the standard schema components. Each field's\nfieldPath is the parameter name; nullable=false means a required argument."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.schema.SchemaField"
        }
      ],
      "name": "outputFields",
      "default": null,
      "doc": "The output shape the API returns when invoked, modeled as schema fields\n(a scalar return is a single field; an object return is a field per member)."
    }
  ],
  "doc": "The input/output signature of an API \u2014 the schema that defines what the\ncallable accepts and returns. Kept separate from apiProperties because the\nsignature is scraped from the endpoint (an OpenAPI/JSON-Schema doc, an MCP\ntool manifest, a function definition) and evolves on its own cadence: when\nthe endpoint's contract changes, only this aspect is re-ingested, leaving the\ncatalog identity (name, ownership, docs) on apiProperties untouched."
}
```





#### restApiProperties
REST-specific properties for APIs of subtype REST_ENDPOINT.

Only attached to API entities where subType = REST_ENDPOINT. The (method, path)
pair identifies a REST operation, so an endpoint that serves the same path
under multiple HTTP methods (e.g. GET vs POST /orders) is modeled as a
distinct API entity per method, each carrying its own apiSignature. Kept on
this subtype-specific aspect rather than the protocol-agnostic apiProperties
so callers can filter by method and group endpoints that share a path.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| method | HttpMethod | ✓ | The HTTP method this endpoint responds to. | Searchable |
| path | string | ✓ | The route/path template of the endpoint, relative to the owning service's base URL, e.g. "/orders... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "restApiProperties"
  },
  "name": "RestApiProperties",
  "namespace": "com.linkedin.api",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "HTTP Method"
      },
      "type": {
        "type": "enum",
        "name": "HttpMethod",
        "namespace": "com.linkedin.api",
        "symbols": [
          "GET",
          "POST",
          "PUT",
          "PATCH",
          "DELETE",
          "HEAD",
          "OPTIONS",
          "TRACE"
        ]
      },
      "name": "method",
      "doc": "The HTTP method this endpoint responds to."
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Route"
      },
      "type": "string",
      "name": "path",
      "doc": "The route/path template of the endpoint, relative to the owning service's\nbase URL, e.g. \"/orders/{orderId}\". Endpoints that share a path (across\nmethods) can be grouped by filtering on this value."
    }
  ],
  "doc": "REST-specific properties for APIs of subtype REST_ENDPOINT.\n\nOnly attached to API entities where subType = REST_ENDPOINT. The (method, path)\npair identifies a REST operation, so an endpoint that serves the same path\nunder multiple HTTP methods (e.g. GET vs POST /orders) is modeled as a\ndistinct API entity per method, each carrying its own apiSignature. Kept on\nthis subtype-specific aspect rather than the protocol-agnostic apiProperties\nso callers can filter by method and group endpoints that share a path."
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





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
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
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
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
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
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





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
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
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
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
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
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
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
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
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
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
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### versionProperties
Properties about a versioned asset i.e. dataset, ML Model, etc.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| versionSet | string | ✓ | The linked Version Set entity that ties multiple versioned assets together | Searchable, → VersionOf |
| version | [VersionTag](#versiontag) | ✓ | Label for this versioned asset, is unique within a version set | Searchable |
| aliases | [VersionTag](#versiontag)[] | ✓ | Associated aliases for this versioned asset | Searchable |
| comment | string |  | Comment documenting what this version was created for, changes, or represents |  |
| sortId | string | ✓ | Sort identifier that determines where a version lives in the order of the Version Set. What this ... | Searchable (versionSortId) |
| versioningScheme | VersioningScheme | ✓ | What versioning scheme `sortId` belongs to. Defaults to a plain string that is lexicographically ... |  |
| sourceCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when this asset version was created in the source system. |  |
| metadataCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when the metadata for this version was created in DataHub |  |
| isLatest | boolean |  | Marks whether this version is currently the latest. Set by a side effect and should not be modifi... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionProperties"
  },
  "name": "VersionProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "versionSet"
        ],
        "name": "VersionOf"
      },
      "Searchable": {
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "versionSet",
      "doc": "The linked Version Set entity that ties multiple versioned assets together"
    },
    {
      "Searchable": {
        "/versionTag": {
          "fieldName": "version",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "record",
        "name": "VersionTag",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "string"
            ],
            "name": "versionTag",
            "default": null
          },
          {
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
            "name": "metadataAttribution",
            "default": null
          }
        ],
        "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
      },
      "name": "version",
      "doc": "Label for this versioned asset, is unique within a version set"
    },
    {
      "Searchable": {
        "/*/versionTag": {
          "fieldName": "aliases",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.VersionTag"
      },
      "name": "aliases",
      "default": [],
      "doc": "Associated aliases for this versioned asset"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "comment",
      "default": null,
      "doc": "Comment documenting what this version was created for, changes, or represents"
    },
    {
      "Searchable": {
        "fieldName": "versionSortId",
        "queryByDefault": false
      },
      "type": "string",
      "name": "sortId",
      "doc": "Sort identifier that determines where a version lives in the order of the Version Set.\nWhat this looks like depends on the Version Scheme. For sort ids generated by DataHub we use an 8 character string representation."
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "default": "LEXICOGRAPHIC_STRING",
      "doc": "What versioning scheme `sortId` belongs to.\nDefaults to a plain string that is lexicographically sorted."
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
      "name": "sourceCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when this asset version was created in the source system."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "metadataCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when the metadata for this version was created in DataHub"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": [
        "null",
        "boolean"
      ],
      "name": "isLatest",
      "default": null,
      "doc": "Marks whether this version is currently the latest. Set by a side effect and should not be modified by API."
    }
  ],
  "doc": "Properties about a versioned asset i.e. dataset, ML Model, etc."
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

#### SchemaField

SchemaField to describe metadata related to dataset schema.

**Fields:**

- `fieldPath` (string): Flattened name of the field. Field is computed from jsonPath field.
- `jsonPath` (string?): Flattened name of a field in JSON Path notation.
- `nullable` (boolean): Indicates if this field is optional or nullable
- `description` (string?): Description
- `label` (string?): Label of the field. Provides a more human-readable name for the field than fi...
- `created` (AuditStamp?): An AuditStamp corresponding to the creation of this schema field.
- `lastModified` (AuditStamp?): An AuditStamp corresponding to the last modification of this schema field.
- `type` (SchemaFieldDataType): Platform independent field type of the field.
- `nativeDataType` (string): The native type of the field in the dataset's platform as declared by platfor...
- `recursive` (boolean): There are use cases when a field in type B references type A. A field in A re...
- `globalTags` (GlobalTags?): Tags associated with the field
- `glossaryTerms` (GlossaryTerms?): Glossary terms associated with the field
- `isPartOfKey` (boolean): For schema fields that are part of complex keys, set this field to true We do...
- `isPartitioningKey` (boolean?): For Datasets which are partitioned, this determines the partitioning key. Not...
- `jsonProps` (string?): For schema fields that have other properties that are not modeled explicitly,...

#### VersionTag

A resource-defined string representing the resource state for the purpose of concurrency control

**Fields:**

- `versionTag` (string?): 
- `metadataAttribution` (MetadataAttribution?): 


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- SourcedFrom

   - Repository via `apiProperties.sourceRepository`
- SchemaFieldTaggedWith

   - Tag via `apiSignature.inputFields.globalTags`
   - Tag via `apiSignature.outputFields.globalTags`
- TaggedWith

   - Tag via `apiSignature.inputFields.globalTags.tags`
   - Tag via `apiSignature.outputFields.globalTags.tags`
   - Tag via `globalTags.tags`
- SchemaFieldWithGlossaryTerm

   - GlossaryTerm via `apiSignature.inputFields.glossaryTerms`
   - GlossaryTerm via `apiSignature.outputFields.glossaryTerms`
- TermedWith

   - GlossaryTerm via `apiSignature.inputFields.glossaryTerms.terms.urn`
   - GlossaryTerm via `apiSignature.outputFields.glossaryTerms.terms.urn`
   - GlossaryTerm via `glossaryTerms.terms.urn`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- AssociatedWith

   - Domain via `domains.domains`
- VersionOf

   - VersionSet via `versionProperties.versionSet`
#### Incoming
These are the relationships stored in other entity's aspects
- Consumes

   - Application via `applicationLineage.inputEdges`
- Produces

   - Application via `applicationLineage.outputEdges`
- ServiceComposesApi

   - Service via `serviceProperties.apis`
- AgentUsesTool

   - AiAgent via `aiAgentDependencies.tools`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
