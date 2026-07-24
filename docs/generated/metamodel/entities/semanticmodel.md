# Semantic Model

The semantic model entity represents a logical data model that groups datasets and defines the
dimensional context (fields, dimensions, relationships) used by metrics. It serves as the bridge
between raw datasets and the business metrics calculated over them.

## Identity

Semantic models are identified by three fields:

- **`platform`** — the DataPlatform URN that owns this semantic model
  (e.g. `urn:li:dataPlatform:dbt`, `urn:li:dataPlatform:snowflake`). Searchable as a URN field
  with autocomplete and a "Platform" filter pill.
- **`path`** — the namespace path that scopes this semantic model within its platform, preventing
  name collisions when two teams define models with the same `id` on the same platform.
- **`id`** — the model name within that platform and path
  (e.g. `orders_model`, `customer_360`).

An example URN: `urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics,orders_model)`.

## Important Capabilities

### Semantic Model Info

Core metadata is stored in the `semanticModelInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the model represents.
- **`created`** -- `AuditStamp` (`time` + `actor`) capturing when the semantic model was created
  and by whom. Search-indexed as `createdAt` (DATETIME).
- **`lastModified`** -- `AuditStamp` capturing the most recent modification.
  Search-indexed as `lastModifiedAt` (DATETIME).
- **`nativeDefinition`** — optional verbatim source definition (e.g. the Snowflake
  `CREATE SEMANTIC VIEW` DDL, the dbt `semantic_model` YAML, or the Databricks
  `CREATE METRIC VIEW` DDL). Preserved as-is for round-tripping and debugging; not
  parsed by DataHub.
- **`datasets`** — array of `ModelDataset` records, each linking a logical dataset name to a
  source URN. The source is normally a `dataset` URN, but may be a `query` URN when the semantic
  model uses an inline SQL query with no backing table.
- **`relationships`** — optional array of `SemanticModelRelationship` records describing join
  paths between the logical datasets in this model (from-table, to-table, join columns, optional
  name, optional cardinality reusing `ERModelRelationshipCardinality`, and AI context).
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.

### Inline Query Sources

Some semantic-layer platforms allow a metric view or model to source from an inline SQL query
rather than a persistent table (e.g. Databricks metric views with `source: <SELECT ...>`).
DataHub represents these using the `query` entity:

1. **Emit the `query` entity.** The ingestion source constructs a `query` URN whose id is a
   content-hash of the normalized SQL
2. **Point `ModelDataset.source` at the query URN.** No other change is needed on the
   SemanticModel side; the `SourcedBy` edge is emitted the same way for both target kinds.

### Fields and Dimensions

Each `ModelDataset` entry can carry a list of `SemanticField` records that describe the columns
exposed by the semantic model:

- **`schemaField`** — required inline `SchemaField` that gives this field its identity and
  governance surface. The `fieldPath` inside it becomes the field-path component of the
  `urn:li:schemaField:(<semanticModelUrn>,<fieldPath>)` URN used for column-level lineage edges.
- **`type`** — required `SemanticFieldType` enum identifying the kind of field: `DIMENSION` (grouping /
  filtering attribute), `MEASURE` (aggregatable numeric value), `FILTER` (named boolean predicate),
  or `OTHER` (forward-compat escape hatch for source constructs that do not map cleanly to the
  three named kinds).
- **`expression`** — the underlying SQL expression(s) in one or more dialects.
- **`dimension`** — optional `Dimension` record; populated only when `type == DIMENSION`. Currently
  exposes `isTime: boolean` to flag time dimensions used for date-range filtering.
- **`aiContext`** — AI hints specific to this field.

### Lineage

Semantic models support both table-level and column-level lineage via the `upstreamLineage` aspect,
the same aspect used by datasets.

**Table-level upstream lineage** — populate `upstreamLineage.upstreams` with one `Upstream` entry
per source table or query.

**Column-level lineage** — populate `upstreamLineage.fineGrainedLineages` with `FineGrainedLineage`
records.

**Metrics as downstream nodes** — metrics that declare `metricInfo.semanticModel` pointing at this
entity automatically appear as downstream nodes in the lineage explorer.

### Governance

The semantic model entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`.

## Relationships with Other Entities

| Relationship | Direction | Target entity      | Aspect / edge name  |
| ------------ | --------- | ------------------ | ------------------- |
| SourcedBy    | outbound  | `dataset`, `query` | `semanticModelInfo` |
| UpstreamOf   | outbound  | `dataset`, `query` | `upstreamLineage`   |
| ModeledBy    | inbound   | `metric`           | `metricInfo`        |

The `SourcedBy` edges are derived from the `datasets[].source` URN fields in `semanticModelInfo`.
The `upstreamLineage` aspect provides the full lineage graph traversal path (table-level and
column-level), so both upstream sources and downstream metrics appear in the lineage explorer.

## Notable Exceptions

### SemanticModelRelationship vs common Relationship

The join-path record is named `SemanticModelRelationship` (rather than `Relationship`) to avoid a
name collision with DataHub's `com.linkedin.common.Relationship` model.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `semanticModel` entity. Structured properties support typed values, governance controls,
search facets, and PATCH semantics — they are the recommended mechanism for any platform-specific
metadata that does not warrant a first-class PDL field.

Ingestion sources that need to store per-field vendor blobs should hoist the data into entity-level
`structuredProperties` keyed by field name.



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

#### semanticModelInfo
Core information about a SemanticModel entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| externalUrl | string |  | URL where the reference exist | Searchable |
| name | string | ✓ | Display name of the semantic model. | Searchable |
| description | string |  | Human-readable description of the semantic model. | Searchable |
| created | [AuditStamp](#auditstamp) |  | Audit stamp capturing when this semantic model was created and by whom (as reported by the source... | Searchable |
| lastModified | [AuditStamp](#auditstamp) |  | Audit stamp capturing when this semantic model was last modified and by whom. Equals `created` wh... | Searchable |
| nativeDefinition | string |  | The raw, native definition of this semantic model as authored on the source platform (e.g. the Sn... |  |
| aiContext | AiContext |  | AI-specific context for improved disambiguation and retrieval. |  |
| datasets | ModelDataset[] | ✓ | The logical datasets that this semantic model exposes. |  |
| relationships | SemanticModelRelationship[] |  | Join relationships between the datasets of this semantic model. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "semanticModelInfo",
    "schemaVersion": 3
  },
  "name": "SemanticModelInfo",
  "namespace": "com.linkedin.semanticmodel",
  "fields": [
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the semantic model."
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Human-readable description of the semantic model."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME",
          "searchLabel": "createdAt"
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
      "doc": "Audit stamp capturing when this semantic model was created and by whom\n(as reported by the source platform / ingestor)."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME",
          "searchLabel": "lastModifiedAt"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "Audit stamp capturing when this semantic model was last modified and by whom.\nEquals `created` when no modification has occurred since ingestion."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "nativeDefinition",
      "default": null,
      "doc": "The raw, native definition of this semantic model as authored on the\nsource platform (e.g. the Snowflake `CREATE SEMANTIC VIEW` DDL, the\ndbt `semantic_model` YAML, or the Databricks `CREATE METRIC VIEW`\nDDL). Preserved verbatim so consumers can round-trip / debug against\nthe original source; not itself parsed by DataHub."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AiContext",
          "namespace": "com.linkedin.metric",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "synonyms",
              "default": null,
              "doc": "Alternative names or abbreviations by which this metric is known."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "instructions",
              "default": null,
              "doc": "Human-readable guidance for AI models on how to interpret this metric."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "examples",
              "default": null,
              "doc": "Example values or usage patterns to ground AI reasoning."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "customInstructions",
              "default": null,
              "doc": "Additional free-form instructions for AI model customisation."
            }
          ],
          "doc": "AI-specific context attached to a metric or field to improve disambiguation,\nretrieval, and generation quality (OSI ai_context shape)."
        }
      ],
      "name": "aiContext",
      "default": null,
      "doc": "AI-specific context for improved disambiguation and retrieval."
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "ModelDataset",
          "namespace": "com.linkedin.semanticmodel",
          "fields": [
            {
              "type": "string",
              "name": "name",
              "doc": "Logical alias used to reference this dataset within the semantic model."
            },
            {
              "Relationship": {
                "entityTypes": [
                  "dataset",
                  "query"
                ],
                "name": "SourcedBy"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "source",
              "doc": "URN of the DataHub entity that backs this logical dataset. Accepts either:\n - a `dataset` URN \u2014 when the semantic model reads from a physical table or persisted view\n   (the common case).\n - a `query` URN \u2014 when the source is an inline SQL query with no persistent backing table\n   (e.g. Databricks metric views that use `source: <SELECT ...>`). Ingestion emits a `query`\n   entity carrying the SQL text (`queryProperties.statement`) and its upstream tables\n   (`querySubjects`), then points this field at that URN. The `query`'s `origin` field\n   should point back at the enclosing SemanticModel URN."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "SemanticField",
                    "namespace": "com.linkedin.semanticmodel",
                    "fields": [
                      {
                        "type": {
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
                                "com.linkedin.common.AuditStamp"
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
                        },
                        "name": "schemaField",
                        "doc": "Inline SchemaField describing this field's schema and identity for column-level lineage edges and as the anchor for\ncolumn-level governance (tags, glossary terms, description)."
                      },
                      {
                        "type": {
                          "type": "enum",
                          "symbolDocs": {
                            "DIMENSION": " A grouping or filtering attribute \u2014 maps to a \"dimension\" in the semantic layer. ",
                            "FILTER": " A named boolean predicate used to filter results, distinct from a dimension. ",
                            "MEASURE": " An aggregatable numeric value \u2014 maps to a \"measure\" or \"fact\" in the semantic layer. ",
                            "OTHER": " A field kind not yet represented as a named symbol. "
                          },
                          "name": "SemanticFieldType",
                          "namespace": "com.linkedin.semanticmodel",
                          "symbols": [
                            "DIMENSION",
                            "MEASURE",
                            "FILTER",
                            "OTHER"
                          ],
                          "doc": "Discriminator enum that identifies the kind of a SemanticField inside a SemanticModel dataset."
                        },
                        "name": "type",
                        "doc": "The kind of this field. Must be set for every field; determines how downstream tools\ninterpret the field and whether kind-specific sub-records (e.g. `dimension`) are relevant."
                      },
                      {
                        "type": {
                          "type": "record",
                          "name": "MetricExpression",
                          "namespace": "com.linkedin.metric",
                          "fields": [
                            {
                              "type": {
                                "type": "array",
                                "items": {
                                  "type": "record",
                                  "name": "DialectExpression",
                                  "namespace": "com.linkedin.metric",
                                  "fields": [
                                    {
                                      "type": {
                                        "type": "enum",
                                        "symbolDocs": {
                                          "ANSI_SQL": " Standard SQL dialect. ",
                                          "DATABRICKS": " Databricks SQL. ",
                                          "MAQL": " GoodData MAQL (Metric Analysis and Query Language). ",
                                          "MDX": " Multi-Dimensional Expressions (OLAP cubes). ",
                                          "OTHER": "A dialect not yet represented as a named symbol.",
                                          "SNOWFLAKE": " Snowflake SQL. ",
                                          "TABLEAU": " Tableau calculations. "
                                        },
                                        "name": "Dialect",
                                        "namespace": "com.linkedin.metric",
                                        "symbols": [
                                          "ANSI_SQL",
                                          "SNOWFLAKE",
                                          "MDX",
                                          "TABLEAU",
                                          "DATABRICKS",
                                          "MAQL",
                                          "OTHER"
                                        ],
                                        "doc": "The SQL or expression-language dialect that produced a metric expression.\nValue set is aligned 1:1 with the OSI (Open Semantic Interchange) spec."
                                      },
                                      "name": "dialect",
                                      "doc": "The dialect in which this expression is written."
                                    },
                                    {
                                      "type": "string",
                                      "name": "expression",
                                      "doc": "The raw expression string."
                                    }
                                  ],
                                  "doc": "A metric expression in a specific SQL dialect or semantic layer language."
                                }
                              },
                              "name": "dialects",
                              "doc": "The expression in one or more dialects."
                            }
                          ],
                          "doc": "A metric's logical expression represented across one or more SQL dialects"
                        },
                        "name": "expression",
                        "doc": "The SQL expression that computes this field, in one or more dialects."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "record",
                            "name": "Dimension",
                            "namespace": "com.linkedin.semanticmodel",
                            "fields": [
                              {
                                "type": "boolean",
                                "name": "isTime",
                                "default": false,
                                "doc": "Whether this dimension represents a time axis."
                              }
                            ],
                            "doc": "Marks a field as a time dimension within a semantic model dataset.\n\nNOTE: this record is intentionally minimal, can add more as per needs. "
                          }
                        ],
                        "name": "dimension",
                        "default": null,
                        "doc": "Dimension-specific metadata. Populated when `type == DIMENSION`; ignored otherwise."
                      },
                      {
                        "type": [
                          "null",
                          "com.linkedin.metric.AiContext"
                        ],
                        "name": "aiContext",
                        "default": null,
                        "doc": "AI-specific context for improved disambiguation and retrieval."
                      }
                    ],
                    "doc": "A named field defined inside a `SemanticModel` dataset (dimension, measure, filter, or other).\nFields are classified by `type` into four kinds \u2014 DIMENSION, MEASURE, FILTER, and OTHER."
                  }
                }
              ],
              "name": "fields",
              "default": null,
              "doc": "Fields (dimensions, measures, filters) defined over this dataset."
            }
          ],
          "doc": "A logical dataset referenced by a SemanticModel, together with its exposed fields."
        }
      },
      "name": "datasets",
      "default": [],
      "doc": "The logical datasets that this semantic model exposes."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "SemanticModelRelationship",
            "namespace": "com.linkedin.semanticmodel",
            "fields": [
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "name",
                "default": null,
                "doc": "Optional human-readable name for this relationship."
              },
              {
                "type": "string",
                "name": "from",
                "doc": "Alias of the source dataset (as declared in ModelDataset.name)."
              },
              {
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "fromColumns",
                "doc": "Column names on the source side of the join."
              },
              {
                "type": "string",
                "name": "to",
                "doc": "Alias of the target dataset (as declared in ModelDataset.name)."
              },
              {
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "toColumns",
                "doc": "Column names on the target side of the join."
              },
              {
                "type": [
                  "null",
                  "com.linkedin.metric.AiContext"
                ],
                "name": "aiContext",
                "default": null,
                "doc": "AI-specific context for improved disambiguation and retrieval."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "enum",
                    "name": "ERModelRelationshipCardinality",
                    "namespace": "com.linkedin.ermodelrelation",
                    "symbols": [
                      "ONE_ONE",
                      "ONE_N",
                      "N_ONE",
                      "N_N"
                    ],
                    "doc": "Cardinality of a relationship between two datasets or logical datasets.\nUsed by both ERModelRelationship (physical) and SemanticModelRelationship\n(semantic-layer join paths). Directionality is carried by the source/from\nand destination/to fields on those records, not by this enum."
                  }
                ],
                "name": "cardinality",
                "default": null,
                "doc": "Cardinality of this join. "
              }
            ],
            "doc": "A join relationship between two logical datasets defined within a SemanticModel.\nNamed SemanticModelRelationship to avoid colliding with common relationship models."
          }
        }
      ],
      "name": "relationships",
      "default": null,
      "doc": "Join relationships between the datasets of this semantic model."
    }
  ],
  "doc": "Core information about a SemanticModel entity."
}
```





#### upstreamLineage
Upstream lineage of a dataset



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| upstreams | Upstream[] | ✓ | List of upstream dataset lineage information |  |
| fineGrainedLineages | FineGrainedLineage[] |  | List of fine-grained lineage information, including field-level lineage | → DownstreamOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "upstreamLineage",
    "schemaVersion": 2
  },
  "name": "UpstreamLineage",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Upstream",
          "namespace": "com.linkedin.dataset",
          "fields": [
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
              "default": {
                "actor": "urn:li:corpuser:unknown",
                "impersonator": null,
                "time": 0,
                "message": null
              },
              "doc": "Audit stamp containing who reported the lineage and when."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created the lineage and when."
            },
            {
              "Relationship": {
                "createdActor": "upstreams/*/created/actor",
                "createdOn": "upstreams/*/created/time",
                "entityTypes": [
                  "dataset"
                ],
                "isLineage": true,
                "name": "DownstreamOf",
                "properties": "upstreams/*/properties",
                "updatedActor": "upstreams/*/auditStamp/actor",
                "updatedOn": "upstreams/*/auditStamp/time",
                "via": "upstreams/*/query"
              },
              "Searchable": {
                "fieldName": "upstreams",
                "fieldType": "URN",
                "hasValuesFieldName": "hasUpstreams",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "The upstream dataset the lineage points to"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "COPY": "Direct copy without modification",
                  "TRANSFORMED": "Transformed data with modification (format or content change)",
                  "VIEW": "Represents a view defined on the sources e.g. Hive view defined on underlying hive tables or a Hive table pointing to a HDFS dataset or DALI view defined on multiple sources"
                },
                "name": "DatasetLineageType",
                "namespace": "com.linkedin.dataset",
                "symbols": [
                  "COPY",
                  "TRANSFORMED",
                  "VIEW"
                ],
                "doc": "The various types of supported dataset lineage"
              },
              "name": "type",
              "doc": "The type of the lineage"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "properties",
              "default": null,
              "doc": "A generic properties bag that allows us to store specific information on this graph edge."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "query",
              "default": null,
              "doc": "If the lineage is generated by a query, a reference to the query"
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "symbolDocs": {
                    "EXACT": "The reference matched an existing entity exactly, including URN casing.",
                    "NORMALIZED": "The reference was case-normalized to match an existing entity whose URN uses\ndifferent casing (i.e. the reference was rewritten to heal a casing mismatch).",
                    "UNRESOLVED": "The reference is on a configured upstream platform but could not be resolved to\na single existing entity \u2014 either no entity matched (under any casing), or the\ncasing was ambiguous (multiple entities share the case-insensitive form). The\nreference was left unchanged; this flags potentially broken lineage."
                  },
                  "name": "LineageMatchType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "EXACT",
                    "NORMALIZED",
                    "UNRESOLVED"
                  ],
                  "doc": "How an upstream lineage reference's URN was resolved against the entities stored\nin DataHub. Populated by the lineage URN casing normalization processor for\nreferences on a configured upstream platform; absent when the reference is out of\nscope (platform not configured, feature disabled) or was ingested before the\nfeature was enabled.\n\nThis verdict reflects DataHub's knowledge AT THE TIME THE LINEAGE EDGE WAS\nINGESTED, not the current state of the graph. It is a point-in-time record and is\nnot re-evaluated automatically: e.g. a reference recorded as UNRESOLVED (its target\ndid not exist yet) keeps that value even after the target is later ingested and the\nedge in fact resolves exactly \u2014 the verdict only refreshes when the referencing\nsource is re-ingested."
                }
              ],
              "name": "matchType",
              "default": null,
              "doc": "How this upstream reference's URN was resolved against the entities stored in\nDataHub. Set by the lineage URN casing normalization processor: EXACT when the\nreference already matched an existing entity, NORMALIZED when it was rewritten to\nheal a casing mismatch, UNRESOLVED when it could not be resolved to a single\nexisting entity. Absent when no reconciliation was performed (out of scope).\nReflects DataHub's knowledge at ingestion time and is not re-evaluated later; see\nLineageMatchType."
            }
          ],
          "doc": "Upstream lineage information about a dataset including the source reporting the lineage"
        }
      },
      "name": "upstreams",
      "doc": "List of upstream dataset lineage information"
    },
    {
      "Relationship": {
        "/*/upstreams/*": {
          "entityTypes": [
            "dataset",
            "schemaField"
          ],
          "name": "DownstreamOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FineGrainedLineage",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "DATASET": " Indicates that this lineage is originating from upstream dataset(s)",
                    "FIELD_SET": " Indicates that this lineage is originating from upstream field(s)",
                    "NONE": " Indicates that there is no upstream lineage i.e. the downstream field is not a derived field"
                  },
                  "name": "FineGrainedLineageUpstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD_SET",
                    "DATASET",
                    "NONE"
                  ],
                  "doc": "The type of upstream entity in a fine-grained lineage"
                },
                "name": "upstreamType",
                "doc": "The type of upstream entity"
              },
              {
                "Searchable": {
                  "/*": {
                    "fieldName": "fineGrainedUpstreams",
                    "fieldType": "URN",
                    "hasValuesFieldName": "hasFineGrainedUpstreams",
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
                "name": "upstreams",
                "default": null,
                "doc": "Upstream entities in the lineage"
              },
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "FIELD": " Indicates that the lineage is for a single, specific, downstream field",
                    "FIELD_SET": " Indicates that the lineage is for a set of downstream fields"
                  },
                  "name": "FineGrainedLineageDownstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD",
                    "FIELD_SET"
                  ],
                  "doc": "The type of downstream field(s) in a fine-grained lineage"
                },
                "name": "downstreamType",
                "doc": "The type of downstream field(s)"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "downstreams",
                "default": null,
                "doc": "Downstream fields in the lineage"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "transformOperation",
                "default": null,
                "doc": "The transform operation applied to the upstream entities to produce the downstream field(s)"
              },
              {
                "type": "float",
                "name": "confidenceScore",
                "default": 1.0,
                "doc": "The confidence in this lineage between 0 (low confidence) and 1 (high confidence)"
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "query",
                "default": null,
                "doc": "The query that was used to generate this lineage.\nPresent only if the lineage was generated from a detected query."
              },
              {
                "type": [
                  "null",
                  "com.linkedin.dataset.LineageMatchType"
                ],
                "name": "matchType",
                "default": null,
                "doc": "Aggregate of how the upstream field references' URNs were resolved against the\nentities stored in DataHub. Set by the lineage URN casing normalization processor:\nNORMALIZED if any field was rewritten to heal a casing mismatch, else UNRESOLVED if\nany could not be resolved, else EXACT. Absent when no reconciliation was performed\n(out of scope). Reflects DataHub's knowledge at ingestion time and is not\nre-evaluated later; see LineageMatchType."
              }
            ],
            "doc": "A fine-grained lineage from upstream fields/datasets to downstream field(s)"
          }
        }
      ],
      "name": "fineGrainedLineages",
      "default": null,
      "doc": " List of fine-grained lineage information, including field-level lineage"
    }
  ],
  "doc": "Upstream lineage of a dataset"
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





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
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





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
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
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
}
```





#### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| path | BrowsePathEntry[] | ✓ | A valid browse path for the entity. This field is provided by DataHub by default. This aspect is ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
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


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- SourcedBy

   - Dataset via `semanticModelInfo.datasets.source`
   - Query via `semanticModelInfo.datasets.source`
- SchemaFieldTaggedWith

   - Tag via `semanticModelInfo.datasets.fields.schemaField.globalTags`
- TaggedWith

   - Tag via `semanticModelInfo.datasets.fields.schemaField.globalTags.tags`
   - Tag via `globalTags.tags`
- SchemaFieldWithGlossaryTerm

   - GlossaryTerm via `semanticModelInfo.datasets.fields.schemaField.glossaryTerms`
- TermedWith

   - GlossaryTerm via `semanticModelInfo.datasets.fields.schemaField.glossaryTerms.terms.urn`
   - GlossaryTerm via `glossaryTerms.terms.urn`
- DownstreamOf

   - Dataset via `upstreamLineage.upstreams.dataset`
   - Dataset via `upstreamLineage.fineGrainedLineages`
   - SchemaField via `upstreamLineage.fineGrainedLineages`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
