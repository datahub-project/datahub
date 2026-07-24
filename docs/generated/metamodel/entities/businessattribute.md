# BusinessAttribute


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

#### businessAttributeInfo
Properties associated with a BusinessAttribute



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| fieldPath | string | ✓ | FieldPath uniquely identifying the SchemaField this metadata is associated with |  |
| description | string |  | Description | Searchable (editedFieldDescriptions) |
| globalTags | GlobalTags |  | Tags associated with the field | Searchable, → EditableSchemaFieldTaggedWith |
| glossaryTerms | GlossaryTerms |  | Glossary terms associated with the field | Searchable, → EditableSchemaFieldWithGlossaryTerm |
| customProperties | map | ✓ | Custom property bag. | Searchable |
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| name | string | ✓ | Display name of the BusinessAttribute | Searchable |
| type | SchemaFieldDataType |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "businessAttributeInfo"
  },
  "name": "BusinessAttributeInfo",
  "namespace": "com.linkedin.businessattribute",
  "fields": [
    {
      "type": "string",
      "name": "fieldPath",
      "doc": "FieldPath uniquely identifying the SchemaField this metadata is associated with"
    },
    {
      "Searchable": {
        "boostScore": 0.1,
        "fieldName": "editedFieldDescriptions",
        "fieldType": "TEXT",
        "sanitizeRichText": true,
        "searchTier": 2
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
      "Relationship": {
        "/tags/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "EditableSchemaFieldTaggedWith"
        }
      },
      "Searchable": {
        "/tags/*/attribution/actor": {
          "fieldName": "editedFieldTagAttributionActors",
          "fieldType": "URN",
          "queryByDefault": false
        },
        "/tags/*/attribution/source": {
          "fieldName": "editedFieldTagAttributionSources",
          "fieldType": "URN",
          "queryByDefault": false
        },
        "/tags/*/attribution/time": {
          "fieldName": "editedFieldTagAttributionDates",
          "fieldType": "DATETIME"
        },
        "/tags/*/tag": {
          "boostScore": 0.5,
          "fieldName": "editedFieldTags",
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
          "name": "EditableSchemaFieldWithGlossaryTerm"
        }
      },
      "Searchable": {
        "/terms/*/attribution/actor": {
          "fieldName": "editedFieldTermAttributionActors",
          "fieldType": "URN",
          "queryByDefault": false
        },
        "/terms/*/attribution/source": {
          "fieldName": "editedFieldTermAttributionSources",
          "fieldType": "URN",
          "queryByDefault": false
        },
        "/terms/*/attribution/time": {
          "fieldName": "editedFieldTermAttributionDates",
          "fieldType": "DATETIME"
        },
        "/terms/*/urn": {
          "boostScore": 0.5,
          "fieldName": "editedFieldGlossaryTerms",
          "fieldType": "URN",
          "includeSystemModifiedAt": true,
          "systemModifiedAtFieldName": "schemaFieldTermsModifiedAt"
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
      ],
      "name": "glossaryTerms",
      "default": null,
      "doc": "Glossary terms associated with the field"
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
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
      "doc": "Display name of the BusinessAttribute"
    },
    {
      "type": [
        "null",
        {
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
        }
      ],
      "name": "type",
      "default": null
    }
  ],
  "doc": "Properties associated with a BusinessAttribute"
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
- EditableSchemaFieldTaggedWith

   - Tag via `businessAttributeInfo.globalTags`
- TaggedWith

   - Tag via `businessAttributeInfo.globalTags.tags`
- EditableSchemaFieldWithGlossaryTerm

   - GlossaryTerm via `businessAttributeInfo.glossaryTerms`
- TermedWith

   - GlossaryTerm via `businessAttributeInfo.glossaryTerms.terms.urn`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
#### Incoming
These are the relationships stored in other entity's aspects
- BusinessAttributeOf

   - SchemaField via `businessAttributes.businessAttribute`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
