---
sidebar_position: 39
title: StructuredProperty
slug: /generated/metamodel/entities/structuredproperty
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/structuredProperty.md
---
# StructuredProperty
Structured Property represents a property meant for extending the core model of a logical entity
## Aspects

### propertyDefinition
None
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "propertyDefinition"
  },
  "name": "StructuredPropertyDefinition",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "Searchable": {},
      "type": "string",
      "name": "qualifiedName",
      "doc": "The fully qualified name of the property. e.g. io.acryl.datahub.myProperty"
    },
    {
      "Searchable": {},
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "The display name of the property. This is the name that will be shown in the UI and can be used to look up the property id."
    },
    {
      "UrnValidation": {
        "entityTypes": [
          "dataType"
        ],
        "exist": true,
        "strict": true
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "valueType",
      "doc": "The value type of the property. Must be a dataType.\ne.g. To indicate that the property is of type DATE, use urn:li:dataType:datahub.date"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        }
      ],
      "name": "typeQualifier",
      "default": null,
      "doc": "A map that allows for type specialization of the valueType.\ne.g. a valueType of urn:li:dataType:datahub.urn\ncan be specialized to be a USER or GROUP URN by adding a typeQualifier like \n{ \"allowedTypes\": [\"urn:li:entityType:datahub.corpuser\", \"urn:li:entityType:datahub.corpGroup\"] }"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "PropertyValue",
            "namespace": "com.linkedin.structured",
            "fields": [
              {
                "type": [
                  "string",
                  "double"
                ],
                "name": "value"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Optional description of the property value"
              }
            ]
          }
        }
      ],
      "name": "allowedValues",
      "default": null,
      "doc": "A list of allowed values that the property is allowed to take. \nIf this is not specified, then the property can take any value of given type."
    },
    {
      "type": [
        {
          "type": "enum",
          "name": "PropertyCardinality",
          "namespace": "com.linkedin.structured",
          "symbols": [
            "SINGLE",
            "MULTIPLE"
          ]
        },
        "null"
      ],
      "name": "cardinality",
      "default": "SINGLE",
      "doc": "The cardinality of the property. If not specified, then the property is assumed to be single valued.."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "entityType"
          ],
          "name": "StructuredPropertyOf"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "entityTypes"
        }
      },
      "UrnValidation": {
        "entityTypes": [
          "entityType"
        ],
        "exist": true,
        "strict": true
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "entityTypes"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "The description of the property. This is the description that will be shown in the UI."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataHubSearchConfig",
          "namespace": "com.linkedin.datahub",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "fieldName",
              "default": null,
              "doc": "Name of the field in the search index. Defaults to the field name otherwise"
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "name": "SearchFieldType",
                  "namespace": "com.linkedin.datahub",
                  "symbols": [
                    "KEYWORD",
                    "TEXT",
                    "TEXT_PARTIAL",
                    "BROWSE_PATH",
                    "URN",
                    "URN_PARTIAL",
                    "BOOLEAN",
                    "COUNT",
                    "DATETIME",
                    "OBJECT",
                    "BROWSE_PATH_V2",
                    "WORD_GRAM"
                  ]
                }
              ],
              "name": "fieldType",
              "default": null,
              "doc": "Type of the field. Defines how the field is indexed and matched"
            },
            {
              "type": "boolean",
              "name": "queryByDefault",
              "default": false,
              "doc": "Whether we should match the field for the default search query"
            },
            {
              "type": "boolean",
              "name": "enableAutocomplete",
              "default": false,
              "doc": "Whether we should use the field for default autocomplete"
            },
            {
              "type": "boolean",
              "name": "addToFilters",
              "default": false,
              "doc": "Whether or not to add field to filters."
            },
            {
              "type": "boolean",
              "name": "addHasValuesToFilters",
              "default": true,
              "doc": "Whether or not to add the \"has values\" to filters.\ncheck if this is conditional on addToFilters being true"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "filterNameOverride",
              "default": null,
              "doc": "Display name of the filter"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "hasValuesFilterNameOverride",
              "default": null,
              "doc": "Display name of the has values filter"
            },
            {
              "type": "double",
              "name": "boostScore",
              "default": 1.0,
              "doc": "Boost multiplier to the match score. Matches on fields with higher boost score ranks higher"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "hasValuesFieldName",
              "default": null,
              "doc": "If set, add a index field of the given name that checks whether the field exists"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "numValuesFieldName",
              "default": null,
              "doc": "If set, add a index field of the given name that checks the number of elements"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "double"
                }
              ],
              "name": "weightsPerFieldValue",
              "default": null,
              "doc": "(Optional) Weights to apply to score for a given value"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "fieldNameAliases",
              "default": null,
              "doc": "(Optional) Aliases for this given field that can be used for sorting etc."
            }
          ],
          "doc": "Configuration for how any given field should be indexed and matched in the DataHub search index."
        }
      ],
      "name": "searchConfiguration",
      "default": null,
      "doc": "Search configuration for this property. If not specified, then the property is indexed using the default mapping.\nfrom the logical type."
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "immutable",
      "default": false,
      "doc": "Whether the structured property value is immutable once applied to an entity."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "version",
      "default": null,
      "doc": "Definition version - Allows breaking schema changes. String is compared case-insensitive and new\n                     versions must be monotonically increasing. Cannot use periods/dots.\n                     Suggestions: v1, v2\n                                  20240610, 20240611"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdTime",
          "fieldType": "DATETIME"
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
      "doc": "Created Audit stamp"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModified",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "Last Modified Audit stamp"
    }
  ]
}
```
</details>

### structuredPropertySettings
Settings specific to a structured property entity
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredPropertySettings"
  },
  "name": "StructuredPropertySettings",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "isHidden",
      "default": false,
      "doc": "Whether or not this asset should be hidden in the main application"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInSearchFilters",
      "default": false,
      "doc": "Whether or not this asset should be displayed as a search filter"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInAssetSummary",
      "default": false,
      "doc": "Whether or not this asset should be displayed in the asset sidebar"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showAsAssetBadge",
      "default": false,
      "doc": "Whether or not this asset should be displayed as an asset badge on other\nasset's headers"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInColumnsTable",
      "default": false,
      "doc": "Whether or not this asset should be displayed as a column in the schema field table\nin a Dataset's \"Columns\" tab."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedSettings",
          "fieldType": "DATETIME"
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
      "name": "lastModified",
      "default": null,
      "doc": "Last Modified Audit stamp"
    }
  ],
  "doc": "Settings specific to a structured property entity"
}
```
</details>

### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.
<details>
<summary>Schema</summary>

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
</details>

### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.
<details>
<summary>Schema</summary>

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
      "doc": "Whether the entity has been removed (soft-deleted)."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```
</details>

## Relationships

### Outgoing
These are the relationships stored in this entity's aspects
- StructuredPropertyOf

   - EntityType via `propertyDefinition.entityTypes`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
