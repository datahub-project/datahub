---
sidebar_position: 15
title: GlossaryTerm
slug: /generated/metamodel/entities/glossaryterm
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/glossaryTerm.md
---

# GlossaryTerm

## Aspects

### glossaryTermKey

Key for a GlossaryTerm

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTermKey"
  },
  "name": "GlossaryTermKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldName": "id",
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "The term name, which serves as a unique id"
    }
  ],
  "doc": "Key for a GlossaryTerm"
}
```

</details>

### glossaryTermInfo

Properties associated with a GlossaryTerm

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTermInfo"
  },
  "name": "GlossaryTermInfo",
  "namespace": "com.linkedin.glossary",
  "fields": [
    {
      "Searchable": {
        "/*": {
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
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "id",
      "default": null,
      "doc": "Optional id for the term"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the term"
    },
    {
      "Searchable": {},
      "type": "string",
      "name": "definition",
      "doc": "Definition of business term."
    },
    {
      "Relationship": {
        "entityTypes": [
          "glossaryNode"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "fieldName": "parentNode",
        "fieldType": "URN",
        "hasValuesFieldName": "hasParentNode"
      },
      "java": {
        "class": "com.linkedin.common.urn.GlossaryNodeUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentNode",
      "default": null,
      "doc": "Parent node of the glossary term"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "termSource",
      "doc": "Source of the Business Term (INTERNAL or EXTERNAL) with default value as INTERNAL"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "sourceRef",
      "default": null,
      "doc": "External Reference to the business-term"
    },
    {
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "sourceUrl",
      "default": null,
      "doc": "The abstracted URL such as https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/CashInstrument."
    },
    {
      "deprecated": true,
      "type": [
        "null",
        "string"
      ],
      "name": "rawSchema",
      "default": null,
      "doc": "Schema definition of the glossary term"
    }
  ],
  "doc": "Properties associated with a GlossaryTerm"
}
```

</details>

### ownership

Ownership information of an entity.

<details>
<summary>Schema</summary>

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
                "queryByDefault": false
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
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
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

### browsePaths

Shared aspect containing Browse Paths to be indexed for an entity.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```

</details>

### glossaryRelatedTerms

Has A / Is A lineage information about a glossary Term reporting the lineage

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryRelatedTerms"
  },
  "name": "GlossaryRelatedTerms",
  "namespace": "com.linkedin.glossary",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "IsA"
        }
      },
      "Searchable": {
        "/*": {
          "boostScore": 2.0,
          "fieldName": "isRelatedTerms",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "isRelatedTerms",
      "default": null,
      "doc": "The relationship Is A with glossary term"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "HasA"
        }
      },
      "Searchable": {
        "/*": {
          "boostScore": 2.0,
          "fieldName": "hasRelatedTerms",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "hasRelatedTerms",
      "default": null,
      "doc": "The relationship Has A with glossary term"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "HasValue"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "values",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "values",
      "default": null,
      "doc": "The relationship Has Value with glossary term.\nThese are fixed value a term has. For example a ColorEnum where RED, GREEN and YELLOW are fixed values."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "glossaryTerm"
          ],
          "name": "IsRelatedTo"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "relatedTerms",
          "fieldType": "URN"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "relatedTerms",
      "default": null,
      "doc": "The relationship isRelatedTo with glossary term"
    }
  ],
  "doc": "Has A / Is A lineage information about a glossary Term reporting the lineage"
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

### schemaMetadata

SchemaMetadata to describe metadata related to store schema

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaMetadata"
  },
  "name": "SchemaMetadata",
  "namespace": "com.linkedin.schema",
  "fields": [
    {
      "validate": {
        "strlen": {
          "max": 500,
          "min": 1
        }
      },
      "type": "string",
      "name": "schemaName",
      "doc": "Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DataPlatformUrn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"
    },
    {
      "type": "long",
      "name": "version",
      "doc": "Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."
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
      "java": {
        "class": "com.linkedin.common.urn.DatasetUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "dataset",
      "default": null,
      "doc": "Dataset this schema metadata is associated with."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "cluster",
      "default": null,
      "doc": "The cluster this schema metadata resides from"
    },
    {
      "type": "string",
      "name": "hash",
      "doc": "the SHA1 hash of the schema content"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "EspressoSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native espresso document schema."
            },
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The espresso table schema definition."
            }
          ],
          "doc": "Schema text of an espresso table schema."
        },
        {
          "type": "record",
          "name": "OracleDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for oracle data definition language that describes an oracle table."
        },
        {
          "type": "record",
          "name": "MySqlDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for MySql data definition language that describes an MySql table."
        },
        {
          "type": "record",
          "name": "PrestoDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."
            }
          ],
          "doc": "Schema holder for presto data definition language that describes a presto view."
        },
        {
          "type": "record",
          "name": "KafkaSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native kafka document schema. This is a human readable avro document schema."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "documentSchemaType",
              "default": null,
              "doc": "The native kafka document schema type. This can be AVRO/PROTOBUF/JSON."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchema",
              "default": null,
              "doc": "The native kafka key schema as retrieved from Schema Registry"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchemaType",
              "default": null,
              "doc": "The native kafka key schema type. This can be AVRO/PROTOBUF/JSON."
            }
          ],
          "doc": "Schema holder for kafka schema."
        },
        {
          "type": "record",
          "name": "BinaryJsonSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema text for binary JSON file format."
            }
          ],
          "doc": "Schema text of binary JSON schema."
        },
        {
          "type": "record",
          "name": "OrcSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema for ORC file format."
            }
          ],
          "doc": "Schema text of an ORC schema."
        },
        {
          "type": "record",
          "name": "Schemaless",
          "namespace": "com.linkedin.schema",
          "fields": [],
          "doc": "The dataset has no specific schema associated with it"
        },
        {
          "type": "record",
          "name": "KeyValueSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "keySchema",
              "doc": "The raw schema for the key in the key-value store."
            },
            {
              "type": "string",
              "name": "valueSchema",
              "doc": "The raw schema for the value in the key-value store."
            }
          ],
          "doc": "Schema text of a key-value store schema."
        },
        {
          "type": "record",
          "name": "OtherSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The native schema in the dataset's platform."
            }
          ],
          "doc": "Schema holder for undefined schema types."
        }
      ],
      "name": "platformSchema",
      "doc": "The native schema in the dataset's platform."
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SchemaField",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "Searchable": {
                "boostScore": 5.0,
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
                "fieldType": "TEXT"
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
              "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description."
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
                          "filterNameOverride": "Tag",
                          "hasValuesFieldName": "hasTags",
                          "queryByDefault": true
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
                                "hasValuesFieldName": "hasGlossaryTerms"
                              },
                              "java": {
                                "class": "com.linkedin.common.urn.GlossaryTermUrn"
                              },
                              "type": "string",
                              "name": "urn",
                              "doc": "Urn of the applied glossary term"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
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
              "doc": "For Datasets which are partitioned, this determines the partitioning key."
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
      },
      "name": "fields",
      "doc": "Client provided a list of fields from document schema."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "primaryKeys",
      "default": null,
      "doc": "Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."
    },
    {
      "deprecated": "Use foreignKeys instead.",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "record",
            "name": "ForeignKeySpec",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": [
                  {
                    "type": "record",
                    "name": "DatasetFieldForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.DatasetUrn"
                        },
                        "type": "string",
                        "name": "parentDataset",
                        "doc": "dataset that stores the resource."
                      },
                      {
                        "type": {
                          "type": "array",
                          "items": "string"
                        },
                        "name": "currentFieldPaths",
                        "doc": "List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."
                      },
                      {
                        "type": "string",
                        "name": "parentField",
                        "doc": "SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."
                      }
                    ],
                    "doc": "For non-urn based foregin keys."
                  },
                  {
                    "type": "record",
                    "name": "UrnForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "type": "string",
                        "name": "currentFieldPath",
                        "doc": "Field in hosting(current) SchemaMetadata."
                      }
                    ],
                    "doc": "If SchemaMetadata fields make any external references and references are of type com.linkedin.common.Urn or any children, this models can be used to mark it."
                  }
                ],
                "name": "foreignKey",
                "doc": "Foreign key definition in metadata schema."
              }
            ],
            "doc": "Description of a foreign key in a schema."
          }
        }
      ],
      "name": "foreignKeysSpecs",
      "default": null,
      "doc": "Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "ForeignKeyConstraint",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the constraint, likely provided from the source"
              },
              {
                "Relationship": {
                  "/*": {
                    "entityTypes": [
                      "schemaField"
                    ],
                    "name": "ForeignKeyTo"
                  }
                },
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "foreignFields",
                "doc": "Fields the constraint maps to on the foreign dataset"
              },
              {
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "sourceFields",
                "doc": "Fields the constraint maps to on the source dataset"
              },
              {
                "Relationship": {
                  "entityTypes": [
                    "dataset"
                  ],
                  "name": "ForeignKeyToDataset"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "foreignDataset",
                "doc": "Reference to the foreign dataset for ease of lookup"
              }
            ],
            "doc": "Description of a foreign key constraint in a schema."
          }
        }
      ],
      "name": "foreignKeys",
      "default": null,
      "doc": "List of foreign key constraints for the schema"
    }
  ],
  "doc": "SchemaMetadata to describe metadata related to store schema"
}
```

</details>

### deprecation

Deprecation status of an entity

<details>
<summary>Schema</summary>

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
        "fieldType": "BOOLEAN",
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
    }
  ],
  "doc": "Deprecation status of an entity"
}
```

</details>

### domains

Links from an Asset to its Domains

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains"
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
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```

</details>

## Relationships

### Self

These are the relationships to itself, stored in this entity's aspects

- IsA (via `glossaryRelatedTerms.isRelatedTerms`)
- HasA (via `glossaryRelatedTerms.hasRelatedTerms`)
- HasValue (via `glossaryRelatedTerms.values`)
- IsRelatedTo (via `glossaryRelatedTerms.relatedTerms`)
- SchemaFieldWithGlossaryTerm (via `schemaMetadata.fields.glossaryTerms`)
- TermedWith (via `schemaMetadata.fields.glossaryTerms.terms.urn`)

### Outgoing

These are the relationships stored in this entity's aspects

- IsPartOf

  - GlossaryNode via `glossaryTermInfo.parentNode`

- OwnedBy

  - Corpuser via `ownership.owners.owner`
  - CorpGroup via `ownership.owners.owner`

- ownershipType

  - OwnershipType via `ownership.owners.typeUrn`

- SchemaFieldTaggedWith

  - Tag via `schemaMetadata.fields.globalTags`

- TaggedWith

  - Tag via `schemaMetadata.fields.globalTags.tags`

- ForeignKeyTo

  - SchemaField via `schemaMetadata.foreignKeys.foreignFields`

- ForeignKeyToDataset

  - Dataset via `schemaMetadata.foreignKeys.foreignDataset`

- AssociatedWith

  - Domain via `domains.domains`

### Incoming

These are the relationships stored in other entity's aspects

- SchemaFieldWithGlossaryTerm

  - Dataset via `schemaMetadata.fields.glossaryTerms`
  - Chart via `inputFields.fields.schemaField.glossaryTerms`
  - Dashboard via `inputFields.fields.schemaField.glossaryTerms`

- TermedWith

  - Dataset via `schemaMetadata.fields.glossaryTerms.terms.urn`
  - DataJob via `glossaryTerms.terms.urn`
  - DataFlow via `glossaryTerms.terms.urn`
  - Chart via `glossaryTerms.terms.urn`
  - Dashboard via `glossaryTerms.terms.urn`
  - Notebook via `glossaryTerms.terms.urn`
  - Container via `glossaryTerms.terms.urn`

- EditableSchemaFieldWithGlossaryTerm

  - Dataset via `editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms`

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
