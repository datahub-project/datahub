---
sidebar_position: 40
title: Form
slug: /generated/metamodel/entities/form
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/form.md
---
# Form
## Aspects

### formInfo
Information about a form to help with filling out metadata on entities.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "formInfo"
  },
  "name": "FormInfo",
  "namespace": "com.linkedin.form",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the form"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the form"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "COMPLETION": "A form simply used for collecting metadata fields for an entity.",
          "VERIFICATION": "This form is used for \"verifying\" that entities comply with a policy via presence of a specific set of metadata fields."
        },
        "name": "FormType",
        "namespace": "com.linkedin.form",
        "symbols": [
          "COMPLETION",
          "VERIFICATION"
        ]
      },
      "name": "type",
      "default": "COMPLETION",
      "doc": "The type of this form"
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormPrompt",
          "namespace": "com.linkedin.form",
          "fields": [
            {
              "Searchable": {
                "fieldName": "promptId",
                "fieldType": "KEYWORD",
                "queryByDefault": false
              },
              "type": "string",
              "name": "id",
              "doc": "The unique id for this prompt. This must be GLOBALLY unique."
            },
            {
              "type": "string",
              "name": "title",
              "doc": "The title of this prompt"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "description",
              "default": null,
              "doc": "The description of this prompt"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FIELDS_STRUCTURED_PROPERTY": "This prompt is meant to apply a structured property to a schema fields entity",
                  "STRUCTURED_PROPERTY": "This prompt is meant to apply a structured property to an entity"
                },
                "name": "FormPromptType",
                "namespace": "com.linkedin.form",
                "symbols": [
                  "STRUCTURED_PROPERTY",
                  "FIELDS_STRUCTURED_PROPERTY"
                ]
              },
              "name": "type",
              "doc": "The type of prompt"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "StructuredPropertyParams",
                  "namespace": "com.linkedin.form",
                  "fields": [
                    {
                      "Searchable": {
                        "fieldName": "structuredPropertyPromptUrns",
                        "fieldType": "URN"
                      },
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "urn",
                      "doc": "The structured property that is required on this entity"
                    }
                  ]
                }
              ],
              "name": "structuredPropertyParams",
              "default": null,
              "doc": "An optional set of information specific to structured properties prompts.\nThis should be filled out if the prompt is type STRUCTURED_PROPERTY or FIELDS_STRUCTURED_PROPERTY."
            },
            {
              "type": "boolean",
              "name": "required",
              "default": false,
              "doc": "Whether the prompt is required to be completed, in order for the form to be marked as complete."
            }
          ],
          "doc": "A prompt to present to the user to encourage filling out metadata"
        }
      },
      "name": "prompts",
      "default": [],
      "doc": "List of prompts to present to the user to encourage filling out metadata"
    },
    {
      "type": {
        "type": "record",
        "name": "FormActorAssignment",
        "namespace": "com.linkedin.form",
        "fields": [
          {
            "Searchable": {
              "fieldName": "isOwnershipForm",
              "fieldType": "BOOLEAN"
            },
            "type": "boolean",
            "name": "owners",
            "default": true,
            "doc": "Whether the form should be assigned to the owners of assets that it is applied to.\nThis is the default."
          },
          {
            "Searchable": {
              "/*": {
                "fieldName": "assignedGroups",
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
            "name": "groups",
            "default": null,
            "doc": "Optional: Specific set of groups that are targeted by this form assignment."
          },
          {
            "Searchable": {
              "/*": {
                "fieldName": "assignedUsers",
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
            "name": "users",
            "default": null,
            "doc": "Optional: Specific set of users that are targeted by this form assignment."
          }
        ]
      },
      "name": "actors",
      "default": {
        "groups": null,
        "owners": true,
        "users": null
      },
      "doc": "Who the form is assigned to, e.g. who should see the form when visiting the entity page or governance center"
    }
  ],
  "doc": "Information about a form to help with filling out metadata on entities."
}
```
</details>

### dynamicFormAssignment
Information about how a form is assigned to entities dynamically. Provide a filter to
match a set of entities instead of explicitly applying a form to specific entities.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dynamicFormAssignment"
  },
  "name": "DynamicFormAssignment",
  "namespace": "com.linkedin.form",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "Filter",
        "namespace": "com.linkedin.metadata.query.filter",
        "fields": [
          {
            "type": [
              "null",
              {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "ConjunctiveCriterion",
                  "namespace": "com.linkedin.metadata.query.filter",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "Criterion",
                          "namespace": "com.linkedin.metadata.query.filter",
                          "fields": [
                            {
                              "type": "string",
                              "name": "field",
                              "doc": "The name of the field that the criterion refers to"
                            },
                            {
                              "type": "string",
                              "name": "value",
                              "doc": "The value of the intended field"
                            },
                            {
                              "type": {
                                "type": "array",
                                "items": "string"
                              },
                              "name": "values",
                              "default": [],
                              "doc": "Values. one of which the intended field should match\nNote, if values is set, the above \"value\" field will be ignored"
                            },
                            {
                              "type": {
                                "type": "enum",
                                "symbolDocs": {
                                  "ANCESTORS_INCL": "Represent the relation: URN field matches any nested parent in addition to the given URN",
                                  "CONTAIN": "Represent the relation: String field contains value, e.g. name contains Profile",
                                  "DESCENDANTS_INCL": "Represent the relation: URN field any nested children in addition to the given URN",
                                  "END_WITH": "Represent the relation: String field ends with value, e.g. name ends with Event",
                                  "EQUAL": "Represent the relation: field = value, e.g. platform = hdfs",
                                  "EXISTS": "Represents the relation: field exists and is non-empty, e.g. owners is not null and != [] (empty)",
                                  "GREATER_THAN": "Represent the relation greater than, e.g. ownerCount > 5",
                                  "GREATER_THAN_OR_EQUAL_TO": "Represent the relation greater than or equal to, e.g. ownerCount >= 5",
                                  "IEQUAL": "Represent the relation: field = value and support case insensitive values, e.g. platform = hdfs",
                                  "IN": "Represent the relation: String field is one of the array values to, e.g. name in [\"Profile\", \"Event\"]",
                                  "IS_NULL": "Represent the relation: field is null, e.g. platform is null",
                                  "LESS_THAN": "Represent the relation less than, e.g. ownerCount < 3",
                                  "LESS_THAN_OR_EQUAL_TO": "Represent the relation less than or equal to, e.g. ownerCount <= 3",
                                  "RELATED_INCL": "Represent the relation: URN field matches any nested child or parent in addition to the given URN",
                                  "START_WITH": "Represent the relation: String field starts with value, e.g. name starts with PageView"
                                },
                                "name": "Condition",
                                "namespace": "com.linkedin.metadata.query.filter",
                                "symbols": [
                                  "CONTAIN",
                                  "END_WITH",
                                  "EQUAL",
                                  "IEQUAL",
                                  "IS_NULL",
                                  "EXISTS",
                                  "GREATER_THAN",
                                  "GREATER_THAN_OR_EQUAL_TO",
                                  "IN",
                                  "LESS_THAN",
                                  "LESS_THAN_OR_EQUAL_TO",
                                  "START_WITH",
                                  "DESCENDANTS_INCL",
                                  "ANCESTORS_INCL",
                                  "RELATED_INCL"
                                ],
                                "doc": "The matching condition in a filter criterion"
                              },
                              "name": "condition",
                              "default": "EQUAL",
                              "doc": "The condition for the criterion, e.g. EQUAL, START_WITH"
                            },
                            {
                              "type": "boolean",
                              "name": "negated",
                              "default": false,
                              "doc": "Whether the condition should be negated"
                            }
                          ],
                          "doc": "A criterion for matching a field with given value"
                        }
                      },
                      "name": "and",
                      "doc": "A list of and criteria the filter applies to the query"
                    }
                  ],
                  "doc": "A list of criterion and'd together."
                }
              }
            ],
            "name": "or",
            "default": null,
            "doc": "A list of disjunctive criterion for the filter. (or operation to combine filters)"
          },
          {
            "type": [
              "null",
              {
                "type": "array",
                "items": "com.linkedin.metadata.query.filter.Criterion"
              }
            ],
            "name": "criteria",
            "default": null,
            "doc": "Deprecated! A list of conjunctive criterion for the filter. If \"or\" field is provided, then this field is ignored."
          }
        ],
        "doc": "The filter for finding a record or a collection of records"
      },
      "name": "filter",
      "doc": "The filter applied when assigning this form to entities. Entities that match this filter\nwill have this form applied to them. Right now this filter only supports filtering by\nplatform, entity type, container, and domain through the UI."
    }
  ],
  "doc": "Information about how a form is assigned to entities dynamically. Provide a filter to\nmatch a set of entities instead of explicitly applying a form to specific entities."
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
        "/*": {
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
</details>

## Relationships

### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
