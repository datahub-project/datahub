---
sidebar_position: 7
title: Chart
slug: /generated/metamodel/entities/chart
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/chart.md
---

# Chart

## Aspects

### chartKey

Key for a Chart

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "chartKey"
  },
  "name": "ChartKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 4.0,
        "fieldName": "tool",
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "dashboardTool",
      "doc": "The name of the dashboard tool such as looker, redash etc."
    },
    {
      "type": "string",
      "name": "chartId",
      "doc": "Unique id for the chart. This id should be globally unique for a dashboarding tool even when there are multiple deployments of it. As an example, chart URL could be used here for Looker such as 'looker.linkedin.com/looks/1234'"
    }
  ],
  "doc": "Key for a Chart"
}
```

</details>

### chartInfo

Information about a chart

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "chartInfo"
  },
  "name": "ChartInfo",
  "namespace": "com.linkedin.chart",
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
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "title",
      "doc": "Title of the chart"
    },
    {
      "Searchable": {},
      "type": "string",
      "name": "description",
      "doc": "Detailed description about the chart"
    },
    {
      "type": {
        "type": "record",
        "name": "ChangeAuditStamps",
        "namespace": "com.linkedin.common",
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
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations"
      },
      "name": "lastModified",
      "doc": "Captures information about who created/last modified/deleted this chart and when"
    },
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
      "name": "chartUrl",
      "default": null,
      "doc": "URL for the chart. This could be used as an external link on DataHub to allow users access/view the chart"
    },
    {
      "Relationship": {
        "/*/string": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "deprecated": true,
      "type": [
        "null",
        {
          "type": "array",
          "items": [
            "string"
          ]
        }
      ],
      "name": "inputs",
      "default": null,
      "doc": "Data sources for the chart\nDeprecated! Use inputEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputEdges/*/created/actor",
          "createdOn": "inputEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes",
          "properties": "inputEdges/*/properties",
          "updatedActor": "inputEdges/*/lastModified/actor",
          "updatedOn": "inputEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Edge",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "sourceUrn",
                "default": null,
                "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "destinationUrn",
                "doc": "Urn of the destination of this relationship edge."
              },
              {
                "type": [
                  "null",
                  "com.linkedin.common.AuditStamp"
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
              }
            ],
            "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
          }
        }
      ],
      "name": "inputEdges",
      "default": null,
      "doc": "Data sources for the chart"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Chart Type"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "BAR": "Chart showing a Bar chart",
            "PIE": "Chart showing a Pie chart",
            "SCATTER": "Chart showing a Scatter plot",
            "TABLE": "Chart showing a table",
            "TEXT": "Chart showing Markdown formatted text"
          },
          "name": "ChartType",
          "namespace": "com.linkedin.chart",
          "symbols": [
            "BAR",
            "PIE",
            "SCATTER",
            "TABLE",
            "TEXT",
            "LINE",
            "AREA",
            "HISTOGRAM",
            "BOX_PLOT",
            "WORD_CLOUD",
            "COHORT"
          ],
          "doc": "The various types of charts"
        }
      ],
      "name": "type",
      "default": null,
      "doc": "Type of the chart"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Access Level"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "PRIVATE": "Private availability to certain set of users",
            "PUBLIC": "Publicly available access level"
          },
          "name": "AccessLevel",
          "namespace": "com.linkedin.common",
          "symbols": [
            "PUBLIC",
            "PRIVATE"
          ],
          "doc": "The various access levels"
        }
      ],
      "name": "access",
      "default": null,
      "doc": "Access level for the chart"
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "lastRefreshed",
      "default": null,
      "doc": "The time when this chart last refreshed"
    }
  ],
  "doc": "Information about a chart"
}
```

</details>

### chartQuery

Information for chart query which is used for getting data of the chart

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "chartQuery"
  },
  "name": "ChartQuery",
  "namespace": "com.linkedin.chart",
  "fields": [
    {
      "type": "string",
      "name": "rawQuery",
      "doc": "Raw query to build a chart from input datasets"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "queryType",
        "fieldType": "KEYWORD",
        "filterNameOverride": "Query Type"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "LOOKML": "LookML queries",
          "SQL": "SQL type queries"
        },
        "name": "ChartQueryType",
        "namespace": "com.linkedin.chart",
        "symbols": [
          "LOOKML",
          "SQL"
        ]
      },
      "name": "type",
      "doc": "Chart query type"
    }
  ],
  "doc": "Information for chart query which is used for getting data of the chart"
}
```

</details>

### editableChartProperties

Stores editable changes made to properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableChartProperties"
  },
  "name": "EditableChartProperties",
  "namespace": "com.linkedin.chart",
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
        "fieldName": "editedDescription",
        "fieldType": "TEXT"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Edited documentation of the chart "
    }
  ],
  "doc": "Stores editable changes made to properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
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

### globalTags

Tag aspect used for applying tags to an entity

<details>
<summary>Schema</summary>

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

### glossaryTerms

Related business terms information

<details>
<summary>Schema</summary>

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

### dataPlatformInstance

The specific instance of the data platform that this entity belongs to

<details>
<summary>Schema</summary>

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

</details>

### browsePathsV2

Shared aspect containing a Browse Path to be indexed for an entity.

<details>
<summary>Schema</summary>

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

</details>

### inputFields

Information about the fields a chart or dashboard references

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "inputFields"
  },
  "name": "InputFields",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InputField",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "schemaField"
                ],
                "name": "consumesField"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "schemaFieldUrn",
              "doc": "Urn of the schema being referenced for lineage purposes"
            },
            {
              "type": [
                "null",
                {
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
              ],
              "name": "schemaField",
              "default": null,
              "doc": "Copied version of the referenced schema field object for indexing purposes"
            }
          ],
          "doc": "Information about a field a chart or dashboard references"
        }
      },
      "name": "fields",
      "doc": "List of fields being referenced"
    }
  ],
  "doc": "Information about the fields a chart or dashboard references"
}
```

</details>

### embed

Information regarding rendering an embed for an asset.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "embed"
  },
  "name": "Embed",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "renderUrl",
      "default": null,
      "doc": "An embed URL to be rendered inside of an iframe."
    }
  ],
  "doc": "Information regarding rendering an embed for an asset."
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

### container

Link from an asset to its parent container

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
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

### chartUsageStatistics (Timeseries)

Experimental (Subject to breaking change) -- Stats corresponding to chart's usage.

If this aspect represents the latest snapshot of the statistics about a Chart, the eventGranularity field should be null.
If this aspect represents a bucketed window of usage statistics (e.g. over a day), then the eventGranularity field should be set accordingly.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "chartUsageStatistics",
    "type": "timeseries"
  },
  "name": "ChartUsageStatistics",
  "namespace": "com.linkedin.chart",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION"
            },
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "String representation of the partition"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition if applicable"
            }
          ],
          "doc": "Defines how the data is partitioned"
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "viewsCount",
      "default": null,
      "doc": "The total number of times chart has been viewed"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "uniqueUserCount",
      "default": null,
      "doc": "Unique user count"
    },
    {
      "TimeseriesFieldCollection": {
        "key": "user"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "ChartUserUsageCounts",
            "namespace": "com.linkedin.chart",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "user",
                "doc": "The unique id of the user."
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "int"
                ],
                "name": "viewsCount",
                "default": null,
                "doc": "The number of times the user has viewed the chart"
              }
            ],
            "doc": "Records a single user's usage counts for a given resource"
          }
        }
      ],
      "name": "userCounts",
      "default": null,
      "doc": "Users within this bucket, with frequency counts"
    }
  ],
  "doc": "Experimental (Subject to breaking change) -- Stats corresponding to chart's usage.\n\nIf this aspect represents the latest snapshot of the statistics about a Chart, the eventGranularity field should be null.\nIf this aspect represents a bucketed window of usage statistics (e.g. over a day), then the eventGranularity field should be set accordingly."
}
```

</details>

## Relationships

### Outgoing

These are the relationships stored in this entity's aspects

- Consumes

  - Dataset via `chartInfo.inputs`
  - Dataset via `chartInfo.inputEdges`

- OwnedBy

  - Corpuser via `ownership.owners.owner`
  - CorpGroup via `ownership.owners.owner`

- ownershipType

  - OwnershipType via `ownership.owners.typeUrn`

- TaggedWith

  - Tag via `globalTags.tags`
  - Tag via `inputFields.fields.schemaField.globalTags.tags`

- TermedWith

  - GlossaryTerm via `glossaryTerms.terms.urn`

- consumesField

  - SchemaField via `inputFields.fields.schemaFieldUrn`

- SchemaFieldTaggedWith

  - Tag via `inputFields.fields.schemaField.globalTags`

- SchemaFieldWithGlossaryTerm

  - GlossaryTerm via `inputFields.fields.schemaField.glossaryTerms`

- AssociatedWith

  - Domain via `domains.domains`

- IsPartOf

  - Container via `container.container`

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
