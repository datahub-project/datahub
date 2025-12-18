---
sidebar_position: 26
title: Incident
slug: /generated/metamodel/entities/incident
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/incident.md
---
# Incident
An incident for an asset.
## Aspects

### incidentInfo
Information about an incident raised on an asset.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentInfo"
  },
  "name": "IncidentInfo",
  "namespace": "com.linkedin.incident",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "filterNameOverride": "Type"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CUSTOM": "A custom type of incident",
          "DATA_SCHEMA": "A Data Schema assertion has failed, triggering the incident.\nRaised on entities where assertions are configured to generate incidents.",
          "FIELD": "A Field Assertion has failed, triggering the incident.\nRaised on entities where assertions are configured to generate incidents.",
          "FRESHNESS": "An Freshness Assertion has failed, triggering the incident.\nRaised on entities where assertions are configured to generate incidents.",
          "OPERATIONAL": "A misc. operational incident, e.g. failure to materialize a dataset.",
          "SQL": "A raw SQL-statement based assertion has failed, triggering the incident.\nRaised on entities where assertions are configured to generate incidents.",
          "VOLUME": "An Volume Assertion has failed, triggering the incident.\nRaised on entities where assertions are configured to generate incidents."
        },
        "name": "IncidentType",
        "namespace": "com.linkedin.incident",
        "symbols": [
          "FRESHNESS",
          "VOLUME",
          "FIELD",
          "SQL",
          "DATA_SCHEMA",
          "OPERATIONAL",
          "CUSTOM"
        ],
        "doc": "A type of asset incident"
      },
      "name": "type",
      "doc": "The type of incident"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "filterNameOverride": "Other Type"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "customType",
      "default": null,
      "doc": "An optional custom incident type. Present only if type is 'CUSTOM'."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "title",
      "default": null,
      "doc": "Optional title associated with the incident"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Optional description associated with the incident"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset",
            "chart",
            "dashboard",
            "dataFlow",
            "dataJob",
            "schemaField"
          ],
          "name": "IncidentOn"
        }
      },
      "Searchable": {
        "/*": {
          "fieldType": "URN"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "entities",
      "doc": "A reference to the entity associated with the incident."
    },
    {
      "Searchable": {
        "addToFilters": true,
        "filterNameOverride": "Priority"
      },
      "type": [
        "null",
        "int"
      ],
      "name": "priority",
      "default": null,
      "doc": "A numeric severity or priority for the incident. On the UI we will translate this into something easy to understand.\nCurrently supported: 0 - CRITICAL, 1 - HIGH, 2 - MED, 3 - LOW\n(We probably should have modeled as an enum)"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "IncidentAssignee",
            "namespace": "com.linkedin.incident",
            "fields": [
              {
                "Searchable": {
                  "addToFilters": true,
                  "fieldName": "assignees",
                  "filterNameOverride": "Assignee"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "actor",
                "doc": "The user or group assigned to the incident."
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
                "name": "assignedAt",
                "doc": "The time & actor responsible for assiging the assignee."
              }
            ],
            "doc": "The incident assignee type.\nThis is in a record so that we can add additional fields if we need to later (e.g.\nthe type of the assignee."
          }
        }
      ],
      "name": "assignees",
      "default": null,
      "doc": "The parties assigned with resolving the incident"
    },
    {
      "type": {
        "type": "record",
        "name": "IncidentStatus",
        "namespace": "com.linkedin.incident",
        "fields": [
          {
            "Searchable": {
              "addToFilters": true,
              "filterNameOverride": "Status"
            },
            "type": {
              "type": "enum",
              "symbolDocs": {
                "ACTIVE": "The incident is ongoing, or active.",
                "RESOLVED": "The incident is resolved."
              },
              "name": "IncidentState",
              "namespace": "com.linkedin.incident",
              "symbols": [
                "ACTIVE",
                "RESOLVED"
              ]
            },
            "name": "state",
            "doc": "The top-level state of the incident, whether it's active or resolved."
          },
          {
            "Searchable": {
              "addToFilters": true,
              "filterNameOverride": "Stage"
            },
            "type": [
              "null",
              {
                "type": "enum",
                "symbolDocs": {
                  "FIXED": "The incident is in the resolved as completed stage.",
                  "INVESTIGATION": "The incident root cause is being investigated.",
                  "NO_ACTION_REQUIRED": "The incident is in the resolved with no action required state, e.g. the\nincident was a false positive, or was expected.",
                  "TRIAGE": "The impact and priority of the incident is being actively assessed.",
                  "WORK_IN_PROGRESS": "The incident is in the remediation stage."
                },
                "name": "IncidentStage",
                "namespace": "com.linkedin.incident",
                "symbols": [
                  "TRIAGE",
                  "INVESTIGATION",
                  "WORK_IN_PROGRESS",
                  "FIXED",
                  "NO_ACTION_REQUIRED"
                ]
              }
            ],
            "name": "stage",
            "default": null,
            "doc": "The lifecycle stage for the incident - Null means no stage was assigned yet.\nIn the future, we may add CUSTOM here with a customStage string field for user-defined stages."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Optional message associated with the incident"
          },
          {
            "Searchable": {
              "/time": {
                "fieldName": "lastUpdated",
                "fieldType": "COUNT"
              }
            },
            "type": "com.linkedin.common.AuditStamp",
            "name": "lastUpdated",
            "doc": "The time at which the request was initially created"
          }
        ],
        "doc": "Information about an incident raised on an asset"
      },
      "name": "status",
      "doc": "The current status of an incident, i.e. active or inactive."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "Aspect": {
            "name": "incidentSource"
          },
          "name": "IncidentSource",
          "namespace": "com.linkedin.incident",
          "fields": [
            {
              "Searchable": {
                "addToFilters": true,
                "filterNameOverride": "Source"
              },
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "ASSERTION_FAILURE": "An assertion has failed, triggering the incident.",
                  "MANUAL": "Manually created incident, via UI or API."
                },
                "name": "IncidentSourceType",
                "namespace": "com.linkedin.incident",
                "symbols": [
                  "MANUAL",
                  "ASSERTION_FAILURE"
                ]
              },
              "name": "type",
              "doc": "Message associated with the incident"
            },
            {
              "Searchable": {
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "sourceUrn",
              "default": null,
              "doc": "Reference to an URN related to the source of an incident."
            }
          ],
          "doc": "Information about the source of an incident raised on an asset."
        }
      ],
      "name": "source",
      "default": null,
      "doc": "The source of an incident, i.e. how it was generated."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "startedAt",
          "fieldType": "COUNT"
        }
      },
      "type": [
        "null",
        "long"
      ],
      "name": "startedAt",
      "default": null,
      "doc": "The time at which the incident actually started (may be before the date it was raised)."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "created",
          "fieldType": "COUNT"
        }
      },
      "type": "com.linkedin.common.AuditStamp",
      "name": "created",
      "doc": "The time at which the request was initially created"
    }
  ],
  "doc": "Information about an incident raised on an asset."
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
</details>

## Relationships

### Outgoing
These are the relationships stored in this entity's aspects
- IncidentOn

   - Dataset via `incidentInfo.entities`
   - Chart via `incidentInfo.entities`
   - Dashboard via `incidentInfo.entities`
   - DataFlow via `incidentInfo.entities`
   - DataJob via `incidentInfo.entities`
   - SchemaField via `incidentInfo.entities`
- TaggedWith

   - Tag via `globalTags.tags`
### Incoming
These are the relationships stored in other entity's aspects
- ResolvedIncidents

   - Dataset via `incidentsSummary.resolvedIncidentDetails`
   - DataJob via `incidentsSummary.resolvedIncidentDetails`
   - DataFlow via `incidentsSummary.resolvedIncidentDetails`
   - Chart via `incidentsSummary.resolvedIncidentDetails`
   - Dashboard via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Dataset via `incidentsSummary.activeIncidentDetails`
   - DataJob via `incidentsSummary.activeIncidentDetails`
   - DataFlow via `incidentsSummary.activeIncidentDetails`
   - Chart via `incidentsSummary.activeIncidentDetails`
   - Dashboard via `incidentsSummary.activeIncidentDetails`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
