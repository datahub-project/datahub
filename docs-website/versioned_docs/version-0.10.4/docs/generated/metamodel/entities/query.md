---
sidebar_position: 31
title: Query
slug: /generated/metamodel/entities/query
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/query.md
---

# Query

## Aspects

### queryProperties

Information about a Query against one or more data assets (e.g. Tables or Views).

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "queryProperties"
  },
  "name": "QueryProperties",
  "namespace": "com.linkedin.query",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "QueryStatement",
        "namespace": "com.linkedin.query",
        "fields": [
          {
            "type": "string",
            "name": "value",
            "doc": "The query text"
          },
          {
            "type": {
              "type": "enum",
              "symbolDocs": {
                "SQL": "A SQL Query"
              },
              "name": "QueryLanguage",
              "namespace": "com.linkedin.query",
              "symbols": [
                "SQL"
              ]
            },
            "name": "language",
            "default": "SQL",
            "doc": "The language of the Query, e.g. SQL."
          }
        ],
        "doc": "A query statement against one or more data assets."
      },
      "name": "statement",
      "doc": "The Query Statement."
    },
    {
      "Searchable": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "MANUAL": "The query was entered manually by a user (via the UI)."
        },
        "name": "QuerySource",
        "namespace": "com.linkedin.query",
        "symbols": [
          "MANUAL"
        ]
      },
      "name": "source",
      "doc": "The source of the Query"
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
      "doc": "Optional display name to identify the query."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "The Query description."
    },
    {
      "Searchable": {
        "/actor": {
          "fieldName": "createdBy",
          "fieldType": "URN"
        },
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME"
        }
      },
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
      "doc": "Audit stamp capturing the time and actor who created the Query."
    },
    {
      "Searchable": {
        "/actor": {
          "fieldName": "lastModifiedBy",
          "fieldType": "URN"
        },
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "doc": "Audit stamp capturing the time and actor who last modified the Query."
    }
  ],
  "doc": "Information about a Query against one or more data assets (e.g. Tables or Views)."
}
```

</details>

### querySubjects

Information about the subjects of a particular Query, i.e. the assets
being queried.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "querySubjects"
  },
  "name": "QuerySubjects",
  "namespace": "com.linkedin.query",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "QuerySubject",
          "namespace": "com.linkedin.query",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "dataset",
                  "schemaField"
                ],
                "name": "IsAssociatedWith"
              },
              "Searchable": {
                "fieldName": "entities",
                "fieldType": "URN"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "entity",
              "doc": "An entity which is the subject of a query."
            }
          ],
          "doc": "A single subject of a particular query.\nIn the future, we may evolve this model to include richer details\nabout the Query Subject in relation to the query."
        }
      },
      "name": "subjects",
      "doc": "One or more subjects of the query.\n\nIn single-asset queries (e.g. table select), this will contain the Table reference\nand optionally schema field references.\n\nIn multi-asset queries (e.g. table joins), this may contain multiple Table references\nand optionally schema field references."
    }
  ],
  "doc": "Information about the subjects of a particular Query, i.e. the assets\nbeing queried."
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

- IsAssociatedWith

  - Dataset via `querySubjects.subjects.entity`
  - SchemaField via `querySubjects.subjects.entity`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
