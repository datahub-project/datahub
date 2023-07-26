---
sidebar_position: 33
title: OwnershipType
slug: /generated/metamodel/entities/ownershiptype
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/ownershipType.md
---

# OwnershipType

Ownership Type represents a user-created ownership category for a person or group who is responsible for an asset.

## Aspects

### ownershipTypeInfo

Information about an ownership type

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownershipTypeInfo"
  },
  "name": "OwnershipTypeInfo",
  "namespace": "com.linkedin.ownership",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the Ownership Type"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the Ownership Type"
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
      "doc": "Audit stamp capturing the time and actor who created the Ownership Type."
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
      "doc": "Audit stamp capturing the time and actor who last modified the Ownership Type."
    }
  ],
  "doc": "Information about an ownership type"
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

### Incoming

These are the relationships stored in other entity's aspects

- ownershipType

  - Dataset via `ownership.owners.typeUrn`
  - DataJob via `ownership.owners.typeUrn`
  - DataFlow via `ownership.owners.typeUrn`
  - DataProcess via `ownership.owners.typeUrn`
  - Chart via `ownership.owners.typeUrn`
  - Dashboard via `ownership.owners.typeUrn`
  - Notebook via `ownership.owners.typeUrn`
  - CorpGroup via `ownership.owners.typeUrn`
  - Domain via `ownership.owners.typeUrn`
  - Container via `ownership.owners.typeUrn`
  - Tag via `ownership.owners.typeUrn`
  - GlossaryTerm via `ownership.owners.typeUrn`
  - GlossaryNode via `ownership.owners.typeUrn`
  - DataPlatformInstance via `ownership.owners.typeUrn`
  - MlModel via `ownership.owners.typeUrn`
  - MlModelGroup via `ownership.owners.typeUrn`
  - MlModelDeployment via `ownership.owners.typeUrn`
  - MlFeatureTable via `ownership.owners.typeUrn`
  - MlFeature via `ownership.owners.typeUrn`
  - MlPrimaryKey via `ownership.owners.typeUrn`
  - DataProduct via `ownership.owners.typeUrn`

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
