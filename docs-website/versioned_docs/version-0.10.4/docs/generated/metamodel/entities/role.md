---
sidebar_position: 1
title: Role
slug: /generated/metamodel/entities/role
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/role.md
---

# Role

## Aspects

### roleProperties

Information about a ExternalRoleProperties

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "roleProperties"
  },
  "name": "RoleProperties",
  "namespace": "com.linkedin.role",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the IAM Role in the external system"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the IAM Role"
    },
    {
      "type": "string",
      "name": "type",
      "doc": "Can be READ, ADMIN, WRITE"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "requestUrl",
      "default": null,
      "doc": "Link to access external access management"
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
      "doc": "Created Audit stamp"
    }
  ],
  "doc": "Information about a ExternalRoleProperties"
}
```

</details>

### actors

Provisioned users of a role

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "actors"
  },
  "name": "Actors",
  "namespace": "com.linkedin.role",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "RoleUser",
            "namespace": "com.linkedin.role",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "corpuser"
                  ],
                  "name": "Has"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "user",
                "doc": "Link provisioned corp user for a role"
              }
            ],
            "doc": "Provisioned users of a role"
          }
        }
      ],
      "name": "users",
      "default": null,
      "doc": "List of provisioned users of a role"
    }
  ],
  "doc": "Provisioned users of a role"
}
```

</details>

## Relationships

### Outgoing

These are the relationships stored in this entity's aspects

- Has

  - Corpuser via `actors.users.user`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
