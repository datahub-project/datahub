---
sidebar_position: 27
title: DataHubRole
slug: /generated/metamodel/entities/datahubrole
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubRole.md
---

# DataHubRole

## Aspects

### dataHubRoleInfo

Information about a DataHub Role.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubRoleInfo"
  },
  "name": "DataHubRoleInfo",
  "namespace": "com.linkedin.policy",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Name of the Role"
    },
    {
      "Searchable": {
        "fieldType": "TEXT"
      },
      "type": "string",
      "name": "description",
      "doc": "Description of the Role"
    },
    {
      "type": "boolean",
      "name": "editable",
      "default": false,
      "doc": "Whether the role should be editable via the UI"
    }
  ],
  "doc": "Information about a DataHub Role."
}
```

</details>

## Relationships

### Incoming

These are the relationships stored in other entity's aspects

- IsAssociatedWithRole

  - DataHubPolicy via `dataHubPolicyInfo.actors.roles`

- IsMemberOfRole

  - Corpuser via `roleMembership.roles`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
