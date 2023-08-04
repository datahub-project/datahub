---
sidebar_position: 25
title: InviteToken
slug: /generated/metamodel/entities/invitetoken
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/inviteToken.md
---

# InviteToken

## Aspects

### inviteToken

Aspect used to store invite tokens.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "inviteToken"
  },
  "name": "InviteToken",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "type": "string",
      "name": "token",
      "doc": "The encrypted invite token."
    },
    {
      "Searchable": {
        "fieldName": "role",
        "fieldType": "KEYWORD",
        "hasValuesFieldName": "hasRole"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "role",
      "default": null,
      "doc": "The role that this invite token may be associated with"
    }
  ],
  "doc": "Aspect used to store invite tokens."
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
