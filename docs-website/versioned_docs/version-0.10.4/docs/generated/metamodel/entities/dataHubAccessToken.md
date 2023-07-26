---
sidebar_position: 41
title: DataHubAccessToken
slug: /generated/metamodel/entities/datahubaccesstoken
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubAccessToken.md
---

# DataHubAccessToken

## Aspects

### dataHubAccessTokenInfo

Information about a DataHub Access Token

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubAccessTokenInfo"
  },
  "name": "DataHubAccessTokenInfo",
  "namespace": "com.linkedin.access.token",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "User defined name for the access token if defined."
    },
    {
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actorUrn",
      "doc": "Urn of the actor to which this access token belongs to."
    },
    {
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "ownerUrn",
      "doc": "Urn of the actor which created this access token."
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "queryByDefault": false
      },
      "type": "long",
      "name": "createdAt",
      "doc": "When the token was created."
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "queryByDefault": false
      },
      "type": [
        "null",
        "long"
      ],
      "name": "expiresAt",
      "default": null,
      "doc": "When the token expires."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the token if defined."
    }
  ],
  "doc": "Information about a DataHub Access Token"
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
