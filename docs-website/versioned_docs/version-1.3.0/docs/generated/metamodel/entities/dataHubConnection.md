---
sidebar_position: 58
title: DataHubConnection
slug: /generated/metamodel/entities/datahubconnection
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubConnection.md
---
# DataHubConnection
## Aspects

### dataHubConnectionDetails
Information about a connection to an external platform.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubConnectionDetails"
  },
  "name": "DataHubConnectionDetails",
  "namespace": "com.linkedin.connection",
  "fields": [
    {
      "Searchable": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "JSON": "A json-encoded set of connection details"
        },
        "name": "DataHubConnectionDetailsType",
        "namespace": "com.linkedin.connection",
        "symbols": [
          "JSON"
        ]
      },
      "name": "type",
      "doc": "The type of the connection. This defines the schema / encoding of the connection details."
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
      "doc": "Display name of the connection"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataHubJsonConnection",
          "namespace": "com.linkedin.connection",
          "fields": [
            {
              "type": "string",
              "name": "encryptedBlob",
              "doc": "The encrypted JSON connection details."
            }
          ],
          "doc": "A set of connection details consisting of an encrypted JSON blob."
        }
      ],
      "name": "json",
      "default": null,
      "doc": "An JSON payload containing raw connection details.\nThis will be present if the type is JSON."
    }
  ],
  "doc": "Information about a connection to an external platform."
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

## Relationships

## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
