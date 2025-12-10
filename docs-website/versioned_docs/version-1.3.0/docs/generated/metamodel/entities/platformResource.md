---
sidebar_position: 43
title: PlatformResource
slug: /generated/metamodel/entities/platformresource
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/platformResource.md
---
# PlatformResource
Platform Resources are assets that are unmodeled and stored outside of the core data model. They are stored in DataHub primarily to help with application-specific use-cases that are not sufficiently generalized to move into the core data model.
## Aspects

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

### platformResourceInfo
Platform Resource Info.
These entities are for miscelaneous data that is used in non-core parts of the system.
For instance, if we want to persist & retrieve data from auxiliary integrations such as Slack or Microsoft Teams.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "platformResourceInfo"
  },
  "name": "PlatformResourceInfo",
  "namespace": "com.linkedin.platformresource",
  "fields": [
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "resourceType",
      "doc": "The type of the resource. \nIntended as a loose specifier of the generic type of the resource.\nProducer is not forced to conform to a specific set of symbols for\nresource types.\nThe @PlatformResourceType enumeration offers a paved path for agreed upon\ncommon terms, but is not required to be followed.\nExample values could be: conversation, user, grant, etc.\nResource types are indexed for ease of access. \ne.g. Get me all platform resources of type user for the platform looker"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "primaryKey",
      "doc": "The primary key for this platform resource.\ne.g. for a slack member this would be the memberID.\nprimary keys specified here don't need to include any additional specificity for the\n     dataPlatform\nThe @PlatformResourceKey is supposed to represent that"
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "KEYWORD"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "secondaryKeys",
      "default": null,
      "doc": "The secondary keys this platform resource can be located by.\nI.e., for a slack member this would be email or phone."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SerializedValue",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "bytes",
              "name": "blob",
              "doc": "The serialized blob value."
            },
            {
              "type": {
                "type": "enum",
                "name": "SerializedValueContentType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "JSON",
                  "BINARY"
                ]
              },
              "name": "contentType",
              "default": "JSON",
              "doc": "The content-type of the serialized blob value."
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "name": "SerializedValueSchemaType",
                  "namespace": "com.linkedin.common",
                  "symbols": [
                    "AVRO",
                    "PROTOBUF",
                    "PEGASUS",
                    "THRIFT",
                    "JSON",
                    "NONE"
                  ]
                }
              ],
              "name": "schemaType",
              "default": null,
              "doc": "The schema type for the schema that models the object that was serialized\n       into the blob.\nAbsence of this field indicates that the schema is not known.\nIf the schema is known, the value should be set to the appropriate schema\ntype.\nUse the NONE value if the existing schema categories do not apply."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "schemaRef",
              "default": null,
              "doc": "An optional reference to the schema that models the object.\ne.g., 'com.linkedin.platformresource.slack.SlackConversation'"
            }
          ],
          "doc": "Captures the serialized value of a (usually) schema-d blob."
        }
      ],
      "name": "value",
      "default": null,
      "doc": "The serialized value of this platform resource item."
    }
  ],
  "doc": "Platform Resource Info.\nThese entities are for miscelaneous data that is used in non-core parts of the system.\nFor instance, if we want to persist & retrieve data from auxiliary integrations such as Slack or Microsoft Teams."
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

## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
