---
sidebar_position: 59
title: DataHubOpenAPISchema
slug: /generated/metamodel/entities/datahubopenapischema
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubOpenAPISchema.md
---
# DataHubOpenAPISchema
Contains aspects which are used in OpenAPI requests/responses which are not otherwise present in the data model.
## Aspects

### systemMetadata
Metadata associated with each metadata change that is processed by the system
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "systemMetadata"
  },
  "name": "SystemMetadata",
  "namespace": "com.linkedin.mxe",
  "fields": [
    {
      "type": [
        "long",
        "null"
      ],
      "name": "lastObserved",
      "default": 0,
      "doc": "The timestamp the metadata was observed at"
    },
    {
      "type": [
        "string",
        "null"
      ],
      "name": "runId",
      "default": "no-run-id-provided",
      "doc": "The original run id that produced the metadata. Populated in case of batch-ingestion."
    },
    {
      "type": [
        "string",
        "null"
      ],
      "name": "lastRunId",
      "default": "no-run-id-provided",
      "doc": "The last run id that produced the metadata. Populated in case of batch-ingestion."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "pipelineName",
      "default": null,
      "doc": "The ingestion pipeline id that produced the metadata. Populated in case of batch ingestion."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "registryName",
      "default": null,
      "doc": "The model registry name that was used to process this event"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "registryVersion",
      "default": null,
      "doc": "The model registry version that was used to process this event"
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
      "doc": "Additional properties"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "version",
      "default": null,
      "doc": "Aspect version\n   Initial implementation will use the aspect version's number, however stored as\n   a string in the case where a different aspect versioning scheme is later adopted."
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
      "name": "aspectCreated",
      "default": null,
      "doc": "When the aspect was initially created and who created it, detected by version 0 -> 1 change"
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "aspectModified",
      "default": null,
      "doc": "When the aspect was last modified and the actor that performed the modification"
    }
  ],
  "doc": "Metadata associated with each metadata change that is processed by the system"
}
```
</details>

## Relationships

## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
