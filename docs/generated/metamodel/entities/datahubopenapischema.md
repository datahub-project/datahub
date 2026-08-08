# DataHubOpenAPISchema
Contains aspects which are used in OpenAPI requests/responses which are not otherwise present in the data model.


## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### systemMetadata
Metadata associated with each metadata change that is processed by the system



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| lastObserved | long |  | The timestamp the metadata was observed at |  |
| runId | string |  | The original run id that produced the metadata. Populated in case of batch-ingestion. |  |
| lastRunId | string |  | The last run id that produced the metadata. Populated in case of batch-ingestion. |  |
| pipelineName | string |  | The ingestion pipeline id that produced the metadata. Populated in case of batch ingestion. |  |
| registryName | string |  | The model registry name that was used to process this event |  |
| registryVersion | string |  | The model registry version that was used to process this event |  |
| properties | map |  | Additional properties |  |
| version | string |  | Aspect version    Initial implementation will use the aspect version's number, however stored as ... |  |
| schemaVersion | long |  | Schema version of the aspect data model. Used to determine aspects that need migrations. Defaults... |  |
| aspectCreated | [AuditStamp](#auditstamp) |  | When the aspect was initially created and who created it, detected by version 0 -> 1 change |  |
| aspectModified | [AuditStamp](#auditstamp) |  | When the aspect was last modified and the actor that performed the modification |  |



#### Raw Schema


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
        "long"
      ],
      "name": "schemaVersion",
      "default": null,
      "doc": "Schema version of the aspect data model. Used to determine aspects that need migrations.\nDefaults to 1 when not present as a baseline. Incremented when an aspect is modified."
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





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...


### Relationships

### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
