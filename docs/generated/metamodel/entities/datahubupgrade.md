# DataHubUpgrade


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

#### dataHubUpgradeRequest
Information collected when kicking off a DataHubUpgrade



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| timestampMs | long | ✓ | Timestamp when we started this DataHubUpgrade |  |
| version | string | ✓ | Version of this upgrade |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubUpgradeRequest"
  },
  "name": "DataHubUpgradeRequest",
  "namespace": "com.linkedin.upgrade",
  "fields": [
    {
      "type": "long",
      "name": "timestampMs",
      "doc": "Timestamp when we started this DataHubUpgrade"
    },
    {
      "type": "string",
      "name": "version",
      "doc": "Version of this upgrade"
    }
  ],
  "doc": "Information collected when kicking off a DataHubUpgrade"
}
```





#### dataHubUpgradeResult
Information collected when a DataHubUpgrade successfully finishes



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| state | DataHubUpgradeState |  | Upgrade state  UpgradeResult.Result |  |
| timestampMs | long | ✓ | Timestamp when we started this DataHubUpgrade |  |
| result | map |  | Result map to place helpful information about this upgrade job |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubUpgradeResult"
  },
  "name": "DataHubUpgradeResult",
  "namespace": "com.linkedin.upgrade",
  "fields": [
    {
      "type": [
        {
          "type": "enum",
          "symbolDocs": {
            "ABORTED": "Upgrade with an error state and should not be re-run.",
            "FAILED": "Upgrade with an error state, however the upgrade should be re-run.",
            "IN_PROGRESS": "Upgrade in progress.",
            "SUCCEEDED": "Upgrade was successful."
          },
          "name": "DataHubUpgradeState",
          "namespace": "com.linkedin.upgrade",
          "symbols": [
            "IN_PROGRESS",
            "SUCCEEDED",
            "FAILED",
            "ABORTED"
          ]
        },
        "null"
      ],
      "name": "state",
      "default": "SUCCEEDED",
      "doc": "Upgrade state  UpgradeResult.Result"
    },
    {
      "type": "long",
      "name": "timestampMs",
      "doc": "Timestamp when we started this DataHubUpgrade"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "result",
      "default": null,
      "doc": "Result map to place helpful information about this upgrade job"
    }
  ],
  "doc": "Information collected when a DataHubUpgrade successfully finishes"
}
```





### Relationships

### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
