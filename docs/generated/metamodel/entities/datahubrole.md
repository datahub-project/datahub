# DataHubRole


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

#### dataHubRoleInfo
Information about a DataHub Role.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Name of the Role | Searchable |
| description | string | ✓ | Description of the Role | Searchable |
| editable | boolean | ✓ | Whether the role should be editable via the UI |  |



#### Raw Schema


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





### Relationships

#### Incoming
These are the relationships stored in other entity's aspects
- IsAssociatedWithRole

   - DataHubPolicy via `dataHubPolicyInfo.actors.roles`
- IsMemberOfRole

   - Corpuser via `roleMembership.roles`
   - CorpGroup via `roleMembership.roles`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
