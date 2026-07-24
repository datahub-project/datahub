# DataHubFile


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

#### dataHubFileInfo
Information about a DataHub file - a file stored in S3 for use within DataHub platform features like documentation, home pages, and announcements.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| bucketStorageLocation | BucketStorageLocation | ✓ | Info about where a file is stored |  |
| originalFileName | string | ✓ | The original filename as uploaded by the user | Searchable |
| mimeType | string | ✓ | MIME type of the file (e.g., image/png, application/pdf) | Searchable |
| sizeInBytes | long | ✓ | Size of the file in bytes |  |
| scenario | FileUploadScenario | ✓ | The scenario/context in which this file was uploaded | Searchable |
| referencedByAsset | string |  | Optional URN of the entity this file is associated with (e.g., the dataset whose docs contain thi... | Searchable, → ReferencedBy |
| schemaField | string |  | The dataset schema field urn this file is referenced by | Searchable, → ReferencedBy |
| created | [AuditStamp](#auditstamp) | ✓ | Timestamp when this file was created and by whom | Searchable |
| contentHash | string |  | SHA-256 hash of file contents | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubFileInfo"
  },
  "name": "DataHubFileInfo",
  "namespace": "com.linkedin.file",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "BucketStorageLocation",
        "namespace": "com.linkedin.file",
        "fields": [
          {
            "Searchable": {
              "fieldType": "KEYWORD"
            },
            "type": "string",
            "name": "storageBucket",
            "doc": "The storage bucket this file is stored in"
          },
          {
            "Searchable": {
              "fieldType": "KEYWORD"
            },
            "type": "string",
            "name": "storageKey",
            "doc": "The key for where this file is stored inside of the given bucket"
          }
        ],
        "doc": "Information where a file is stored"
      },
      "name": "bucketStorageLocation",
      "doc": "Info about where a file is stored"
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "originalFileName",
      "doc": "The original filename as uploaded by the user"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "mimeType",
      "doc": "MIME type of the file (e.g., image/png, application/pdf)"
    },
    {
      "type": "long",
      "name": "sizeInBytes",
      "doc": "Size of the file in bytes"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ASSET_DOCUMENTATION": "File uploaded for entity documentation",
          "ASSET_DOCUMENTATION_LINKS": "Upload for asset documentation links."
        },
        "name": "FileUploadScenario",
        "namespace": "com.linkedin.file",
        "symbols": [
          "ASSET_DOCUMENTATION",
          "ASSET_DOCUMENTATION_LINKS"
        ]
      },
      "name": "scenario",
      "doc": "The scenario/context in which this file was uploaded"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataset",
          "chart",
          "container",
          "dashboard",
          "dataFlow",
          "dataJob",
          "glossaryTerm",
          "glossaryNode",
          "mlModel",
          "mlFeature",
          "notebook",
          "mlFeatureTable",
          "mlPrimaryKey",
          "mlModelGroup",
          "domain",
          "dataProduct",
          "businessAttribute",
          "document"
        ],
        "name": "ReferencedBy"
      },
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "referencedByAsset",
      "default": null,
      "doc": "Optional URN of the entity this file is associated with (e.g., the dataset whose docs contain this file)"
    },
    {
      "Relationship": {
        "entityTypes": [
          "schemaField"
        ],
        "name": "ReferencedBy"
      },
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "schemaField",
      "default": null,
      "doc": "The dataset schema field urn this file is referenced by"
    },
    {
      "Searchable": {
        "/actor": {
          "fieldName": "createdBy",
          "fieldType": "URN"
        },
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME"
        }
      },
      "type": {
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
      },
      "name": "created",
      "doc": "Timestamp when this file was created and by whom"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "contentHash",
      "default": null,
      "doc": "SHA-256 hash of file contents"
    }
  ],
  "doc": "Information about a DataHub file - a file stored in S3 for use within DataHub platform features like documentation, home pages, and announcements."
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


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
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
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
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
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

#### Outgoing
These are the relationships stored in this entity's aspects
- ReferencedBy

   - Dataset via `dataHubFileInfo.referencedByAsset`
   - Chart via `dataHubFileInfo.referencedByAsset`
   - Container via `dataHubFileInfo.referencedByAsset`
   - Dashboard via `dataHubFileInfo.referencedByAsset`
   - DataFlow via `dataHubFileInfo.referencedByAsset`
   - DataJob via `dataHubFileInfo.referencedByAsset`
   - GlossaryTerm via `dataHubFileInfo.referencedByAsset`
   - GlossaryNode via `dataHubFileInfo.referencedByAsset`
   - MlModel via `dataHubFileInfo.referencedByAsset`
   - MlFeature via `dataHubFileInfo.referencedByAsset`
   - Notebook via `dataHubFileInfo.referencedByAsset`
   - MlFeatureTable via `dataHubFileInfo.referencedByAsset`
   - MlPrimaryKey via `dataHubFileInfo.referencedByAsset`
   - MlModelGroup via `dataHubFileInfo.referencedByAsset`
   - Domain via `dataHubFileInfo.referencedByAsset`
   - DataProduct via `dataHubFileInfo.referencedByAsset`
   - BusinessAttribute via `dataHubFileInfo.referencedByAsset`
   - Document via `dataHubFileInfo.referencedByAsset`
   - SchemaField via `dataHubFileInfo.schemaField`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
