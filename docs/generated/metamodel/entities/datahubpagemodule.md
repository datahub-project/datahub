# DataHubPageModule


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

#### dataHubPageModuleProperties
The main properties of a DataHub page module



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | The display name of this module |  |
| type | DataHubPageModuleType | ✓ | The type of this module - the purpose it serves | Searchable |
| visibility | DataHubPageModuleVisibility | ✓ | Info about the visibility of this module |  |
| params | DataHubPageModuleParams | ✓ | The specific parameters stored for this module |  |
| created | [AuditStamp](#auditstamp) | ✓ | Audit stamp for when and by whom this template was created | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp for when and by whom this template was last updated | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubPageModuleProperties",
    "schemaVersion": 3
  },
  "name": "DataHubPageModuleProperties",
  "namespace": "com.linkedin.module",
  "fields": [
    {
      "type": "string",
      "name": "name",
      "doc": "The display name of this module"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "AI_CONTEXT": "Module displaying AI context (synonyms, instructions, examples) for an asset",
          "ASSETS": "Module displaying the assets of parent entity",
          "ASSET_COLLECTION": "A module with a collection of assets",
          "CHILD_HIERARCHY": "Module displaying the hierarchy of the children of a given entity. Glossary or Domains.",
          "COLUMNS": "Module displaying the columns of a dataset",
          "DATA_PRODUCTS": "Module displaying child data products of a given domain",
          "DOMAINS": "Module displaying the top domains",
          "HIERARCHY": "A module displaying a hierarchy to navigate",
          "LINEAGE": "Module displaying the lineage of an asset",
          "LINK": "Link type module",
          "METRIC_SQL": "Module displaying the SQL expression of a metric",
          "OUTPUT_PORTS": "Module displaying the output ports of a data product",
          "OWNED_ASSETS": "Module displaying assets owned by a user",
          "PLATFORMS": "Module displaying the platforms in an instance",
          "RELATED_METRICS": "Module displaying related metrics",
          "RELATED_TERMS": "Module displaying the related terms of a given glossary term",
          "RICH_TEXT": "Module containing rich text to be rendered",
          "SEMANTIC_MODEL_DATASETS": "Module displaying the datasets referenced by a semantic model",
          "SEMANTIC_MODEL_DIMENSIONS": "Module displaying the dimensions of a semantic model",
          "SEMANTIC_MODEL_METRICS": "Module displaying the metrics defined within a semantic model",
          "SEMANTIC_MODEL_RELATIONSHIPS": "Module displaying the relationships of a semantic model",
          "UNKNOWN": "Unknown module type - this can occur with corrupted data or rolling back to versions without new modules"
        },
        "name": "DataHubPageModuleType",
        "namespace": "com.linkedin.module",
        "symbols": [
          "LINK",
          "RICH_TEXT",
          "ASSET_COLLECTION",
          "HIERARCHY",
          "OWNED_ASSETS",
          "DOMAINS",
          "ASSETS",
          "OUTPUT_PORTS",
          "CHILD_HIERARCHY",
          "DATA_PRODUCTS",
          "RELATED_TERMS",
          "PLATFORMS",
          "LINEAGE",
          "COLUMNS",
          "SEMANTIC_MODEL_DATASETS",
          "SEMANTIC_MODEL_METRICS",
          "SEMANTIC_MODEL_RELATIONSHIPS",
          "SEMANTIC_MODEL_DIMENSIONS",
          "AI_CONTEXT",
          "METRIC_SQL",
          "RELATED_METRICS",
          "UNKNOWN"
        ],
        "doc": "Enum containing the types of page modules that there are"
      },
      "name": "type",
      "doc": "The type of this module - the purpose it serves"
    },
    {
      "type": {
        "type": "record",
        "name": "DataHubPageModuleVisibility",
        "namespace": "com.linkedin.module",
        "fields": [
          {
            "Searchable": {
              "fieldType": "KEYWORD"
            },
            "type": {
              "type": "enum",
              "symbolDocs": {
                "GLOBAL": "This module is discoverable and can be used by any user on the platform",
                "PERSONAL": "This module is used for individual use only"
              },
              "name": "PageModuleScope",
              "namespace": "com.linkedin.module",
              "symbols": [
                "PERSONAL",
                "GLOBAL"
              ]
            },
            "name": "scope",
            "doc": "Audit stamp for when and by whom this module was created"
          }
        ],
        "doc": "Info about the visibility of this module"
      },
      "name": "visibility",
      "doc": "Info about the visibility of this module"
    },
    {
      "type": {
        "type": "record",
        "name": "DataHubPageModuleParams",
        "namespace": "com.linkedin.module",
        "fields": [
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "LinkModuleParams",
                "namespace": "com.linkedin.module",
                "fields": [
                  {
                    "type": "string",
                    "name": "linkUrl"
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "imageUrl",
                    "default": null
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "description",
                    "default": null
                  }
                ]
              }
            ],
            "name": "linkParams",
            "default": null,
            "doc": "The params required if the module is type LINK"
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "RichTextModuleParams",
                "namespace": "com.linkedin.module",
                "fields": [
                  {
                    "type": "string",
                    "name": "content"
                  }
                ]
              }
            ],
            "name": "richTextParams",
            "default": null,
            "doc": "The params required if the module is type RICH_TEXT"
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "AssetCollectionModuleParams",
                "namespace": "com.linkedin.module",
                "fields": [
                  {
                    "type": {
                      "type": "array",
                      "items": "string"
                    },
                    "name": "assetUrns"
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "dynamicFilterJson",
                    "default": null,
                    "doc": "Optional dynamic filters\n\nThe stringified json representing the logical predicate built in the UI to select assets.\nThis predicate is turned into orFilters to send through graphql since graphql doesn't support\narbitrary nesting. This string is used to restore the UI for this logical predicate."
                  }
                ],
                "doc": "The params required if the module is type ASSET_COLLECTION"
              }
            ],
            "name": "assetCollectionParams",
            "default": null,
            "doc": "The params required if the module is type ASSET_COLLECTION"
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "HierarchyModuleParams",
                "namespace": "com.linkedin.module",
                "fields": [
                  {
                    "type": [
                      "null",
                      {
                        "type": "array",
                        "items": "string"
                      }
                    ],
                    "name": "assetUrns",
                    "default": null
                  },
                  {
                    "type": "boolean",
                    "name": "showRelatedEntities"
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "relatedEntitiesFilterJson",
                    "default": null,
                    "doc": "Optional filters to filter relatedEntities (assetUrns) out\n\nThe stringified json representing the logical predicate built in the UI to select assets.\nThis predicate is turned into orFilters to send through graphql since graphql doesn't support\narbitrary nesting. This string is used to restore the UI for this logical predicate."
                  }
                ],
                "doc": "The params required if the module is type HIERARCHY_VIEW"
              }
            ],
            "name": "hierarchyViewParams",
            "default": null,
            "doc": "The params required if the module is type HIERARCHY_VIEW"
          }
        ],
        "doc": "The specific parameters stored for a module"
      },
      "name": "params",
      "doc": "The specific parameters stored for this module"
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
      "doc": "Audit stamp for when and by whom this template was created"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "doc": "Audit stamp for when and by whom this template was last updated"
    }
  ],
  "doc": "The main properties of a DataHub page module"
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

#### Incoming
These are the relationships stored in other entity's aspects
- ContainedIn

   - DataHubPageTemplate via `dataHubPageTemplateProperties.rows`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
