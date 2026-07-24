# DataHubPageTemplate


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

#### dataHubPageTemplateProperties
The main properties of a DataHub page template



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| rows | DataHubPageTemplateRow[] | ✓ | The rows of modules contained in this template | → ContainedIn |
| assetSummary | DataHubPageTemplateAssetSummary |  | The optional info for asset summaries. Should be populated if surfaceType is ASSET_SUMMARY |  |
| surface | DataHubPageTemplateSurface | ✓ | Info about the surface area of the product that this template is deployed in |  |
| visibility | DataHubPageTemplateVisibility | ✓ | Info about the visibility of this template |  |
| created | [AuditStamp](#auditstamp) | ✓ | Audit stamp for when and by whom this template was created | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp for when and by whom this template was last updated | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubPageTemplateProperties",
    "schemaVersion": 2
  },
  "name": "DataHubPageTemplateProperties",
  "namespace": "com.linkedin.template",
  "fields": [
    {
      "Relationship": {
        "/*/modules/*": {
          "entityTypes": [
            "dataHubPageModule"
          ],
          "name": "ContainedIn"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DataHubPageTemplateRow",
          "namespace": "com.linkedin.template",
          "fields": [
            {
              "type": {
                "type": "array",
                "items": "string"
              },
              "name": "modules",
              "doc": "The modules that exist in this template row"
            }
          ],
          "doc": "A row of modules contained in a template"
        }
      },
      "name": "rows",
      "doc": "The rows of modules contained in this template"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataHubPageTemplateAssetSummary",
          "namespace": "com.linkedin.template",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "SummaryElement",
                    "namespace": "com.linkedin.template",
                    "fields": [
                      {
                        "type": {
                          "type": "enum",
                          "name": "SummaryElementType",
                          "namespace": "com.linkedin.template",
                          "symbols": [
                            "CREATED",
                            "LAST_MODIFIED",
                            "LAST_INGESTED",
                            "TAGS",
                            "GLOSSARY_TERMS",
                            "OWNERS",
                            "DOMAIN",
                            "STRUCTURED_PROPERTY",
                            "DOCUMENT_STATUS",
                            "DOCUMENT_TYPE",
                            "SEMANTIC_MODEL"
                          ]
                        },
                        "name": "elementType",
                        "doc": "The type of element/property"
                      },
                      {
                        "Relationship": {
                          "entityTypes": [
                            "structuredProperty"
                          ],
                          "name": "ContainsStructuredProperty"
                        },
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "structuredPropertyUrn",
                        "default": null,
                        "doc": "The urn of the structured property shown. Required if propertyType is STRUCTURED_PROPERTY"
                      }
                    ],
                    "doc": "Info for a given asset summary element"
                  }
                }
              ],
              "name": "summaryElements",
              "default": null,
              "doc": "The optional list of properties shown on an asset summary page header."
            }
          ],
          "doc": "The page template info for asset summaries"
        }
      ],
      "name": "assetSummary",
      "default": null,
      "doc": "The optional info for asset summaries. Should be populated if surfaceType is ASSET_SUMMARY"
    },
    {
      "type": {
        "type": "record",
        "name": "DataHubPageTemplateSurface",
        "namespace": "com.linkedin.template",
        "fields": [
          {
            "Searchable": {
              "fieldType": "KEYWORD"
            },
            "type": {
              "type": "enum",
              "symbolDocs": {
                "ASSET_SUMMARY": "This template applies to what to display on asset summary pages",
                "HOME_PAGE": "This template applies to what to display on the home page for users."
              },
              "name": "PageTemplateSurfaceType",
              "namespace": "com.linkedin.template",
              "symbols": [
                "HOME_PAGE",
                "ASSET_SUMMARY"
              ]
            },
            "name": "surfaceType",
            "doc": "Where exactly is this template being used"
          }
        ],
        "doc": "Info about the surface area of the product that this template is deployed in"
      },
      "name": "surface",
      "doc": "Info about the surface area of the product that this template is deployed in"
    },
    {
      "type": {
        "type": "record",
        "name": "DataHubPageTemplateVisibility",
        "namespace": "com.linkedin.template",
        "fields": [
          {
            "Searchable": {
              "fieldType": "KEYWORD"
            },
            "type": {
              "type": "enum",
              "symbolDocs": {
                "GLOBAL": "This template is used across users",
                "PERSONAL": "This template is used for individual use only"
              },
              "name": "PageTemplateScope",
              "namespace": "com.linkedin.template",
              "symbols": [
                "PERSONAL",
                "GLOBAL"
              ]
            },
            "name": "scope",
            "doc": "The scope of this template and who can use/see it"
          }
        ],
        "doc": "Info about the visibility of this template"
      },
      "name": "visibility",
      "doc": "Info about the visibility of this template"
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
  "doc": "The main properties of a DataHub page template"
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
- ContainedIn

   - DataHubPageModule via `dataHubPageTemplateProperties.rows`
- ContainsStructuredProperty

   - StructuredProperty via `dataHubPageTemplateProperties.assetSummary.summaryElements.structuredPropertyUrn`
#### Incoming
These are the relationships stored in other entity's aspects
- HasSummaryTemplate

   - Dataset via `assetSettings.assetSummary.templates`
   - Domain via `assetSettings.assetSummary.templates`
   - GlossaryTerm via `assetSettings.assetSummary.templates`
   - GlossaryNode via `assetSettings.assetSummary.templates`
   - DataProduct via `assetSettings.assetSummary.templates`
- HasPersonalPageTemplate

   - Corpuser via `corpUserSettings.homePage.pageTemplate`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
