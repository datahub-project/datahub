# Post


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

#### postInfo
Information about a DataHub Post.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| type | PostType | ✓ | Type of the Post. | Searchable |
| content | PostContent | ✓ | Content stored in the post. |  |
| created | long | ✓ | The time at which the post was initially created | Searchable |
| lastModified | long | ✓ | The time at which the post was last modified | Searchable |
| auditStamp | [AuditStamp](#auditstamp) |  | The audit stamp at which the request was last updated | Searchable |
| target | string |  | Optional Entity URN that the post is associated with. | Searchable, → PostTarget |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "postInfo"
  },
  "name": "PostInfo",
  "namespace": "com.linkedin.post",
  "fields": [
    {
      "Searchable": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ENTITY_ANNOUNCEMENT": "The Post is an Entity level announcement.",
          "HOME_PAGE_ANNOUNCEMENT": "The Post is an Home Page announcement."
        },
        "name": "PostType",
        "namespace": "com.linkedin.post",
        "symbols": [
          "HOME_PAGE_ANNOUNCEMENT",
          "ENTITY_ANNOUNCEMENT"
        ],
        "doc": "Enum defining types of Posts."
      },
      "name": "type",
      "doc": "Type of the Post."
    },
    {
      "type": {
        "type": "record",
        "name": "PostContent",
        "namespace": "com.linkedin.post",
        "fields": [
          {
            "Searchable": {
              "fieldType": "TEXT_PARTIAL"
            },
            "type": "string",
            "name": "title",
            "doc": "Title of the post."
          },
          {
            "type": {
              "type": "enum",
              "symbolDocs": {
                "LINK": "Link content",
                "TEXT": "Text content"
              },
              "name": "PostContentType",
              "namespace": "com.linkedin.post",
              "symbols": [
                "TEXT",
                "LINK"
              ],
              "doc": "Enum defining the type of content held in a Post."
            },
            "name": "type",
            "doc": "Type of content held in the post."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "description",
            "default": null,
            "doc": "Optional description of the post."
          },
          {
            "java": {
              "class": "com.linkedin.common.url.Url",
              "coercerClass": "com.linkedin.common.url.UrlCoercer"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "link",
            "default": null,
            "doc": "Optional link that the post is associated with."
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "Media",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": {
                      "type": "enum",
                      "symbolDocs": {
                        "IMAGE": "The Media holds an image."
                      },
                      "name": "MediaType",
                      "namespace": "com.linkedin.common",
                      "symbols": [
                        "IMAGE"
                      ],
                      "doc": "Enum defining the type of content a Media object holds."
                    },
                    "name": "type",
                    "doc": "Type of content the Media is storing, e.g. image, video, etc."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.url.Url",
                      "coercerClass": "com.linkedin.common.url.UrlCoercer"
                    },
                    "type": "string",
                    "name": "location",
                    "doc": "Where the media content is stored."
                  }
                ],
                "doc": "Carries information about which roles a user is assigned to."
              }
            ],
            "name": "media",
            "default": null,
            "doc": "Optional media that the post is storing"
          }
        ],
        "doc": "Content stored inside a Post."
      },
      "name": "content",
      "doc": "Content stored in the post."
    },
    {
      "Searchable": {
        "fieldType": "COUNT"
      },
      "type": "long",
      "name": "created",
      "doc": "The time at which the post was initially created"
    },
    {
      "Searchable": {
        "fieldType": "COUNT"
      },
      "type": "long",
      "name": "lastModified",
      "doc": "The time at which the post was last modified"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "created",
          "fieldType": "COUNT"
        }
      },
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
      "name": "auditStamp",
      "default": null,
      "doc": "The audit stamp at which the request was last updated"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataset",
          "schemaField",
          "chart",
          "container",
          "dashboard",
          "dataFlow",
          "dataJob",
          "dataProduct",
          "glossaryTerm",
          "glossaryNode",
          "mlModel",
          "mlFeature",
          "notebook",
          "mlFeatureTable",
          "mlPrimaryKey",
          "mlModelGroup",
          "domain",
          "dataProduct"
        ],
        "name": "PostTarget"
      },
      "Searchable": {},
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "target",
      "default": null,
      "doc": "Optional Entity URN that the post is associated with."
    }
  ],
  "doc": "Information about a DataHub Post."
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
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
- PostTarget

   - Dataset via `postInfo.target`
   - SchemaField via `postInfo.target`
   - Chart via `postInfo.target`
   - Container via `postInfo.target`
   - Dashboard via `postInfo.target`
   - DataFlow via `postInfo.target`
   - DataJob via `postInfo.target`
   - DataProduct via `postInfo.target`
   - GlossaryTerm via `postInfo.target`
   - GlossaryNode via `postInfo.target`
   - MlModel via `postInfo.target`
   - MlFeature via `postInfo.target`
   - Notebook via `postInfo.target`
   - MlFeatureTable via `postInfo.target`
   - MlPrimaryKey via `postInfo.target`
   - MlModelGroup via `postInfo.target`
   - Domain via `postInfo.target`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
