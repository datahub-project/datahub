# LifecycleStageType
Defines a lifecycle stage that entities can be placed in (e.g., Proposed, Certified, Archived). Controls search visibility and transition policies.


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

#### lifecycleStageTypeInfo
Information about a lifecycle stage type.

Lifecycle stages control entity visibility and behavior in the platform.
Each stage can apply to specific entity types and define search visibility
and transition policies.

When an entity's Status.lifecycleStage is set to a lifecycle stage type URN,
the stage's settings determine how the entity is treated (e.g., hidden from
default search when hideInSearch=true).

The entityTypes field controls which entity types this stage can be applied to:
  - null/absent: the stage applies to ALL entity types
  - empty list: the stage applies to NO entity types (disabled)
  - explicit list: the stage only applies to those entity types



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the lifecycle stage type. | Searchable |
| description | string |  | Description of what this lifecycle stage represents. |  |
| entityTypes | string[] |  | Entity type names this stage applies to (e.g., ["document", "glossaryTerm"]). When null/absent, a... |  |
| settings | LifecycleStageSettings | ✓ | Settings that control platform behavior for entities in this stage. |  |
| transitionPolicy | LifecycleStageTransitionPolicy |  | Optional policy defining which prior stages can transition INTO this stage. When null, any prior ... |  |
| created | [AuditStamp](#auditstamp) | ✓ | Audit stamp capturing the time and actor who created this lifecycle stage type. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp capturing the time and actor who last modified this lifecycle stage type. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "lifecycleStageTypeInfo"
  },
  "name": "LifecycleStageTypeInfo",
  "namespace": "com.linkedin.lifecycle",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the lifecycle stage type."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of what this lifecycle stage represents."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "entityTypes",
      "default": null,
      "doc": "Entity type names this stage applies to (e.g., [\"document\", \"glossaryTerm\"]).\nWhen null/absent, applies to all entity types.\nWhen empty, applies to no entity types (effectively disabled)."
    },
    {
      "type": {
        "type": "record",
        "name": "LifecycleStageSettings",
        "namespace": "com.linkedin.lifecycle",
        "fields": [
          {
            "type": "boolean",
            "name": "hideInSearch",
            "default": false,
            "doc": "When true, entities in this stage are excluded from default search results.\nUsers can still discover them by explicitly filtering on the lifecycleStage field."
          }
        ],
        "doc": "Settings that control how entities in a given lifecycle stage behave in the platform."
      },
      "name": "settings",
      "doc": "Settings that control platform behavior for entities in this stage."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "LifecycleStageTransitionPolicy",
          "namespace": "com.linkedin.lifecycle",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "allowedPreviousStages",
              "default": null,
              "doc": "Lifecycle stage type URNs that an entity must currently be in to transition\nINTO this stage. Use the sentinel value \"urn:li:lifecycleStageType:__NONE__\"\nto allow entry from the default active state (no stage set).\n\nnull/absent: any prior stage (or no stage) can transition to this one.\nempty list: nothing can transition to this stage (unreachable)."
            }
          ],
          "doc": "Defines which prior stages (or no stage) are allowed to transition INTO this stage.\nEnforced server-side via a MutationHook on the Status aspect.\n\nModeled as entry constraints rather than exit constraints so that adding a new\nstage is self-contained \u2014 you declare its own entry policy without editing\nevery existing stage."
        }
      ],
      "name": "transitionPolicy",
      "default": null,
      "doc": "Optional policy defining which prior stages can transition INTO this stage.\nWhen null, any prior stage can transition to this one (no enforcement)."
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
      "doc": "Audit stamp capturing the time and actor who created this lifecycle stage type."
    },
    {
      "Searchable": {
        "/actor": {
          "fieldName": "lastModifiedBy",
          "fieldType": "URN"
        },
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "doc": "Audit stamp capturing the time and actor who last modified this lifecycle stage type."
    }
  ],
  "doc": "Information about a lifecycle stage type.\n\nLifecycle stages control entity visibility and behavior in the platform.\nEach stage can apply to specific entity types and define search visibility\nand transition policies.\n\nWhen an entity's Status.lifecycleStage is set to a lifecycle stage type URN,\nthe stage's settings determine how the entity is treated (e.g., hidden from\ndefault search when hideInSearch=true).\n\nThe entityTypes field controls which entity types this stage can be applied to:\n  - null/absent: the stage applies to ALL entity types\n  - empty list: the stage applies to NO entity types (disabled)\n  - explicit list: the stage only applies to those entity types"
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

### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
