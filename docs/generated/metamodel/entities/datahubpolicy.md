# DataHubPolicy
DataHub Policies represent access policies granted to users or groups on metadata operations like edit, view etc.


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

#### dataHubPolicyKey
Key for a DataHub Policy



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| id | string | ✓ | A unique id for the DataHub access policy record. Generated on the server side at policy creation... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubPolicyKey"
  },
  "name": "DataHubPolicyKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "type": "string",
      "name": "id",
      "doc": "A unique id for the DataHub access policy record. Generated on the server side at policy creation time."
    }
  ],
  "doc": "Key for a DataHub Policy"
}
```





#### dataHubPolicyInfo
Information about a DataHub (UI) access policy.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| displayName | string | ✓ | Display name of the Policy | Searchable |
| description | string | ✓ | Description of the Policy | Searchable |
| type | string | ✓ | The type of policy | Searchable |
| state | string | ✓ | The state of policy, ACTIVE or INACTIVE | Searchable |
| resources | DataHubResourceFilter |  | The resource that the policy applies to. Not required for some 'Platform' privileges. |  |
| privileges | string[] | ✓ | The privileges that the policy grants. | Searchable |
| actors | DataHubActorFilter | ✓ | The actors that the policy applies to. |  |
| editable | boolean | ✓ | Whether the policy should be editable via the UI | Searchable |
| lastUpdatedTimestamp | long |  | Timestamp when the policy was last updated | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubPolicyInfo"
  },
  "name": "DataHubPolicyInfo",
  "namespace": "com.linkedin.policy",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "displayName",
      "doc": "Display name of the Policy"
    },
    {
      "Searchable": {
        "fieldType": "TEXT"
      },
      "type": "string",
      "name": "description",
      "doc": "Description of the Policy"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "type",
      "doc": "The type of policy"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "state",
      "doc": "The state of policy, ACTIVE or INACTIVE"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataHubResourceFilter",
          "namespace": "com.linkedin.policy",
          "fields": [
            {
              "deprecated": true,
              "type": [
                "null",
                "string"
              ],
              "name": "type",
              "default": null,
              "doc": "The type of resource that the policy applies to. This will most often be a data asset entity name, for\nexample 'dataset'. It is not strictly required because in the future we will want to support filtering a resource\nby domain, as well."
            },
            {
              "deprecated": true,
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "resources",
              "default": null,
              "doc": "A specific set of resources to apply the policy to, e.g. asset urns"
            },
            {
              "deprecated": true,
              "type": "boolean",
              "name": "allResources",
              "default": false,
              "doc": "Whether the policy should be applied to all assets matching the filter."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "PolicyMatchFilter",
                  "namespace": "com.linkedin.policy",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "PolicyMatchCriterion",
                          "namespace": "com.linkedin.policy",
                          "fields": [
                            {
                              "type": "string",
                              "name": "field",
                              "doc": "The name of the field that the criterion refers to"
                            },
                            {
                              "type": {
                                "type": "array",
                                "items": "string"
                              },
                              "name": "values",
                              "doc": "Values. Matches criterion if any one of the values matches condition (OR-relationship)"
                            },
                            {
                              "type": {
                                "type": "enum",
                                "symbolDocs": {
                                  "EQUALS": "Whether the field matches the value",
                                  "NOT_EQUALS": "Whether the field does not match the value",
                                  "STARTS_WITH": "Whether the field value starts with the value"
                                },
                                "name": "PolicyMatchCondition",
                                "namespace": "com.linkedin.policy",
                                "symbols": [
                                  "EQUALS",
                                  "STARTS_WITH",
                                  "NOT_EQUALS"
                                ],
                                "doc": "The matching condition in a filter criterion"
                              },
                              "name": "condition",
                              "default": "EQUALS",
                              "doc": "The condition for the criterion"
                            }
                          ],
                          "doc": "A criterion for matching a field with given value"
                        }
                      },
                      "name": "criteria",
                      "doc": "A list of criteria to apply conjunctively (so all criteria must pass)"
                    }
                  ],
                  "doc": "The filter for specifying the resource or actor to apply privileges to"
                }
              ],
              "name": "filter",
              "default": null,
              "doc": "Filter to apply privileges to"
            },
            {
              "type": [
                "null",
                "com.linkedin.policy.PolicyMatchFilter"
              ],
              "name": "privilegeConstraints",
              "default": null,
              "doc": "Constraints around what sub-resources operations are allowed to modify, i.e. NOT_EQUALS - cannot modify a particular defined tag, EQUALS - can only modify a particular defined tag, STARTS_WITH - can only modify a tag starting with xyz"
            }
          ],
          "doc": "Information used to filter DataHub resource."
        }
      ],
      "name": "resources",
      "default": null,
      "doc": "The resource that the policy applies to. Not required for some 'Platform' privileges."
    },
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "privileges",
      "doc": "The privileges that the policy grants."
    },
    {
      "type": {
        "type": "record",
        "name": "DataHubActorFilter",
        "namespace": "com.linkedin.policy",
        "fields": [
          {
            "Searchable": {
              "/*": {
                "fieldType": "URN"
              }
            },
            "type": [
              "null",
              {
                "type": "array",
                "items": "string"
              }
            ],
            "name": "users",
            "default": null,
            "doc": "A specific set of users to apply the policy to (disjunctive)"
          },
          {
            "Searchable": {
              "/*": {
                "fieldType": "URN"
              }
            },
            "type": [
              "null",
              {
                "type": "array",
                "items": "string"
              }
            ],
            "name": "groups",
            "default": null,
            "doc": "A specific set of groups to apply the policy to (disjunctive)"
          },
          {
            "type": "boolean",
            "name": "resourceOwners",
            "default": false,
            "doc": "Whether the filter should return true for owners of a particular resource.\nOnly applies to policies of type 'Metadata', which have a resource associated with them."
          },
          {
            "type": [
              "null",
              {
                "type": "array",
                "items": "string"
              }
            ],
            "name": "resourceOwnersTypes",
            "default": null,
            "doc": "Define type of ownership for the policy"
          },
          {
            "Searchable": {
              "fieldType": "BOOLEAN"
            },
            "type": "boolean",
            "name": "allUsers",
            "default": false,
            "doc": "Whether the filter should apply to all users."
          },
          {
            "Searchable": {
              "fieldType": "BOOLEAN"
            },
            "type": "boolean",
            "name": "allGroups",
            "default": false,
            "doc": "Whether the filter should apply to all groups."
          },
          {
            "Relationship": {
              "/*": {
                "entityTypes": [
                  "dataHubRole"
                ],
                "name": "IsAssociatedWithRole"
              }
            },
            "Searchable": {
              "/*": {
                "fieldType": "URN"
              }
            },
            "type": [
              "null",
              {
                "type": "array",
                "items": "string"
              }
            ],
            "name": "roles",
            "default": null,
            "doc": "A specific set of roles to apply the policy to (disjunctive)."
          }
        ],
        "doc": "Information used to filter DataHub actors."
      },
      "name": "actors",
      "doc": "The actors that the policy applies to."
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "editable",
      "default": true,
      "doc": "Whether the policy should be editable via the UI"
    },
    {
      "Searchable": {
        "fieldType": "DATETIME"
      },
      "type": [
        "null",
        "long"
      ],
      "name": "lastUpdatedTimestamp",
      "default": null,
      "doc": "Timestamp when the policy was last updated"
    }
  ],
  "doc": "Information about a DataHub (UI) access policy."
}
```





### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- IsAssociatedWithRole

   - DataHubRole via `dataHubPolicyInfo.actors.roles`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
