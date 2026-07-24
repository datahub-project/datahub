# CorpGroup

The corpGroup entity represents organizational groups, teams, or departments within an enterprise. These groups can be synchronized from external identity providers like LDAP, Active Directory, or SAML/SSO systems, or created natively within DataHub. CorpGroups are essential for managing access control, ownership assignments, and organizational metadata in DataHub.

## Identity

CorpGroups are uniquely identified by a single string field: the group name.

The URN structure for a corpGroup is:

```
urn:li:corpGroup:<encoded-group-name>
```

The `<encoded-group-name>` is a URL-encoded version of the group name that serves as a globally unique identifier within DataHub. The encoding is handled automatically by the SDK.

### Examples

Here are some typical URN patterns for different group naming conventions:

```
urn:li:corpGroup:eng-team
urn:li:corpGroup:data-platform
urn:li:corpGroup:cn%3Dadmins%2Cou%3Dgroups%2Cdc%3Dexample%2Cdc%3Dcom  # LDAP DN
urn:li:corpGroup:S-1-5-21-123456789-123456789-123456789-1234          # Active Directory SID
urn:li:corpGroup:marketing-team
```

The name field is searchable and supports autocomplete, making it easy to find groups across DataHub.

## Important Capabilities

### Group Information

Group information is stored in two aspects:

- **corpGroupInfo**: Contains metadata typically synchronized from external identity providers (LDAP, AD, SSO)
- **corpGroupEditableInfo**: Contains metadata that can be edited through the DataHub UI

#### CorpGroupInfo

This aspect stores the source-of-truth information from external systems:

- **displayName**: The human-readable name of the group
- **email**: Contact email for the group
- **description**: A description of the group's purpose and scope
- **slack**: Slack channel associated with the group
- **created**: Timestamp of when the group was created

**Note**: The `admins`, `members`, and `groups` fields in corpGroupInfo are deprecated and maintained only for backwards compatibility. Group membership is now managed through the GroupMembership aspect.

#### CorpGroupEditableInfo

This aspect stores information that can be edited in the DataHub UI:

- **description**: An editable description of the group
- **pictureLink**: URL to a profile picture for the group
- **slack**: Slack channel for the group
- **email**: Contact email for the group

When both aspects contain the same field (like description), the UI typically prioritizes the editable version for display.

### Group Membership

Group membership is managed through the **groupMembership** aspect, which is attached to corpUser entities (not the group itself). This design allows for efficient queries of which groups a user belongs to.

To add a user to a group, you update the groupMembership aspect on the user entity to include the group's URN.

### Origin Tracking

The **origin** aspect tracks where a group originated from:

- **NATIVE**: The group was created directly in DataHub
- **EXTERNAL**: The group was synchronized from an external identity provider

For external groups, the `externalType` field can specify the source system (e.g., "LDAP", "AzureAD", "Okta").

### Ownership

Groups can have owners assigned through the standard **ownership** aspect. Owners are typically administrators or managers responsible for the group. Ownership types include TECHNICAL_OWNER, BUSINESS_OWNER, and others.

### Tags and Properties

Like other entities in DataHub, groups support:

- **globalTags**: Tagging groups for organization and discovery
- **structuredProperties**: Custom metadata properties defined by your organization
- **forms**: Metadata forms for structured data collection

## Code Examples

### Create a CorpGroup


**Python SDK: Create a group and emit to DataHub**

```python
# Inlined from /metadata-ingestion/examples/library/corpgroup_create.py
# metadata-ingestion/examples/library/corpgroup_create.py
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    OriginClass,
    OriginTypeClass,
    StatusClass,
)
from datahub.utilities.urns.corp_group_urn import CorpGroupUrn

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

group_urn = CorpGroupUrn("data-engineering")

group_info = CorpGroupInfoClass(
    displayName="Data Engineering",
    description="The data engineering team builds and maintains data pipelines and infrastructure",
    email="data-eng@example.com",
    slack="data-engineering",
    admins=[],
    members=[],
    groups=[],
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=group_info,
)
emitter.emit(metadata_event)

status_aspect = StatusClass(removed=False)
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=status_aspect,
)
emitter.emit(metadata_event)

origin_aspect = OriginClass(type=OriginTypeClass.NATIVE)
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=str(group_urn),
    aspect=origin_aspect,
)
emitter.emit(metadata_event)

print(f"Created group: {group_urn}")

```



### Add Members to a Group


**Python SDK: Add members to an existing group**

```python
# Inlined from /metadata-ingestion/examples/library/corpgroup_add_members.py
# metadata-ingestion/examples/library/corpgroup_add_members.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GroupMembershipClass
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

group_urn = str(CorpGroupUrn("data-engineering"))

users_to_add = [
    CorpUserUrn("jdoe"),
    CorpUserUrn("asmith"),
    CorpUserUrn("bwilliams"),
]

for user_urn in users_to_add:
    user_urn_str = str(user_urn)

    current_membership = graph.get_aspect(user_urn_str, GroupMembershipClass)

    if current_membership is None:
        current_membership = GroupMembershipClass(groups=[])

    if group_urn not in current_membership.groups:
        current_membership.groups.append(group_urn)

        metadata_event = MetadataChangeProposalWrapper(
            entityUrn=user_urn_str,
            aspect=current_membership,
        )
        emitter.emit(metadata_event)

        print(f"Added {user_urn_str} to group {group_urn}")
    else:
        print(f"{user_urn_str} is already a member of {group_urn}")

```



### Update Group Information


**Python SDK: Update group description and metadata**

```python
# Inlined from /metadata-ingestion/examples/library/corpgroup_update_info.py
# metadata-ingestion/examples/library/corpgroup_update_info.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

group_urn = str(CorpGroupUrn("data-engineering"))

editable_info = CorpGroupEditableInfoClass(
    description="Updated description: The data engineering team builds and maintains data pipelines, infrastructure, and ensures data quality across the organization",
    pictureLink="https://example.com/images/data-engineering-logo.png",
    slack="data-engineering",
    email="data-eng@example.com",
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=group_urn,
    aspect=editable_info,
)
emitter.emit(metadata_event)

print(f"Updated editable info for group: {group_urn}")

admin_urn = str(CorpUserUrn("jdoe"))

ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=admin_urn,
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ]
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=group_urn,
    aspect=ownership,
)
emitter.emit(metadata_event)

print(f"Added {admin_urn} as owner of group: {group_urn}")

```



### Query Groups via REST API


**Fetch a group entity and its membership**

To retrieve a group entity with all its aspects:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AcorpGroup%3Aeng-team'
```

To find all users who are members of a specific group:

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AcorpGroup%3Aeng-team&types=IsMemberOfGroup'
```

The response will include all corpUser entities that have the group in their groupMembership aspect:

```json
{
  "start": 0,
  "count": 3,
  "relationships": [
    {
      "type": "IsMemberOfGroup",
      "entity": "urn:li:corpuser:jdoe"
    },
    {
      "type": "IsMemberOfGroup",
      "entity": "urn:li:corpuser:asmith"
    },
    {
      "type": "IsMemberOfGroup",
      "entity": "urn:li:corpuser:bwilliams"
    }
  ],
  "total": 3
}
```



## Integration Points

### User Management

CorpGroups are tightly integrated with corpUser entities through the groupMembership aspect. When a user is added to a group, their groupMembership aspect is updated to include the group's URN, establishing a bidirectional relationship.

### Ownership Relationships

Groups can be assigned as owners of any DataHub entity (datasets, dashboards, charts, etc.) through the ownership aspect. This allows team-based ownership where all group members are considered owners.

Example ownership assignment:

```python
# A dataset can have a group as an owner
dataset.add_owner(CorpGroupUrn("data-engineering"))
```

### Access Control

While not directly stored in the corpGroup aspects, groups are a fundamental component of DataHub's RBAC (Role-Based Access Control) system. Groups can be:

- Assigned roles through the roleMembership aspect
- Referenced in DataHub policies for fine-grained access control
- Used to manage permissions for metadata operations

### External Identity Provider Integration

DataHub provides ingestion connectors for syncing groups from external systems:

#### LDAP Integration

The LDAP source connector can extract groups and their memberships:

```yaml
source:
  type: ldap
  config:
    ldap_server: "ldap://ldap.example.com"
    ldap_user: "cn=admin,dc=example,dc=com"
    ldap_password: "${LDAP_PASSWORD}"
    base_dn: "ou=groups,dc=example,dc=com"
    filter: "(objectClass=groupOfNames)"
```

Groups extracted from LDAP will have:

- The `origin` aspect set to EXTERNAL with externalType="LDAP"
- Display names and descriptions from LDAP attributes
- Group membership automatically synchronized

#### Azure AD Integration

The Azure AD source connector syncs groups from Microsoft Azure Active Directory:

```yaml
source:
  type: azure-ad
  config:
    client_id: "${AZURE_CLIENT_ID}"
    tenant_id: "${AZURE_TENANT_ID}"
    client_secret: "${AZURE_CLIENT_SECRET}"
    ingest_users: true
    ingest_groups: true
```

Azure AD groups will have:

- The `origin` aspect set to EXTERNAL with externalType="AzureAD"
- Microsoft 365 group information (if applicable)
- Nested group memberships flattened

### GraphQL API

The corpGroup entity is fully supported in DataHub's GraphQL API. Common queries include:

```graphql
query GetGroup {
  corpGroup(urn: "urn:li:corpGroup:eng-team") {
    urn
    name
    properties {
      displayName
      description
      email
    }
    ownership {
      owners {
        owner {
          ... on CorpUser {
            urn
            username
          }
        }
      }
    }
  }
}
```

## Notable Exceptions

### Deprecated Membership Fields

The `members`, `admins`, and `groups` fields in the corpGroupInfo aspect are deprecated. These fields were originally used to store group membership directly on the group entity, but this approach had scalability and consistency issues.

Current best practice is to:

1. Use the groupMembership aspect on corpUser entities to track which groups a user belongs to
2. Use the ownership aspect to designate group administrators
3. Query relationships via the REST API to find group members

### Native vs External Groups

Groups can be created in two ways:

1. **Native Groups**: Created directly in DataHub through the UI or API, with origin type NATIVE
2. **External Groups**: Synchronized from identity providers, with origin type EXTERNAL

External groups are typically treated as read-only in DataHub to prevent conflicts with the source system. Updates should be made in the source system (LDAP, Azure AD, etc.) and re-synchronized to DataHub.

### Group Name Encoding

Group names are URL-encoded in URNs to handle special characters commonly found in LDAP DNs and Active Directory paths. When using the SDK, encoding is handled automatically. However, when constructing URNs manually or in API requests, ensure proper URL encoding:

```python
# Correct - SDK handles encoding
CorpGroupUrn("cn=admins,ou=groups,dc=example,dc=com")
# Result: urn:li:corpGroup:cn%3Dadmins%2Cou%3Dgroups%2Cdc%3Dexample%2Cdc%3Dcom

# Incorrect - manual construction without encoding
"urn:li:corpGroup:cn=admins,ou=groups,dc=example,dc=com"  # Will fail
```

### Group Hierarchy

While the corpGroupInfo aspect includes a deprecated `groups` field for nested groups, DataHub does not currently have first-class support for group hierarchies. Group membership is flat - a user is either a member of a group or not. If you need hierarchical group structures, consider:

1. Flattening the hierarchy during ingestion (e.g., if a user is in a child group, add them to all parent groups)
2. Using naming conventions to indicate hierarchy (e.g., "engineering", "engineering-platform", "engineering-platform-data")
3. Using domains or tags to represent organizational structure



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

#### corpGroupKey
Key for a CorpGroup



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | The URL-encoded name of the AD/LDAP group. Serves as a globally unique identifier within DataHub. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpGroupKey"
  },
  "name": "CorpGroupKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM",
        "queryByDefault": true
      },
      "type": "string",
      "name": "name",
      "doc": "The URL-encoded name of the AD/LDAP group. Serves as a globally unique identifier within DataHub."
    }
  ],
  "doc": "Key for a CorpGroup"
}
```





#### corpGroupInfo
Information about a Corp Group ingested from a third party source



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| displayName | string |  | The name of the group. | Searchable |
| email | string |  | email of this group |  |
| admins | string[] | ✓ | owners of this group Deprecated! Replaced by Ownership aspect. | ⚠️ Deprecated, → OwnedBy |
| members | string[] | ✓ | List of ldap urn in this group. Deprecated! Replaced by GroupMembership aspect. | ⚠️ Deprecated, → IsPartOf |
| groups | string[] | ✓ | List of groups in this group. Deprecated! This field is unused. | ⚠️ Deprecated, → IsPartOf |
| description | string |  | A description of the group. | Searchable |
| slack | string |  | Slack channel for the group |  |
| created | [AuditStamp](#auditstamp) |  | Created Audit stamp | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "EntityUrns": [
      "com.linkedin.common.CorpGroupUrn"
    ],
    "name": "corpGroupInfo"
  },
  "name": "CorpGroupInfo",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "TEXT_PARTIAL",
        "queryByDefault": true,
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "The name of the group."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "email",
      "default": null,
      "doc": "email of this group"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "corpuser"
          ],
          "name": "OwnedBy"
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "admins",
      "doc": "owners of this group\nDeprecated! Replaced by Ownership aspect."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "corpuser"
          ],
          "name": "IsPartOf"
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "members",
      "doc": "List of ldap urn in this group.\nDeprecated! Replaced by GroupMembership aspect."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "corpGroup"
          ],
          "name": "IsPartOf"
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "groups",
      "doc": "List of groups in this group.\nDeprecated! This field is unused."
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "A description of the group."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "slack",
      "default": null,
      "doc": "Slack channel for the group"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdTime",
          "fieldType": "DATETIME",
          "searchLabel": "createdAt"
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
      "name": "created",
      "default": null,
      "doc": "Created Audit stamp"
    }
  ],
  "doc": "Information about a Corp Group ingested from a third party source"
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
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





#### corpGroupEditableInfo
Group information that can be edited from UI



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| description | string |  | A description of the group | Searchable (editedDescription) |
| pictureLink | string | ✓ | A URL which points to a picture which user wants to set as the photo for the group |  |
| slack | string |  | Slack channel for the group |  |
| email | string |  | Email address to contact the group |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpGroupEditableInfo"
  },
  "name": "CorpGroupEditableInfo",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "A description of the group"
    },
    {
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": "string",
      "name": "pictureLink",
      "default": "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png",
      "doc": "A URL which points to a picture which user wants to set as the photo for the group"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "slack",
      "default": null,
      "doc": "Slack channel for the group"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "email",
      "default": null,
      "doc": "Email address to contact the group"
    }
  ],
  "doc": "Group information that can be edited from UI"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
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
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### origin
Carries information about where an entity originated from.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| type | OriginType | ✓ | Where an entity originated from. Either NATIVE or EXTERNAL. |  |
| externalType | string |  | Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the ident... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "origin"
  },
  "name": "Origin",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "EXTERNAL": "The entity is external to DataHub.",
          "NATIVE": "The entity is native to DataHub."
        },
        "name": "OriginType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "NATIVE",
          "EXTERNAL"
        ],
        "doc": "Enum to define where an entity originated from."
      },
      "name": "type",
      "doc": "Where an entity originated from. Either NATIVE or EXTERNAL."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "externalType",
      "default": null,
      "doc": "Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the identity provider."
    }
  ],
  "doc": "Carries information about where an entity originated from."
}
```





#### roleMembership
Carries information about which roles a user or group is assigned to.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| roles | string[] | ✓ |  | → IsMemberOfRole |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "roleMembership"
  },
  "name": "RoleMembership",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataHubRole"
          ],
          "name": "IsMemberOfRole"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "roles"
    }
  ],
  "doc": "Carries information about which roles a user or group is assigned to."
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
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
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
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
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
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
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
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

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- IsPartOf (via `corpGroupInfo.groups`)
- OwnedBy (via `ownership.owners.owner`)
#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `corpGroupInfo.admins`
   - Corpuser via `ownership.owners.owner`
- IsPartOf

   - Corpuser via `corpGroupInfo.members`
- TaggedWith

   - Tag via `globalTags.tags`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- IsMemberOfRole

   - DataHubRole via `roleMembership.roles`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
#### Incoming
These are the relationships stored in other entity's aspects
- Has

   - Role via `actors.groups.group`
- OwnedBy

   - Dataset via `ownership.owners.owner`
   - DataJob via `ownership.owners.owner`
   - DataFlow via `ownership.owners.owner`
   - DataProcess via `ownership.owners.owner`
   - Chart via `ownership.owners.owner`
   - Dashboard via `ownership.owners.owner`
   - Notebook via `ownership.owners.owner`
- IsMemberOfGroup

   - Corpuser via `groupMembership.groups`
- IsMemberOfNativeGroup

   - Corpuser via `nativeGroupMembership.nativeGroups`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
