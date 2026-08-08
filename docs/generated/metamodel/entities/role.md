# Role

The role entity represents external access management roles from source systems (e.g., Snowflake, BigQuery) that control access to data assets. This entity enables DataHub to model and display which roles provide access to datasets, helping data consumers understand what permissions they need to access specific data resources.

## Identity

Roles are identified by a single piece of information:

- A unique identifier for the role: This is typically derived from the external IAM system where the role is defined. The identifier should be stable and unique across your organization's data platforms.

An example of a role identifier is `urn:li:role:snowflake_reader_role`.

## Important Capabilities

### Role Properties

Role properties are stored in the `roleProperties` aspect and contain key information about the external access management role:

- **Name**: The display name of the role in the external system (e.g., "Snowflake Reader Role")
- **Description**: A human-readable description explaining the purpose and scope of the role
- **Type**: The access level this role provides (e.g., "READ", "WRITE", "ADMIN")
- **Request URL**: An optional link to the external system where users can request access to this role

The following code snippet shows how to create a role with properties:


**Python SDK: Create a role with properties**

```python
# Inlined from /metadata-ingestion/examples/library/role_create.py
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import RolePropertiesClass

# Create the role URN
# Role URNs follow the pattern: urn:li:role:{role_id}
role_urn = "urn:li:role:snowflake_reader_role"

# Define the role properties
role_properties = RolePropertiesClass(
    name="Snowflake Reader Role",
    description="Provides read-only access to analytics datasets in Snowflake",
    type="READ",
    requestUrl="https://mycompany.okta.com/access/request/snowflake-reader",
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    entityUrn=role_urn,
    aspect=role_properties,
)

# Emit the metadata change
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)
emitter.emit(mcp)
print(f"Created role: {role_urn}")

```



### Role Membership (Actors)

Roles can be assigned to users and groups through the `actors` aspect. This tracks which users (corpuser entities) and groups (corpGroup entities) have been provisioned with the role in the external system.

The actors aspect contains:

- **Users**: A list of corp users who have been granted this role
- **Groups**: A list of corp groups that have been assigned this role

The following code snippet shows how to assign users and groups to a role:


**Python SDK: Assign users and groups to a role**

```python
# Inlined from /metadata-ingestion/examples/library/role_assign_actors.py
from datahub.emitter.mce_builder import make_group_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ActorsClass,
    RoleGroupClass,
    RoleUserClass,
)

# Create the role URN
# Role URNs follow the pattern: urn:li:role:{role_id}
role_urn = "urn:li:role:snowflake_reader_role"

# Define the users and groups assigned to this role
actors = ActorsClass(
    users=[
        RoleUserClass(user=make_user_urn("john.doe")),
        RoleUserClass(user=make_user_urn("jane.smith")),
    ],
    groups=[
        RoleGroupClass(group=make_group_urn("data-analysts")),
        RoleGroupClass(group=make_group_urn("business-intelligence")),
    ],
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    entityUrn=role_urn,
    aspect=actors,
)

# Emit the metadata change
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mcp)
print(f"Assigned users and groups to role: {role_urn}")

```



### Dataset Access Integration

Roles are connected to datasets through the `access` aspect on dataset entities. This aspect lists which roles provide access to a specific dataset, creating a clear view of the access control landscape.

The following code snippet shows how to associate roles with a dataset:


**Python SDK: Associate roles with a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/role_assign_to_dataset.py
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import AccessClass, RoleAssociationClass

# Create the dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="analytics_db.public.user_events", env="PROD"
)

# Define the roles that provide access to this dataset
# Role URNs follow the pattern: urn:li:role:{role_id}
access_aspect = AccessClass(
    roles=[
        RoleAssociationClass(urn="urn:li:role:snowflake_reader_role"),
        RoleAssociationClass(urn="urn:li:role:snowflake_writer_role"),
        RoleAssociationClass(urn="urn:li:role:snowflake_admin_role"),
    ]
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=access_aspect,
)

# Emit the metadata change
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mcp)
print(f"Associated roles with dataset: {dataset_urn}")

```



### Querying Role Information

You can retrieve role information using the standard REST API endpoints. The response includes all aspects of the role entity.


**Query a role entity via REST API**

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Arole%3Asnowflake_reader_role'
```

This will return the complete role entity including:

- `roleKey`: The identity aspect
- `roleProperties`: Name, description, type, and request URL
- `actors`: Users and groups assigned to the role




**Python SDK: Query a role entity**

```python
# Inlined from /metadata-ingestion/examples/library/role_query.py
import os

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create a DataHub REST emitter
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

# Query a role entity by URN
role_urn = "urn:li:role:snowflake_reader_role"

# Get the role entity
role_entity = emitter._session.get(
    f"{emitter._gms_server}/entities/{role_urn.replace(':', '%3A').replace('(', '%28').replace(')', '%29')}"
)

if role_entity.status_code == 200:
    role_data = role_entity.json()
    print(f"Role URN: {role_data.get('urn')}")

    # Extract role properties
    if "aspects" in role_data:
        aspects = role_data["aspects"]

        # Role properties
        if "roleProperties" in aspects:
            props = aspects["roleProperties"]["value"]
            print(f"Name: {props.get('name')}")
            print(f"Description: {props.get('description')}")
            print(f"Type: {props.get('type')}")
            print(f"Request URL: {props.get('requestUrl')}")

        # Actors (users and groups)
        if "actors" in aspects:
            actors = aspects["actors"]["value"]
            if "users" in actors:
                print(f"Users: {[u['user'] for u in actors['users']]}")
            if "groups" in actors:
                print(f"Groups: {[g['group'] for g in actors['groups']]}")
else:
    print(f"Failed to retrieve role: {role_entity.status_code}")

```




**Search for roles via REST API**

```bash
curl -X POST 'http://localhost:8080/entities?action=search' \
  -H 'Content-Type: application/json' \
  -d '{
    "entity": "role",
    "input": "reader",
    "start": 0,
    "count": 10
  }'
```

This searches across role names and returns matching role URNs.



## Integration Points

### Relationship with CorpUser and CorpGroup

Roles have direct relationships with user and group entities:

- **RoleUser**: Links a role to a `corpuser` entity via the "Has" relationship
- **RoleGroup**: Links a role to a `corpGroup` entity via the "Has" relationship

These relationships enable:

- Viewing which users/groups have specific roles
- Discovering all roles assigned to a particular user or group
- Auditing access patterns across your data ecosystem

### Distinction from DataHub Roles

It's important to distinguish between the `role` entity and the `dataHubRole` entity:

- **role** (this entity): Represents external access management roles from source systems (e.g., Snowflake roles, BigQuery roles) that control access to data assets in those platforms
- **dataHubRole**: Represents roles within DataHub itself that control permissions for DataHub features (e.g., admin role, editor role)

The `roleMembership` aspect on corpuser and corpGroup entities refers to `dataHubRole` entities, not the external `role` entities documented here.

### Usage Patterns

Common usage patterns for the role entity include:

1. **Access Discovery**: Data consumers can view which roles provide access to datasets they need
2. **Self-Service Access Requests**: Users can identify the appropriate role and request access via the provided request URL
3. **Access Auditing**: Compliance teams can track which roles provide access to sensitive datasets
4. **Unified Access View**: Platform teams can create a centralized view of access control across multiple data platforms

### GraphQL API

The role entity is exposed through DataHub's GraphQL API with the `Role` type. Key resolvers include:

- `RoleType`: Provides search and batch load capabilities for role entities
- `ListRolesResolver`: Queries all roles in the system
- `BatchAssignRoleResolver`: Bulk assignment of roles to users/groups
- `AcceptRoleResolver`: Workflow for accepting role assignments

## Notable Exceptions

### Limited Entity Support

Currently, the role entity and access management features only support **dataset entities**. While roles conceptually could apply to other data assets (dashboards, charts, etc.), the `access` aspect is currently only defined for datasets.

Future enhancements may extend role-based access management to additional entity types.

### External System Integration

The role entity is designed to represent roles that exist in external systems. DataHub does not create or manage these roles directly - it only models them for discovery and documentation purposes. The actual provisioning and de-provisioning of role memberships must be performed in the source systems.

### Configuration Required

The Access Management UI features are disabled by default in self-hosted deployments. To enable role visualization in the UI, set the `SHOW_ACCESS_MANAGEMENT` environment variable to `true` for the `datahub-gms` service.

### Active Development

The role entity and access management features are under active development and subject to change. Planned enhancements include:

- Modeling external policies in addition to roles
- Automatic extraction of roles from sources like BigQuery, Snowflake, etc.
- Extended support for more entity types beyond datasets
- Advanced access request workflows with approval processes



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

#### roleProperties
Information about a ExternalRoleProperties



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the IAM Role in the external system | Searchable |
| description | string |  | Description of the IAM Role |  |
| type | string | ✓ | Can be READ, ADMIN, WRITE |  |
| requestUrl | string |  | Link to access external access management |  |
| created | [AuditStamp](#auditstamp) |  | Created Audit stamp |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "roleProperties"
  },
  "name": "RoleProperties",
  "namespace": "com.linkedin.role",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the IAM Role in the external system"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the IAM Role"
    },
    {
      "type": "string",
      "name": "type",
      "doc": "Can be READ, ADMIN, WRITE"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "requestUrl",
      "default": null,
      "doc": "Link to access external access management"
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
      "doc": "Created Audit stamp"
    }
  ],
  "doc": "Information about a ExternalRoleProperties"
}
```





#### actors
Provisioned users and groups of a role



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| users | RoleUser[] |  | List of provisioned users of a role |  |
| groups | RoleGroup[] |  | List of provisioned groups of a role |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "actors"
  },
  "name": "Actors",
  "namespace": "com.linkedin.role",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "RoleUser",
            "namespace": "com.linkedin.role",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "corpuser"
                  ],
                  "name": "Has"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "user",
                "doc": "Link provisioned corp user for a role"
              }
            ],
            "doc": "Provisioned users of a role"
          }
        }
      ],
      "name": "users",
      "default": null,
      "doc": "List of provisioned users of a role"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "RoleGroup",
            "namespace": "com.linkedin.role",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "corpGroup"
                  ],
                  "name": "Has"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "group",
                "doc": "Link provisioned corp group for a role"
              }
            ],
            "doc": "Provisioned groups of a role"
          }
        }
      ],
      "name": "groups",
      "default": null,
      "doc": "List of provisioned groups of a role"
    }
  ],
  "doc": "Provisioned users and groups of a role"
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
- Has

   - Corpuser via `actors.users.user`
   - CorpGroup via `actors.groups.group`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
