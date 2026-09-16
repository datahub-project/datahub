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

<details>
<summary>Python SDK: Create a role with properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/role_create.py show_path_as_comment }}
```

</details>

### Role Membership (Actors)

Roles can be assigned to users and groups through the `actors` aspect. This tracks which users (corpuser entities) and groups (corpGroup entities) have been provisioned with the role in the external system.

The actors aspect contains:

- **Users**: A list of corp users who have been granted this role
- **Groups**: A list of corp groups that have been assigned this role

The following code snippet shows how to assign users and groups to a role:

<details>
<summary>Python SDK: Assign users and groups to a role</summary>

```python
{{ inline /metadata-ingestion/examples/library/role_assign_actors.py show_path_as_comment }}
```

</details>

### Dataset Access Integration

Roles are connected to datasets through the `access` aspect on dataset entities. This aspect lists which roles provide access to a specific dataset, creating a clear view of the access control landscape.

The following code snippet shows how to associate roles with a dataset:

<details>
<summary>Python SDK: Associate roles with a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/role_assign_to_dataset.py show_path_as_comment }}
```

</details>

### Querying Role Information

You can retrieve role information using the standard REST API endpoints. The response includes all aspects of the role entity.

<details>
<summary>Query a role entity via REST API</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Arole%3Asnowflake_reader_role'
```

This will return the complete role entity including:

- `roleKey`: The identity aspect
- `roleProperties`: Name, description, type, and request URL
- `actors`: Users and groups assigned to the role

</details>

<details>
<summary>Python SDK: Query a role entity</summary>

```python
{{ inline /metadata-ingestion/examples/library/role_query.py show_path_as_comment }}
```

</details>

<details>
<summary>Search for roles via REST API</summary>

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

</details>

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
