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

<details>
<summary>Python SDK: Create a group and emit to DataHub</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpgroup_create.py show_path_as_comment }}
```

</details>

### Add Members to a Group

<details>
<summary>Python SDK: Add members to an existing group</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpgroup_add_members.py show_path_as_comment }}
```

</details>

### Update Group Information

<details>
<summary>Python SDK: Update group description and metadata</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpgroup_update_info.py show_path_as_comment }}
```

</details>

### Query Groups via REST API

<details>
<summary>Fetch a group entity and its membership</summary>

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

</details>

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
