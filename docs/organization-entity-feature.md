# Organization Entity Feature

## Overview

The Organization entity feature introduces multi-tenant organization support to DataHub, enabling organizations to manage their metadata in isolated tenant boundaries. This feature provides:

- **Organization Management**: Create, update, and manage organizations
- **Entity-Organization Associations**: Link any entity (datasets, users, dashboards, etc.) to organizations
- **User-Organization Membership**: Assign users to organizations for access control
- **Organization Hierarchy**: Support for parent-child organization relationships
- **Multi-Tenant Access Control**: Enforce strict isolation between organizations
- **Search Filtering**: Automatically filter search results by organization membership

## Architecture

### Metadata Model

The organization feature is built on DataHub's schema-first architecture:

#### Core Aspects

1. **OrganizationProperties** (`organizationProperties`)

   - `name`: Display name of the organization
   - `description`: Description of the organization
   - `logoUrl`: URL to organization logo
   - `externalUrl`: URL to organization's external website
   - `created`: Audit stamp for creation time

2. **OrganizationHierarchy** (`organizationHierarchy`)

   - `parent`: Optional parent organization URN
   - Enables hierarchical organization structures

3. **Organizations** (`organizations`)

   - Array of organization URNs
   - Used to associate entities with organizations
   - Supports multiple organizations per entity

4. **UserOrganizations** (`userOrganizations`)
   - Array of organization URNs
   - Defines which organizations a user belongs to
   - Used for access control and multi-tenant isolation

#### Entity Registry

The organization entity is registered in `metadata-models/src/main/resources/entity-registry.yml`:

```yaml
- name: organization
  doc: An organization (company). Serves as a tenant boundary for multi-tenant isolation.
  category: core
  keyAspect: organizationKey
  searchGroup: primary
  aspects:
    - organizationProperties
    - organizationHierarchy
    - organizations
    - ownership
    - institutionalMemory
    - status
    - globalTags
    - glossaryTerms
    - structuredProperties
    - forms
    - displayProperties
    - dataPlatformInstance
```

### Backend Components

#### GraphQL API

**Queries:**

- `organization(urn: String!)`: Get a single organization by URN
- `listOrganizations(input: ListOrganizationsInput!)`: List all organizations with pagination
- `getEntitiesByOrganization(organizationUrn: String!, entityTypes: [EntityType!], start: Int, count: Int)`: Get all entities belonging to an organization

**Mutations:**

- `createOrganization(input: CreateOrganizationInput!)`: Create a new organization
- `updateOrganization(urn: String!, input: OrganizationUpdateInput!)`: Update organization properties
- `deleteOrganization(urn: String!)`: Delete an organization
- `addEntityToOrganizations(entityUrn: String!, organizationUrns: [String!]!)`: Add an entity to organizations
- `removeEntityFromOrganizations(entityUrn: String!, organizationUrns: [String!]!)`: Remove an entity from organizations
- `setEntityOrganizations(entityUrn: String!, organizationUrns: [String!]!)`: Set (replace) organizations for an entity
- `addUserToOrganizations(userUrn: String!, organizationUrns: [String!]!)`: Add a user to organizations
- `removeUserFromOrganizations(userUrn: String!, organizationUrns: [String!]!)`: Remove a user from organizations

#### Resolvers

All resolvers are located in `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/organization/`:

- `OrganizationResolver`: Main query resolver with authorization checks
- `CreateOrganizationResolver`: Creates new organizations
- `UpdateOrganizationResolver`: Updates organization properties
- `DeleteOrganizationResolver`: Deletes organizations
- `ListOrganizationsResolver`: Lists organizations with search
- `OrganizationParentResolver`: Resolves parent organization
- `OrganizationChildrenResolver`: Resolves child organizations
- `GetEntitiesByOrganizationResolver`: Gets entities by organization
- `AddEntityToOrganizationsResolver`: Adds entities to organizations
- `RemoveEntityFromOrganizationsResolver`: Removes entities from organizations
- `SetEntityOrganizationsResolver`: Sets organizations for entities
- `AddUserToOrganizationsResolver`: Adds users to organizations
- `RemoveUserFromOrganizationsResolver`: Removes users from organizations
- `OrganizationAuthUtils`: Authorization utility functions

#### Access Control

**OrganizationAuthUtils** provides authorization checks:

- `canCreateOrganization(context)`: Check if user can create organizations
- `canManageOrganization(context, organizationUrn)`: Check if user can manage an organization
- `canManageOrganizationMembers(context, organizationUrn)`: Check if user can manage organization members
- `canViewOrganization(context, organizationUrn)`: Check if user can view an organization

**Multi-Tenant Isolation:**

1. **OrganizationFilterRewriter**: Filters search queries to only return entities from user's organizations

   - System actors (`urn:li:corpuser:datahub`, `urn:li:corpuser:__datahub_system`) bypass filtering
   - Users without organizations see no entities (full isolation)
   - Users with organizations see entities from ANY of their organizations

2. **OrganizationResolver**: Enforces authorization on direct organization queries
   - Checks if user belongs to the organization before allowing access
   - System actors bypass checks

### Frontend Components

#### Organization Entity Pages

- `OrganizationEntity.tsx` / `OrganizationEntityV2.tsx`: Main entity definition
- `OrganizationProfile.tsx`: Organization profile page
- `OrganizationsList.tsx`: List view of all organizations

#### Organization Management UI

- `CreateOrganizationModal.tsx`: Create new organizations
- `EditOrganizationDetailsModal.tsx`: Edit organization properties
- `OrganizationEditButton.tsx`: Edit button component
- `OrganizationPicker.tsx`: Organization selection component
- `AddOrganizationMembersModal.tsx`: Add users to organizations
- `OrganizationMembers.tsx`: Display organization members
- `OrganizationEntitiesTab.tsx`: Display entities in organization
- `OrganizationDocumentationTab.tsx`: Organization documentation

#### Sidebar Integration

- `SidebarOrganizationSection.tsx`: Sidebar section for entity organizations
- `EditOrganizationModal.tsx`: Modal for editing entity organizations

## Usage Examples

### Creating an Organization

```graphql
mutation {
  createOrganization(
    input: {
      id: "engineering"
      name: "Engineering Team"
      description: "The engineering organization"
      logoUrl: "https://example.com/logo.png"
      parentUrn: "urn:li:organization:parent-org"
    }
  )
}
```

### Adding a User to Organizations

```graphql
mutation {
  addUserToOrganizations(
    userUrn: "urn:li:corpuser:john"
    organizationUrns: [
      "urn:li:organization:engineering"
      "urn:li:organization:data-platform"
    ]
  )
}
```

### Adding an Entity to Organizations

```graphql
mutation {
  addEntityToOrganizations(
    entityUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,sample_table,PROD)"
    organizationUrns: ["urn:li:organization:engineering"]
  )
}
```

### Querying Organizations

```graphql
query {
  organization(urn: "urn:li:organization:engineering") {
    urn
    properties {
      name
      description
      logoUrl
    }
    parent {
      urn
      properties {
        name
      }
    }
    children {
      urn
      properties {
        name
      }
    }
  }
}
```

### Getting Entities by Organization

```graphql
query {
  getEntitiesByOrganization(
    organizationUrn: "urn:li:organization:engineering"
    entityTypes: [DATASET, DASHBOARD]
    start: 0
    count: 20
  ) {
    entities {
      urn
      type
    }
    total
  }
}
```

## Access Control

### Multi-Tenant Isolation

The organization feature enforces strict multi-tenant isolation:

1. **Search Filtering**: All search queries are automatically filtered to only return entities from the user's organizations
2. **Direct Queries**: Users can only view organizations they belong to
3. **Entity Access**: Users can only see entities that belong to their organizations

### System Actors

System actors bypass all organization checks:

- `urn:li:corpuser:datahub`: DataHub super user
- `urn:li:corpuser:__datahub_system`: DataHub system service

### Authorization Levels

1. **View**: User must belong to the organization
2. **Manage**: User must have MANAGE privilege on the organization entity
3. **Create**: User must have CREATE privilege on organization entity type

## Entity Support

The `organizations` aspect is supported on the following entity types:

- `dataset`
- `corpUser` (via `userOrganizations` aspect)
- `corpGroup`
- `domain`
- `dashboard`
- `chart`
- `dataFlow`
- `dataJob`
- `mlModel`
- `mlModelGroup`
- `mlFeatureTable`
- `mlFeature`
- `mlPrimaryKey`
- `container`
- `tag`
- `glossaryTerm`
- `glossaryNode`
- `dataProduct`

## Testing

### Access Control Test Script

A test script is provided at `test_access_control.sh` that verifies:

1. Organization creation by admin
2. Organization visibility to admin
3. Organization isolation (test user cannot see organization)
4. Adding user to organization
5. Organization visibility after membership
6. Authorization checks (user cannot manage organization)

Run the test:

```bash
./test_access_control.sh
```

## Migration Guide

### For Existing Deployments

1. **No Breaking Changes**: The organization feature is additive and doesn't break existing functionality
2. **Default Behavior**: Users without organizations assigned will see no entities (full isolation)
3. **System Actors**: The default `datahub` user is recognized as a system actor and bypasses all checks

### Recommended Migration Steps

1. **Create Organizations**: Create organizations for your tenant boundaries
2. **Assign Users**: Add users to their respective organizations using `addUserToOrganizations`
3. **Assign Entities**: Optionally assign existing entities to organizations
4. **Verify Access**: Test that users can only see entities from their organizations

## Implementation Details

### Search Integration

The `OrganizationFilterRewriter` is automatically registered as a Spring bean and integrated into the search query pipeline. It:

1. Extracts the authenticated user from the operation context
2. Fetches the user's organizations from the `UserOrganizations` aspect
3. Applies a filter to search queries to only return entities from user's organizations
4. Bypasses filtering for system actors

### GraphQL Data Fetchers

Organization properties are loaded using a custom data fetcher (`LoadOrganizationPropertiesResolver`) that:

1. Extracts organization URNs from the parent entity
2. Batch loads organization entities
3. Maps organization properties using `OrganizationMapper`

### Frontend Integration

The frontend integrates organizations into:

1. **Entity Sidebars**: Organizations section shows on entity profile pages
2. **Organization Picker**: Used in search select actions to filter by organization
3. **Organization Pages**: Dedicated pages for viewing and managing organizations
4. **User Management**: UI for adding/removing users from organizations

## API Reference

### GraphQL Schema

See `datahub-graphql-core/src/main/resources/entity.graphql` and `datahub-graphql-core/src/main/resources/organization.graphql` for complete GraphQL schema definitions.

### REST API

Organization support is available through the standard entity REST API endpoints:

- `GET /entities/v2/{urn}`: Get organization entity
- `POST /entities/v2`: Create organization entity
- `POST /aspects`: Update organization aspects

## Troubleshooting

### Users Cannot See Any Entities

**Problem**: After implementing organizations, users see no entities.

**Solution**:

1. Verify users have organizations assigned via `userOrganizations` aspect
2. Verify entities have organizations assigned via `organizations` aspect
3. Check that the user is not a system actor (should see all entities)

### Organization Not Visible

**Problem**: User cannot view an organization they should have access to.

**Solution**:

1. Verify user is added to the organization using `addUserToOrganizations`
2. Check organization URN is correct
3. Verify `OrganizationAuthUtils.canViewOrganization` is working correctly

### Search Returns No Results

**Problem**: Search queries return no results after organization filtering.

**Solution**:

1. Verify `OrganizationFilterRewriter` is registered in Spring context
2. Check user has organizations assigned
3. Verify entities have organizations assigned
4. Check Elasticsearch/OpenSearch index has `organizations.keyword` field

## Future Enhancements

Potential future improvements:

1. **Organization Roles**: Role-based permissions within organizations
2. **Organization Settings**: Per-organization configuration
3. **Organization Analytics**: Usage analytics per organization
4. **Bulk Operations**: Bulk assign entities/users to organizations
5. **Organization Templates**: Pre-configured organization setups

## Related Documentation

- [DataHub Concepts](what-is-datahub/datahub-concepts.md)
- [Metadata Model](../modeling/metadata-model.md)
- [Access Policies](../authorization/access-policies-guide.md)
- [GraphQL API](../api/graphql/getting-started.md)
