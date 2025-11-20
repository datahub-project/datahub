# OwnershipType

The ownershipType entity represents a custom ownership category in DataHub. Ownership Types define the roles and responsibilities that users or groups can have for data assets. While DataHub provides built-in ownership types (Technical Owner, Business Owner, Data Steward), organizations can create custom ownership types to match their specific governance models and organizational structures.

## Identity

OwnershipType entities are uniquely identified by a single field:

- **id**: A unique identifier string for the ownership type. This is typically a UUID for custom ownership types or a system-prefixed identifier for built-in types.

The URN structure follows the pattern: `urn:li:ownershipType:<id>`

Examples:

- Built-in type: `urn:li:ownershipType:__system__technical_owner`
- Custom type: `urn:li:ownershipType:8b3d78d1-a9d9-4f79-a948-10c52e3e8f9e`
- Named custom type: `urn:li:ownershipType:data_quality_lead`

## Important Capabilities

### Core Information

The `ownershipTypeInfo` aspect contains the essential metadata for an ownership type:

- **name**: Display name of the ownership type (e.g., "Data Quality Lead", "Compliance Officer")
- **description**: Detailed explanation of this ownership type's responsibilities and scope
- **created**: Audit stamp capturing when the ownership type was created and by whom
- **lastModified**: Audit stamp tracking the last modification time and actor

### Built-in vs Custom Ownership Types

DataHub ships with four built-in ownership types that are automatically created:

1. **Technical Owner** (`__system__technical_owner`): Involved in the production, maintenance, or distribution of the asset(s)
2. **Business Owner** (`__system__business_owner`): Principle stakeholders or domain experts associated with the asset(s)
3. **Data Steward** (`__system__data_steward`): Involved in governance of the asset(s)
4. **None** (`__system__none`): No ownership type specified

Built-in types have IDs prefixed with `__system__` and cannot be hard-deleted, only soft-deleted via the `status` aspect. Custom ownership types can be fully deleted and do not have this prefix restriction.

### Usage in Ownership Assignments

Ownership types are referenced in the `ownership` aspect of data assets through the `typeUrn` field of the `Owner` record. This creates a relationship between the asset owner and their specific role:

```
Owner {
  owner: urn:li:corpuser:jdoe
  typeUrn: urn:li:ownershipType:data_quality_lead
  type: CUSTOM  // deprecated field, maintained for backwards compatibility
}
```

The ownership aspect also maintains an `ownerTypes` map that groups owners by their ownership type URN, populated automatically via mutation hooks.

### Status Management

The `status` aspect controls whether an ownership type is active or soft-deleted:

- **removed**: When set to true, the ownership type is considered deleted but references are preserved

Built-in ownership types can only be soft-deleted (status.removed = true), while custom types can be fully removed from the system.

## Code Examples

### Create a Custom Ownership Type

<details>
<summary>Python SDK: Create a custom ownership type</summary>

```python
{{ inline /metadata-ingestion/examples/library/ownership_type_create_custom.py show_path_as_comment }}
```

</details>

### Use Custom Ownership Type with Assets

<details>
<summary>Python SDK: Assign owner with custom ownership type</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_owner_custom_type.py show_path_as_comment }}
```

</details>

### List Ownership Types

<details>
<summary>Python SDK: Query all ownership types</summary>

```python
{{ inline /metadata-ingestion/examples/library/ownership_type_list.py show_path_as_comment }}
```

</details>

### Query via REST API

<details>
<summary>Fetch ownership type via REST API</summary>

Retrieve a specific ownership type:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AownershipType%3A__system__technical_owner'
```

Response includes the `ownershipTypeInfo` and `status` aspects:

```json
{
  "urn": "urn:li:ownershipType:__system__technical_owner",
  "aspects": {
    "ownershipTypeInfo": {
      "value": {
        "name": "Technical Owner",
        "description": "Involved in the production, maintenance, or distribution of the asset(s).",
        "created": {
          "time": 1234567890000,
          "actor": "urn:li:corpuser:datahub"
        },
        "lastModified": {
          "time": 1234567890000,
          "actor": "urn:li:corpuser:datahub"
        }
      }
    }
  }
}
```

</details>

## Integration Points

### Relationship with Owner Aspect

The ownershipType entity has a primary relationship with the `ownership` aspect found on most data assets (datasets, dashboards, charts, etc.). The `Owner` record contains a `typeUrn` field that references an ownershipType entity:

```
@Relationship = {
  "name": "ownershipType",
  "entityTypes": [ "ownershipType" ]
}
typeUrn: optional Urn
```

This relationship enables:

- Filtering assets by ownership type in search and discovery
- Grouping owners by their roles across the organization
- Tracking who has which responsibilities for each asset

### GraphQL API Integration

The GraphQL API exposes ownership types through several resolvers:

- **CreateOwnershipTypeResolver**: Creates new custom ownership types
- **UpdateOwnershipTypeResolver**: Modifies existing ownership types
- **DeleteOwnershipTypeResolver**: Removes ownership types (soft-delete for system types)
- **ListOwnershipTypesResolver**: Returns all available ownership types

The GraphQL entity type is `CUSTOM_OWNERSHIP_TYPE` and maps to the `OwnershipTypeEntity` GraphQL type.

### Authorization

Managing ownership types requires specific authorization:

- Creating, updating, or deleting ownership types requires the `canManageOwnershipTypes` privilege
- This is typically restricted to platform administrators and governance teams

### Common Usage Patterns

1. **Organization-specific roles**: Define ownership types that match your org structure (e.g., "Product Manager", "Data Engineer", "Analytics Lead")

2. **Compliance roles**: Create types for regulatory compliance (e.g., "Privacy Officer", "Compliance Reviewer", "Audit Contact")

3. **Lifecycle roles**: Track different responsibilities through data lifecycle (e.g., "Data Producer", "Data Consumer", "Data Custodian")

4. **Domain-specific roles**: Establish ownership types for specific domains (e.g., "Marketing Data Owner", "Finance Data Steward")

## Notable Exceptions

### Backwards Compatibility

The `Owner` record contains both a deprecated `type` field (OwnershipType enum) and the newer `typeUrn` field (Urn reference to ownershipType entity). The enum-based field is maintained for backwards compatibility but should not be used for new implementations. When a custom ownership type is used, the enum field is set to `CUSTOM`.

### System Type Deletion

Built-in ownership types (those with IDs starting with `__system__`) cannot be fully deleted. The `OwnershipTypeService.deleteOwnershipType()` method will:

- Soft-delete system types by setting `status.removed = true`
- Hard-delete custom types by removing the entity entirely

This ensures that references to system types remain valid even if they are deactivated.

### Migration from Enum to Entity

Historically, ownership types were defined as a fixed enum (`OwnershipType.pdl`). The introduction of the ownershipType entity enables extensibility while maintaining compatibility. Deprecated enum values (DEVELOPER, DATAOWNER, DELEGATE, PRODUCER, CONSUMER, STAKEHOLDER) should be migrated to the appropriate system ownership types (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD).

### ID Generation

When creating custom ownership types:

- The system automatically generates a UUID for the `id` field
- Organizations can also use human-readable IDs (e.g., "data_quality_lead") for easier management
- IDs must not contain reserved characters and must be URL-safe
- The `__system__` prefix is reserved for built-in types
