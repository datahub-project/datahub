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
2. **Business Owner** (`__system__business_owner`): Principal stakeholders or domain experts associated with the asset(s)
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


**Python SDK: Create a custom ownership type**

```python
# Inlined from /metadata-ingestion/examples/library/ownership_type_create_custom.py
import os
import time

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnershipTypeInfoClass,
    OwnershipTypeKeyClass,
)

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

ownership_type_id = "data_quality_lead"
ownership_type_urn = f"urn:li:ownershipType:{ownership_type_id}"

current_timestamp = int(time.time() * 1000)
actor_urn = make_user_urn("datahub")

# Emit the key aspect
ownership_type_key = OwnershipTypeKeyClass(id=ownership_type_id)
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=ownership_type_urn,
        aspect=ownership_type_key,
    )
)

# Emit the info aspect
ownership_type_info = OwnershipTypeInfoClass(
    name="Data Quality Lead",
    description="Responsible for ensuring data quality standards and monitoring data quality metrics",
    created=AuditStampClass(time=current_timestamp, actor=actor_urn),
    lastModified=AuditStampClass(time=current_timestamp, actor=actor_urn),
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=ownership_type_urn,
        aspect=ownership_type_info,
    )
)

print(f"Created custom ownership type: {ownership_type_urn}")

```



### Use Custom Ownership Type with Assets


**Python SDK: Assign owner with custom ownership type**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_owner_custom_type.py
# Inlined from /metadata-ingestion/examples/library/dataset_add_owner_custom_type.py

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_ownership_type_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

# Create DataHub client
graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter("http://localhost:8080")

# Create dataset URN
dataset_urn = make_dataset_urn(platform="snowflake", name="analytics.users", env="PROD")

# Create custom ownership type URN
# This should reference a previously created custom ownership type
custom_ownership_type_urn = make_ownership_type_urn("data_quality_lead")

# Create an owner with the custom ownership type
owner = OwnerClass(
    owner=make_user_urn("jdoe"),
    type=OwnershipTypeClass.CUSTOM,  # Use CUSTOM enum for custom types
    typeUrn=custom_ownership_type_urn,  # Reference the custom ownership type entity
)

# Get existing ownership or create new
try:
    existing_ownership = graph.get_aspect(dataset_urn, OwnershipClass)
    if existing_ownership:
        # Add to existing owners
        existing_ownership.owners.append(owner)
        ownership = existing_ownership
    else:
        # Create new ownership aspect
        ownership = OwnershipClass(owners=[owner])
except Exception:
    # Create new ownership aspect if retrieval fails
    ownership = OwnershipClass(owners=[owner])

# Emit the ownership aspect
mcp = MetadataChangeProposalWrapper(
    entityUrn=str(dataset_urn),
    aspect=ownership,
)

emitter.emit_mcp(mcp)

print(
    f"Added owner {owner.owner} with custom ownership type {custom_ownership_type_urn}"
)
print(f"to dataset {dataset_urn}")

```



### List Ownership Types


**Python SDK: Query all ownership types**

```python
# Inlined from /metadata-ingestion/examples/library/ownership_type_list.py
# Inlined from /metadata-ingestion/examples/library/ownership_type_list.py

from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

# Create DataHub client
graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Search for all ownership type entities
# Note: The GraphQL API provides a listOwnershipTypes query, but we can also
# use the search API to find all ownership types
search_query = """
query listOwnershipTypes($input: ListOwnershipTypesInput!) {
  listOwnershipTypes(input: $input) {
    start
    count
    total
    ownershipTypes {
      urn
      type
      info {
        name
        description
      }
    }
  }
}
"""

variables = {
    "input": {
        "start": 0,
        "count": 100,  # Adjust as needed
    }
}

# Execute the GraphQL query
result = graph.execute_graphql(query=search_query, variables=variables)

# Process and display the results
if result and "listOwnershipTypes" in result:
    ownership_types = result["listOwnershipTypes"]["ownershipTypes"]
    total = result["listOwnershipTypes"]["total"]

    print(f"Found {total} ownership types:")
    print("-" * 80)

    for ownership_type in ownership_types:
        urn = ownership_type["urn"]
        name = ownership_type["info"]["name"]
        description = ownership_type["info"].get("description", "No description")

        print(f"URN: {urn}")
        print(f"Name: {name}")
        print(f"Description: {description}")
        print("-" * 80)
else:
    print("No ownership types found or query failed")

```



### Query via REST API


**Fetch ownership type via REST API**

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

#### ownershipTypeInfo
Information about an ownership type



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the Ownership Type | Searchable |
| description | string |  | Description of the Ownership Type |  |
| created | [AuditStamp](#auditstamp) | ✓ | Audit stamp capturing the time and actor who created the Ownership Type. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp capturing the time and actor who last modified the Ownership Type. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownershipTypeInfo"
  },
  "name": "OwnershipTypeInfo",
  "namespace": "com.linkedin.ownership",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the Ownership Type"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the Ownership Type"
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
      "doc": "Audit stamp capturing the time and actor who created the Ownership Type."
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
      "doc": "Audit stamp capturing the time and actor who last modified the Ownership Type."
    }
  ],
  "doc": "Information about an ownership type"
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

#### Incoming
These are the relationships stored in other entity's aspects
- ownershipType

   - Dataset via `ownership.owners.typeUrn`
   - DataJob via `ownership.owners.typeUrn`
   - DataFlow via `ownership.owners.typeUrn`
   - DataProcess via `ownership.owners.typeUrn`
   - Chart via `ownership.owners.typeUrn`
   - Dashboard via `ownership.owners.typeUrn`
   - Notebook via `ownership.owners.typeUrn`
   - CorpGroup via `ownership.owners.typeUrn`
   - Domain via `ownership.owners.typeUrn`
   - Container via `ownership.owners.typeUrn`
   - Tag via `ownership.owners.typeUrn`
   - GlossaryTerm via `ownership.owners.typeUrn`
   - GlossaryNode via `ownership.owners.typeUrn`
   - Document via `ownership.owners.typeUrn`
   - DataHubIngestionSource via `ownership.owners.typeUrn`
   - Assertion via `ownership.owners.typeUrn`
   - DataPlatformInstance via `ownership.owners.typeUrn`
   - MlModel via `ownership.owners.typeUrn`
   - MlModelGroup via `ownership.owners.typeUrn`
   - MlModelDeployment via `ownership.owners.typeUrn`
   - MlFeatureTable via `ownership.owners.typeUrn`
   - MlFeature via `ownership.owners.typeUrn`
   - MlPrimaryKey via `ownership.owners.typeUrn`
   - ErModelRelationship via `ownership.owners.typeUrn`
   - DataProduct via `ownership.owners.typeUrn`
   - Application via `ownership.owners.typeUrn`
   - SemanticModel via `ownership.owners.typeUrn`
   - Metric via `ownership.owners.typeUrn`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
