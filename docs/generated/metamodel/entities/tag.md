# Tag

Tags are one of the core metadata entities in DataHub, providing a flexible mechanism for classification, categorization, and organization of data assets. They represent labels that can be applied to entities such as datasets, dashboards, charts, and more, enabling users to quickly identify, filter, and group related assets across the data ecosystem.

## Identity

Tags are identified by a single piece of information:

- **The tag name**: A unique string identifier that serves as both the technical key and the human-readable reference for the tag. The name should be simple, descriptive, and typically follows lowercase naming conventions (e.g., `pii`, `deprecated`, `quarterly`).

An example of a tag identifier is `urn:li:tag:pii`.

The URN structure is straightforward:

```
urn:li:tag:<tag_name>
```

Where `<tag_name>` is the unique identifier for the tag. Unlike many other DataHub entities, tags do not require platform qualifiers or environment specifications, making them universally applicable across all data assets.

## Important Capabilities

### Tag Properties

Tags support several properties that enhance their usability and appearance in DataHub:

- **Display Name**: A human-friendly name that may differ from the technical identifier. For example, a tag with name `pii` might have display name "Personally Identifiable Information".
- **Description**: Detailed documentation explaining what the tag represents, when it should be used, and any organizational policies related to it.
- **Color**: A hex color code (e.g., `#FF0000`) that allows for visual distinction in the UI, making it easier to spot tagged assets at a glance.

These properties are stored in the `tagProperties` aspect and can be set when creating a tag or updated later.

### Applying Tags to Entities

Tags are applied to other entities through the `globalTags` aspect. Almost all core DataHub entities support tagging, including:

- **Datasets**: Tables, views, streams, and other data collections
- **Dashboards**: BI dashboards and reporting interfaces
- **Charts**: Individual visualizations and reports
- **Data Jobs**: ETL jobs, transformation pipelines
- **Data Flows**: Complete data pipelines and workflows
- **ML Models**: Machine learning models and deployments
- **Containers**: Databases, schemas, and other organizational structures
- **Glossary Terms**: Business terminology and concepts

Tags can be applied at multiple levels:

1. **Entity-level**: Applied to the entire asset (e.g., tagging a whole dataset as `sensitive`)
2. **Field-level**: Applied to specific columns or fields within datasets (e.g., tagging only the `email` column as `pii`)

### Tag vs. Glossary Terms

While both tags and glossary terms provide classification capabilities, they serve different purposes:

- **Tags** are lightweight, informal labels for quick categorization. They're ideal for operational concerns like data quality states (`needs_review`), security classifications (`confidential`), or project associations (`q4_initiative`).
- **Glossary Terms** are formal business vocabulary with rich metadata, relationships, and governance. They're best for business concepts like "Customer", "Revenue", or "Product SKU".

Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) for a detailed comparison.

### Ownership

Like other core entities, tags support the `ownership` aspect. This allows organizations to designate who is responsible for maintaining tag definitions and ensuring consistent usage. Tag owners can be users or groups with various ownership types (e.g., `DATAOWNER`, `STEWARD`).

### Deprecation and Status

Tags can be marked as deprecated through the `deprecation` aspect, signaling that they should no longer be used. The `status` aspect allows tags to be soft-deleted while maintaining historical references.

## Code Examples

### Creating a Tag


**Python SDK: Create a basic tag**

```python
# Inlined from /metadata-ingestion/examples/library/tag_create_basic.py
# metadata-ingestion/examples/library/tag_create_basic.py
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a basic tag with properties
tag = Tag(
    name="pii",
    display_name="Personally Identifiable Information",
    description="This tag indicates that the asset contains PII data and should be handled according to data privacy regulations.",
    color="#FF0000",
)

# Upsert the tag
client.entities.upsert(tag)

print(f"Created tag: {tag.urn}")

```



### Adding Ownership to a Tag


**Python SDK: Add an owner to a tag**

```python
# Inlined from /metadata-ingestion/examples/library/tag_add_ownership.py
# metadata-ingestion/examples/library/tag_add_ownership.py
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a tag with ownership
tag = Tag(
    name="data_quality",
    owners=[
        CorpUserUrn("data_steward"),
    ],
)

# Upsert the tag
client.entities.upsert(tag)

print(f"Created tag with ownership: {tag.urn}")

```



### Applying Tags to Datasets


**Python SDK: Apply a tag to a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/tag_apply_to_dataset.py
# metadata-ingestion/examples/library/tag_apply_to_dataset.py
from datahub.sdk import DataHubClient, Dataset, Tag

client = DataHubClient.from_env()

# Create Dataset entity
dataset = Dataset(platform="snowflake", name="db.schema.customers", env="PROD")

# Create Tag entity
tag = Tag(name="pii")

# Apply tag to dataset
dataset.add_tag(tag.urn)

# Update the dataset with the new tag
client.entities.upsert(dataset)

print(f"Applied tag {tag.urn} to dataset {dataset.urn}")

```



### Querying Tag Information

The standard REST APIs can be used to retrieve tag metadata and see which entities are tagged.


**REST API: Fetch tag entity information**

```python
# Inlined from /metadata-ingestion/examples/library/tag_query_rest.py
# metadata-ingestion/examples/library/tag_query_rest.py
import logging
from urllib.parse import quote

import requests

from datahub.emitter.mce_builder import make_tag_urn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
gms_server = "http://localhost:8080"
tag_urn = make_tag_urn("pii")

# Fetch tag entity
response = requests.get(f"{gms_server}/entities/{quote(tag_urn, safe='')}")

if response.status_code == 200:
    tag_data = response.json()
    log.info(f"Successfully retrieved tag: {tag_urn}")

    # Extract tag properties
    if "aspects" in tag_data and "tagProperties" in tag_data["aspects"]:
        properties = tag_data["aspects"]["tagProperties"]["value"]
        log.info(f"Tag name: {properties.get('name')}")
        log.info(f"Description: {properties.get('description')}")
        log.info(f"Color: {properties.get('colorHex')}")

    # Extract ownership if present
    if "aspects" in tag_data and "ownership" in tag_data["aspects"]:
        ownership = tag_data["aspects"]["ownership"]["value"]
        log.info(f"Number of owners: {len(ownership.get('owners', []))}")
        for owner in ownership.get("owners", []):
            log.info(f"  - Owner: {owner['owner']}, Type: {owner['type']}")
else:
    log.error(f"Failed to retrieve tag: {response.status_code} - {response.text}")

# Query relationships to find all entities tagged with this tag
relationships_url = (
    f"{gms_server}/relationships"
    f"?direction=INCOMING"
    f"&urn={quote(tag_urn, safe='')}"
    f"&types=TaggedWith"
)

response = requests.get(relationships_url)

if response.status_code == 200:
    relationships = response.json()
    total = relationships.get("total", 0)
    log.info(f"Found {total} entities tagged with this tag")

    for rel in relationships.get("relationships", []):
        log.info(f"  - {rel['entity']} (type: {rel['type']})")
else:
    log.error(
        f"Failed to retrieve relationships: {response.status_code} - {response.text}"
    )

```



### Searching for Tagged Assets

Tags are fully integrated with DataHub's search capabilities, allowing you to find all assets with a specific tag.


**Python SDK: Search for assets by tag**

```python
from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient.from_env()

# Find all assets tagged with "pii"
results = client.search.get_urns(filter=F.tag("urn:li:tag:pii"))

print(f"Found {len(results)} assets tagged with 'pii'")
for urn in results:
    print(f"  - {urn}")
```



## Integration Points

### Relationship with Other Entities

Tags create a `TaggedWith` relationship between the tagged entity and the tag entity. This bidirectional relationship enables:

- **Forward navigation**: From a dataset, see all its tags
- **Reverse navigation**: From a tag, see all entities using it
- **Impact analysis**: Understand the scope of a tag before deprecating it

### GraphQL API Support

Tags are fully supported in DataHub's GraphQL API, with dedicated resolvers for:

- **Creating tags**: `CreateTagResolver` allows programmatic tag creation with authorization checks
- **Updating tags**: `SetTagColorResolver` and update operations for tag properties
- **Deleting tags**: `DeleteTagResolver` for removing obsolete tags
- **Adding tags to entities**: `AddTagResolver`, `AddTagsResolver`, and batch operations
- **Removing tags from entities**: `RemoveTagResolver` and batch removal operations

These resolvers enforce authorization policies, ensuring only users with appropriate privileges (`CREATE_TAG`, `MANAGE_TAGS`, or `EDIT_ENTITY`) can modify tags and tag assignments.

### Search and Discovery

Tags are indexed for search with the following capabilities:

- **Full-text search**: Tag names and descriptions are searchable
- **Autocomplete**: Tag names support autocomplete for easy selection
- **Filtering**: Assets can be filtered by tag in all search interfaces
- **Faceting**: Tags appear as filter options in search results

## Notable Exceptions

### Tag Naming Conventions

While DataHub doesn't enforce strict naming conventions, consider these best practices:

- **Use lowercase**: Makes tags case-insensitive in practice (`pii` vs `PII`)
- **Use underscores or hyphens**: For multi-word tags (`data_quality` or `data-quality`)
- **Keep it concise**: Short names are easier to read and apply
- **Avoid special characters**: Stick to alphanumeric characters, underscores, and hyphens

### Tag Proliferation

Organizations should establish governance around tag creation to avoid "tag sprawl":

- **Define a core set**: Start with 10-20 essential tags
- **Document usage**: Maintain clear descriptions for when each tag should be used
- **Regular audits**: Periodically review and consolidate similar or unused tags
- **Ownership model**: Assign tag owners who can approve new tags or changes

### System vs. User Tags

While DataHub doesn't formally distinguish between system and user tags, organizations often establish conventions:

- **System tags**: Created by automated processes (e.g., `ingestion_error`, `schema_drift`)
- **User tags**: Created manually by data practitioners (e.g., `important`, `sandbox`)

Consider using prefixes or namespacing to distinguish these categories if needed.

### Tags and Access Control

Tags themselves don't grant or restrict access to data. However, they can be used in conjunction with DataHub policies to:

- Control who can view certain tagged assets
- Restrict who can apply sensitive tags
- Trigger workflows based on tag presence (e.g., auto-generating documentation for assets tagged `requires_docs`)

Tags are metadata about your data, not a security mechanism. Use DataHub's authorization features for access control.



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

#### tagKey
Key for a Tag



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | The tag name, which serves as a unique id | Searchable (id) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "tagKey"
  },
  "name": "TagKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldName": "id",
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "The tag name, which serves as a unique id"
    }
  ],
  "doc": "Key for a Tag"
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





#### tagProperties
Properties associated with a Tag



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the tag | Searchable |
| description | string |  | Documentation of the tag | Searchable |
| colorHex | string |  | The color associated with the Tag in Hex. For example #FFFFFF. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "tagProperties"
  },
  "name": "TagProperties",
  "namespace": "com.linkedin.tag",
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
      "doc": "Display name of the tag"
    },
    {
      "Searchable": {},
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the tag"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "colorHex",
      "default": null,
      "doc": "The color associated with the Tag in Hex. For example #FFFFFF."
    }
  ],
  "doc": "Properties associated with a Tag"
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





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
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





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
#### Incoming
These are the relationships stored in other entity's aspects
- SchemaFieldTaggedWith

   - Dataset via `schemaMetadata.fields.globalTags`
   - Chart via `inputFields.fields.schemaField.globalTags`
   - Dashboard via `inputFields.fields.schemaField.globalTags`
- TaggedWith

   - Dataset via `schemaMetadata.fields.globalTags.tags`
   - Dataset via `editableSchemaMetadata.editableSchemaFieldInfo.globalTags.tags`
   - Dataset via `globalTags.tags`
   - DataJob via `globalTags.tags`
   - DataFlow via `globalTags.tags`
   - Chart via `globalTags.tags`
   - Chart via `inputFields.fields.schemaField.globalTags.tags`
   - Dashboard via `globalTags.tags`
   - Dashboard via `inputFields.fields.schemaField.globalTags.tags`
   - Notebook via `globalTags.tags`
   - Corpuser via `globalTags.tags`
   - CorpGroup via `globalTags.tags`
   - Container via `globalTags.tags`
- EditableSchemaFieldTaggedWith

   - Dataset via `editableSchemaMetadata.editableSchemaFieldInfo.globalTags`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
