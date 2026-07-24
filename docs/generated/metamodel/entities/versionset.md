# VersionSet

The VersionSet entity is a core metadata model entity in DataHub that groups together related versions of other entities. Version Sets are primarily used to manage versioned entities like ML models, datasets, and other assets that evolve over time with distinct versions. They provide a structured way to organize, track, and navigate between different versions of the same logical asset.

## Identity

Version Sets are identified by two pieces of information:

- **ID**: A unique identifier for the version set, typically generated from the platform and asset name using a GUID. This ensures uniqueness across all version sets in DataHub.
- **Entity Type**: The type of entities that are grouped in this version set (e.g., `mlModel`, `dataset`). All entities within a single version set must be of the same type, ensuring type safety and consistency.

An example of a version set identifier is `urn:li:versionSet:(abc123def456,mlModel)`.

The URN structure follows the pattern: `urn:li:versionSet:(<id>,<entityType>)` where:

- `<id>` is a unique identifier string, often a GUID generated from the platform and asset name
- `<entityType>` is the entity type being versioned (e.g., `mlModel`, `dataset`)

## Important Capabilities

### Version Set Properties

Version Sets maintain metadata about the collection of versioned entities through the `versionSetProperties` aspect. This aspect contains:

#### Latest Version Tracking

The version set automatically tracks which entity is currently the latest version. This is stored in the `latest` field and provides a quick reference to the most recent version without needing to query all versions.

#### Versioning Scheme

Version Sets support different versioning schemes to accommodate various versioning strategies:

- **LEXICOGRAPHIC_STRING**: Versions are sorted lexicographically as strings. This is suitable for semantic versioning (e.g., "1.0.0", "1.1.0", "2.0.0") or date-based versions.
- **ALPHANUMERIC_GENERATED_BY_DATAHUB**: DataHub generates version identifiers automatically using an 8-character alphabetical string. This is useful when the source system doesn't provide its own versioning.

The versioning scheme is static once set and determines how versions are ordered within the set.

#### Custom Properties

Like other DataHub entities, Version Sets support custom properties for storing additional metadata specific to your use case.


**Python SDK: Create a version set with properties**

```python
# Inlined from /metadata-ingestion/examples/library/version_set_add_properties.py
# metadata-ingestion/examples/library/version_set_add_properties.py
"""
Create a version set with custom properties and metadata.

This example demonstrates how to create a version set with rich metadata
including custom properties to track additional information.
"""

from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    VersioningSchemeClass,
    VersionSetPropertiesClass,
)

server = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=server)

# Generate a unique ID for the version set
guid_dict = {"platform": "tensorflow", "name": "image-classifier"}
version_set_id = datahub_guid(guid_dict)

# Create the version set URN for an ML model
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Define the latest version (this should be an existing ML model)
latest_model_urn = (
    "urn:li:mlModel:(urn:li:dataPlatform:tensorflow,image-classifier-v3,PROD)"
)

# Create version set properties with custom metadata
version_set_properties = VersionSetPropertiesClass(
    latest=latest_model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_type": "image_classification",
        "framework": "tensorflow",
        "architecture": "resnet50",
        "dataset": "imagenet",
        "owner_team": "ml-platform",
        "cost_center": "ML-001",
    },
)

# Emit the version set with properties
version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

print(f"Created version set: {version_set_urn}")
print(f"Latest version: {latest_model_urn}")
print("Custom properties:")
for key, value in version_set_properties.customProperties.items():
    print(f"  {key}: {value}")

```



### Linking Entities to Version Sets

Entities are linked to Version Sets through the `versionProperties` aspect on the versioned entity. This aspect contains:

- **versionSet**: URN of the Version Set this entity belongs to
- **version**: A version tag label that should be unique within the version set
- **sortId**: An identifier used for sorting versions according to the versioning scheme
- **aliases**: Alternative version identifiers (e.g., "latest", "stable", "v1")
- **comment**: Optional documentation about what this version represents
- **isLatest**: Boolean flag indicating if this is the latest version (automatically maintained)
- **timestamps**: Creation timestamps both from the source system and in DataHub


**Python SDK: Link an entity to a version set**

```python
# Inlined from /metadata-ingestion/examples/library/version_set_link_entity.py
# metadata-ingestion/examples/library/version_set_link_entity.py
"""
Link an existing entity to an existing version set.

This example shows how to add a new version of an entity to an existing
version set, with proper version tracking and metadata.
"""

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    MetadataAttributionClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)

server = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=server)

# Define the version set and the new model version to link
version_set_urn = "urn:li:versionSet:(abc123def456,mlModel)"
new_model_urn = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model-v2,PROD)"

# Update the version set to mark this as the latest
version_set_properties = VersionSetPropertiesClass(
    latest=new_model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
)

version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

# Create version properties for the new model
version_properties = VersionPropertiesClass(
    versionSet=version_set_urn,
    version=VersionTagClass(
        versionTag="2.0.0",
        metadataAttribution=MetadataAttributionClass(
            time=1675209600000,
            actor="urn:li:corpuser:ml-engineer",
        ),
    ),
    sortId="2.0.0",
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    comment="Major update with improved accuracy",
    aliases=[
        VersionTagClass(versionTag="v2"),
        VersionTagClass(versionTag="latest"),
    ],
    sourceCreatedTimestamp=AuditStampClass(
        time=1675209600000,
        actor="urn:li:corpuser:ml-engineer",
    ),
    metadataCreatedTimestamp=AuditStampClass(
        time=1675209600000,
        actor="urn:li:corpuser:datahub",
    ),
)

# Emit the version properties for the new model
model_version_mcp = MetadataChangeProposalWrapper(
    entityUrn=new_model_urn,
    aspect=version_properties,
)
emitter.emit(model_version_mcp)

print(f"Linked {new_model_urn} to version set {version_set_urn}")
print("Version: 2.0.0")

```



### Creating a New Version Set

When creating a new version set, you typically link the first versioned entity to it. The version set can be created implicitly by linking an entity to a new version set URN.


**Python SDK: Create a version set by linking the first entity**

```python
# Inlined from /metadata-ingestion/examples/library/version_set_create.py
# metadata-ingestion/examples/library/version_set_create.py
"""
Create a new version set by linking the first versioned entity.

This example demonstrates creating a version set for an ML model,
establishing the first version in the set.
"""

import os

from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    MetadataAttributionClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)

server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=server, token=token)

# Define the ML model URN that we want to version
model_urn = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model-v1,PROD)"

# Generate a unique ID for the version set based on the model name
guid_dict = {"platform": "sagemaker", "name": "my-model"}
version_set_id = datahub_guid(guid_dict)

# Create the version set URN
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Create version set properties aspect
version_set_properties = VersionSetPropertiesClass(
    latest=model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_family": "recommendation",
        "framework": "tensorflow",
    },
)

# Emit the version set properties
version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

# Create version properties for the ML model
version_properties = VersionPropertiesClass(
    versionSet=version_set_urn,
    version=VersionTagClass(
        versionTag="1.0.0",
        metadataAttribution=MetadataAttributionClass(
            time=1672531200000,
            actor="urn:li:corpuser:datahub",
        ),
    ),
    sortId="1.0.0",
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    comment="Initial production release",
    aliases=[
        VersionTagClass(versionTag="v1"),
        VersionTagClass(versionTag="stable"),
    ],
    metadataCreatedTimestamp=AuditStampClass(
        time=1672531200000,
        actor="urn:li:corpuser:datahub",
    ),
)

# Emit the version properties for the model
model_version_mcp = MetadataChangeProposalWrapper(
    entityUrn=model_urn,
    aspect=version_properties,
)
emitter.emit(model_version_mcp)

print(f"Created version set: {version_set_urn}")
print(f"Linked first version: {model_urn} with version 1.0.0")

```



### Managing Multiple Versions

As you create new versions of an asset, you link each one to the same version set with a different version label. The version set automatically updates the `latest` pointer to the most recent version based on the versioning scheme.


**Python SDK: Link multiple versions to a version set**

```python
# Inlined from /metadata-ingestion/examples/library/version_set_link_multiple_versions.py
# metadata-ingestion/examples/library/version_set_link_multiple_versions.py
"""
Link multiple versions of an entity to a version set.

This example demonstrates creating a complete version history for an ML model,
showing how to manage multiple versions with semantic versioning.
"""

from typing import TypedDict

from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    MetadataAttributionClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)

server = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=server)


class VersionInfo(TypedDict):
    urn: str
    version: str
    sortId: str
    comment: str
    timestamp: int
    aliases: list[str]
    actor: str


# Generate version set URN
guid_dict = {"platform": "pytorch", "name": "sentiment-analyzer"}
version_set_id = datahub_guid(guid_dict)
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Define the model versions we want to link
versions: list[VersionInfo] = [
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v1,PROD)",
        "version": "1.0.0",
        "sortId": "1.0.0",
        "comment": "Initial release with basic sentiment analysis",
        "timestamp": 1672531200000,
        "aliases": ["v1"],
        "actor": "urn:li:corpuser:data-scientist",
    },
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v1.1,PROD)",
        "version": "1.1.0",
        "sortId": "1.1.0",
        "comment": "Minor improvements to accuracy",
        "timestamp": 1675209600000,
        "aliases": ["v1.1"],
        "actor": "urn:li:corpuser:data-scientist",
    },
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v2,PROD)",
        "version": "2.0.0",
        "sortId": "2.0.0",
        "comment": "Major update with multi-language support",
        "timestamp": 1677628800000,
        "aliases": ["v2", "latest", "production"],
        "actor": "urn:li:corpuser:ml-engineer",
    },
]

# Link each version to the version set
for i, version_info in enumerate(versions):
    is_latest = i == len(versions) - 1

    # Create version properties for each model
    version_properties = VersionPropertiesClass(
        versionSet=version_set_urn,
        version=VersionTagClass(
            versionTag=version_info["version"],
            metadataAttribution=MetadataAttributionClass(
                time=version_info["timestamp"],
                actor=version_info["actor"],
            ),
        ),
        sortId=version_info["sortId"],
        versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
        comment=version_info["comment"],
        aliases=[
            VersionTagClass(versionTag=alias) for alias in version_info["aliases"]
        ],
        sourceCreatedTimestamp=AuditStampClass(
            time=version_info["timestamp"],
            actor=version_info["actor"],
        ),
        metadataCreatedTimestamp=AuditStampClass(
            time=version_info["timestamp"],
            actor="urn:li:corpuser:datahub",
        ),
    )

    # Emit version properties
    model_version_mcp = MetadataChangeProposalWrapper(
        entityUrn=version_info["urn"],
        aspect=version_properties,
    )
    emitter.emit(model_version_mcp)

    print(f"Linked version {version_info['version']}: {version_info['urn']}")

# Update version set to point to the latest version
version_set_properties = VersionSetPropertiesClass(
    latest=versions[-1]["urn"],
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_type": "sentiment_analysis",
        "language_support": "multi-language",
        "framework": "pytorch",
    },
)

version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

print(f"\nVersion set created: {version_set_urn}")
print(f"Total versions: {len(versions)}")
print(f"Latest version: {versions[-1]['version']}")

```



### Querying Version Sets and Versioned Entities

You can query version sets to retrieve information about all versions or find specific versions.


**Python SDK: Query a version set and its versions**

```python
# Inlined from /metadata-ingestion/examples/library/version_set_query.py
# metadata-ingestion/examples/library/version_set_query.py
"""
Query a version set to retrieve information about versions.

This example demonstrates how to fetch version set metadata and query
all versions using both REST API and GraphQL approaches.
"""

from urllib.parse import quote

import requests

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    VersionPropertiesClass,
    VersionSetPropertiesClass,
)

# Initialize DataHub Graph client
config = DatahubClientConfig(server="http://localhost:8080")
graph = DataHubGraph(config)

# Define the version set URN to query
version_set_urn = "urn:li:versionSet:(abc123def456,mlModel)"

# Method 1: Query using the Python SDK
print("=== Querying Version Set via Python SDK ===\n")

# Get the version set properties
version_set_props = graph.get_aspect(
    entity_urn=version_set_urn,
    aspect_type=VersionSetPropertiesClass,
)

if version_set_props:
    print(f"Version Set: {version_set_urn}")
    print(f"Latest Version: {version_set_props.latest}")
    print(f"Versioning Scheme: {version_set_props.versioningScheme}")

    if version_set_props.customProperties:
        print("\nCustom Properties:")
        for key, value in version_set_props.customProperties.items():
            print(f"  {key}: {value}")

    # Get version properties for the latest version
    print("\n=== Latest Version Details ===\n")
    latest_version_props = graph.get_aspect(
        entity_urn=version_set_props.latest,
        aspect_type=VersionPropertiesClass,
    )

    if latest_version_props:
        print(f"Version: {latest_version_props.version.versionTag}")
        print(f"Sort ID: {latest_version_props.sortId}")
        if latest_version_props.comment:
            print(f"Comment: {latest_version_props.comment}")
        if latest_version_props.aliases:
            aliases = [
                alias.versionTag
                for alias in latest_version_props.aliases
                if alias.versionTag is not None
            ]
            print(f"Aliases: {', '.join(aliases)}")
else:
    print(f"Version set {version_set_urn} not found")

# Method 2: Query all versions using GraphQL
print("\n=== Querying All Versions via GraphQL ===\n")

graphql_query = """
query ($urn: String!) {
  versionSet(urn: $urn) {
    urn
    latestVersion {
      urn
      ... on MLModel {
        properties {
          name
          description
        }
      }
    }
    versionsSearch(input: {
      query: "*"
      start: 0
      count: 100
    }) {
      total
      searchResults {
        entity {
          urn
          ... on MLModel {
            versionProperties {
              version {
                versionTag
              }
              sortId
              comment
              isLatest
              aliases {
                versionTag
              }
            }
          }
        }
      }
    }
  }
}
"""

variables = {"urn": version_set_urn}

response = graph.execute_graphql(graphql_query, variables)

if "versionSet" in response and response["versionSet"]:
    version_set_data = response["versionSet"]
    versions = version_set_data.get("versionsSearch", {}).get("searchResults", [])

    print(f"Found {len(versions)} version(s)\n")

    for result in versions:
        entity = result.get("entity", {})
        version_props = entity.get("versionProperties", {})

        version_tag = version_props.get("version", {}).get("versionTag", "Unknown")
        is_latest = version_props.get("isLatest", False)
        comment = version_props.get("comment", "No comment")
        aliases = [
            alias.get("versionTag")
            for alias in version_props.get("aliases", [])
            if alias.get("versionTag")
        ]

        print(f"Version: {version_tag}")
        print(f"  URN: {entity.get('urn')}")
        print(f"  Latest: {is_latest}")
        print(f"  Comment: {comment}")
        if aliases:
            print(f"  Aliases: {', '.join(aliases)}")
        print()
else:
    print("No version set data found")

# Method 3: Query using REST API directly
print("\n=== Querying via REST API ===\n")

rest_url = f"http://localhost:8080/entities/{quote(version_set_urn, safe='')}"

try:
    rest_response = requests.get(rest_url)
    rest_response.raise_for_status()

    entity_data = rest_response.json()
    aspects = entity_data.get("aspects", {})

    if "versionSetProperties" in aspects:
        props = aspects["versionSetProperties"]["value"]
        print(f"Latest (from REST): {props.get('latest')}")
        print(f"Versioning Scheme (from REST): {props.get('versioningScheme')}")
    else:
        print("No versionSetProperties found in REST response")

except requests.exceptions.RequestException as e:
    print(f"REST API error: {e}")

```



#### Querying via REST API

The standard DataHub REST APIs can be used to retrieve version set entities and their properties.


**Fetch version set entity via REST API**

```bash
# Fetch a version set by URN
curl 'http://localhost:8080/entities/urn%3Ali%3AversionSet%3A(abc123def456,mlModel)'

# Get all entities in a version set using relationships
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AversionSet%3A(abc123def456,mlModel)&types=VersionOf'
```



#### Querying via GraphQL

DataHub's GraphQL API provides rich querying capabilities for version sets:


**GraphQL: Query version set with all versions**

```graphql
query {
  versionSet(urn: "urn:li:versionSet:(abc123def456,mlModel)") {
    urn
    latestVersion {
      urn
      ... on MLModel {
        properties {
          name
          description
        }
      }
    }
    versionsSearch(input: { query: "*", start: 0, count: 10 }) {
      total
      searchResults {
        entity {
          urn
          ... on MLModel {
            versionProperties {
              version {
                versionTag
              }
              comment
              isLatest
              created {
                time
              }
            }
          }
        }
      }
    }
  }
}
```



## Integration Points

### Relationships with Other Entities

Version Sets have a specific relationship pattern with other entities:

- **VersionOf**: Versioned entities (datasets, ML models, etc.) have a `VersionOf` relationship to their Version Set
- **Latest Version Reference**: The Version Set maintains a direct reference to the latest versioned entity

### Supported Versioned Entities

Currently, DataHub supports versioning for the following entity types:

- **MLModel**: Machine learning models are commonly versioned to track model evolution
- **Dataset**: Datasets can be versioned to track schema changes, data updates, or snapshots

Future versions of DataHub may extend version set support to additional entity types.

### Feature Flags

Version Set functionality is controlled by the `entityVersioning` feature flag. This must be enabled in your DataHub deployment to use version sets:

```yaml
# In your DataHub configuration
featureFlags:
  entityVersioning: true
```

### Ingestion Connector Usage

Several ingestion connectors automatically create and manage version sets:

- **MLflow**: Creates version sets for Registered Models, with each Model Version linked to the version set
- **Vertex AI**: Creates version sets for models with multiple versions
- **Custom Connectors**: You can create version sets programmatically in custom ingestion sources

## Notable Exceptions

### Single Entity Type Constraint

A Version Set can only contain entities of a single type. This is enforced through the `entityType` field in the Version Set key. You cannot mix different entity types (e.g., datasets and ML models) in the same version set.

### Versioning Scheme Immutability

Once a versioning scheme is set for a Version Set, it should not be changed. The sorting and ordering of versions depend on the scheme, and changing it could break the version ordering.

### Latest Version Maintenance

The `isLatest` flag on versioned entities is automatically maintained by DataHub's versioning service. While it's technically possible to set this field manually through the API, you should rely on the automatic maintenance through the `linkAssetVersion` GraphQL mutation or the Python SDK's versioning methods.

### Authorization

Linking or unlinking entities to/from version sets requires UPDATE permissions on both the version set and the versioned entity. Ensure proper authorization is configured for users who need to manage versions.

### Version Label Uniqueness

While version labels (the `version` field) should be unique within a version set, this is not strictly enforced by the system. It's the responsibility of the client code to ensure uniqueness. Having duplicate version labels can cause confusion when querying or navigating versions.

### Deletion Behavior

When a versioned entity is deleted, it is not automatically unlinked from its version set. The relationship may become stale. Consider explicitly unlinking entities before deletion or implementing cleanup logic to handle orphaned version references.

### Search and Discovery

Version Sets themselves are searchable entities in DataHub. Versioned entities can be searched by their version labels, aliases, and version set membership. Use the `versionSortId` field for ordering search results by version order.



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

#### versionSetProperties
None



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| latest | string | ✓ | The latest versioned entity linked to in this version set | Searchable |
| versioningScheme | VersioningScheme | ✓ | What versioning scheme is being utilized for the versioned entities sort criterion. Static once set |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionSetProperties"
  },
  "name": "VersionSetProperties",
  "namespace": "com.linkedin.versionset",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "queryByDefault": "false"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "latest",
      "doc": "The latest versioned entity linked to in this version set"
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "doc": "What versioning scheme is being utilized for the versioned entities sort criterion. Static once set"
    }
  ]
}
```





### Relationships

#### Incoming
These are the relationships stored in other entity's aspects
- VersionOf

   - Dataset via `versionProperties.versionSet`
   - MlModel via `versionProperties.versionSet`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
