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

<details>
<summary>Python SDK: Create a version set with properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/version_set_add_properties.py show_path_as_comment }}
```

</details>

### Linking Entities to Version Sets

Entities are linked to Version Sets through the `versionProperties` aspect on the versioned entity. This aspect contains:

- **versionSet**: URN of the Version Set this entity belongs to
- **version**: A version tag label that should be unique within the version set
- **sortId**: An identifier used for sorting versions according to the versioning scheme
- **aliases**: Alternative version identifiers (e.g., "latest", "stable", "v1")
- **comment**: Optional documentation about what this version represents
- **isLatest**: Boolean flag indicating if this is the latest version (automatically maintained)
- **timestamps**: Creation timestamps both from the source system and in DataHub

<details>
<summary>Python SDK: Link an entity to a version set</summary>

```python
{{ inline /metadata-ingestion/examples/library/version_set_link_entity.py show_path_as_comment }}
```

</details>

### Creating a New Version Set

When creating a new version set, you typically link the first versioned entity to it. The version set can be created implicitly by linking an entity to a new version set URN.

<details>
<summary>Python SDK: Create a version set by linking the first entity</summary>

```python
{{ inline /metadata-ingestion/examples/library/version_set_create.py show_path_as_comment }}
```

</details>

### Managing Multiple Versions

As you create new versions of an asset, you link each one to the same version set with a different version label. The version set automatically updates the `latest` pointer to the most recent version based on the versioning scheme.

<details>
<summary>Python SDK: Link multiple versions to a version set</summary>

```python
{{ inline /metadata-ingestion/examples/library/version_set_link_multiple_versions.py show_path_as_comment }}
```

</details>

### Querying Version Sets and Versioned Entities

You can query version sets to retrieve information about all versions or find specific versions.

<details>
<summary>Python SDK: Query a version set and its versions</summary>

```python
{{ inline /metadata-ingestion/examples/library/version_set_query.py show_path_as_comment }}
```

</details>

#### Querying via REST API

The standard DataHub REST APIs can be used to retrieve version set entities and their properties.

<details>
<summary>Fetch version set entity via REST API</summary>

```bash
# Fetch a version set by URN
curl 'http://localhost:8080/entities/urn%3Ali%3AversionSet%3A(abc123def456,mlModel)'

# Get all entities in a version set using relationships
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AversionSet%3A(abc123def456,mlModel)&types=VersionOf'
```

</details>

#### Querying via GraphQL

DataHub's GraphQL API provides rich querying capabilities for version sets:

<details>
<summary>GraphQL: Query version set with all versions</summary>

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

</details>

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
