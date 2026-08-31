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

<details>
<summary>Python SDK: Create a basic tag</summary>

```python
{{ inline /metadata-ingestion/examples/library/tag_create_basic.py show_path_as_comment }}
```

</details>

### Adding Ownership to a Tag

<details>
<summary>Python SDK: Add an owner to a tag</summary>

```python
{{ inline /metadata-ingestion/examples/library/tag_add_ownership.py show_path_as_comment }}
```

</details>

### Applying Tags to Datasets

<details>
<summary>Python SDK: Apply a tag to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/tag_apply_to_dataset.py show_path_as_comment }}
```

</details>

### Querying Tag Information

The standard REST APIs can be used to retrieve tag metadata and see which entities are tagged.

<details>
<summary>REST API: Fetch tag entity information</summary>

```python
{{ inline /metadata-ingestion/examples/library/tag_query_rest.py show_path_as_comment }}
```

</details>

### Searching for Tagged Assets

Tags are fully integrated with DataHub's search capabilities, allowing you to find all assets with a specific tag.

<details>
<summary>Python SDK: Search for assets by tag</summary>

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

</details>

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
