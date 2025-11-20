# SchemaField

The schemaField entity represents an individual column or field within a dataset's schema. While schema information is typically ingested as part of a dataset's `schemaMetadata` aspect, schemaField entities exist as first-class entities to enable direct attachment of metadata like tags, glossary terms, documentation, and structured properties at the field level.

SchemaField entities are automatically created by DataHub when datasets with schemas are ingested. They serve as the link between dataset-level metadata and column-level metadata, enabling fine-grained data governance and lineage tracking at the field level.

## Identity

SchemaField entities are uniquely identified by two components:

- **Parent URN**: The URN of the dataset that contains this field
- **Field Path**: The path identifying the field within the schema (e.g., `user_id`, `address.zipcode` for nested fields)

The URN structure for a schemaField follows this pattern:

```
urn:li:schemaField:(<parent_dataset_urn>,<encoded_field_path>)
```

### Examples

**Simple field:**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD),user_id)
```

**Nested field:**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD),address.zipcode)
```

**Field with special characters (URL encoded):**

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),first%20name)
```

Note that the field path component may be URL-encoded if it contains special characters. The v1 field path uses `.` notation for nested structures, while v2 field paths include type information (e.g., `[version=2.0].[type=struct].address.[type=string].zipcode`).

## Important Capabilities

### Field Information (schemafieldInfo)

The `schemafieldInfo` aspect contains basic identifying information about the schema field:

- **name**: The display name of the field
- **schemaFieldAliases**: Alternative URNs for this field, used to store field path variations

This aspect is primarily used internally by DataHub to support field path variations and search functionality.

### Documentation

The `documentation` aspect stores field-level documentation from multiple sources. Unlike the dataset-level description pattern which uses separate aspects (`datasetProperties` and `editableDatasetProperties`), field-level documentation uses a single unified aspect that can contain multiple documentation entries from different sources.

Each documentation entry includes:

- The documentation text/description
- The source system or attribution information

<details>
<summary>Python SDK: Add or update documentation for a schema field</summary>

```python
{{ inline /metadata-ingestion/examples/library/schemafield_add_documentation.py show_path_as_comment }}
```

</details>

### Tags

Tags can be added directly to schema fields using the `globalTags` aspect. This is separate from tags added at the dataset level, allowing for fine-grained classification of individual columns.

Tags on fields are commonly used to:

- Mark sensitive data (PII, PHI, confidential)
- Indicate data quality issues
- Flag deprecated fields
- Classify data by security level or compliance requirements

<details>
<summary>Python SDK: Add a tag to a schema field</summary>

```python
{{ inline /metadata-ingestion/examples/library/schemafield_add_tag.py show_path_as_comment }}
```

</details>

### Glossary Terms

Glossary terms can be attached to schema fields via the `glossaryTerms` aspect, enabling semantic annotation at the column level. This helps users understand the business meaning of individual fields.

<details>
<summary>Python SDK: Add a glossary term to a schema field</summary>

```python
{{ inline /metadata-ingestion/examples/library/schemafield_add_term.py show_path_as_comment }}
```

</details>

### Business Attributes

The `businessAttributes` aspect allows association of business attribute definitions with schema fields. Business attributes provide a way to attach enterprise-specific metadata dimensions (like data classification, retention policies, or business rules) directly to fields.

This is particularly useful for organizations that need to track custom governance metadata at the field level that isn't covered by standard aspects.

### Structured Properties

Schema fields support structured properties via the `structuredProperties` aspect, allowing organizations to extend the metadata model with custom typed properties. This is useful for tracking field-level metadata like:

- Data quality scores
- Business criticality ratings
- Custom classification schemes
- Regulatory compliance markers

<details>
<summary>Python SDK: Add structured properties to a schema field</summary>

```python
{{ inline /metadata-ingestion/examples/library/schemafield_add_structured_properties.py show_path_as_comment }}
```

</details>

### Field Aliases (schemaFieldAliases)

The `schemaFieldAliases` aspect stores alternative URNs for a schema field. This is useful when:

- Field paths change due to schema evolution
- Multiple field path formats are used (v1 vs v2)
- Cross-platform field references need to be maintained

### Deprecation

Fields can be marked as deprecated using the `deprecation` aspect, indicating they should not be used in new applications or analyses. The deprecation aspect includes:

- Deprecation timestamp
- Optional note explaining the deprecation
- Optional actor who deprecated the field

### Logical Parent

The `logicalParent` aspect can associate a schema field with a logical parent entity (like a container or domain), enabling organizational hierarchies that differ from the physical dataset structure.

### Forms

Forms can be attached to schema fields via the `forms` aspect, enabling structured data collection and validation at the field level. This is useful for capturing field-level certifications, approvals, or additional metadata.

### Status

The `status` aspect indicates whether a schema field is active or has been soft-deleted.

### Test Results

The `testResults` aspect can store results of data quality tests run on specific fields, linking test outcomes directly to the columns they validate.

### SubTypes

The `subTypes` aspect allows categorization of schema fields beyond their data type, enabling custom classification schemes.

## Code Examples

### Querying a Schema Field via REST API

The standard GET API can be used to retrieve schema field entities and their aspects:

<details>
<summary>Fetch a schemaField entity</summary>

```python
{{ inline /metadata-ingestion/examples/library/schemafield_query_entity.py show_path_as_comment }}
```

Example API call:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AschemaField%3A(urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres%2Cpublic.users%2CPROD)%2Cuser_id)'
```

This returns all aspects associated with the schema field, including tags, terms, documentation, and structured properties.

</details>

### Working with Fine-Grained Lineage

Schema fields are central to fine-grained (column-level) lineage. When defining lineage between datasets, you can specify which fields flow from upstream to downstream:

<details>
<summary>Example lineage query showing field-level relationships</summary>

```bash
# Find upstream fields of a specific schema field
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AschemaField%3A(urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres%2Cpublic.orders%2CPROD)%2Cuser_id)&types=DownstreamOf'
```

This shows which upstream fields contribute to this field's values, enabling impact analysis at the column level.

</details>

## Integration Points

### Relationship with Datasets

Schema fields have a parent-child relationship with datasets. The dataset's `schemaMetadata` aspect defines the structure and metadata of fields, while individual schemaField entities allow direct metadata attachment at the field level.

Key integration points:

- Fields are referenced in `schemaMetadata` and `editableSchemaMetadata` aspects of datasets
- Field-level tags and terms can be set via dataset aspects (`schemaMetadata`) or directly on schemaField entities
- The UI typically modifies `editableSchemaMetadata` on the dataset, while ingestion connectors set `schemaMetadata`

### Fine-Grained Lineage

Schema fields are essential for column-level lineage:

- **DataJob entities**: The `dataJobInputOutput` aspect can specify `inputDatasetFields` and `outputDatasetFields`
- **Dataset lineage**: The `upstreamLineage` aspect on datasets can include `fineGrainedLineages` that map specific fields
- **Lineage queries**: Field-level lineage appears as relationships between schemaField entities

### GraphQL API

The GraphQL API exposes schema field entities as first-class entities with the `SchemaFieldEntity` type. Key resolvers include:

- Fetching field metadata (tags, terms, documentation)
- Querying field lineage relationships
- Searching for fields across datasets

Note: Field fetching via GraphQL is controlled by the `schemaFieldEntityFetchEnabled` feature flag. When disabled, schema field metadata is accessed only through the parent dataset's schema aspects.

### Search and Discovery

Schema fields are indexed for search, enabling users to:

- Find datasets by column names
- Search for fields with specific tags or terms
- Discover fields by description content
- Filter by field-level classifications

## Notable Exceptions

### Dual Access Patterns

Schema field metadata can be accessed and modified in two ways:

1. **Via the parent dataset**: Using `schemaMetadata` or `editableSchemaMetadata` aspects on the dataset
2. **Directly on schemaField entities**: Using aspects like `globalTags`, `glossaryTerms`, `documentation` on the schemaField URN

Best practices:

- Ingestion connectors should use dataset-level aspects (`schemaMetadata`)
- UI edits typically use dataset-level aspects (`editableSchemaMetadata`)
- Direct schemaField entity updates are useful for programmatic bulk operations or when working with field-level lineage

### Feature Flag Dependency

The ability to fetch schemaField entities via GraphQL depends on the `schemaFieldEntityFetchEnabled` feature flag. When disabled:

- Schema field entities are not directly queryable
- Field metadata must be accessed through parent datasets
- Field-level operations may have limited functionality

This flag exists for performance reasons, as materializing individual field entities can be expensive for datasets with hundreds of columns.

### Field Path Encoding

Field paths in schemaField URNs must be URL-encoded if they contain special characters (spaces, special symbols, etc.). Always use the `make_schema_field_urn` utility function from `datahub.emitter.mce_builder` to construct URNs correctly:

```python
from datahub.emitter.mce_builder import make_schema_field_urn

# Automatically handles encoding
field_urn = make_schema_field_urn(
    parent_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
    field_path="first name"  # Will be encoded as "first%20name"
)
```

### V1 vs V2 Field Paths

DataHub supports two field path formats:

- **V1**: Simple dot notation (e.g., `address.zipcode`)
- **V2**: Type-aware notation (e.g., `[version=2.0].[type=struct].address.[type=string].zipcode`)

V2 field paths are required for:

- Union types where field names alone are ambiguous
- Complex nested structures with type information
- Precise field path disambiguation

Most simple schemas can use v1 field paths. Use v2 when dealing with complex types or when ingestion connectors generate them.
