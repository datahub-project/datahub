# Structured Property

Structured Properties are custom, typed metadata fields that can be attached to any entity type in DataHub. They enable organizations to extend the core metadata model with domain-specific attributes that support governance, compliance, and data discovery initiatives.

## Identity

Structured Properties are identified by a single piece of information:

- The **id** or **qualified name** of the property: This is a unique string identifier that follows a namespace convention, typically using dot notation (e.g., `io.acryl.privacy.retentionTime` or `companyName.department.propertyName`). The qualified name becomes part of the URN and must be globally unique across all structured properties in your DataHub instance.

An example of a structured property identifier is `urn:li:structuredProperty:io.acryl.privacy.retentionTime`.

Unlike other entities that may require multiple pieces of information (platform, name, environment), structured properties have a simple, flat identity model based solely on their qualified name.

## Important Capabilities

### Property Definition

The core metadata for a structured property is captured in the `propertyDefinition` aspect, which defines:

#### Type System

Structured properties support strongly-typed values through the `valueType` field, which must reference a valid `dataType` entity. The supported types are:

- **string**: Text values (plain text)
- **rich_text**: Rich text with formatting
- **number**: Numeric values (integers or decimals)
- **date**: Date values
- **urn**: References to other DataHub entities

The type is specified as a URN: `urn:li:dataType:datahub.string`, `urn:li:dataType:datahub.number`, etc.

#### Type Qualifiers

For URN-type properties, you can restrict which entity types are allowed using the `typeQualifier` field. This enables creating properties that reference only specific entity types:

```json
{
  "valueType": "urn:li:dataType:datahub.urn",
  "typeQualifier": {
    "allowedTypes": [
      "urn:li:entityType:datahub.corpuser",
      "urn:li:entityType:datahub.corpGroup"
    ]
  }
}
```

#### Cardinality

Structured properties can accept either single or multiple values via the `cardinality` field:

- **SINGLE**: Only one value can be assigned (default)
- **MULTIPLE**: Multiple values can be assigned as an array

#### Entity Type Restrictions

The `entityTypes` array specifies which entity types this property can be applied to. For example, a property might only be applicable to:

- Datasets: `urn:li:entityType:datahub.dataset`
- Schema Fields (columns): `urn:li:entityType:datahub.schemaField`
- Dashboards: `urn:li:entityType:datahub.dashboard`
- Any combination of entity types

#### Validation Through Allowed Values

To enforce data quality and consistency, you can define a whitelist of acceptable values using the `allowedValues` array. Each allowed value can include:

- **value**: The actual value (string or number)
- **description**: An explanation of what this value represents

When allowed values are defined, the system validates that any property value assignment matches one of the allowed values.

#### Immutability

The `immutable` field (default: `false`) determines whether a property value can be changed once set. When `true`, the property value becomes permanent and cannot be modified or removed, ensuring data integrity for critical metadata.

#### Versioning

The `version` field enables breaking schema changes by allowing you to update the property definition in backwards-incompatible ways. Versions must follow the format `yyyyMMddhhmmss` (e.g., `20240614080000`). When a new version is applied:

- Old values with the previous schema become inaccessible
- New values must conform to the updated schema
- The old values are subject to asynchronous deletion

### Display Settings

The `structuredPropertySettings` aspect controls how properties appear in the DataHub UI:

- **isHidden**: When `true`, the property is not visible in the UI (useful for internal metadata)
- **showInSearchFilters**: When `true`, users can filter search results by this property's values
- **showInAssetSummary**: When `true`, displays the property in the asset's sidebar
- **hideInAssetSummaryWhenEmpty**: When `true` and `showInAssetSummary` is enabled, hides the property from the sidebar when it has no value
- **showAsAssetBadge**: When `true`, displays the property value as a badge on the asset card (only available for string/number types with allowed values)
- **showInColumnsTable**: When `true`, displays the property as a column in dataset schema tables (useful for column-level properties)

### Common Aspects

Structured properties also support standard DataHub aspects:

- **status**: Enables soft deletion by setting `removed: true`, which hides the property without deleting underlying data
- **institutionalMemory**: Allows attaching documentation links explaining the property's purpose and usage
- **deprecation**: Can mark properties as deprecated while preserving historical data

## Code Examples

### Create a Structured Property

This example creates a structured property for tracking data retention time with numeric values and validation:

<details>
<summary>Python SDK: Create a structured property with allowed values</summary>

```python
{{ inline /metadata-ingestion/examples/library/structured_property_create_basic.py show_path_as_comment }}
```

</details>

### Create a Structured Property with Entity Type Restrictions

This example creates a property that accepts URN values but only allows references to users and groups:

<details>
<summary>Python SDK: Create a structured property with type qualifiers</summary>

```python
{{ inline /metadata-ingestion/examples/library/structured_property_create_with_type_qualifier.py show_path_as_comment }}
```

</details>

### Apply Structured Properties to Entities

Once structured properties are defined, you can assign them to entities:

<details>
<summary>Python SDK: Apply structured properties to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_update_structured_properties.py show_path_as_comment }}
```

</details>

### Patch Operations on Structured Properties

For more granular control, use patch operations to add or remove individual properties:

<details>
<summary>Python SDK: Add and remove structured properties using patches</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_structured_properties_patch.py show_path_as_comment }}
```

</details>

### Query Structured Property Definitions

Retrieve structured property definitions to understand their configuration:

<details>
<summary>Python SDK: Query a structured property</summary>

```python
{{ inline /metadata-ingestion/examples/library/structured_property_query.py show_path_as_comment }}
```

</details>

### Query via REST API

You can also retrieve structured property definitions using the REST API:

<details>
<summary>REST API: Get structured property definition</summary>

```bash
curl -X 'GET' \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json'
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "propertyDefinition": {
    "value": {
      "qualifiedName": "io.acryl.privacy.retentionTime",
      "displayName": "Retention Time",
      "valueType": "urn:li:dataType:datahub.number",
      "description": "Number of days to retain data",
      "entityTypes": ["urn:li:entityType:datahub.dataset"],
      "cardinality": "SINGLE",
      "allowedValues": [
        {
          "value": { "double": 30 },
          "description": "30 days for ephemeral data"
        },
        {
          "value": { "double": 90 },
          "description": "90 days for regular data"
        }
      ]
    }
  }
}
```

</details>

## Integration Points

### How Structured Properties Relate to Other Entities

Structured properties enable flexible metadata extension across the entire DataHub ecosystem:

#### Applied to Core Data Assets

Structured properties can be attached to any core data asset:

- **Datasets**: Track data quality metrics, retention policies, or compliance classifications
- **Schema Fields**: Add business glossary terms, sensitivity labels, or validation rules at the column level
- **Charts and Dashboards**: Capture certification status, review dates, or usage guidelines
- **Data Jobs and Pipelines**: Record SLA requirements, criticality levels, or maintenance schedules

#### Applied to Organizational Entities

Properties also extend to organizational and governance entities:

- **Domains**: Track ownership model, maturity level, or strategic importance
- **Glossary Terms**: Add approval status, version numbers, or related regulations
- **Data Products**: Capture product lifecycle stage, target audience, or support tier
- **Users and Groups**: Store employee IDs, cost centers, or access review dates

#### Relationship with Forms

Structured properties work in conjunction with DataHub Forms to enable:

- **Compliance workflows**: Forms can collect structured property values to ensure policy compliance
- **Bulk updates**: Forms allow updating structured properties across multiple assets simultaneously
- **Validation**: Forms enforce property constraints during data collection

#### Search and Discovery

Structured properties enhance search capabilities:

- Properties marked with `showInSearchFilters: true` become available as faceted search filters
- Users can filter results by property values to find assets meeting specific criteria
- Aggregation queries can summarize property value distributions across your data landscape

#### GraphQL Resolvers

The GraphQL layer exposes structured properties through several resolvers:

- `CreateStructuredPropertyResolver`: Creates new structured property definitions
- `UpdateStructuredPropertyResolver`: Modifies existing property definitions (within allowed constraints)
- `DeleteStructuredPropertyResolver`: Removes structured property definitions
- `UpsertStructuredPropertiesResolver`: Assigns property values to entities
- `RemoveStructuredPropertiesResolver`: Removes property values from entities

These resolvers are located in `/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/structuredproperties/`.

## Notable Exceptions

### Property Definition Constraints

Once a structured property is created and in use, certain fields cannot be modified to prevent data inconsistency:

**Immutable Fields:**

- **valueType**: Cannot change the data type (e.g., from string to number)
- **allowedValues**: Cannot remove or modify existing allowed values (can only add new ones)
- **cardinality**: Cannot change between SINGLE and MULTIPLE without a version bump

**Modifiable Fields:**

- **displayName**: Can be updated for better readability
- **description**: Can be enhanced or clarified
- **entityTypes**: Can add new entity types (but not remove existing ones)
- **allowedValues**: Can append additional allowed values
- **Display settings**: All `structuredPropertySettings` can be freely modified

### Breaking Changes with Versioning

To make backwards-incompatible changes (like changing cardinality or removing allowed values), you must:

1. Set a new `version` value in format `yyyyMMddhhmmss`
2. Apply the updated definition
3. Accept that old values will become inaccessible
4. Re-apply property values with the new schema

This is a destructive operation and should be carefully planned.

### Soft Delete vs Hard Delete

Structured properties support two deletion modes:

**Soft Delete** (via `status` aspect with `removed: true`):

- Property definition remains in the system
- Property values are hidden but not deleted
- Assignment of new values is blocked
- Search filters using the property are disabled
- Fully reversible by setting `removed: false`

**Hard Delete** (via entity deletion):

- Removes the property definition entity completely
- Triggers asynchronous removal of all property values across all entities
- Not reversible
- Elasticsearch mappings persist until reindexing occurs

### Column-Level Properties

When creating properties for schema fields (columns), be aware:

- Setting `showInColumnsTable: true` displays the property in all dataset schema views
- Column-level properties automatically appear in the column sidebar
- Too many visible properties can clutter the UI, so use display settings judiciously

### Search Indexing

Structured property values are indexed in Elasticsearch using a special naming convention:

- **Unversioned**: `structuredProperties.io_acryl_privacy_retentionTime`
- **Versioned**: `structuredProperties._versioned.io_acryl_privacy_retentionTime02.20240614080000.string`

Understanding this convention is important for:

- Advanced search queries using the OpenAPI
- Debugging search issues
- Creating custom aggregations

For more information, refer to:

- [Structured Properties Feature Guide](/docs/features/feature-guides/properties/overview.md)
- [Structured Properties API Tutorial](/docs/api/tutorials/structured-properties.md)
