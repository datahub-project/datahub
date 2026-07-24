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


**Python SDK: Create a structured property with allowed values**

```python
# Inlined from /metadata-ingestion/examples/library/structured_property_create_basic.py
import os

from datahub.api.entities.structuredproperties.structuredproperties import (
    AllowedValue,
    StructuredProperties,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

# Create a DataHub client
client = DataHubGraph(
    DataHubGraphConfig(
        server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        token=os.getenv("DATAHUB_GMS_TOKEN"),
    )
)

# Define a structured property for data retention time
retention_property = StructuredProperties(
    id="io.acryl.privacy.retentionTime",
    qualified_name="io.acryl.privacy.retentionTime",
    display_name="Retention Time",
    type="number",
    description="Number of days to retain data based on privacy and compliance requirements",
    entity_types=["dataset", "dataFlow"],
    cardinality="SINGLE",
    allowed_values=[
        AllowedValue(
            value=30.0,
            description="30 days - for ephemeral data containing PII",
        ),
        AllowedValue(
            value=90.0,
            description="90 days - for monthly reporting data with PII",
        ),
        AllowedValue(
            value=365.0,
            description="365 days - for non-sensitive data",
        ),
    ],
)

# Emit the structured property to DataHub
for mcp in retention_property.generate_mcps():
    client.emit(mcp)

print(f"Created structured property: {retention_property.urn}")

```



### Create a Structured Property with Entity Type Restrictions

This example creates a property that accepts URN values but only allows references to users and groups:


**Python SDK: Create a structured property with type qualifiers**

```python
# Inlined from /metadata-ingestion/examples/library/structured_property_create_with_type_qualifier.py
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
    TypeQualifierAllowedTypes,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub client
client = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# Define a structured property that references DataHub entities
# This property can only reference CorpUser or CorpGroup entities
data_steward_property = StructuredProperties(
    id="io.acryl.governance.dataSteward",
    qualified_name="io.acryl.governance.dataSteward",
    display_name="Data Steward",
    type="urn",
    description="The designated data steward responsible for this asset's governance and quality",
    entity_types=["dataset", "dashboard", "chart", "dataJob"],
    cardinality="SINGLE",
    type_qualifier=TypeQualifierAllowedTypes(
        allowed_types=[
            "urn:li:entityType:datahub.corpuser",
            "urn:li:entityType:datahub.corpGroup",
        ]
    ),
)

# Emit the structured property to DataHub
for mcp in data_steward_property.generate_mcps():
    client.emit_mcp(mcp)

print(f"Created structured property: {data_steward_property.urn}")

# Example: Create a multi-value property for related datasets
related_datasets_property = StructuredProperties(
    id="io.acryl.lineage.relatedDatasets",
    qualified_name="io.acryl.lineage.relatedDatasets",
    display_name="Related Datasets",
    type="urn",
    description="Other datasets that are semantically or functionally related to this asset",
    entity_types=["dataset"],
    cardinality="MULTIPLE",
    type_qualifier=TypeQualifierAllowedTypes(
        allowed_types=["urn:li:entityType:datahub.dataset"]
    ),
)

# Emit the second structured property
for mcp in related_datasets_property.generate_mcps():
    client.emit_mcp(mcp)

print(f"Created structured property: {related_datasets_property.urn}")

```



### Apply Structured Properties to Entities

Once structured properties are defined, you can assign them to entities:


**Python SDK: Apply structured properties to a dataset**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_update_structured_properties.py
import logging

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DataHubRestEmitter(gms_server="http://localhost:8080")

dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")


for patch_mcp in (
    DatasetPatchBuilder(dataset_urn)
    .set_structured_property("io.acryl.dataManagement.replicationSLA", 120)
    .build()
):
    rest_emitter.emit(patch_mcp)


log.info(f"Added cluster_name, retention_time properties to dataset {dataset_urn}")

```



### Patch Operations on Structured Properties

For more granular control, use patch operations to add or remove individual properties:


**Python SDK: Add and remove structured properties using patches**

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_structured_properties_patch.py
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Create Dataset Patch to Add and Remove Structured Properties
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_structured_property(
    "urn:li:structuredProperty:retentionTimeInDays", 12
)
patch_builder.remove_structured_property(
    "urn:li:structuredProperty:customClassification"
)
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```



### Query Structured Property Definitions

Retrieve structured property definitions to understand their configuration:


**Python SDK: Query a structured property**

```python
# Inlined from /metadata-ingestion/examples/library/structured_property_query.py
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Create a DataHub client
client = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

# List all structured properties in the system
print("Listing all structured properties:")
print("-" * 80)

for structured_property in StructuredProperties.list(client):
    print(f"\nURN: {structured_property.urn}")
    print(f"Display Name: {structured_property.display_name}")
    print(f"Type: {structured_property.type}")
    print(f"Cardinality: {structured_property.cardinality}")
    print(f"Entity Types: {', '.join(structured_property.entity_types or [])}")

    if structured_property.allowed_values:
        print("Allowed Values:")
        for av in structured_property.allowed_values:
            print(f"  - {av.value}: {av.description}")

# Retrieve a specific structured property by URN
print("\n" + "=" * 80)
print("Retrieving specific structured property:")
print("-" * 80)

property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

try:
    specific_property = StructuredProperties.from_datahub(client, property_urn)

    print(f"\nURN: {specific_property.urn}")
    print(f"Qualified Name: {specific_property.qualified_name}")
    print(f"Display Name: {specific_property.display_name}")
    print(f"Description: {specific_property.description}")
    print(f"Type: {specific_property.type}")
    print(f"Cardinality: {specific_property.cardinality}")
    print(f"Immutable: {specific_property.immutable}")
    print(f"Entity Types: {', '.join(specific_property.entity_types or [])}")

    if specific_property.allowed_values:
        print("\nAllowed Values:")
        for av in specific_property.allowed_values:
            print(f"  - {av.value}: {av.description}")

    if specific_property.type_qualifier:
        print("\nType Qualifier - Allowed Entity Types:")
        for entity_type in specific_property.type_qualifier.allowed_types:
            print(f"  - {entity_type}")

except Exception as e:
    print(f"Error retrieving structured property: {e}")

# Example: List just the URNs (for scripting)
print("\n" + "=" * 80)
print("All structured property URNs:")
print("-" * 80)

for urn in StructuredProperties.list_urns(client):
    print(urn)

```



### Query via REST API

You can also retrieve structured property definitions using the REST API:


**REST API: Get structured property definition**

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
- `UpsertStructuredPropertiesResolver`: Assigns property values to entities (builds MCPs; orphan filtering is handled in GMS)
- `RemoveStructuredPropertiesResolver`: Removes property values from entities

These resolvers are located in `/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/structuredproperties/`.

#### GMS assignment mutator

`StructuredPropertiesAssignmentMutator` (`metadata-io/.../structuredproperties/hooks/`) is a `MutationHook` registered for the `structuredProperties` aspect on all entities:

- **Read**: Filters out assignments whose definition is soft-deleted (`StructuredPropertyUtils.filterSoftDelete`).
- **Write**: When `structuredProperties.dropMissingPropertyValuesWithWarning` is `true` (default), `StructuredPropertiesValidator` skips validation for missing definitions and `StructuredPropertiesAssignmentMutator` drops those assignments before commit (`StructuredPropertyUtils.filterMissingPropertyDefinitions`), logs a warning per dropped URN, and rejects the write if no valid assignments remain.

Configured in `application.yaml` under `structuredProperties.dropMissingPropertyValuesWithWarning` / env `STRUCTURED_PROPERTIES_DROP_MISSING_PROPERTY_VALUES_WITH_WARNING`.

#### Hard delete side effects

On hard delete of a `structuredProperty` entity, `EntityServiceImpl` captures the `propertyDefinition` aspect before `deleteUrn` and emits a companion `propertyDefinition` DELETE metadata change log so `PropertyDefinitionDeleteSideEffect` can remove assignments from other entities even when only the key aspect DELETE is visible after storage removal.

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
- Property values are hidden on read via `StructuredPropertiesAssignmentMutator` but not deleted from storage
- Assignment of new values is blocked
- Search filters using the property are disabled
- Fully reversible by setting `removed: false`

**Hard Delete** (via entity deletion):

- Removes the property definition entity completely
- Emits a companion `propertyDefinition` DELETE event before entity removal so assignment cleanup runs reliably
- Triggers asynchronous PATCH REMOVE of property values on other entities via `PropertyDefinitionDeleteSideEffect`
- Orphaned assignments left on entities can be stripped on subsequent writes when `dropMissingPropertyValuesWithWarning` is enabled (default)
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

#### propertyDefinition
None



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| qualifiedName | string | ✓ | The fully qualified name of the property. e.g. io.acryl.datahub.myProperty | Searchable |
| displayName | string |  | The display name of the property. This is the name that will be shown in the UI and can be used t... | Searchable |
| valueType | string | ✓ | The value type of the property. Must be a dataType. e.g. To indicate that the property is of type... |  |
| typeQualifier | map |  | A map that allows for type specialization of the valueType. e.g. a valueType of urn:li:dataType:d... |  |
| allowedValues | PropertyValue[] |  | A list of allowed values that the property is allowed to take.  If this is not specified, then th... |  |
| cardinality | PropertyCardinality |  | The cardinality of the property. If not specified, then the property is assumed to be single valu... |  |
| entityTypes | string[] | ✓ |  | Searchable, → StructuredPropertyOf |
| allowedPlatforms | string[] |  | An optional list of data platforms that this property is restricted to. If specified, the propert... | Searchable |
| description | string |  | The description of the property. This is the description that will be shown in the UI. |  |
| searchConfiguration | DataHubSearchConfig |  | Search configuration for this property. If not specified, then the property is indexed using the ... |  |
| immutable | boolean | ✓ | Whether the structured property value is immutable once applied to an entity. | Searchable |
| version | string |  | Definition version - Allows breaking schema changes. String is compared case-insensitive and new ... |  |
| created | [AuditStamp](#auditstamp) |  | Created Audit stamp | Searchable |
| lastModified | [AuditStamp](#auditstamp) |  | Last Modified Audit stamp | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "propertyDefinition",
    "schemaVersion": 2
  },
  "name": "StructuredPropertyDefinition",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "searchLabel": "qualifiedName",
        "searchTier": 1
      },
      "type": "string",
      "name": "qualifiedName",
      "doc": "The fully qualified name of the property. e.g. io.acryl.datahub.myProperty"
    },
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "The display name of the property. This is the name that will be shown in the UI and can be used to look up the property id."
    },
    {
      "UrnValidation": {
        "entityTypes": [
          "dataType"
        ],
        "exist": true,
        "strict": true
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "valueType",
      "doc": "The value type of the property. Must be a dataType.\ne.g. To indicate that the property is of type DATE, use urn:li:dataType:datahub.date"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        }
      ],
      "name": "typeQualifier",
      "default": null,
      "doc": "A map that allows for type specialization of the valueType.\ne.g. a valueType of urn:li:dataType:datahub.urn\ncan be specialized to be a USER or GROUP URN by adding a typeQualifier like \n{ \"allowedTypes\": [\"urn:li:entityType:datahub.corpuser\", \"urn:li:entityType:datahub.corpGroup\"] }"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "PropertyValue",
            "namespace": "com.linkedin.structured",
            "fields": [
              {
                "type": [
                  "string",
                  "double"
                ],
                "name": "value"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Optional description of the property value"
              }
            ]
          }
        }
      ],
      "name": "allowedValues",
      "default": null,
      "doc": "A list of allowed values that the property is allowed to take. \nIf this is not specified, then the property can take any value of given type."
    },
    {
      "type": [
        {
          "type": "enum",
          "name": "PropertyCardinality",
          "namespace": "com.linkedin.structured",
          "symbols": [
            "SINGLE",
            "MULTIPLE"
          ]
        },
        "null"
      ],
      "name": "cardinality",
      "default": "SINGLE",
      "doc": "The cardinality of the property. If not specified, then the property is assumed to be single valued.."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "entityType"
          ],
          "name": "StructuredPropertyOf"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "entityTypes"
        }
      },
      "UrnValidation": {
        "entityTypes": [
          "entityType"
        ],
        "exist": true,
        "strict": true
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "entityTypes"
    },
    {
      "Searchable": {
        "/*": {
          "fieldName": "allowedPlatforms"
        }
      },
      "UrnValidation": {
        "entityTypes": [
          "dataPlatform"
        ],
        "exist": true,
        "strict": true
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "allowedPlatforms",
      "default": null,
      "doc": "An optional list of data platforms that this property is restricted to. If specified, the property\ncan only be assigned to entities that belong to one of these data platforms. If not specified,\nthe property can be assigned to entities on any data platform."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "The description of the property. This is the description that will be shown in the UI."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataHubSearchConfig",
          "namespace": "com.linkedin.datahub",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "fieldName",
              "default": null,
              "doc": "Name of the field in the search index. Defaults to the field name otherwise"
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "name": "SearchFieldType",
                  "namespace": "com.linkedin.datahub",
                  "symbols": [
                    "KEYWORD",
                    "TEXT",
                    "TEXT_PARTIAL",
                    "BROWSE_PATH",
                    "URN",
                    "URN_PARTIAL",
                    "BOOLEAN",
                    "COUNT",
                    "DATETIME",
                    "OBJECT",
                    "BROWSE_PATH_V2",
                    "WORD_GRAM"
                  ]
                }
              ],
              "name": "fieldType",
              "default": null,
              "doc": "Type of the field. Defines how the field is indexed and matched"
            },
            {
              "type": "boolean",
              "name": "queryByDefault",
              "default": false,
              "doc": "Whether we should match the field for the default search query"
            },
            {
              "type": "boolean",
              "name": "enableAutocomplete",
              "default": false,
              "doc": "Whether we should use the field for default autocomplete"
            },
            {
              "type": "boolean",
              "name": "addToFilters",
              "default": false,
              "doc": "Whether or not to add field to filters."
            },
            {
              "type": "boolean",
              "name": "addHasValuesToFilters",
              "default": true,
              "doc": "Whether or not to add the \"has values\" to filters.\ncheck if this is conditional on addToFilters being true"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "filterNameOverride",
              "default": null,
              "doc": "Display name of the filter"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "hasValuesFilterNameOverride",
              "default": null,
              "doc": "Display name of the has values filter"
            },
            {
              "type": "double",
              "name": "boostScore",
              "default": 1.0,
              "doc": "Boost multiplier to the match score. Matches on fields with higher boost score ranks higher"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "hasValuesFieldName",
              "default": null,
              "doc": "If set, add a index field of the given name that checks whether the field exists"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "numValuesFieldName",
              "default": null,
              "doc": "If set, add a index field of the given name that checks the number of elements"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "double"
                }
              ],
              "name": "weightsPerFieldValue",
              "default": null,
              "doc": "(Optional) Weights to apply to score for a given value"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "fieldNameAliases",
              "default": null,
              "doc": "(Optional) Aliases for this given field that can be used for sorting etc."
            }
          ],
          "doc": "Configuration for how any given field should be indexed and matched in the DataHub search index."
        }
      ],
      "name": "searchConfiguration",
      "default": null,
      "doc": "Search configuration for this property. If not specified, then the property is indexed using the default mapping.\nfrom the logical type."
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "immutable",
      "default": false,
      "doc": "Whether the structured property value is immutable once applied to an entity."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "version",
      "default": null,
      "doc": "Definition version - Allows breaking schema changes. String is compared case-insensitive and new\n                     versions must be monotonically increasing. Cannot use periods/dots.\n                     Suggestions: v1, v2\n                                  20240610, 20240611"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdTime",
          "fieldType": "DATETIME",
          "searchLabel": "createdAt"
        }
      },
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
      "name": "created",
      "default": null,
      "doc": "Created Audit stamp"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModified",
          "fieldType": "DATETIME",
          "searchLabel": "lastModifiedAt"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "Last Modified Audit stamp"
    }
  ]
}
```





#### structuredPropertySettings
Settings specific to a structured property entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| isHidden | boolean | ✓ | Whether or not this asset should be hidden in the main application | Searchable |
| showInSearchFilters | boolean | ✓ | Whether or not this asset should be displayed as a search filter | Searchable |
| showInAssetSummary | boolean | ✓ | Whether or not this asset should be displayed in the asset sidebar | Searchable |
| hideInAssetSummaryWhenEmpty | boolean | ✓ | Whether or not this asset should be hidden in the asset sidebar (showInAssetSummary should be ena... | Searchable |
| showAsAssetBadge | boolean | ✓ | Whether or not this asset should be displayed as an asset badge on other asset's headers | Searchable |
| showInColumnsTable | boolean | ✓ | Whether or not this asset should be displayed as a column in the schema field table in a Dataset'... | Searchable |
| lastModified | [AuditStamp](#auditstamp) |  | Last Modified Audit stamp | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredPropertySettings"
  },
  "name": "StructuredPropertySettings",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "isHidden",
      "default": false,
      "doc": "Whether or not this asset should be hidden in the main application"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInSearchFilters",
      "default": false,
      "doc": "Whether or not this asset should be displayed as a search filter"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInAssetSummary",
      "default": false,
      "doc": "Whether or not this asset should be displayed in the asset sidebar"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "hideInAssetSummaryWhenEmpty",
      "default": false,
      "doc": "Whether or not this asset should be hidden in the asset sidebar (showInAssetSummary should be enabled)\nwhen its value is empty"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showAsAssetBadge",
      "default": false,
      "doc": "Whether or not this asset should be displayed as an asset badge on other\nasset's headers"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "showInColumnsTable",
      "default": false,
      "doc": "Whether or not this asset should be displayed as a column in the schema field table\nin a Dataset's \"Columns\" tab."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedSettings",
          "fieldType": "DATETIME"
        }
      },
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
      "name": "lastModified",
      "default": null,
      "doc": "Last Modified Audit stamp"
    }
  ],
  "doc": "Settings specific to a structured property entity"
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
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
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
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

#### Outgoing
These are the relationships stored in this entity's aspects
- StructuredPropertyOf

   - EntityType via `propertyDefinition.entityTypes`
#### Incoming
These are the relationships stored in other entity's aspects
- RelatedAsset

   - Document via `documentInfo.relatedAssets.asset`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
