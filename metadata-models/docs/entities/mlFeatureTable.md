# ML Feature Table

The ML Feature Table entity represents a collection of related machine learning features organized together in a feature store. Feature tables are fundamental building blocks in the ML feature management ecosystem, grouping features that share common characteristics such as the same primary keys, update cadence, or data source. They bridge the gap between raw data in data warehouses and the features consumed by ML models during training and inference.

## Identity

ML Feature Tables are identified by two pieces of information:

- The platform that hosts the feature table: this is the specific feature store or ML platform technology. Examples include `feast`, `tecton`, `sagemaker`, etc. See [dataplatform](./dataPlatform.md) for more details.
- The name of the feature table: a unique identifier within the specific platform that represents this collection of features.

An example of an ML Feature Table identifier is `urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,users_feature_table)`.

The identity is defined by the `mlFeatureTableKey` aspect, which contains:

- `platform`: A URN reference to the data platform hosting the feature table
- `name`: The unique name of the feature table within that platform

## Important Capabilities

### Feature Table Properties

ML Feature Tables support comprehensive metadata through the `mlFeatureTableProperties` aspect. This aspect captures the essential characteristics of the feature table:

#### Description and Documentation

Feature tables can have detailed descriptions explaining their purpose, the type of features they contain, and when they should be used. This documentation helps data scientists and ML engineers discover and understand feature tables in their organization.

<details>
<summary>Python SDK: Create an ML Feature Table with properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_table_create_with_properties.py show_path_as_comment }}
```

</details>

#### Features

The most important property of a feature table is the collection of features it contains. Feature tables maintain explicit relationships to their constituent features through the `mlFeatures` property. This creates a "Contains" relationship between the feature table and each individual feature, enabling:

- Discovery of all features in a table
- Navigation from feature table to individual features
- Understanding of feature organization and grouping

<details>
<summary>Python SDK: Add features to a feature table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_table_add_features.py show_path_as_comment }}
```

</details>

#### Primary Keys

Feature tables define one or more primary keys that uniquely identify each row in the table. These primary keys are critical for:

- Joining features with training datasets
- Looking up feature values during model inference
- Understanding the entity granularity of the features (e.g., user-level, transaction-level)

When multiple primary keys are specified, they act as a composite key. The `mlPrimaryKeys` property creates a "KeyedBy" relationship to each primary key entity.

<details>
<summary>Python SDK: Add primary keys to a feature table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_table_add_primary_keys.py show_path_as_comment }}
```

</details>

#### Custom Properties

Feature tables support custom properties through the `customProperties` field, allowing you to capture platform-specific or organization-specific metadata that doesn't fit into the standard schema. This might include information like:

- Update frequency or freshness SLAs
- Feature store configuration settings
- Cost or resource usage information
- Team or project ownership details

### Primary Key Properties

While primary keys are referenced from feature tables, they are separate entities with their own properties defined in the `mlPrimaryKeyProperties` aspect. Understanding primary key metadata is essential for proper feature table usage:

#### Data Type

Primary keys have a data type (defined using `MLFeatureDataType`) that specifies the type of values:

- `ORDINAL`: Integer values
- `NOMINAL`: Categorical values
- `BINARY`: Boolean values
- `COUNT`: Count values
- `TIME`: Timestamp values
- `TEXT`: String values
- Other numeric types like `CONTINUOUS`, `INTERVAL`

#### Source Lineage

Primary keys can declare their source datasets through the `sources` property. This creates lineage relationships showing which upstream datasets the primary key values are derived from. This is crucial for understanding data provenance and impact analysis.

#### Versioning

Primary keys support versioning through the `version` property, allowing teams to track changes to key definitions over time and maintain multiple versions in parallel.

### Tags and Glossary Terms

Like other DataHub entities, ML Feature Tables support tags and glossary terms for classification and discovery:

- Tags (via `globalTags` aspect) provide lightweight categorization
- Glossary Terms (via `glossaryTerms` aspect) link to business definitions and concepts

Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand when to use tags vs terms.

### Ownership

Ownership is associated with feature tables using the `ownership` aspect. Owners can be individuals or teams responsible for maintaining the feature table. Clear ownership is essential for:

- Knowing who to contact with questions about features
- Understanding responsibility for feature quality and updates
- Governance and access control decisions

### Domains and Organization

Feature tables can be organized into domains (via the `domains` aspect) to represent organizational structure or functional areas. This helps teams manage large feature catalogs by grouping related feature tables together.

## Code Examples

### Creating a Complete ML Feature Table

Here's a comprehensive example that creates a feature table with all core aspects:

<details>
<summary>Python SDK: Create a complete ML Feature Table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_table_create_complete.py show_path_as_comment }}
```

</details>

### Querying ML Feature Tables

You can retrieve ML Feature Table metadata using both the Python SDK and REST API:

<details>
<summary>Python SDK: Read an ML Feature Table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_table_read.py show_path_as_comment }}
```

</details>

<details>
<summary>REST API: Fetch ML Feature Table metadata</summary>

```bash
# Get the complete entity with all aspects
curl 'http://localhost:8080/entities/urn%3Ali%3AmlFeatureTable%3A(urn%3Ali%3AdataPlatform%3Afeast,users_feature_table)'

# Get relationships to see features and primary keys
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AmlFeatureTable%3A(urn%3Ali%3AdataPlatform%3Afeast,users_feature_table)&types=Contains,KeyedBy'
```

</details>

## Integration Points

ML Feature Tables integrate with multiple other entities in DataHub's metadata model:

### Relationships with ML Features

Feature tables contain ML Features through the "Contains" relationship. Each feature in the `mlFeatures` array represents an individual feature that can be:

- Used independently by ML models
- Have its own metadata, lineage, and documentation
- Shared across multiple feature tables in some feature store implementations

Navigation works bidirectionally - from feature table to features, and from features back to their parent tables.

### Relationships with ML Primary Keys

Feature tables reference ML Primary Keys through the "KeyedBy" relationship. Primary keys:

- Define the entity granularity of the feature table
- Enable joining features with entity identifiers in training datasets
- Can be shared across multiple feature tables when they represent the same entity type
- Have their own lineage to upstream datasets through the `sources` property

### Relationships with ML Models

While not directly referenced in feature table metadata, ML Models consume features through the `mlFeatures` property in `MLModelProperties`. This creates a "Consumes" lineage relationship showing which models use features from a particular feature table. This lineage enables:

- Understanding downstream impact when feature tables change
- Discovering which models depend on specific feature tables
- Tracking feature usage and adoption across models

### Relationships with Datasets

Feature tables have indirect relationships to datasets through two paths:

1. **Via ML Features**: Individual features can declare source datasets through their `sources` property, creating "DerivedFrom" lineage
2. **Via ML Primary Keys**: Primary keys can declare source datasets, showing where entity identifiers originate

This lineage connects the feature store to upstream data warehouses, enabling end-to-end data lineage from raw data to model predictions.

### Platform Integration

Feature tables are associated with a specific data platform (e.g., Feast, Tecton) through the `platform` property in the key aspect. This creates a "SourcePlatform" relationship that:

- Identifies which feature store system hosts the feature table
- Enables filtering and organization by platform
- Supports multi-platform feature store environments

## Notable Exceptions

### Feature Store Platform Variations

Different feature store platforms have different capabilities and concepts:

- **Feast**: Uses the term "feature table" directly. Feature tables in Feast correspond 1:1 with this entity.
- **Tecton**: Uses "feature views" and "feature services" as similar concepts. These can be modeled as feature tables.
- **SageMaker Feature Store**: Uses "feature groups" which map to feature tables.
- **Databricks Feature Store**: Uses "feature tables" but with database.schema.table naming patterns.

When ingesting from these platforms, ensure the naming conventions match the platform's terminology for consistency.

### Custom Properties Usage

Unlike datasets which have both `datasetProperties` and `editableDatasetProperties`, feature tables have:

- `mlFeatureTableProperties`: The main properties aspect (usually from ingestion)
- `editableMlFeatureTableProperties`: UI-editable description only

For custom metadata, use the `customProperties` map in `mlFeatureTableProperties` rather than creating custom aspects.

### Entity References vs. Entity Creation

When using the SDK to create feature tables:

- You **must create the referenced entities first**: Create individual ML Features and ML Primary Keys before referencing them in the feature table
- The feature table only stores URN references - it doesn't create the feature or primary key entities
- If you reference non-existent entities, they will appear as broken references in the UI

This is different from some other DataHub entities where child entities can be created inline.

### Lineage Considerations

Feature table lineage is typically established through the features and primary keys it contains:

- Feature tables themselves don't have direct `upstreamLineage` aspects
- Instead, lineage flows through the contained features' `sources` properties
- When querying lineage, you'll need to traverse through the "Contains" relationships to find upstream datasets

This design reflects that features are the atomic unit of lineage in ML systems, while feature tables are organizational constructs.
