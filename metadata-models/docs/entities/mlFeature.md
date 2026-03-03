# ML Feature

The ML Feature entity represents an individual input variable used by machine learning models. Features are the building blocks of feature engineering - they transform raw data into meaningful signals that ML algorithms can learn from. In modern ML systems, features are first-class citizens that can be discovered, documented, versioned, and reused across multiple models and teams.

## Identity

ML Features are identified by two pieces of information:

- The feature namespace: A logical grouping or namespace that organizes related features together. This often corresponds to a feature table name, domain area, or team. Examples include `user_features`, `transaction_features`, `product_features`.
- The feature name: The unique name of the feature within its namespace. Examples include `age`, `lifetime_value`, `days_since_signup`.

An example of an ML Feature identifier is `urn:li:mlFeature:(user_features,age)`.

The identity is defined by the `mlFeatureKey` aspect, which contains:

- `featureNamespace`: A string representing the logical namespace or grouping for the feature
- `name`: The unique name of the feature within that namespace

### URN Structure Examples

```
urn:li:mlFeature:(user_features,age)
urn:li:mlFeature:(user_features,lifetime_value)
urn:li:mlFeature:(transaction_features,amount_last_7d)
urn:li:mlFeature:(product_features,price)
urn:li:mlFeature:(product_features,category_embedding)
```

The namespace and name together form a globally unique identifier. Multiple features can share the same namespace (representing a logical grouping), but each feature name must be unique within its namespace.

## Important Capabilities

### Feature Properties

ML Features support comprehensive metadata through the `mlFeatureProperties` aspect. This aspect captures the essential characteristics that define a feature:

#### Description and Documentation

Features should have clear descriptions explaining what they represent, how they're calculated, and when they should be used. Good feature documentation is critical for:

- Helping data scientists discover relevant features for their models
- Preventing duplicate feature creation
- Understanding feature semantics and appropriate use cases
- Facilitating feature reuse across teams

<details>
<summary>Python SDK: Create an ML Feature with description</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_create_with_description.py show_path_as_comment }}
```

</details>

#### Data Type

Features have a data type specified using `MLFeatureDataType` that describes the nature of the feature values. Understanding data type is essential for proper feature handling, preprocessing, and model training. DataHub supports a rich taxonomy of data types:

**Categorical Types:**

- `NOMINAL`: Discrete values with no inherent order (e.g., country, product category)
- `ORDINAL`: Discrete values that can be ranked (e.g., education level, rating)
- `BINARY`: Two-category values (e.g., is_subscriber, has_clicked)

**Numeric Types:**

- `CONTINUOUS`: Real-valued numeric data (e.g., height, price, temperature)
- `COUNT`: Non-negative integer counts (e.g., number of purchases, page views)
- `INTERVAL`: Numeric data with equal spacing (e.g., percentages, scores)

**Temporal:**

- `TIME`: Time-based cyclical features (e.g., hour_of_day, day_of_week)

**Unstructured:**

- `TEXT`: Text data requiring NLP processing
- `IMAGE`: Image data
- `VIDEO`: Video data
- `AUDIO`: Audio data

**Structured:**

- `MAP`: Dictionary or mapping structures
- `SEQUENCE`: Lists, arrays, or sequences
- `SET`: Unordered collections
- `BYTE`: Binary-encoded complex objects

**Special:**

- `USELESS`: High-cardinality unique values with no predictive relationship (e.g., random IDs)
- `UNKNOWN`: Type is not yet determined

<details>
<summary>Python SDK: Create features with different data types</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_create_with_datatypes.py show_path_as_comment }}
```

</details>

#### Source Lineage

One of the most powerful capabilities of ML Features in DataHub is their ability to declare source datasets through the `sources` property. This creates explicit "DerivedFrom" lineage relationships between features and the upstream datasets they're computed from.

Source lineage enables:

- **End-to-end traceability**: Track a model prediction back to the raw data that generated its features
- **Impact analysis**: Understand which features (and downstream models) are affected when a dataset changes
- **Data quality**: Identify the root cause when feature values appear incorrect
- **Compliance**: Document data provenance for regulatory requirements
- **Discovery**: Find all features derived from a particular dataset

<details>
<summary>Python SDK: Add source lineage to a feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_add_source_lineage.py show_path_as_comment }}
```

</details>

#### Versioning

Features support versioning through the `version` property. Version information helps teams:

- Track breaking changes to feature definitions or calculations
- Maintain multiple feature versions during migration periods
- Understand which feature version a model was trained with
- Coordinate feature rollouts across training and serving systems

<details>
<summary>Python SDK: Create a versioned feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_create_versioned.py show_path_as_comment }}
```

</details>

#### Custom Properties

Features support arbitrary key-value custom properties through the `customProperties` field, allowing you to capture platform-specific or organization-specific metadata:

- Feature importance scores
- Update frequency or freshness SLAs
- Cost or compute requirements
- Feature store specific configuration
- Team or project ownership details

### Tags and Glossary Terms

ML Features support tags and glossary terms for classification, discovery, and governance:

- Tags (via `globalTags` aspect) provide lightweight categorization such as PII indicators, feature maturity levels, or domain areas
- Glossary Terms (via `glossaryTerms` aspect) link features to standardized business definitions and concepts

Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand when to use tags vs terms.

<details>
<summary>Python SDK: Add tags and terms to a feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_add_tags_terms.py show_path_as_comment }}
```

</details>

### Ownership

Ownership is associated with features using the `ownership` aspect. Clear feature ownership is essential for:

- Knowing who to contact with questions about feature behavior
- Understanding responsibility for feature quality and updates
- Managing access control and governance
- Coordinating feature changes across teams

<details>
<summary>Python SDK: Add ownership to a feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_add_ownership.py show_path_as_comment }}
```

</details>

### Domains and Organization

Features can be organized into domains (via the `domains` aspect) to represent organizational structure or functional areas. Domain organization helps teams:

- Manage large feature catalogs by grouping related features
- Apply consistent governance policies to related features
- Facilitate discovery within organizational boundaries
- Track feature adoption by business unit

## Code Examples

### Creating a Complete ML Feature

Here's a comprehensive example that creates a feature with all core metadata:

<details>
<summary>Python SDK: Create a complete ML Feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_create_complete.py show_path_as_comment }}
```

</details>

### Linking Features to Feature Tables

Features are typically organized into feature tables. While the feature entity itself doesn't directly reference its parent table (the relationship is inverse - tables reference features), you can discover the containing table through relationships:

<details>
<summary>Python SDK: Find feature table containing a feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_find_table.py show_path_as_comment }}
```

</details>

### Querying ML Features

You can retrieve ML Feature metadata using both the Python SDK and REST API:

<details>
<summary>Python SDK: Read an ML Feature</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_read.py show_path_as_comment }}
```

</details>

<details>
<summary>REST API: Fetch ML Feature metadata</summary>

```bash
# Get the complete entity with all aspects
curl 'http://localhost:8080/entities/urn%3Ali%3AmlFeature%3A(user_features,age)'

# Get relationships to see source datasets and consuming models
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AmlFeature%3A(user_features,age)&types=DerivedFrom'
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AmlFeature%3A(user_features,age)&types=Consumes'
```

</details>

### Batch Feature Creation

When creating many features at once (e.g., from a feature store ingestion connector), batch operations improve performance:

<details>
<summary>Python SDK: Create multiple features efficiently</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_create_batch.py show_path_as_comment }}
```

</details>

## Integration Points

ML Features integrate with multiple other entities in DataHub's metadata model to form a comprehensive ML metadata ecosystem:

### Relationships with Datasets

Features declare their source datasets through the `sources` property in `mlFeatureProperties`. This creates a "DerivedFrom" lineage relationship that:

- Shows which raw data tables feed into each feature
- Enables impact analysis when datasets change
- Provides end-to-end lineage from data warehouse to model predictions
- Supports data quality root cause analysis

The relationship is directional: features point to their source datasets. Multiple features can derive from the same dataset, and a single feature can derive from multiple datasets if it's computed via a join or union.

### Relationships with ML Models

ML Models consume features through the `mlFeatures` property in `MLModelProperties`. This creates a "Consumes" lineage relationship showing:

- Which features are used by each model
- Which models depend on a particular feature
- The complete set of inputs for model training and inference
- Impact analysis for feature changes on downstream models

This relationship enables critical use cases like:

- **Feature usage tracking**: Identify unused features that can be deprecated
- **Model impact analysis**: Find all models affected when a feature changes
- **Feature importance correlation**: Link model performance to feature changes
- **Compliance documentation**: Show exactly what data influences model decisions

<details>
<summary>Python SDK: Link features to a model</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_add_to_mlmodel.py show_path_as_comment }}
```

</details>

### Relationships with ML Feature Tables

Feature tables contain ML Features through the "Contains" relationship. The feature table's `mlFeatures` property lists the URNs of features it contains. This relationship:

- Organizes features into logical groupings
- Enables navigation from table to features and back
- Represents the physical or logical organization in the feature store
- Helps discover related features that share characteristics

While features don't explicitly store their parent table, you can discover it by querying incoming "Contains" relationships.

<details>
<summary>Python SDK: Add a feature to a feature table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlfeature_add_to_mlfeature_table.py show_path_as_comment }}
```

</details>

### Platform Integration

Features are often associated with a platform through their namespace or through related entities (feature tables). While features themselves don't have a direct platform reference in their key, the namespace often encodes platform-specific organization, and related feature tables declare their platform explicitly.

### GraphQL Resolvers

Features are accessible through DataHub's GraphQL API via the `MLFeatureType` class. The GraphQL interface provides:

- Search and discovery capabilities for features
- Autocomplete for feature names during searches
- Batch loading of feature metadata
- Filtering features by properties and relationships

## Notable Exceptions

### Feature Namespace vs Feature Table

The `featureNamespace` in the feature key is a logical grouping concept and doesn't necessarily correspond 1:1 with feature tables:

- **In many feature stores**: The namespace matches the feature table name. A feature table named `user_features` contains features with namespace `user_features`.
- **In some systems**: The namespace might represent a broader domain or project, with multiple feature tables sharing a namespace.
- **Best practice**: Use consistent namespace naming that aligns with your feature table organization for clarity.

When ingesting features, ensure namespace values match the corresponding feature table names for proper relationship establishment.

### Feature Identity and Feature Tables

A feature's identity (`featureNamespace` + `name`) is independent of any feature table. This means:

- The same feature URN could theoretically be referenced by multiple feature tables (though this is uncommon)
- Changing a feature's containing table requires updating the table's metadata, not the feature itself
- Features can exist without being part of any feature table (though this reduces discoverability)

Most feature stores enforce 1:1 relationships between features and feature tables to avoid ambiguity.

### Versioning Strategies

There are multiple approaches to versioning features:

**Option 1: Version in the URN (namespace or name)**

```
urn:li:mlFeature:(user_features_v2,age)
urn:li:mlFeature:(user_features,age_v2)
```

- Pros: Each version is a separate entity with independent metadata
- Cons: Harder to track version history; requires manual version management

**Option 2: Version in the properties**

```python
MLFeatureProperties(
    description="User age in years",
    version=VersionTag(versionTag="2.0")
)
```

- Pros: Version history attached to single entity; easier lineage tracking
- Cons: Point-in-time queries are harder; version changes mutate entity

**Recommendation**: Use the `version` property in `mlFeatureProperties` for most use cases. Only use versioned URNs when breaking changes require fully separate entities (e.g., changing data type from continuous to categorical).

### Composite Features and Feature Engineering

Composite features (features derived from other features) can be modeled in two ways:

**Approach 1: Intermediate features as entities**
Create explicit feature entities for each transformation step, with lineage between them:

```
raw_feature -> transformed_feature -> composite_feature
```

**Approach 2: Direct source lineage**
Skip intermediate features and link composite features directly to source datasets, documenting the transformation in the description.

Choose Approach 1 when:

- Intermediate features are reused by multiple downstream features/models
- You need to track transformations explicitly for governance
- Feature engineering pipelines are complex and multi-stage

Choose Approach 2 when:

- Transformations are simple and one-off
- Intermediate features have no independent value
- You want to reduce metadata entity count

### Feature Drift and Monitoring

While DataHub's ML Feature entity doesn't include built-in drift monitoring aspects, you can use:

- **Custom Properties**: Store drift metrics or monitoring status
- **Tags**: Apply tags like `HIGH_DRIFT_DETECTED` or `MONITORING_ENABLED`
- **Documentation**: Link to external monitoring dashboards via `institutionalMemory`
- **External Systems**: Integrate with specialized feature monitoring tools and reference them in feature metadata

Feature drift detection typically happens in runtime feature stores or model monitoring systems, with DataHub serving as the metadata catalog that links to those systems.

### Search and Discovery

Features are searchable by:

- Name (with autocomplete)
- Namespace (partial text search)
- Description (full text search)
- Tags and glossary terms
- Owner
- Source datasets (via lineage)

The `name` field has the highest search boost score (8.0), making feature name the primary discovery mechanism. Ensure feature names are descriptive and follow consistent naming conventions across your organization.
