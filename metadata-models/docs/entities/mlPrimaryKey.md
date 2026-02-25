# MLPrimaryKey

MLPrimaryKey represents a primary key entity within a machine learning feature store. Primary keys uniquely identify records in feature tables and are essential for joining features with entities in online and offline feature serving. In feature stores like Feast, Tecton, or AWS SageMaker Feature Store, primary keys define the identifier columns that link features to the entities they describe (e.g., `user_id`, `product_id`, `transaction_id`).

## Identity

MLPrimaryKeys are identified by two pieces of information:

- **Feature Namespace**: A logical grouping or namespace for the primary key, typically corresponding to a feature table or feature group. This allows for organizational hierarchy and prevents naming conflicts across different feature sets.
- **Primary Key Name**: The specific name of the primary key within the namespace. This is the identifier that would be used in the feature store to reference this key.

An example of an MLPrimaryKey identifier is `urn:li:mlPrimaryKey:(users_feature_table,user_id)`.

The URN structure follows this pattern:

```
urn:li:mlPrimaryKey:(<feature_namespace>,<primary_key_name>)
```

Where:

- `<feature_namespace>` is the namespace, often matching the feature table name
- `<primary_key_name>` is the unique name of the primary key

For example:

- `urn:li:mlPrimaryKey:(users_feature_table,user_id)` - User ID in a user features table
- `urn:li:mlPrimaryKey:(product_features,product_id)` - Product ID in a product features table
- `urn:li:mlPrimaryKey:(transactions,transaction_id)` - Transaction ID in a transactions feature table

## Important Capabilities

### Primary Key Properties

The core metadata about an MLPrimaryKey is stored in the `mlPrimaryKeyProperties` aspect. This includes:

- **Description**: Documentation explaining what this primary key represents, what entities it identifies, and how it should be used in feature serving.
- **Data Type**: The data type of the primary key (e.g., TEXT, NUMERIC, BOOLEAN, BYTE, etc.). This corresponds to the MLFeatureDataType enum and helps with validation and type checking during feature serving.
- **Version**: A version tag that can track the evolution of the primary key definition over time. This is useful when primary key schemas change or when multiple versions need to coexist.
- **Sources**: URN references to upstream dataset entities that this primary key is derived from. This creates lineage connections between your data warehouse tables and your ML feature store, establishing data provenance.
- **Custom Properties**: Additional key-value pairs for platform-specific metadata that doesn't fit into standard fields.

The following code snippet shows you how to create an MLPrimaryKey with properties:

<details>
<summary>Python SDK: Create an MLPrimaryKey</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlprimarykey_create.py show_path_as_comment }}
```

</details>

### Editable Properties

Like other DataHub entities, MLPrimaryKeys separate ingested metadata from user-edited metadata. The `editableMlPrimaryKeyProperties` aspect allows users to enhance the metadata through the DataHub UI without interfering with automated ingestion:

- **Description**: A user-provided description that can supplement or override the description from the ingestion source.

This separation ensures that:

- User edits are preserved across ingestion runs
- Source system metadata remains authoritative for its fields
- Documentation can be improved incrementally by data practitioners

### Data Lineage

MLPrimaryKeys support lineage tracking through their `sources` field. By linking primary keys to upstream datasets, you can:

- **Trace Data Origins**: Understand which warehouse tables or data sources provide the key values
- **Impact Analysis**: Identify downstream ML models and feature tables affected by changes to source data
- **Data Quality Monitoring**: Track data quality issues from source systems through to feature stores
- **Compliance and Auditing**: Document the complete data flow from raw data to ML features

The lineage relationships created are of type `DerivedFrom` and explicitly marked as lineage relationships (`isLineage: true`), ensuring they appear in DataHub's lineage visualization.

### Tags and Glossary Terms

MLPrimaryKeys can have Tags or Terms attached to them through the `globalTags` and `glossaryTerms` aspects. This enables:

- **Classification**: Tag primary keys with security classifications (e.g., `pii`, `sensitive`)
- **Organization**: Group related primary keys with project or domain tags
- **Business Context**: Link primary keys to business glossary terms to provide business context

Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms.

### Ownership

Ownership is associated with an MLPrimaryKey using the `ownership` aspect. Owners can be data scientists, ML engineers, or feature store administrators responsible for maintaining the primary key definition. Ownership helps with:

- **Accountability**: Clear ownership for maintaining key definitions
- **Access Control**: Integration with DataHub policies for permission management
- **Contact Information**: Quick identification of who to ask about a primary key

### Domains and Data Products

MLPrimaryKeys support the `domains` aspect, allowing them to be organized into logical business domains or data products. This helps with:

- **Organizational Structure**: Group ML assets by team, department, or business unit
- **Discovery**: Filter and search for primary keys within specific domains
- **Governance**: Apply domain-specific policies and ownership models

### Structured Properties

MLPrimaryKeys support the `structuredProperties` aspect, allowing organizations to extend the metadata model with custom fields that are validated and searchable. This enables:

- **Custom Metadata**: Add organization-specific fields beyond standard properties
- **Validation**: Enforce data quality on custom metadata
- **Advanced Search**: Filter and search on custom properties

## Code Examples

### Creating an MLPrimaryKey

<details>
<summary>Python SDK: Create an MLPrimaryKey with lineage</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlprimarykey_create.py show_path_as_comment }}
```

</details>

### Reading MLPrimaryKey Information

<details>
<summary>Python SDK: Read MLPrimaryKey using the v2 SDK</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlprimarykey_read.py show_path_as_comment }}
```

</details>

### Adding MLPrimaryKeys to Feature Tables

MLPrimaryKeys are typically associated with feature tables to define how records should be uniquely identified. A feature table can have one or more primary keys (composite keys).

<details>
<summary>Python SDK: Add primary keys to a feature table</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlprimarykey_add_to_mlfeature_table.py show_path_as_comment }}
```

</details>

### Querying MLPrimaryKey via REST API

The standard REST APIs can be used to retrieve MLPrimaryKey metadata and relationships.

<details>
<summary>REST API: Fetch MLPrimaryKey entity information</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlprimarykey_query_rest.py show_path_as_comment }}
```

</details>

## Integration Points

### Relationship with MLFeatureTable

The most important relationship for MLPrimaryKeys is with MLFeatureTables. Feature tables reference primary keys through their `mlPrimaryKeyProperties` aspect, creating a `KeyedBy` relationship. This relationship indicates that:

- The feature table uses these primary key(s) to uniquely identify records
- When serving features, these keys are used for lookups and joins
- Multiple primary keys on a table form a composite key

This bidirectional relationship enables:

- **Forward Navigation**: From a feature table, see all its primary keys
- **Reverse Navigation**: From a primary key, see all feature tables that use it
- **Reusability**: The same primary key can be shared across multiple feature tables

### Relationship with Datasets

MLPrimaryKeys can be linked to Dataset entities through the `sources` field in `mlPrimaryKeyProperties`. This creates `DerivedFrom` lineage relationships to upstream data warehouse tables, establishing:

- **Data Provenance**: Track where primary key values originate
- **Impact Analysis**: Understand how changes to source tables affect the feature store
- **Cross-Platform Lineage**: Connect data warehouse assets to ML platform assets

### Relationship with MLFeatures

While not a direct relationship, MLPrimaryKeys and MLFeatures both belong to the same feature namespace (typically a feature table). Primary keys identify the entity, while features provide the attributes of that entity. Together, they form the complete feature table schema.

### Search and Discovery

MLPrimaryKeys are fully indexed for search with the following capabilities:

- **Name Search**: The primary key name is indexed with autocomplete support and high relevance boosting
- **Namespace Search**: The feature namespace is searchable with partial matching
- **Description Search**: Full-text search on descriptions (when present)
- **Relationship Search**: Find primary keys by their associated feature tables or source datasets

### Platform Instance Support

MLPrimaryKeys support the `dataPlatformInstance` aspect, which is useful when:

- Multiple feature store instances exist (e.g., dev, staging, prod)
- The same logical primary key exists in different environments
- Organizations need to track metadata separately per instance

## Notable Exceptions

### Composite Primary Keys

When a feature table requires multiple columns to uniquely identify a record, it uses composite primary keys. In DataHub:

- Create separate MLPrimaryKey entities for each column in the composite key
- Link all of them to the feature table via the `mlPrimaryKeys` array in `MLFeatureTableProperties`
- The order in the array may be significant for some feature stores (e.g., for indexing optimization)

Example:

```python
# For a feature table keyed by (user_id, date)
primary_keys = [
    "urn:li:mlPrimaryKey:(daily_user_features,user_id)",
    "urn:li:mlPrimaryKey:(daily_user_features,date)"
]
```

### Primary Key vs. Entity Key vs. Join Key

Different feature stores use different terminology:

- **Feast**: Uses "entity" to refer to what DataHub calls a primary key
- **Tecton**: Uses "entity keys" and "join keys"
- **SageMaker Feature Store**: Uses "record identifier"
- **Databricks Feature Store**: Uses "primary keys"

DataHub normalizes these concepts under the `mlPrimaryKey` entity type. When ingesting from different platforms, connectors map these platform-specific terms to MLPrimaryKey.

### Primary Keys as Features

In some feature stores, primary keys can also serve as features themselves (e.g., using `user_id` as both the key and a feature for training). In DataHub:

- Create both an MLPrimaryKey entity for the identifier role
- Create an MLFeature entity for the feature role
- Both can reference the same source dataset column

This dual representation accurately reflects the different roles the same data plays in the feature store.

### Namespace Consistency

The feature namespace in an MLPrimaryKey URN should typically match the feature table name where it's used. However, DataHub doesn't enforce this requirement, allowing for flexibility in cases where:

- Primary keys are shared across multiple feature tables
- Organizations use different namespacing schemes
- Platform-specific naming conventions differ from logical groupings

### Data Type Evolution

Primary key data types should remain stable to avoid breaking feature serving. However, if a type change is necessary:

- Consider creating a new MLPrimaryKey with a versioned name
- Use the `version` field to track the schema evolution
- Maintain both old and new primary keys during migration periods
- Update feature tables to reference the new primary key once migration is complete

### Primary Keys and Privacy

Primary keys often contain or directly map to personally identifiable information (PII). Organizations should:

- Apply appropriate tags (e.g., `pii`, `gdpr_sensitive`) to MLPrimaryKey entities
- Document any hashing or encryption applied to key values
- Use DataHub policies to control who can view primary key metadata
- Link to upstream dataset entities that may have additional privacy metadata
