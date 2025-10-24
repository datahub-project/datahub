# Data Product

Data Products are curated collections of data assets designed for easy discovery and consumption. They represent an innovative way to organize and package related data assets such as Tables, Dashboards, Charts, Pipelines, and other entities within DataHub. Data Products are a key concept in data mesh architecture, where they serve as independent units of data managed by specific domain teams.

Unlike other entities in DataHub that typically represent technical assets in source systems, Data Products are a DataHub-invented concept that provides a logical grouping mechanism for organizing assets into consumable offerings.

## Identity

Data Products are identified by a single field:

- **id**: A unique identifier for the Data Product, typically a human-readable string such as `pet_of_the_week` or `customer_360`.

An example of a Data Product identifier is `urn:li:dataProduct:pet_of_the_week`.

The simplicity of the identifier makes Data Products easy to create and reference, as they don't need to be tied to any particular platform or technology.

## Important Capabilities

### Data Product Properties

The core properties of a Data Product are captured in the `dataProductProperties` aspect, which includes:

- **name**: The display name of the Data Product, which is searchable and used for autocomplete
- **description**: Documentation describing what the Data Product offers and how to use it
- **assets**: A list of data assets that are part of this Data Product, with each asset having an optional `outputPort` flag

#### Asset Associations

Data Products can contain a wide variety of asset types as defined in the `dataProductProperties` aspect:

- Datasets (tables, views, streams)
- Data Jobs and Data Flows (pipelines)
- Dashboards and Charts (visualizations)
- Notebooks
- Containers (schemas, databases)
- ML Models, ML Model Groups, ML Feature Tables, ML Features, and ML Primary Keys

Each asset association can be marked as an **output port**, which in data mesh terminology represents a data asset that is intended to be shared and consumed by other teams. This allows Data Product owners to distinguish between:

- **Internal assets**: Data used internally within the Data Product for processing
- **Output ports**: Data explicitly published for external consumption

The following code snippet shows how to create a Data Product with multiple assets, including marking one as an output port.

<details>
<summary>Python: Create a Data Product with assets</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataproduct_create_sdk.py show_path_as_comment }}
```

</details>

### Asset Settings

The `assetSettings` aspect allows Data Products to configure custom settings, such as custom asset summary configurations. This aspect is shared with other organizational entities like Domains and Glossary Terms, providing a consistent way to customize how assets are displayed and summarized.

### Tags and Glossary Terms

Data Products support Tags and Glossary Terms, allowing you to categorize and document your data offerings. Tags can be used for informal categorization (e.g., "adoption", "experimental"), while Glossary Terms provide formal business vocabulary linkage.

Here is an example of adding metadata to a Data Product:

<details>
<summary>Python SDK: Add tags and terms to a Data Product</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataproduct_add_metadata.py show_path_as_comment }}
```

</details>

### Ownership

Data Products support ownership through the `ownership` aspect. Owners can be individuals or groups, and can have different ownership types (BUSINESS_OWNER, TECHNICAL_OWNER, DATA_STEWARD, etc.). When a Data Product is created through the UI, the creator is automatically added as an owner.

Ownership helps establish accountability and makes it clear who is responsible for maintaining the Data Product and ensuring data quality.

### Domains

Every Data Product must belong to exactly one Domain. This is a core organizational principle in DataHub's Data Product model - Data Products cannot exist independently but must be associated with a Domain that represents the business area or team responsible for the Data Product.

The Domain association is captured in the `domains` aspect and is enforced by the UI and API when creating Data Products.

### Documentation and Institutional Memory

Data Products can have rich documentation beyond the basic description field:

- **institutionalMemory**: Links to external resources like Confluence pages, Google Docs, or other documentation
- **forms**: Structured documentation through DataHub's Forms feature
- **structuredProperties**: Custom metadata fields defined by your organization

### Adding Assets to a Data Product

Assets can be associated with a Data Product in two ways:

1. **From the Data Product page**: Use the "Add Assets" button to search for and add multiple assets at once
2. **From the Asset page**: Use the "Set Data Product" option in the asset's sidebar to add it to a Data Product

<details>
<summary>Python SDK: Add assets to an existing Data Product</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataproduct_add_assets.py show_path_as_comment }}
```

</details>

### Querying Data Products

Data Products can be queried using the REST API to retrieve their properties and associated assets.

<details>
<summary>Query a Data Product via REST API</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataproduct_query_rest.py show_path_as_comment }}
```

</details>

## Integration Points

Data Products integrate with several key areas of DataHub:

### Relationship to Domains

Data Products must belong to a Domain, creating a hierarchical organization:

```
Domain (e.g., "Marketing")
  └── Data Product (e.g., "Customer 360")
      ├── Dataset: customer_profile
      ├── Dataset: customer_transactions
      ├── Dashboard: customer_overview
      └── DataFlow: customer_pipeline
```

This hierarchy allows organizations to implement data mesh principles where each domain owns and manages its Data Products.

### Relationship to Assets

Data Products create a `DataProductContains` relationship with their assets. This relationship is bidirectional:

- From the Data Product, you can see all contained assets
- From any asset, you can see which Data Product(s) it belongs to

An asset can belong to multiple Data Products, allowing for flexible organization schemes (e.g., an asset could be part of both a "Customer 360" product and a "Marketing Analytics" product).

### Authorization and Access Control

DataHub provides fine-grained permissions for Data Products:

- **Manage Data Product**: Required to create/delete Data Products within a Domain
- **Edit Data Product**: Required to add/remove assets from a Data Product

These privileges can be granted through [Metadata Policies](../../../authorization/policies.md), allowing organizations to control who can create and modify Data Products.

### GraphQL API

The DataHub GraphQL API provides several mutations for working with Data Products:

- `createDataProduct`: Create a new Data Product within a Domain
- `updateDataProduct`: Update Data Product properties
- `deleteDataProduct`: Delete a Data Product
- `batchSetDataProduct`: Add or remove multiple assets from a Data Product
- `listDataProductAssets`: Query assets belonging to a Data Product

### Search and Discovery

Data Products are searchable entities in DataHub. The `name` and `description` fields are indexed, and Data Products can be filtered by:

- Domain
- Ownership
- Tags
- Glossary Terms
- Structured Properties

This makes it easy for data consumers to discover relevant Data Products across the organization.

## Notable Exceptions

### Domain Requirement

Unlike many other entities in DataHub, Data Products have a hard requirement to belong to a Domain. This is by design to support data mesh principles where every Data Product must have a clear organizational owner. You cannot create a Data Product without first having a Domain to associate it with.

### Output Ports

The `outputPort` flag on asset associations is a forward-looking feature aligned with data mesh principles. While the flag can be set today, advanced features around output ports (such as differentiated access control or versioning) are still being developed. The current roadmap includes:

- Support for marking data assets in a Data Product as private versus shareable
- Support for declaring data lineage manually between Data Products
- Support for declaring logical schemas for Data Products
- Support for associating data contracts with Data Products
- Support for semantic versioning of Data Products

### YAML-based Management

DataHub supports managing Data Products as code through YAML files. This enables GitOps workflows where Data Product definitions are version-controlled and deployed through CI/CD pipelines. The `datahub` CLI provides commands to:

- `datahub dataproduct upsert`: Create or update Data Products from YAML
- `datahub dataproduct diff`: Compare YAML with current state
- `datahub dataproduct delete`: Remove Data Products

This allows for a hybrid model where business users can manage Data Products through the UI while technical teams can use infrastructure-as-code practices.

### Multi-Asset Membership

Unlike some organizational constructs in other systems, an asset in DataHub can belong to multiple Data Products simultaneously. This flexibility supports different organizational perspectives - for example, a dataset might be part of a domain-specific product while also being included in a cross-functional analytics product.
