# Container

The container entity is a core entity in the metadata model that represents a grouping of related data assets. Containers provide hierarchical organization for datasets, charts, dashboards, and other containers, enabling navigation and structure discovery within data platforms.

## Identity

Containers are uniquely identified by a GUID (Globally Unique Identifier) that is typically derived from a combination of attributes specific to the container type. Unlike datasets which use platform, name, and environment, containers use a more flexible identification scheme based on their hierarchical properties.

The URN structure for a container is: `urn:li:container:{guid}`

The GUID is typically computed from container-specific properties such as:

- **Database containers**: platform + instance + database name
- **Schema containers**: platform + instance + database + schema name
- **Project containers**: platform + instance + project_id
- **Folder containers**: platform + instance + folder_abs_path
- **Bucket containers**: platform + instance + bucket_name

### URN Examples

```
urn:li:container:b5e95fce839e7d78151ed7e0a7420d84
```

The GUID is generated using the `datahub_guid()` function from a dictionary of properties. For example, a Snowflake schema container would be identified by:

```python
{
  "platform": "snowflake",
  "instance": "prod_instance",
  "database": "analytics",
  "schema": "reporting"
}
```

## Real-World Concepts

Containers represent various hierarchical structures in data platforms:

- **Databases**: Top-level organizational units in relational systems (MySQL, PostgreSQL, Snowflake)
- **Schemas**: Logical groupings within databases (Snowflake schemas, PostgreSQL schemas)
- **Projects**: Organizational units in cloud platforms (BigQuery projects)
- **Datasets**: Logical groupings in cloud platforms (BigQuery datasets)
- **Folders**: Directory structures in file systems and data lakes (S3 folders, ADLS directories)
- **Buckets**: Top-level storage containers in cloud object stores (S3 buckets, GCS buckets)
- **Workspaces**: Organizational units in BI platforms (Power BI workspaces, Tableau sites)
- **Catalogs**: Top-level organizational units in data catalogs (Unity Catalog, Iceberg catalogs)
- **Metastores**: Storage metadata repositories (Hive metastore, Unity metastore)

## Important Capabilities

### Container Properties

The `containerProperties` aspect contains metadata inherited from the source system:

- **name**: Display name of the container (required)
- **qualifiedName**: Fully-qualified name (optional, e.g., "prod.analytics.reporting")
- **description**: Description from the source system
- **env**: Environment indicator (PROD, DEV, QA, etc.)
- **customProperties**: Additional key-value properties from the source system
- **externalUrl**: Link to the container in the source system
- **created**: Timestamp when the container was created in the source system
- **lastModified**: Timestamp when the container was last modified in the source system

### Editable Container Properties

The `editableContainerProperties` aspect allows users to override or add information via the UI:

- **description**: User-provided description that supplements or overrides the source system description

This separation ensures that metadata from source systems doesn't conflict with user-provided annotations.

### Hierarchical Relationships

Containers support nested hierarchies through the `container` aspect, which links a container to its parent container. This enables multi-level organizational structures:

```
Platform (implicit)
└── Database Container
    └── Schema Container
        └── Dataset
```

For example, in Snowflake:

```
Snowflake Platform
└── ANALYTICS_DB (Database Container)
    └── REPORTING (Schema Container)
        └── SALES_METRICS (Dataset)
        └── REVENUE_TABLE (Dataset)
```

### Subtypes

The `subTypes` aspect specifies the type of container, which helps the UI render appropriate icons and behaviors. Common subtypes include:

- **Database**: Relational database containers
- **Schema**: Schema-level containers within databases
- **Project**: Cloud project containers (GCP, Azure)
- **Dataset**: BigQuery dataset containers
- **Folder**: File system folders
- **Bucket**: Object storage buckets
- **Workspace**: BI platform workspaces
- **Catalog**: Data catalog containers
- **Metastore**: Metadata storage containers
- **MLflow Experiment** (`MLAssetSubTypes.MLFLOW_EXPERIMENT`): ML experiment containers that organize training runs

### ML Experiments as Containers

Machine learning experiments are modeled as containers with the `MLFLOW_EXPERIMENT` subtype. This pattern enables organizing related training runs (which are `dataProcessInstance` entities) into logical groups for comparison and tracking:

```
ML Experiment (Container)
├── Training Run 1 (DataProcessInstance)
├── Training Run 2 (DataProcessInstance)
└── Training Run 3 (DataProcessInstance)
```

Training runs belong to experiments through the `container` aspect. This structure mirrors common ML platform patterns (like MLflow) and enables:

- Comparing metrics across multiple training attempts
- Tracking the evolution of a model through iterations
- Organizing training work by project or objective

For more information on ML experiments and training runs, see:

- [ML Model entity documentation](mlModel.md#training-runs-and-experiments)
- [DataProcessInstance documentation for training runs](dataProcessInstance.md#tracking-ml-training-run-in-a-container)

### Containable Entities

The following entity types can be contained within a container:

- Datasets
- Charts
- Dashboards
- DataProcessInstances (e.g., training runs in ML experiments)
- Other Containers (for nested hierarchies)

## Code Examples

### Create a Database Container

<details>
<summary>Python SDK: Create a database container</summary>

```python
{{ inline /metadata-ingestion/examples/library/container_create_database.py show_path_as_comment }}
```

</details>

### Create a Schema Container with Parent

<details>
<summary>Python SDK: Create a schema container with parent database</summary>

```python
{{ inline /metadata-ingestion/examples/library/container_create_schema.py show_path_as_comment }}
```

</details>

### Add Metadata to a Container

<details>
<summary>Python SDK: Add tags, terms, and ownership to a container</summary>

```python
{{ inline /metadata-ingestion/examples/library/container_add_metadata.py show_path_as_comment }}
```

</details>

### Query Container via REST API

Containers can be retrieved using the standard entity retrieval APIs:

<details>
<summary>Fetch container entity including all aspects</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Acontainer%3Ab5e95fce839e7d78151ed7e0a7420d84'
```

The response will include all aspects associated with the container, including properties, ownership, tags, terms, etc.

</details>

To find all entities within a container, use the relationships API:

<details>
<summary>Find all entities contained within a container</summary>

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Acontainer%3Ab5e95fce839e7d78151ed7e0a7420d84&types=IsPartOf'
```

This returns all entities (datasets, charts, dashboards, sub-containers) that have this container as their parent.

</details>

## Integration Points

### Relationship with Datasets

Datasets are the most common entities contained within containers. The relationship is established through the `container` aspect on the dataset, which points to the container URN.

```python
# Dataset links to its parent container (schema)
dataset = Dataset(
    platform="snowflake",
    name="analytics_db.reporting.sales_table",
    env="PROD",
    parent_container=schema_key,  # Links to schema container
)
```

### Hierarchical Navigation

Containers enable hierarchical navigation in the DataHub UI through parent-child relationships:

1. **Top-down browsing**: Users can navigate from databases to schemas to tables
2. **Bottom-up breadcrumbs**: Datasets show their parent containers in breadcrumb trails
3. **Browse paths**: Containers are used to generate browse paths automatically

### GraphQL Resolvers

The container entity has specialized GraphQL resolvers:

- **ContainerEntitiesResolver**: Retrieves all entities (datasets, charts, dashboards, sub-containers) within a container
- **ParentContainersResolver**: Retrieves the full hierarchy of parent containers for any entity

These resolvers power the UI's hierarchical navigation and container overview pages.

### Common Usage Patterns

1. **Database/Schema Hierarchy**: Relational databases use Database and Schema containers
2. **Project/Dataset Hierarchy**: BigQuery uses Project and Dataset containers
3. **Workspace/Folder Hierarchy**: BI tools use Workspace containers for organization
4. **Bucket/Folder Hierarchy**: Data lakes use Bucket and Folder containers
5. **Catalog/Schema Hierarchy**: Modern catalogs (Unity, Iceberg) use Catalog and Schema containers

## Notable Exceptions

### GUID Stability

Container GUIDs must remain stable across ingestion runs. Since containers are identified by GUID rather than explicit properties in the URN, changing the GUID computation will create a new container entity instead of updating the existing one.

When creating custom containers, ensure that the properties used to generate the GUID are:

- Stable across time
- Unique within the platform
- Derived from immutable source system identifiers

### Self-Referential Containers

While containers can contain other containers, be careful not to create circular references. The parent-child relationship should form a directed acyclic graph (DAG), not a cycle.

### Environment Handling

The `env` field in ContainerKey has special handling for backwards compatibility. In some sources, the platform instance was incorrectly set to the environment value. The `backcompat_env_as_instance` flag handles this case.

When using the `env` field:

- Set it to a valid FabricType (PROD, DEV, QA, etc.)
- Don't use it for platform instance identification
- Use the separate `instance` field for multi-instance deployments

### Platform Instance Association

Unlike datasets which embed platform instance in their URN, containers associate platform instances through the `dataPlatformInstance` aspect. This allows containers to be associated with specific instances of a data platform while maintaining a stable GUID.

### Access Control

Containers support the `access` aspect, which can be used to model access control policies at the container level. This is particularly useful for:

- Database-level permissions
- Schema-level access control
- Project-level authorization
- Workspace-level security

Access controls set on containers can be inherited by contained entities, though this behavior depends on the specific platform's implementation.
