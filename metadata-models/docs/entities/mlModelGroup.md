# ML Model Group

ML Model Groups represent collections of related machine learning models within an organization's ML infrastructure. They serve as logical containers for organizing model versions, experimental variants, or families of models that share common characteristics. Model groups are essential for managing the lifecycle of ML models, tracking model evolution over time, and organizing models by purpose, architecture, or business function.

## Identity

ML Model Groups are identified by three pieces of information:

- **Platform**: The ML platform or tool where the model group exists. This represents the specific ML technology that hosts the model group. Examples include `mlflow`, `sagemaker`, `databricks`, `kubeflow`, `tensorflow`, `pytorch`, etc. The platform is represented as a URN like `urn:li:dataPlatform:mlflow`.

- **Name**: The unique name of the model group within the specific platform. This is typically a human-readable identifier that describes the purpose or family of models. Names should be meaningful and follow your organization's naming conventions. Examples include `recommendation-models`, `fraud-detection-v2`, or `customer-churn-prediction`.

- **Origin (Fabric)**: The environment or fabric where the model group belongs or was generated. This qualifier helps distinguish between models in different environments such as Production (PROD), Staging (QA), Development (DEV), or Testing environments. The full list of supported environments is available in [FabricType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/li-utils/src/main/pegasus/com/linkedin/common/FabricType.pdl).

An example of an ML Model Group identifier is `urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,recommendation-models,PROD)`.

## Important Capabilities

### ML Model Group Properties

Model group properties are stored in the `mlModelGroupProperties` aspect and contain the core metadata about a model group:

- **Name**: The display name of the model group, which can be more descriptive than the identifier
- **Description**: Detailed documentation about what the model group represents, its purpose, and any important context
- **Version**: An optional version tag for the entire model group (distinct from individual model versions)
- **Created Timestamp**: Audit information about when and who created the model group
- **Last Modified Timestamp**: When the model group was last updated
- **Custom Properties**: Extensible key-value pairs for storing additional metadata specific to your organization or platform
- **External References**: Links to external systems or documentation (e.g., model registry URLs, experiment tracking systems)

Here is an example of creating an ML Model Group with properties:

<details>
<summary>Python SDK: Create an ML Model Group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_create.py show_path_as_comment }}
```

</details>

### Lineage and Training Information

ML Model Groups inherit lineage capabilities from the `MLModelLineageInfo` record, which captures important information about how models in the group are created and used:

- **Training Jobs**: References to data jobs or process instances that were used to train models in this group. This creates lineage relationships showing where models come from.
- **Downstream Jobs**: References to data jobs or process instances that consume or use models from this group. This tracks how models are deployed and utilized.

These lineage relationships are visible in DataHub's lineage graph and help track the full lifecycle of ML models from training data through deployment.

<details>
<summary>Python SDK: Add training lineage to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_lineage.py show_path_as_comment }}
```

</details>

### Ownership

Like other entities in DataHub, ML Model Groups can have owners assigned using the `ownership` aspect. Model group owners are typically responsible for:

- Managing which models belong to the group
- Maintaining model group metadata and documentation
- Overseeing model quality and governance standards
- Serving as points of contact for model-related questions
- Coordinating model deployment and monitoring

Ownership types for model groups follow the same patterns as other entities, including `TECHNICAL_OWNER`, `BUSINESS_OWNER`, `DATA_STEWARD`, `DATAOWNER`, `PRODUCER`, `DEVELOPER`, etc.

<details>
<summary>Python SDK: Add an owner to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_owner.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

ML Model Groups support both tags and glossary terms for categorization and discovery:

- **Tags** (via `globalTags` aspect): Informal labels for quick categorization. Use tags for properties like `experimental`, `production-ready`, `high-priority`, `computer-vision`, `nlp`, etc.
- **Glossary Terms** (via `glossaryTerms` aspect): Formal business vocabulary terms that provide standardized definitions. Use terms to link model groups to business concepts.

<details>
<summary>Python SDK: Add tags to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_tags.py show_path_as_comment }}
```

</details>

<details>
<summary>Python SDK: Add glossary terms to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_terms.py show_path_as_comment }}
```

</details>

### Documentation and Links

Model groups support documentation through multiple aspects:

- **Description** in `mlModelGroupProperties`: Primary documentation field for describing the model group
- **Institutional Memory** (via `institutionalMemory` aspect): Links to external resources such as:
  - Confluence pages describing the model group's purpose and architecture
  - Model cards and documentation
  - Experiment tracking dashboards (MLflow, Weights & Biases, etc.)
  - Research papers or technical specifications
  - Team wikis or runbooks

<details>
<summary>Python SDK: Add documentation links to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_documentation.py show_path_as_comment }}
```

</details>

### Domains

ML Model Groups can be assigned to domains using the `domains` aspect. This allows organizing model groups by business unit, department, or functional area. A model group can belong to only one domain at a time.

<details>
<summary>Python SDK: Assign a model group to a domain</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_add_domain.py show_path_as_comment }}
```

</details>

### Deprecation

Model groups can be marked as deprecated using the `deprecation` aspect when they are no longer actively maintained or should be replaced. This helps users understand which model families are still supported.

<details>
<summary>Python SDK: Deprecate a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_deprecate.py show_path_as_comment }}
```

</details>

### Structured Properties

Model groups support structured properties for storing typed, schema-validated metadata that goes beyond simple key-value pairs. This is useful for enforcing organizational standards around model metadata.

## Integration Points

### Relationship to ML Models

The primary relationship for ML Model Groups is with ML Models themselves. Models can be associated with a model group through the `groups` field in the `mlModelProperties` aspect. This creates a `MemberOf` relationship from the model to the model group.

Common patterns for organizing models in groups include:

1. **Version-based grouping**: All versions of a model (v1, v2, v3) belong to the same group. Note that versioning is handled through the `versionProperties` aspect on individual models (which includes version numbers, versionSet URNs, and aliases like "champion" or "challenger"), while the model group serves as the organizational container.

2. **Experiment-based grouping**: Different experimental variants of a model belong to the same group

3. **Architecture-based grouping**: Models sharing the same architecture or approach

4. **Purpose-based grouping**: Models serving the same business purpose or use case

<details>
<summary>Python SDK: Add a model to a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlgroup_add_to_mlmodel.py show_path_as_comment }}
```

</details>

### Relationship to Data Jobs

Through the `MLModelLineageInfo` fields, model groups can be connected to:

- **Training Jobs**: Data jobs that produce models in the group (upstream lineage)
- **Downstream Jobs**: Data jobs that consume models from the group (downstream lineage)

These relationships enable end-to-end lineage tracking from training data through model deployment.

### Relationship to ML Features

While ML Model Groups don't directly reference ML Features, the individual models within the group often consume ML Features. The model group serves as an organizational layer above the model-to-feature relationships.

### Relationship to Containers

Model groups can be organized within containers using the `container` aspect. This is useful for representing hierarchical structures like:

- ML Workspace > Project > Model Group
- Registry > Namespace > Model Group

### Querying Model Groups

You can query model groups and their associated models using both the REST API and the Python SDK.

#### Fetching Model Group Information via REST API

<details>
<summary>REST API: Get model group by URN</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AmlModelGroup%3A(urn%3Ali%3AdataPlatform%3Amlflow,recommendation-models,PROD)' \
  -H 'Authorization: Bearer <token>'
```

This will return the model group entity with all its aspects, including:

- `mlModelGroupKey`: The unique identifier
- `mlModelGroupProperties`: Name, description, version, timestamps
- `ownership`: Owners of the model group
- `globalTags`: Tags attached to the group
- `glossaryTerms`: Business terms associated with the group
- `domains`: Domain assignment
- `institutionalMemory`: Links and documentation

</details>

<details>
<summary>Python SDK: Read a model group</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_group_read.py show_path_as_comment }}
```

</details>

#### Finding Models in a Model Group

To find all models that belong to a specific model group, you can query the relationships.

<details>
<summary>REST API: Find all models in a model group</summary>

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AmlModelGroup%3A(urn%3Ali%3AdataPlatform%3Amlflow,recommendation-models,PROD)&types=MemberOf' \
  -H 'Authorization: Bearer <token>'
```

This returns all ML Model entities that are members of the specified model group.

</details>

## Integration with ML Platforms

ML Model Groups work seamlessly with popular ML platforms:

### MLflow

MLflow's registered models naturally map to DataHub model groups, with model versions becoming individual MLModel entities within the group. The MLflow ingestion connector automatically creates these relationships.

### SageMaker

Amazon SageMaker model packages and model package groups can be represented as model groups in DataHub, providing a unified view across AWS environments.

### Databricks

Databricks ML models registered in Unity Catalog can be organized into model groups for better organization and governance.

### Kubeflow

Kubeflow model registries can leverage model groups to organize models by pipeline or serving configuration.

### Custom Platforms

Any custom ML platform can use model groups by specifying an appropriate platform identifier in the URN.

## Notable Exceptions

### Model Group vs Individual Models

It's important to understand when to use a model group versus tracking individual models:

- **Use Model Groups when**: You have multiple related versions or variants of models that should be organized together. For example, all versions of a "customer churn prediction" model.
- **Use Individual Models when**: You have standalone models that don't have multiple versions or aren't part of a logical family.

### Lineage Inheritance

Lineage information stored at the model group level (via `trainingJobs` and `downstreamJobs`) represents common lineage across all models in the group. Individual models can also have their own specific lineage information in their `mlModelProperties` aspect. The two levels of lineage are complementary:

- Model group lineage: Shared training pipelines or common downstream consumers
- Individual model lineage: Specific training runs or deployment-specific consumers

### Naming Considerations

When creating model groups, consider your naming strategy:

- Names should be stable over time as they are part of the identifier
- Avoid including version numbers in the group name (use individual model names for versioning)
- Use clear, descriptive names that indicate the model family or purpose
- Follow consistent naming conventions across your organization

### Platform Instance Support

Model groups support platform instances via the `dataPlatformInstance` aspect. This is useful when you have multiple instances of the same platform (e.g., multiple MLflow registries) and need to distinguish model groups across them.

### Search and Discovery

Model groups are searchable in DataHub by:

- Name (with autocomplete support)
- Description (full-text search)
- Platform
- Origin/Fabric
- Tags and glossary terms
- Domain
- Owners

This makes it easy to discover relevant model groups through the DataHub UI or search API.
