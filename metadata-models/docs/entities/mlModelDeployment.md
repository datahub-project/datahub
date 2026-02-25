# ML Model Deployment

ML Model Deployments represent deployed instances of machine learning models running in production or other environments. They track the operational aspects of ML models, including their deployment status, platform, configuration, and lifecycle. Model deployments are distinct from ML models themselves - while an ML model represents the trained artifact, a deployment represents a specific instance of that model serving predictions in a particular environment.

## Identity

ML Model Deployments are identified by three pieces of information:

- **The platform where the deployment is hosted**: This is the specific deployment or serving platform that hosts the model. Examples include `sagemaker`, `azureml`, `vertexai`, `mlflow`, `seldon`, `kserve`, etc. This corresponds to a data platform URN.
- **The name of the deployment**: Each platform has its own way of naming deployments. For example, SageMaker uses endpoint names, while Azure ML uses deployment names within a workspace.
- **The environment or origin**: This qualifier indicates the environment where the deployment runs (PROD, DEV, TEST, etc.), similar to the fabric concept used for datasets.

An example of an ML model deployment identifier is `urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,recommendation-endpoint,PROD)`.

## Important Capabilities

### Deployment Properties

The core metadata about a deployment is stored in the `mlModelDeploymentProperties` aspect. This includes:

- **Description**: Documentation explaining the purpose and configuration of the deployment
- **Deployment Status**: The current operational state of the deployment, which can be:
  - `IN_SERVICE`: Active deployments serving predictions
  - `OUT_OF_SERVICE`: Deployments that are not currently serving traffic
  - `CREATING`: Deployments being provisioned
  - `UPDATING`: Deployments being updated with new configurations
  - `ROLLING_BACK`: Deployments being reverted to a previous version
  - `DELETING`: Deployments being removed
  - `FAILED`: Deployments in an error state
  - `UNKNOWN`: Deployments with unmapped or unknown status
- **Version**: The version of the deployment configuration
- **Created At**: Timestamp indicating when the deployment was created
- **Custom Properties**: Additional platform-specific configuration details
- **External URL**: Link to view the deployment in its native platform

The following code snippet shows you how to create an ML model deployment.

<details>
<summary>Python SDK: Create an ML model deployment</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_deployment_create.py show_path_as_comment }}
```

</details>

### Linking Deployments to Models

ML Model Deployments are connected to their underlying ML Models through the `mlModelProperties.deployments` field. This relationship enables:

- **Deployment Tracking**: Understanding where and how a model is deployed
- **Version Management**: Tracking which model version is deployed in each environment
- **Impact Analysis**: Understanding the production footprint of a model
- **Rollback Planning**: Identifying active deployments when model issues arise

<details>
<summary>Python SDK: Link a deployment to an ML model</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_deployment_add_to_model.py show_path_as_comment }}
```

</details>

### Tags and Classification

ML Model Deployments can have tags attached to them using the `globalTags` aspect. Tags are useful for:

- Categorizing deployments by purpose (production, canary, shadow)
- Tracking cost centers or teams
- Marking deployments for compliance or security review
- Organizing deployments by model type or use case

### Ownership

Ownership is associated with a deployment using the `ownership` aspect. Owners can be technical owners (ML engineers, MLOps teams) or business owners (product managers, data scientists) who are responsible for the deployment. Ownership helps with:

- Accountability for deployment health and performance
- Contact information for incidents
- Access control and permissions management
- Organizational governance

<details>
<summary>Python SDK: Add an owner to a deployment</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_deployment_add_owner.py show_path_as_comment }}
```

</details>

### Platform Instance

The `dataPlatformInstance` aspect provides additional context about the specific instance of the deployment platform. This is important when organizations have multiple instances of the same platform (e.g., multiple SageMaker accounts, different Azure ML workspaces, separate Kubernetes clusters).

### Deprecation and Lifecycle Management

Deployments can be marked as deprecated using the `deprecation` aspect when they are planned for decommissioning. This includes:

- **Deprecation Flag**: Marking the deployment as deprecated
- **Decommission Time**: Planned date for removing the deployment
- **Deprecation Note**: Information about why the deployment is being deprecated and what replaces it
- **Replacement**: Reference to the new deployment that should be used instead

The `status` aspect can also be used to soft-delete a deployment (mark it as removed) when it has been taken down but you want to preserve the historical metadata.

### Organizational Context

#### Containers

Deployments can belong to a parent container (such as an ML workspace, namespace, or project) using the `container` aspect. This creates a hierarchical structure:

```
ML Workspace (Container)
├── Model Deployment 1
├── Model Deployment 2
└── Model Deployment 3
```

This hierarchy helps with:

- Organizing deployments by project or team
- Managing access control at the workspace level
- Understanding resource utilization and costs
- Navigating related deployments

## Integration with External Systems

### Ingestion from ML Platforms

ML Model Deployments are typically ingested automatically from ML platforms using DataHub's ingestion connectors:

- **SageMaker**: Ingests endpoint deployments with their configuration and status
- **Azure ML**: Ingests model deployments from workspaces
- **Vertex AI**: Ingests endpoint deployments from Google Cloud
- **MLflow**: Ingests registered model deployments
- **Seldon Core / KServe**: Ingests inference services from Kubernetes

Each connector maps platform-specific deployment metadata to DataHub's standardized deployment model.

### Querying Deployment Information

You can retrieve deployment information using DataHub's REST API:

<details>
<summary>Fetch deployment entity snapshot</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AmlModelDeployment%3A(urn%3Ali%3AdataPlatform%3Asagemaker,recommendation-endpoint,PROD)'
```

The response includes all aspects of the deployment, including:

- Deployment properties (status, version, description)
- Ownership information
- Tags and classification
- Platform instance details
- Deprecation status

</details>

### Relationships API

You can query deployment relationships to understand its connections to other entities:

<details>
<summary>Find deployments for a specific ML model</summary>

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3AmlModel%3A(urn%3Ali%3AdataPlatform%3Asagemaker,recommendation-model,PROD)&types=DeployedTo'
```

This returns all deployments of the specified ML model.

</details>

<details>
<summary>Find the ML model for a deployment</summary>

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AmlModel%3A(urn%3Ali%3AdataPlatform%3Asagemaker,recommendation-model,PROD)&types=DeployedTo'
```

This returns the ML model that is deployed to the specified deployment.

</details>

## Notable Exceptions

### Deployment vs Model Distinction

It's important to understand the distinction between ML Models and ML Model Deployments:

- **ML Model**: Represents the trained model artifact, including training data, metrics, hyperparameters, and model metadata. A model can have multiple versions and can exist independently of any deployment.

- **ML Model Deployment**: Represents a running instance of a model serving predictions. A deployment references a specific model version and includes operational metadata like status, configuration, and serving platform details.

This separation enables:

- Tracking the same model deployed to multiple environments (dev, staging, prod)
- Understanding deployment history and rollbacks
- Managing the operational lifecycle independently from model development
- Supporting A/B testing and canary deployments of the same model

### Multiple Deployments per Model

A single ML model can have multiple deployments simultaneously:

- Different environments (production, staging, development)
- Different regions (US, EU, Asia)
- Different configurations (batch vs real-time, different instance types)
- A/B testing scenarios (challenger vs champion)

Each deployment is tracked as a separate entity with its own metadata.

### Ephemeral Deployments

Some platforms create temporary or ephemeral deployments (e.g., for batch inference jobs). These deployments may have short lifespans but are still valuable to track for:

- Cost attribution
- Compliance and audit trails
- Understanding model usage patterns
- Debugging and troubleshooting

### Platform-Specific Deployment Concepts

Different ML platforms have varying concepts that map to deployments:

- **SageMaker Endpoints**: Real-time inference endpoints with one or more model variants
- **Azure ML Online Endpoints**: Managed endpoints with blue-green deployment support
- **Vertex AI Endpoints**: Deployed models with traffic splitting capabilities
- **Kubernetes Inference Services**: Pod-based model serving with auto-scaling

DataHub's deployment model abstracts these platform differences while preserving important platform-specific details in custom properties.

## Related Entities

ML Model Deployments frequently interact with these other DataHub entities:

- **MLModel**: The underlying trained model that is deployed
- **MLModelGroup**: A collection of related models that may share deployment infrastructure
- **DataPlatform**: The platform hosting the deployment (SageMaker, Azure ML, etc.)
- **Container**: The workspace, namespace, or project containing the deployment
- **CorpUser / CorpGroup**: Owners and operators of the deployment
- **Tag**: Classification and organizational tags
- **DataPlatformInstance**: The specific instance of the deployment platform
