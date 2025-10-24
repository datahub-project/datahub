# ML Model

The ML Model entity represents trained machine learning models across various ML platforms and frameworks. ML Models can be trained using different algorithms and frameworks (TensorFlow, PyTorch, Scikit-learn, etc.) and deployed to various platforms (MLflow, SageMaker, Vertex AI, etc.).

## Identity

ML Models are identified by three pieces of information:

- The platform where the model is registered or deployed: this is the specific ML platform that hosts or manages this model. Examples are `mlflow`, `sagemaker`, `vertexai`, `databricks`, etc. See [dataplatform](./dataPlatform.md) for more details.
- The name of the model: this is the unique identifier for the model within the platform. The naming convention varies by platform:
  - MLflow: typically uses the registered model name (e.g., `recommendation-model`)
  - SageMaker: uses the model name or model package group name (e.g., `product-recommendation-v1`)
  - Vertex AI: uses the model resource name (e.g., `projects/123/locations/us-central1/models/456`)
- The environment or origin where the model was trained: this is similar to the fabric concept for datasets, allowing you to distinguish between models in different environments (PROD, DEV, QA, etc.). The full list of supported environments is available in [FabricType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/li-utils/src/main/pegasus/com/linkedin/common/FabricType.pdl).

An example of an ML Model identifier is `urn:li:mlModel:(urn:li:dataPlatform:mlflow,my-recommendation-model,PROD)`.

## Important Capabilities

### Basic Model Information

The core information about an ML Model is captured in the `mlModelProperties` aspect. This includes:

- **Name and Description**: Human-readable name and description of what the model does
- **Model Type**: The algorithm or architecture used (e.g., "Convolutional Neural Network", "Random Forest", "BERT")
- **Version**: Version information using the `versionProperties` aspect
- **Timestamps**: Created and last modified timestamps
- **Custom Properties**: Flexible key-value pairs for platform-specific metadata (e.g., framework version, model format)

The following code snippet shows you how to create a basic ML Model:

<details>
<summary>Python SDK: Create an ML Model</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_create.py show_path_as_comment }}
```

</details>

### Hyperparameters and Metrics

ML Models can capture both the hyperparameters used during training and various metrics from training and production:

- **Hyperparameters**: Configuration values that control the training process (learning rate, batch size, number of epochs, etc.)
- **Training Metrics**: Performance metrics from the training process (accuracy, loss, F1 score, etc.)
- **Online Metrics**: Performance metrics from production deployment (latency, throughput, drift, etc.)

These are stored in the `mlModelProperties` aspect as structured lists of parameters and metrics.

<details>
<summary>Python SDK: Add hyperparameters and metrics to an ML Model</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_add_metadata.py show_path_as_comment }}
```

</details>

### Intended Use and Ethical Considerations

DataHub supports comprehensive model documentation following ML model card best practices. These aspects help stakeholders understand the appropriate use cases and ethical implications of using the model:

- **Intended Use** (`intendedUse` aspect): Documents primary use cases, intended users, and out-of-scope applications
- **Ethical Considerations** (`mlModelEthicalConsiderations` aspect): Documents use of sensitive data, risks and harms, mitigation strategies
- **Caveats and Recommendations** (`mlModelCaveatsAndRecommendations` aspect): Additional considerations, ideal dataset characteristics, and usage recommendations

These aspects align with responsible AI practices and help ensure models are used appropriately.

### Training and Evaluation Data

ML Models can document their training and evaluation datasets in two complementary ways:

#### Direct Dataset References

- **Training Data** (`mlModelTrainingData` aspect): Datasets used to train the model, including preprocessing information and motivation for dataset selection
- **Evaluation Data** (`mlModelEvaluationData` aspect): Datasets used for model evaluation and testing

Each dataset reference includes the dataset URN, motivation for using that dataset, and any preprocessing steps applied. This creates direct lineage relationships between models and their training data.

#### Lineage via Training Runs

Training runs (`dataProcessInstance` entities) provide an alternative and often more detailed way to capture training lineage:

- Training runs declare their input datasets via `dataProcessInstanceInput` aspect
- Training runs declare their output datasets via `dataProcessInstanceOutput` aspect
- Models reference training runs via the `trainingJobs` field

This creates indirect lineage: `Dataset → Training Run → Model`

**When to use each approach:**

- Use **direct dataset references** for simple documentation of what data was used
- Use **training runs** for complete lineage tracking including:
  - Multiple training/validation/test datasets
  - Metrics and hyperparameters from the training process
  - Temporal tracking (when the training occurred)
  - Connection to experiments for comparing multiple training attempts

Most production ML systems should use training runs for comprehensive lineage tracking.

### Factor Prompts and Quantitative Analysis

For detailed model analysis and performance reporting:

- **Factor Prompts** (`mlModelFactorPrompts` aspect): Factors that may affect model performance (demographic groups, environmental conditions, etc.)
- **Quantitative Analyses** (`mlModelQuantitativeAnalyses` aspect): Links to dashboards or reports showing disaggregated performance metrics across different factors
- **Metrics** (`mlModelMetrics` aspect): Detailed metrics with descriptions beyond simple training/online metrics

### Source Code and Cost

- **Source Code** (`sourceCode` aspect): Links to model training code, notebooks, or repositories (GitHub, GitLab, etc.)
- **Cost** (`cost` aspect): Cost attribution information for tracking model training and inference expenses

### Training Runs and Experiments

ML Models in DataHub can be linked to their training runs and experiments, providing complete lineage from raw data through training to deployed models.

#### Training Runs

Training runs represent specific executions of model training jobs. In DataHub, training runs are modeled as `dataProcessInstance` entities with a specialized subtype:

- **Entity Type**: `dataProcessInstance`
- **Subtype**: `MLAssetSubTypes.MLFLOW_TRAINING_RUN`
- **Key Aspects**:
  - `dataProcessInstanceProperties`: Basic properties like name, timestamps, and custom properties
  - `mlTrainingRunProperties`: ML-specific properties including:
    - Training metrics (accuracy, loss, F1 score, etc.)
    - Hyperparameters (learning rate, batch size, epochs, etc.)
    - Output URLs (model artifacts, checkpoints)
    - External URLs (links to training dashboards)
  - `dataProcessInstanceInput`: Input datasets used for training
  - `dataProcessInstanceOutput`: Output datasets (predictions, feature importance, etc.)
  - `dataProcessInstanceRunEvent`: Start, completion, and failure events

Training runs create lineage relationships showing:

- **Upstream**: Which datasets were used for training
- **Downstream**: Which models were produced by the training run

Models reference their training runs through the `trainingJobs` field in `mlModelProperties`, and model groups can also reference training runs to track all training activity for a model family.

#### Experiments

Experiments organize related training runs into logical groups, typically representing a series of attempts to optimize a model or compare different approaches. In DataHub, experiments are modeled as `container` entities:

- **Entity Type**: `container`
- **Subtype**: `MLAssetSubTypes.MLFLOW_EXPERIMENT`
- **Purpose**: Group related training runs for organization and comparison

Training runs belong to experiments through the `container` aspect, creating a hierarchy:

```
Experiment: "Customer Churn Prediction"
├── Training Run 1: baseline model
├── Training Run 2: with feature engineering
├── Training Run 3: hyperparameter tuning
└── Training Run 4: final production model
```

This structure mirrors common ML platform patterns (like MLflow's experiment/run hierarchy) and enables:

- Comparing metrics across multiple training attempts
- Tracking the evolution of a model through iterations
- Understanding which approaches were tried and their results
- Organizing training work by project or objective

<details>
<summary>Python SDK: Create training runs and experiments</summary>

```python
{{ inline /metadata-ingestion/examples/ai/dh_ai_docs_demo.py show_path_as_comment }}
```

</details>

### Relationships and Lineage

ML Models support rich relationship modeling through various aspects and fields:

#### Core Relationships

- **Model Groups** (via `groups` field in `mlModelProperties`): Models can belong to `mlModelGroup` entities, creating a `MemberOf` relationship. This organizes related models into logical families or collections.

- **Training Runs** (via `trainingJobs` field in `mlModelProperties`): Models reference `dataProcessInstance` entities with `MLFLOW_TRAINING_RUN` subtype that produced them. This creates upstream lineage showing:

  - Which training run created this model
  - What datasets were used for training (via the training run's input datasets)
  - What hyperparameters and metrics were recorded
  - Which experiment the training run belonged to

- **Features** (via `mlFeatures` field in `mlModelProperties`): Models can consume `mlFeature` entities, creating a `Consumes` relationship. This documents:

  - Which features are required for model inference
  - The complete feature set used during training
  - Dependencies on feature stores or feature tables

- **Deployments** (via `deployments` field in `mlModelProperties`): Models can be deployed to `mlModelDeployment` entities, representing running model endpoints in various environments (production, staging, etc.)

- **Training Datasets** (via `mlModelTrainingData` aspect): Direct references to datasets used for training, including preprocessing information and motivation for dataset selection

- **Evaluation Datasets** (via `mlModelEvaluationData` aspect): References to datasets used for model evaluation and testing

#### Lineage Graph Structure

These relationships create a comprehensive lineage graph:

```
Training Datasets → Training Run → ML Model → ML Model Deployment
                         ↓
                    Experiment

Feature Tables → ML Features → ML Model

ML Model Group ← ML Model
```

This enables powerful queries such as:

- "Show me all datasets that influenced this model's predictions"
- "Which models will be affected if this dataset schema changes?"
- "What's the full history of training runs that created versions of this model?"
- "Which production endpoints are serving this model?"

<details>
<summary>Python SDK: Update model-specific aspects</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_update_aspects.py show_path_as_comment }}
```

</details>

### Tags, Terms, and Ownership

Like other DataHub entities, ML Models support:

- **Tags** (`globalTags` aspect): Flexible categorization (e.g., "pii-model", "production-ready", "experimental")
- **Glossary Terms** (`glossaryTerms` aspect): Business concepts (e.g., "Customer Churn", "Fraud Detection")
- **Ownership** (`ownership` aspect): Individuals or teams responsible for the model (data scientists, ML engineers, etc.)
- **Domains** (`domains` aspect): Organizational grouping (e.g., "Recommendations", "Risk Management")

### Complete ML Workflow Example

The following example demonstrates a complete ML model lifecycle in DataHub, showing how all the pieces work together:

```
1. Create Model Group
   ↓
2. Create Experiment (Container)
   ↓
3. Create Training Run (DataProcessInstance)
   ├── Link input datasets
   ├── Link output datasets
   └── Add metrics and hyperparameters
   ↓
4. Create Model
   ├── Set version and aliases
   ├── Link to model group
   ├── Link to training run
   ├── Add hyperparameters and metrics
   └── Add ownership and tags
   ↓
5. Link Training Run to Experiment
   ↓
6. Update Model properties as needed
   ├── Change version aliases (champion → challenger)
   ├── Add additional tags/terms
   └── Update metrics from production
```

This workflow creates rich lineage showing:

- Which datasets trained the model
- What experiments and training runs were involved
- How the model evolved through versions
- Which version is deployed (via aliases)
- Who owns and maintains the model

<details>
<summary>Complete Python Example: Full ML Workflow</summary>

See the comprehensive example in `/metadata-ingestion/examples/ai/dh_ai_docs_demo.py` which demonstrates:

- Creating model groups with metadata
- Creating experiments to organize training runs
- Creating training runs with metrics, hyperparameters, and dataset lineage
- Creating models with versions and aliases
- Linking all entities together to form complete lineage
- Updating properties and managing the model lifecycle

The example shows both basic patterns for getting started and advanced patterns for production ML systems.

</details>

## Code Examples

### Querying ML Model Information

The standard REST APIs can be used to retrieve ML Model entities and their aspects:

<details>
<summary>Python: Query an ML Model via REST API</summary>

```python
{{ inline /metadata-ingestion/examples/library/mlmodel_query_rest_api.py show_path_as_comment }}
```

</details>

## Integration Points

### Related Entities

ML Models integrate with several other entities in the DataHub metadata model:

- **mlModelGroup**: Logical grouping of related model versions (e.g., all versions of a recommendation model)
- **mlModelDeployment**: Running instances of deployed models with status, endpoint URLs, and deployment metadata
- **mlFeature**: Individual features consumed by the model for inference
- **mlFeatureTable**: Collections of features, often from feature stores
- **dataset**: Training and evaluation datasets used by the model
- **dataProcessInstance** (with `MLFLOW_TRAINING_RUN` subtype): Specific training runs that created model versions, including metrics, hyperparameters, and lineage to input/output datasets
- **container** (with `MLFLOW_EXPERIMENT` subtype): Experiments that organize related training runs for a model or project
- **versionSet**: Groups all versions of a model together for version management

### GraphQL Resolvers

The GraphQL API provides rich querying capabilities for ML Models through resolvers in `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/types/mlmodel/`. These resolvers support:

- Fetching model details with all aspects
- Navigating relationships to features, groups, and deployments
- Searching and filtering models by tags, terms, platform, etc.

### Ingestion Sources

Several ingestion sources automatically extract ML Model metadata:

- **MLflow**: Extracts registered models, versions, metrics, parameters, and lineage from MLflow tracking servers
- **SageMaker**: Ingests models, model packages, and endpoints from AWS SageMaker
- **Vertex AI**: Extracts models and endpoints from Google Cloud Vertex AI
- **Databricks**: Ingests MLflow models from Databricks workspaces
- **Unity Catalog**: Extracts ML models registered in Unity Catalog

These sources are located in `/metadata-ingestion/src/datahub/ingestion/source/` and automatically populate model properties, relationships, and lineage.

## Notable Exceptions

### Model Versioning

ML Model versioning in DataHub uses the `versionProperties` aspect, which provides a robust framework for tracking model versions across their lifecycle. This is the standard approach demonstrated in production ML platforms.

#### Version Properties Aspect

Every ML Model should use the `versionProperties` aspect, which includes:

- **version**: A `VersionTagClass` containing the version identifier (e.g., "1", "2", "v1.0.0")
- **versionSet**: A URN that groups all versions of a model together (e.g., `urn:li:versionSet:(mlModel,mlmodel_my-model_versions)`)
- **sortId**: A string used for ordering versions (typically the version number zero-padded)
- **aliases**: Optional array of `VersionTagClass` objects for named version references

#### Version Aliases for A/B Testing

Version aliases enable flexible model lifecycle management and A/B testing workflows. Common aliases include:

- **"champion"**: The currently deployed production model
- **"challenger"**: A candidate model being tested or evaluated
- **"baseline"**: A reference model for performance comparison
- **"latest"**: The most recently trained version

These aliases allow you to reference models by their role rather than specific version numbers, enabling smooth model promotion workflows:

```
Model v1 (alias: "champion")     # Currently in production
Model v2 (alias: "challenger")   # Being tested in canary deployment
Model v3 (alias: "latest")       # Just completed training
```

When v2 proves superior, you can update aliases without changing infrastructure:

```
Model v1 (no alias)              # Retired
Model v2 (alias: "champion")     # Promoted to production
Model v3 (alias: "challenger")   # Now being tested
```

#### Model Groups and Versioning

Model groups (`mlModelGroup` entities) serve as logical containers for organizing related models. While model groups can contain multiple versions of the same model, versioning is handled through the `versionProperties` aspect on individual models, not through the group structure itself. Model groups are used for:

- Organizing all versions of a model family
- Grouping experimental variants or different architectures solving the same problem
- Managing lineage and metadata common across multiple related models

The relationship between models and model groups is through the `groups` field in `mlModelProperties`, creating a `MemberOf` relationship.

### Platform-Specific Naming

Different ML platforms have different naming conventions:

- **MLflow**: Uses a two-level hierarchy (registered model name + version number). In DataHub, each version can be a separate entity, or versions can be tracked in a single entity.
- **SageMaker**: Has multiple model concepts (model, model package, model package group). DataHub can model these as separate entities or consolidate them.
- **Vertex AI**: Uses fully qualified resource names. These should be simplified to human-readable names when possible.

When ingesting from these platforms, connectors handle platform-specific naming and convert it to appropriate DataHub URNs.

### Model Cards

The various aspects (`intendedUse`, `mlModelFactorPrompts`, `mlModelEthicalConsiderations`, etc.) follow the Model Cards for Model Reporting framework (Mitchell et al., 2019). While these aspects are optional, they are strongly recommended for production models to ensure responsible AI practices and transparent model documentation.
