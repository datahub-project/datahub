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


**Python SDK: Create an ML Model**

```python
# Inlined from /metadata-ingestion/examples/library/mlmodel_create.py
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="customer-churn-predictor",
    name="Customer Churn Prediction Model",
    platform="mlflow",
    description="A gradient boosting model that predicts customer churn based on usage patterns and engagement metrics",
    custom_properties={
        "framework": "xgboost",
        "framework_version": "1.7.0",
        "model_format": "pickle",
    },
    model_group=MlModelGroupUrn(platform="mlflow", name="customer-churn-models"),
)

client.entities.upsert(mlmodel)

```



### Hyperparameters and Metrics

ML Models can capture both the hyperparameters used during training and various metrics from training and production:

- **Hyperparameters**: Configuration values that control the training process (learning rate, batch size, number of epochs, etc.)
- **Training Metrics**: Performance metrics from the training process (accuracy, loss, F1 score, etc.)
- **Online Metrics**: Performance metrics from production deployment (latency, throughput, drift, etc.)

These are stored in the `mlModelProperties` aspect as structured lists of parameters and metrics.


**Python SDK: Add hyperparameters and metrics to an ML Model**

```python
# Inlined from /metadata-ingestion/examples/library/mlmodel_add_metadata.py
from datahub.metadata.urns import CorpUserUrn, DomainUrn, MlModelUrn, TagUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

mlmodel = client.entities.get(
    MlModelUrn(platform="mlflow", name="customer-churn-predictor")
)

mlmodel.set_hyper_params(
    {
        "learning_rate": "0.1",
        "max_depth": "6",
        "n_estimators": "100",
        "subsample": "0.8",
        "colsample_bytree": "0.8",
    }
)

mlmodel.set_training_metrics(
    {
        "accuracy": "0.87",
        "precision": "0.84",
        "recall": "0.82",
        "f1_score": "0.83",
        "auc_roc": "0.91",
    }
)

mlmodel.add_owner(CorpUserUrn("data_science_team"))

mlmodel.add_tag(TagUrn("production"))
mlmodel.add_tag(TagUrn("classification"))

mlmodel.set_domain(DomainUrn("urn:li:domain:customer-analytics"))

client.entities.update(mlmodel)

```



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


**Python SDK: Create training runs and experiments**

```python
# Inlined from /metadata-ingestion/examples/ai/dh_ai_docs_demo.py
import argparse
from datetime import datetime

from dh_ai_client import DatahubAIClient

from datahub.emitter.mcp_builder import (
    ContainerKey,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import RunResultType
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstancePropertiesClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
)
from datahub.metadata.urns import (
    CorpUserUrn,
    DataProcessInstanceUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

parser = argparse.ArgumentParser()
parser.add_argument("--token", required=False, help="DataHub access token")
parser.add_argument(
    "--server_url",
    required=False,
    default="http://localhost:8080",
    help="DataHub server URL (defaults to http://localhost:8080)",
)
args = parser.parse_args()

# Initialize client
client = DatahubAIClient(token=args.token, server_url=args.server_url)

# Use a unique prefix for all IDs to avoid conflicts
prefix = "test"

# Define all entity IDs upfront
# Basic entity IDs
basic_model_group_id = f"{prefix}_basic_group"
basic_model_id = f"{prefix}_basic_model"
basic_experiment_id = f"{prefix}_basic_experiment"
basic_run_id = f"{prefix}_basic_run"
basic_dataset_id = f"{prefix}_basic_dataset"

# Advanced entity IDs
advanced_model_group_id = f"{prefix}_airline_forecast_models_group"
advanced_model_id = f"{prefix}_arima_model"
advanced_experiment_id = f"{prefix}_airline_forecast_experiment"
advanced_run_id = f"{prefix}_simple_training_run"
advanced_input_dataset_id = f"{prefix}_iris_input"
advanced_output_dataset_id = f"{prefix}_iris_output"

# Display names with prefix
basic_model_group_name = f"{prefix} Basic Group"
basic_model_name = f"{prefix} Basic Model"
basic_experiment_name = f"{prefix} Basic Experiment"
basic_run_name = f"{prefix} Basic Run"
basic_dataset_name = f"{prefix} Basic Dataset"

advanced_model_group_name = f"{prefix} Airline Forecast Models Group"
advanced_model_name = f"{prefix} ARIMA Model"
advanced_experiment_name = f"{prefix} Airline Forecast Experiment"
advanced_run_name = f"{prefix} Simple Training Run"
advanced_input_dataset_name = f"{prefix} Iris Training Input Data"
advanced_output_dataset_name = f"{prefix} Iris Model Output Data"


def create_basic_model_group():
    """Create a basic model group."""
    print("Creating basic model group...")
    basic_model_group = MLModelGroup(
        id=basic_model_group_id,
        platform="mlflow",
        name=basic_model_group_name,
    )
    client._emit_mcps(basic_model_group.as_mcps())
    return basic_model_group


def create_advanced_model_group():
    """Create an advanced model group."""
    print("Creating advanced model group...")
    advanced_model_group = MLModelGroup(
        id=advanced_model_group_id,
        platform="mlflow",
        name=advanced_model_group_name,
        description="Group of models for airline passenger forecasting",
        created=datetime.now(),
        last_modified=datetime.now(),
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        external_url="https://www.linkedin.com/in/datahub",
        tags=["urn:li:tag:forecasting", "urn:li:tag:arima"],
        terms=["urn:li:glossaryTerm:forecasting"],
        custom_properties={"team": "forecasting"},
    )
    client._emit_mcps(advanced_model_group.as_mcps())
    return advanced_model_group


def create_basic_model():
    """Create a basic model."""
    print("Creating basic model...")
    basic_model = MLModel(
        id=basic_model_id,
        platform="mlflow",
        name=basic_model_name,
    )
    client._emit_mcps(basic_model.as_mcps())
    return basic_model


def create_advanced_model():
    """Create an advanced model."""
    print("Creating advanced model...")
    advanced_model = MLModel(
        id=advanced_model_id,
        platform="mlflow",
        name=advanced_model_name,
        description="ARIMA model for airline passenger forecasting",
        created=datetime.now(),
        last_modified=datetime.now(),
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        external_url="https://www.linkedin.com/in/datahub",
        tags=["urn:li:tag:forecasting", "urn:li:tag:arima"],
        terms=["urn:li:glossaryTerm:forecasting"],
        custom_properties={"team": "forecasting"},
        version="1",
        aliases=["champion"],
        hyper_params={"learning_rate": "0.01"},
        training_metrics={"accuracy": "0.9"},
    )
    client._emit_mcps(advanced_model.as_mcps())
    return advanced_model


def create_basic_experiment():
    """Create a basic experiment."""
    print("Creating basic experiment...")
    basic_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=basic_experiment_id),
        display_name=basic_experiment_name,
    )
    client._emit_mcps(basic_experiment.as_mcps())
    return basic_experiment


def create_advanced_experiment():
    """Create an advanced experiment."""
    print("Creating advanced experiment...")
    advanced_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=advanced_experiment_id),
        display_name=advanced_experiment_name,
        description="Experiment to forecast airline passenger numbers",
        extra_properties={"team": "forecasting"},
        created=datetime(2025, 4, 9, 22, 30),
        last_modified=datetime(2025, 4, 9, 22, 30),
        subtype=MLAssetSubTypes.MLFLOW_EXPERIMENT,
    )
    client._emit_mcps(advanced_experiment.as_mcps())
    return advanced_experiment


def create_basic_training_run():
    """Create a basic training run."""
    print("Creating basic training run...")
    basic_run_urn = client.create_training_run(
        run_id=basic_run_id,
        run_name=basic_run_name,
    )
    return basic_run_urn


def create_advanced_training_run():
    """Create an advanced training run."""
    print("Creating advanced training run...")
    advanced_run_urn = client.create_training_run(
        run_id=advanced_run_id,
        properties=DataProcessInstancePropertiesClass(
            name=advanced_run_name,
            created=AuditStampClass(
                time=1628580000000, actor="urn:li:corpuser:datahub"
            ),
            customProperties={"team": "forecasting"},
        ),
        training_run_properties=MLTrainingRunPropertiesClass(
            id=advanced_run_id,
            outputUrls=["s3://my-bucket/output"],
            trainingMetrics=[MLMetricClass(name="accuracy", value="0.9")],
            hyperParams=[MLHyperParamClass(name="learning_rate", value="0.01")],
            externalUrl="https:localhost:5000",
        ),
        run_result=RunResultType.FAILURE,
        start_timestamp=1628580000000,
        end_timestamp=1628580001000,
    )
    return advanced_run_urn


def create_basic_dataset():
    """Create a basic dataset."""
    print("Creating basic dataset...")
    basic_input_dataset = Dataset(
        platform="snowflake",
        name=basic_dataset_id,
        display_name=basic_dataset_name,
    )
    client._emit_mcps(basic_input_dataset.as_mcps())
    return basic_input_dataset


def create_advanced_datasets():
    """Create advanced datasets."""
    print("Creating advanced datasets...")
    advanced_input_dataset = Dataset(
        platform="snowflake",
        name=advanced_input_dataset_id,
        description="Raw Iris dataset used for training ML models",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name=advanced_input_dataset_name,
        tags=["urn:li:tag:ml_data", "urn:li:tag:iris"],
        terms=["urn:li:glossaryTerm:raw_data"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "data_source": "UCI Repository",
            "records": "150",
            "features": "4",
        },
    )
    client._emit_mcps(advanced_input_dataset.as_mcps())

    advanced_output_dataset = Dataset(
        platform="snowflake",
        name=advanced_output_dataset_id,
        description="Processed Iris dataset with model predictions",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name=advanced_output_dataset_name,
        tags=["urn:li:tag:ml_data", "urn:li:tag:predictions"],
        terms=["urn:li:glossaryTerm:model_output"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "model_version": "1.0",
            "records": "150",
            "accuracy": "0.95",
        },
    )
    client._emit_mcps(advanced_output_dataset.as_mcps())
    return advanced_input_dataset, advanced_output_dataset


# Split relationship functions into individual top-level functions
def add_model_to_model_group(model, model_group):
    """Add model to model group relationship."""
    print("Adding model to model group...")
    model.set_model_group(model_group.urn)
    client._emit_mcps(model.as_mcps())


def add_run_to_experiment(run_urn, experiment):
    """Add run to experiment relationship."""
    print("Adding run to experiment...")
    client.add_run_to_experiment(run_urn=run_urn, experiment_urn=str(experiment.urn))


def add_run_to_model(model, run_id):
    """Add run to model relationship."""
    print("Adding run to model...")
    model.add_training_job(DataProcessInstanceUrn(run_id))
    client._emit_mcps(model.as_mcps())


def add_run_to_model_group(model_group, run_id):
    """Add run to model group relationship."""
    print("Adding run to model group...")
    model_group.add_training_job(DataProcessInstanceUrn(run_id))
    client._emit_mcps(model_group.as_mcps())


def add_input_dataset_to_run(run_urn, input_dataset):
    """Add input dataset to run relationship."""
    print("Adding input dataset to run...")
    client.add_input_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(input_dataset.urn)]
    )


def add_output_dataset_to_run(run_urn, output_dataset):
    """Add output dataset to run relationship."""
    print("Adding output dataset to run...")
    client.add_output_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(output_dataset.urn)]
    )


def update_model_properties(model):
    """Update model properties."""
    print("Updating model properties...")

    # Update model version
    model.set_version("2")

    # Add tags and terms
    model.add_tag(TagUrn("marketing"))
    model.add_term(GlossaryTermUrn("marketing"))

    # Add version alias
    model.add_version_alias("challenger")

    # Save the changes
    client._emit_mcps(model.as_mcps())


def update_model_group_properties(model_group):
    """Update model group properties."""
    print("Updating model group properties...")

    # Update description
    model_group.set_description("Updated description for airline forecast models")

    # Add tags and terms
    model_group.add_tag(TagUrn("production"))
    model_group.add_term(GlossaryTermUrn("time-series"))

    # Update custom properties
    model_group.set_custom_properties(
        {"team": "forecasting", "business_unit": "operations", "status": "active"}
    )

    # Save the changes
    client._emit_mcps(model_group.as_mcps())


def update_experiment_properties():
    """Update experiment properties."""
    print("Updating experiment properties...")

    # Create a container object for the existing experiment
    existing_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=advanced_experiment_id),
        display_name=advanced_experiment_name,
    )

    # Update properties
    existing_experiment.set_description(
        "Updated experiment for forecasting passenger numbers"
    )
    existing_experiment.add_tag(TagUrn("time-series"))
    existing_experiment.add_term(GlossaryTermUrn("forecasting"))
    existing_experiment.set_custom_properties(
        {"team": "forecasting", "priority": "high", "status": "active"}
    )

    # Save the changes
    client._emit_mcps(existing_experiment.as_mcps())


def main():
    # Parse arguments
    print("Creating AI assets...")

    # Comment in/out the functions you want to run
    # Create basic entities
    create_basic_model_group()
    create_basic_model()
    create_basic_experiment()
    create_basic_training_run()
    create_basic_dataset()

    # Create advanced entities
    advanced_model_group = create_advanced_model_group()
    advanced_model = create_advanced_model()
    advanced_experiment = create_advanced_experiment()
    advanced_run_urn = create_advanced_training_run()
    advanced_input_dataset, advanced_output_dataset = create_advanced_datasets()

    # # Create relationships - each can be commented out independently
    add_model_to_model_group(advanced_model, advanced_model_group)
    add_run_to_experiment(advanced_run_urn, advanced_experiment)
    add_run_to_model(advanced_model, advanced_run_id)
    add_run_to_model_group(advanced_model_group, advanced_run_id)
    add_input_dataset_to_run(advanced_run_urn, advanced_input_dataset)
    add_output_dataset_to_run(advanced_run_urn, advanced_output_dataset)

    # # Update properties - each can be commented out independently
    update_model_properties(advanced_model)
    update_model_group_properties(advanced_model_group)
    update_experiment_properties()

    print("All done! AI entities created successfully.")


if __name__ == "__main__":
    main()

```



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


**Python SDK: Update model-specific aspects**

```python
# Inlined from /metadata-ingestion/examples/library/mlmodel_update_aspects.py
import datahub.metadata.schema_classes as models
from datahub.metadata.urns import DatasetUrn, MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

model_urn = MlModelUrn(platform="mlflow", name="customer-churn-predictor")

mlmodel = client.entities.get(model_urn)

intended_use = models.IntendedUseClass(
    primaryUses=[
        "Predict customer churn to enable proactive retention campaigns",
        "Identify high-risk customers for targeted interventions",
    ],
    primaryUsers=[models.IntendedUserTypeClass.ENTERPRISE],
    outOfScopeUses=[
        "Not suitable for real-time predictions (batch inference only)",
        "Not trained on international markets outside North America",
    ],
)

mlmodel._set_aspect(intended_use)

training_data = models.TrainingDataClass(
    trainingData=[
        models.BaseDataClass(
            dataset=str(
                DatasetUrn(
                    platform="snowflake", name="prod.analytics.customer_features"
                )
            ),
            motivation="Historical customer data with confirmed churn labels",
            preProcessing=[
                "Removed customers with less than 30 days of history",
                "Standardized numerical features using StandardScaler",
                "One-hot encoded categorical variables",
            ],
        )
    ]
)

mlmodel._set_aspect(training_data)

source_code = models.SourceCodeClass(
    sourceCode=[
        models.SourceCodeUrlClass(
            type=models.SourceCodeUrlTypeClass.ML_MODEL_SOURCE_CODE,
            sourceCodeUrl="https://github.com/example/ml-models/tree/main/churn-predictor",
        )
    ]
)

mlmodel._set_aspect(source_code)

ethical_considerations = models.EthicalConsiderationsClass(
    data=["Model uses demographic data (age, location) which may be sensitive"],
    risksAndHarms=[
        "Predictions may disproportionately affect certain customer segments",
        "False positives could lead to unnecessary retention spending",
    ],
    mitigations=[
        "Regular bias audits conducted quarterly",
        "Human review required for high-value customer interventions",
    ],
)

mlmodel._set_aspect(ethical_considerations)

client.entities.update(mlmodel)

print(f"Updated aspects for model: {model_urn}")

```



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


**Complete Python Example: Full ML Workflow**

See the comprehensive example in `/metadata-ingestion/examples/ai/dh_ai_docs_demo.py` which demonstrates:

- Creating model groups with metadata
- Creating experiments to organize training runs
- Creating training runs with metrics, hyperparameters, and dataset lineage
- Creating models with versions and aliases
- Linking all entities together to form complete lineage
- Updating properties and managing the model lifecycle

The example shows both basic patterns for getting started and advanced patterns for production ML systems.



## Code Examples

### Querying ML Model Information

The standard REST APIs can be used to retrieve ML Model entities and their aspects:


**Python: Query an ML Model via REST API**

```python
# Inlined from /metadata-ingestion/examples/library/mlmodel_query_rest_api.py
import urllib.parse

import requests

gms_server = "http://localhost:8080"

model_urn = "urn:li:mlModel:(urn:li:dataPlatform:mlflow,customer-churn-predictor,PROD)"
encoded_urn = urllib.parse.quote(model_urn, safe="")

response = requests.get(f"{gms_server}/entities/{encoded_urn}")

if response.status_code == 200:
    entity = response.json()

    print(f"Entity URN: {entity['urn']}")
    print("\nAspects:")

    if "mlModelProperties" in entity["aspects"]:
        props = entity["aspects"]["mlModelProperties"]
        print(f"  Name: {props.get('name')}")
        print(f"  Description: {props.get('description')}")
        print(f"  Type: {props.get('type')}")

        if props.get("hyperParams"):
            print("\n  Hyperparameters:")
            for param in props["hyperParams"]:
                print(f"    - {param['name']}: {param['value']}")

        if props.get("trainingMetrics"):
            print("\n  Training Metrics:")
            for metric in props["trainingMetrics"]:
                print(f"    - {metric['name']}: {metric['value']}")

    if "globalTags" in entity["aspects"]:
        tags = entity["aspects"]["globalTags"]["tags"]
        print(f"\n  Tags: {[tag['tag'] for tag in tags]}")

    if "ownership" in entity["aspects"]:
        owners = entity["aspects"]["ownership"]["owners"]
        print(f"\n  Owners: {[owner['owner'] for owner in owners]}")

    if "intendedUse" in entity["aspects"]:
        intended = entity["aspects"]["intendedUse"]
        print(f"\n  Primary Uses: {intended.get('primaryUses')}")
        print(f"  Out of Scope Uses: {intended.get('outOfScopeUses')}")

else:
    print(f"Failed to fetch entity: {response.status_code}")
    print(response.text)

```



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

#### mlModelKey
Key for an ML model



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Standardized platform urn for the model |  |
| name | string | ✓ | Name of the MLModel | Searchable (id) |
| origin | FabricType | ✓ | Fabric type where model belongs to or where it was generated | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelKey"
  },
  "name": "MLModelKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Standardized platform urn for the model"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldName": "id",
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": "string",
      "name": "name",
      "doc": "Name of the MLModel"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "TEXT_PARTIAL",
        "filterNameOverride": "Environment",
        "queryByDefault": false
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CERT": "Designates certification fabrics",
          "CORP": "Designates corporation fabrics",
          "DEV": "Designates development fabrics",
          "EI": "Designates early-integration fabrics",
          "NON_PROD": "Designates non-production fabrics",
          "PRD": "Alternative Prod spelling",
          "PRE": "Designates pre-production fabrics",
          "PROD": "Designates production fabrics",
          "QA": "Designates quality assurance fabrics",
          "RVW": "Designates review fabrics",
          "SANDBOX": "Designates sandbox fabrics",
          "SBX": "Alternative spelling for sandbox",
          "SIT": "System Integration Testing",
          "STG": "Designates staging fabrics",
          "TEST": "Designates testing fabrics",
          "TST": "Alternative Test spelling",
          "UAT": "Designates user acceptance testing fabrics"
        },
        "name": "FabricType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "DEV",
          "TEST",
          "QA",
          "UAT",
          "EI",
          "PRE",
          "STG",
          "NON_PROD",
          "PROD",
          "CORP",
          "RVW",
          "PRD",
          "TST",
          "SIT",
          "SBX",
          "SANDBOX",
          "CERT"
        ],
        "doc": "Fabric group type"
      },
      "name": "origin",
      "doc": "Fabric type where model belongs to or where it was generated"
    }
  ],
  "doc": "Key for an ML model"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
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
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### mlModelProperties
Properties associated with a ML Model



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| trainingJobs | string[] |  | List of jobs or process instances (if any) used to train the model or group. Visible in Lineage. ... | → TrainedBy |
| downstreamJobs | string[] |  | List of jobs or process instances (if any) that use the model or group. | → UsedBy |
| name | string |  | Display name of the MLModel | Searchable |
| description | string |  | Documentation of the MLModel | Searchable |
| date | long |  | Date when the MLModel was developed | ⚠️ Deprecated |
| created | [TimeStamp](#timestamp) |  | Audit stamp containing who created this and when |  |
| lastModified | [TimeStamp](#timestamp) |  | Date when the MLModel was last modified |  |
| version | [VersionTag](#versiontag) |  | Version of the MLModel |  |
| type | string |  | Type of Algorithm or MLModel such as whether it is a Naive Bayes classifier, Convolutional Neural... | Searchable |
| hyperParameters | map |  | Hyper Parameters of the MLModel  NOTE: these are deprecated in favor of hyperParams |  |
| hyperParams | MLHyperParam[] |  | Hyperparameters of the MLModel |  |
| trainingMetrics | [MLMetric](#mlmetric)[] |  | Metrics of the MLModel used in training |  |
| onlineMetrics | [MLMetric](#mlmetric)[] |  | Metrics of the MLModel used in production |  |
| mlFeatures | string[] |  | List of features used for MLModel training | → Consumes |
| tags | string[] | ✓ | Tags for the MLModel |  |
| deployments | string[] |  | Deployments for the MLModel | → DeployedTo |
| groups | string[] |  | Groups the model belongs to | → MemberOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelProperties"
  },
  "name": "MLModelProperties",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "name": "TrainedBy"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "trainingJobs",
      "default": null,
      "doc": "List of jobs or process instances (if any) used to train the model or group. Visible in Lineage. Note that ML Models can also be specified as the output of a specific Data Process Instances (runs) via the DataProcessInstanceOutputs aspect."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "UsedBy"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "downstreamJobs",
      "default": null,
      "doc": "List of jobs or process instances (if any) that use the model or group."
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM",
        "queryByDefault": true,
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the MLModel"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the MLModel"
    },
    {
      "deprecated": true,
      "type": [
        "null",
        "long"
      ],
      "name": "date",
      "default": null,
      "doc": "Date when the MLModel was developed"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the event occur"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "Optional: The actor urn involved in the event."
            }
          ],
          "doc": "A standard event timestamp"
        }
      ],
      "name": "created",
      "default": null,
      "doc": "Audit stamp containing who created this and when"
    },
    {
      "type": [
        "null",
        "com.linkedin.common.TimeStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "Date when the MLModel was last modified"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "VersionTag",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "versionTag",
              "default": null
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "metadataAttribution",
              "default": null
            }
          ],
          "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
        }
      ],
      "name": "version",
      "default": null,
      "doc": "Version of the MLModel"
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "type",
      "default": null,
      "doc": "Type of Algorithm or MLModel such as whether it is a Naive Bayes classifier, Convolutional Neural Network, etc"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": [
            "string",
            "int",
            "float",
            "double",
            "boolean"
          ]
        }
      ],
      "name": "hyperParameters",
      "default": null,
      "doc": "Hyper Parameters of the MLModel\n\nNOTE: these are deprecated in favor of hyperParams"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlHyperParam"
            },
            "name": "MLHyperParam",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the MLHyperParam was developed"
              }
            ],
            "doc": "Properties associated with an ML Hyper Param"
          }
        }
      ],
      "name": "hyperParams",
      "default": null,
      "doc": "Hyperparameters of the MLModel"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlMetric"
            },
            "name": "MLMetric",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the mlMetric was developed"
              }
            ],
            "doc": "Properties associated with an ML Metric"
          }
        }
      ],
      "name": "trainingMetrics",
      "default": null,
      "doc": "Metrics of the MLModel used in training"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.ml.metadata.MLMetric"
        }
      ],
      "name": "onlineMetrics",
      "default": null,
      "doc": "Metrics of the MLModel used in production"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlFeature"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "mlFeatures",
      "default": null,
      "doc": "List of features used for MLModel training"
    },
    {
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "tags",
      "default": [],
      "doc": "Tags for the MLModel"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlModelDeployment"
          ],
          "name": "DeployedTo"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "deployments",
      "default": null,
      "doc": "Deployments for the MLModel"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlModelGroup"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "MemberOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "groups",
      "default": null,
      "doc": "Groups the model belongs to"
    }
  ],
  "doc": "Properties associated with a ML Model"
}
```





#### intendedUse
Intended Use for the ML Model



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| primaryUses | string[] |  | Primary Use cases for the MLModel. |  |
| primaryUsers | IntendedUserType[] |  | Primary Intended Users - For example, was the MLModel developed for entertainment purposes, for h... |  |
| outOfScopeUses | string[] |  | Highlight technology that the MLModel might easily be confused with, or related contexts that use... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "intendedUse"
  },
  "name": "IntendedUse",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "primaryUses",
      "default": null,
      "doc": "Primary Use cases for the MLModel."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "enum",
            "name": "IntendedUserType",
            "namespace": "com.linkedin.ml.metadata",
            "symbols": [
              "ENTERPRISE",
              "HOBBY",
              "ENTERTAINMENT"
            ]
          }
        }
      ],
      "name": "primaryUsers",
      "default": null,
      "doc": "Primary Intended Users - For example, was the MLModel developed for entertainment purposes, for hobbyists, or enterprise solutions?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outOfScopeUses",
      "default": null,
      "doc": "Highlight technology that the MLModel might easily be confused with, or related contexts that users could try to apply the MLModel to."
    }
  ],
  "doc": "Intended Use for the ML Model"
}
```





#### mlModelFactorPrompts
Prompts which affect the performance of the MLModel



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| relevantFactors | [MLModelFactors](#mlmodelfactors)[] |  | What are foreseeable salient factors for which MLModel performance may vary, and how were these d... |  |
| evaluationFactors | [MLModelFactors](#mlmodelfactors)[] |  | Which factors are being reported, and why were these chosen? |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelFactorPrompts"
  },
  "name": "MLModelFactorPrompts",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "MLModelFactors",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "groups",
                "default": null,
                "doc": "Groups refers to distinct categories with similar characteristics that are present in the evaluation data instances.\nFor human-centric machine learning MLModels, groups are people who share one or multiple characteristics."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "instrumentation",
                "default": null,
                "doc": "The performance of a MLModel can vary depending on what instruments were used to capture the input to the MLModel.\nFor example, a face detection model may perform differently depending on the camera's hardware and software,\nincluding lens, image stabilization, high dynamic range techniques, and background blurring for portrait mode."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "environment",
                "default": null,
                "doc": "A further factor affecting MLModel performance is the environment in which it is deployed."
              }
            ],
            "doc": "Factors affecting the performance of the MLModel."
          }
        }
      ],
      "name": "relevantFactors",
      "default": null,
      "doc": "What are foreseeable salient factors for which MLModel performance may vary, and how were these determined?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.ml.metadata.MLModelFactors"
        }
      ],
      "name": "evaluationFactors",
      "default": null,
      "doc": "Which factors are being reported, and why were these chosen?"
    }
  ],
  "doc": "Prompts which affect the performance of the MLModel"
}
```





#### mlModelMetrics
Metrics to be featured for the MLModel.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| performanceMeasures | string[] |  | Measures of MLModel performance |  |
| decisionThreshold | string[] |  | Decision Thresholds used (if any)? |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelMetrics"
  },
  "name": "Metrics",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "performanceMeasures",
      "default": null,
      "doc": "Measures of MLModel performance"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "decisionThreshold",
      "default": null,
      "doc": "Decision Thresholds used (if any)?"
    }
  ],
  "doc": "Metrics to be featured for the MLModel."
}
```





#### mlModelEvaluationData
All referenced datasets would ideally point to any set of documents that provide visibility into the source and composition of the dataset.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| evaluationData | [BaseData](#basedata)[] | ✓ | Details on the dataset(s) used for the quantitative analyses in the MLModel |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelEvaluationData"
  },
  "name": "EvaluationData",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BaseData",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "What dataset were used in the MLModel?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "motivation",
              "default": null,
              "doc": "Why was this dataset chosen?"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "preProcessing",
              "default": null,
              "doc": "How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"
            }
          ],
          "doc": "BaseData record"
        }
      },
      "name": "evaluationData",
      "doc": "Details on the dataset(s) used for the quantitative analyses in the MLModel"
    }
  ],
  "doc": "All referenced datasets would ideally point to any set of documents that provide visibility into the source and composition of the dataset."
}
```





#### mlModelTrainingData
Ideally, the MLModel card would contain as much information about the training data as the evaluation data. However, there might be cases where it is not feasible to provide this level of detailed information about the training data. For example, the data may be proprietary, or require a non-disclosure agreement. In these cases, we advocate for basic details about the distributions over groups in the data, as well as any other details that could inform stakeholders on the kinds of biases the model may have encoded.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| trainingData | [BaseData](#basedata)[] | ✓ | Details on the dataset(s) used for training the MLModel |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelTrainingData"
  },
  "name": "TrainingData",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BaseData",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "What dataset were used in the MLModel?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "motivation",
              "default": null,
              "doc": "Why was this dataset chosen?"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "preProcessing",
              "default": null,
              "doc": "How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"
            }
          ],
          "doc": "BaseData record"
        }
      },
      "name": "trainingData",
      "doc": "Details on the dataset(s) used for training the MLModel"
    }
  ],
  "doc": "Ideally, the MLModel card would contain as much information about the training data as the evaluation data. However, there might be cases where it is not feasible to provide this level of detailed information about the training data. For example, the data may be proprietary, or require a non-disclosure agreement. In these cases, we advocate for basic details about the distributions over groups in the data, as well as any other details that could inform stakeholders on the kinds of biases the model may have encoded."
}
```





#### mlModelQuantitativeAnalyses
Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative analyses should provide the results of evaluating the MLModel according to the chosen metrics, providing confidence interval values when possible.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| unitaryResults | string |  | Link to a dashboard with results showing how the MLModel performed with respect to each factor |  |
| intersectionalResults | string |  | Link to a dashboard with results showing how the MLModel performed with respect to the intersecti... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelQuantitativeAnalyses"
  },
  "name": "QuantitativeAnalyses",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "unitaryResults",
      "default": null,
      "doc": "Link to a dashboard with results showing how the MLModel performed with respect to each factor"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "intersectionalResults",
      "default": null,
      "doc": "Link to a dashboard with results showing how the MLModel performed with respect to the intersection of evaluated factors?"
    }
  ],
  "doc": "Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative analyses should provide the results of evaluating the MLModel according to the chosen metrics, providing confidence interval values when possible."
}
```





#### mlModelEthicalConsiderations
This section is intended to demonstrate the ethical considerations that went into MLModel development, surfacing ethical challenges and solutions to stakeholders.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| data | string[] |  | Does the MLModel use any sensitive data (e.g., protected classes)? |  |
| humanLife | string[] |  | Is the MLModel intended to inform decisions about matters central to human life or flourishing - ... |  |
| mitigations | string[] |  | What risk mitigation strategies were used during MLModel development? |  |
| risksAndHarms | string[] |  | What risks may be present in MLModel usage? Try to identify the potential recipients, likelihood,... |  |
| useCases | string[] |  | Are there any known MLModel use cases that are especially fraught? This may connect directly to t... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelEthicalConsiderations"
  },
  "name": "EthicalConsiderations",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "data",
      "default": null,
      "doc": "Does the MLModel use any sensitive data (e.g., protected classes)?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "humanLife",
      "default": null,
      "doc": " Is the MLModel intended to inform decisions about matters central to human life or flourishing - e.g., health or safety? Or could it be used in such a way?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "mitigations",
      "default": null,
      "doc": "What risk mitigation strategies were used during MLModel development?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "risksAndHarms",
      "default": null,
      "doc": "What risks may be present in MLModel usage? Try to identify the potential recipients, likelihood, and magnitude of harms. If these cannot be determined, note that they were considered but remain unknown."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "useCases",
      "default": null,
      "doc": "Are there any known MLModel use cases that are especially fraught? This may connect directly to the intended use section"
    }
  ],
  "doc": "This section is intended to demonstrate the ethical considerations that went into MLModel development, surfacing ethical challenges and solutions to stakeholders."
}
```





#### mlModelCaveatsAndRecommendations
This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| caveats | CaveatDetails |  | This section should list additional concerns that were not covered in the previous sections. For ... |  |
| recommendations | string |  | Recommendations on where this MLModel should be used. |  |
| idealDatasetCharacteristics | string[] |  | Ideal characteristics of an evaluation dataset for this MLModel |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelCaveatsAndRecommendations"
  },
  "name": "CaveatsAndRecommendations",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CaveatDetails",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "type": [
                "null",
                "boolean"
              ],
              "name": "needsFurtherTesting",
              "default": null,
              "doc": "Did the results suggest any further testing?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "caveatDescription",
              "default": null,
              "doc": "Caveat Description\nFor ex: Given gender classes are binary (male/not male), which we include as male/female. Further work needed to evaluate across a spectrum of genders."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "groupsNotRepresented",
              "default": null,
              "doc": "Relevant groups that were not represented in the evaluation dataset?"
            }
          ],
          "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"
        }
      ],
      "name": "caveats",
      "default": null,
      "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset?"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "recommendations",
      "default": null,
      "doc": "Recommendations on where this MLModel should be used."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "idealDatasetCharacteristics",
      "default": null,
      "doc": "Ideal characteristics of an evaluation dataset for this MLModel"
    }
  ],
  "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"
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





#### sourceCode
Source Code



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| sourceCode | SourceCodeUrl[] | ✓ | Source Code along with types |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "sourceCode"
  },
  "name": "SourceCode",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SourceCodeUrl",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "SourceCodeUrlType",
                "namespace": "com.linkedin.ml.metadata",
                "symbols": [
                  "ML_MODEL_SOURCE_CODE",
                  "TRAINING_PIPELINE_SOURCE_CODE",
                  "EVALUATION_PIPELINE_SOURCE_CODE"
                ]
              },
              "name": "type",
              "doc": "Source Code Url Types"
            },
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "sourceCodeUrl",
              "doc": "Source Code Url"
            }
          ],
          "doc": "Source Code Url Entity"
        }
      },
      "name": "sourceCode",
      "doc": "Source Code along with types"
    }
  ],
  "doc": "Source Code"
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





#### cost
None



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| costType | CostType | ✓ |  |  |
| cost | CostCost | ✓ |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "cost"
  },
  "name": "Cost",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ORG_COST_TYPE": "Org Cost Type to which the Cost of this entity should be attributed to"
        },
        "name": "CostType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "ORG_COST_TYPE"
        ],
        "doc": "Type of Cost Code"
      },
      "name": "costType"
    },
    {
      "type": {
        "type": "record",
        "name": "CostCost",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "double"
            ],
            "name": "costId",
            "default": null
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "costCode",
            "default": null
          },
          {
            "type": {
              "type": "enum",
              "name": "CostCostDiscriminator",
              "namespace": "com.linkedin.common",
              "symbols": [
                "costId",
                "costCode"
              ]
            },
            "name": "fieldDiscriminator",
            "doc": "Contains the name of the field that has its value set."
          }
        ]
      },
      "name": "cost"
    }
  ]
}
```





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
}
```





#### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| paths | string[] | ✓ | A list of valid browse paths for the entity.  Browse paths are expected to be forward slash-separ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





#### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| path | BrowsePathEntry[] | ✓ | A valid browse path for the entity. This field is provided by DataHub by default. This aspect is ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```





#### editableMlModelProperties
Properties associated with a ML Model editable from the UI



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| description | string |  | Documentation of the ml model | Searchable (editedDescription) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableMlModelProperties"
  },
  "name": "EditableMLModelProperties",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the ml model"
    }
  ],
  "doc": "Properties associated with a ML Model editable from the UI"
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "context",
                "default": null,
                "doc": "Additional context about the association"
              },
              {
                "Searchable": {
                  "/actor": {
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "MetadataAttribution",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When this metadata was updated."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "source",
                        "default": null,
                        "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                      },
                      {
                        "type": {
                          "type": "map",
                          "values": "string"
                        },
                        "name": "sourceDetail",
                        "default": {},
                        "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                      }
                    ],
                    "doc": "Information about who, why, and how this metadata was applied"
                  }
                ],
                "name": "attribution",
                "default": null,
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
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
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
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
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
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
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### versionProperties
Properties about a versioned asset i.e. dataset, ML Model, etc.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| versionSet | string | ✓ | The linked Version Set entity that ties multiple versioned assets together | Searchable, → VersionOf |
| version | [VersionTag](#versiontag) | ✓ | Label for this versioned asset, is unique within a version set | Searchable |
| aliases | [VersionTag](#versiontag)[] | ✓ | Associated aliases for this versioned asset | Searchable |
| comment | string |  | Comment documenting what this version was created for, changes, or represents |  |
| sortId | string | ✓ | Sort identifier that determines where a version lives in the order of the Version Set. What this ... | Searchable (versionSortId) |
| versioningScheme | VersioningScheme | ✓ | What versioning scheme `sortId` belongs to. Defaults to a plain string that is lexicographically ... |  |
| sourceCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when this asset version was created in the source system. |  |
| metadataCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when the metadata for this version was created in DataHub |  |
| isLatest | boolean |  | Marks whether this version is currently the latest. Set by a side effect and should not be modifi... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionProperties"
  },
  "name": "VersionProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "versionSet"
        ],
        "name": "VersionOf"
      },
      "Searchable": {
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "versionSet",
      "doc": "The linked Version Set entity that ties multiple versioned assets together"
    },
    {
      "Searchable": {
        "/versionTag": {
          "fieldName": "version",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "record",
        "name": "VersionTag",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "string"
            ],
            "name": "versionTag",
            "default": null
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "MetadataAttribution",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When this metadata was updated."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "source",
                    "default": null,
                    "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                  },
                  {
                    "type": {
                      "type": "map",
                      "values": "string"
                    },
                    "name": "sourceDetail",
                    "default": {},
                    "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                  }
                ],
                "doc": "Information about who, why, and how this metadata was applied"
              }
            ],
            "name": "metadataAttribution",
            "default": null
          }
        ],
        "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
      },
      "name": "version",
      "doc": "Label for this versioned asset, is unique within a version set"
    },
    {
      "Searchable": {
        "/*/versionTag": {
          "fieldName": "aliases",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.VersionTag"
      },
      "name": "aliases",
      "default": [],
      "doc": "Associated aliases for this versioned asset"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "comment",
      "default": null,
      "doc": "Comment documenting what this version was created for, changes, or represents"
    },
    {
      "Searchable": {
        "fieldName": "versionSortId",
        "queryByDefault": false
      },
      "type": "string",
      "name": "sortId",
      "doc": "Sort identifier that determines where a version lives in the order of the Version Set.\nWhat this looks like depends on the Version Scheme. For sort ids generated by DataHub we use an 8 character string representation."
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "default": "LEXICOGRAPHIC_STRING",
      "doc": "What versioning scheme `sortId` belongs to.\nDefaults to a plain string that is lexicographically sorted."
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
      "name": "sourceCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when this asset version was created in the source system."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "metadataCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when the metadata for this version was created in DataHub"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": [
        "null",
        "boolean"
      ],
      "name": "isLatest",
      "default": null,
      "doc": "Marks whether this version is currently the latest. Set by a side effect and should not be modified by API."
    }
  ],
  "doc": "Properties about a versioned asset i.e. dataset, ML Model, etc."
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### container
Link from an asset to its parent container



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| container | string | ✓ | The parent container of an asset | Searchable, → IsPartOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
}
```





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
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

#### BaseData

BaseData record

**Fields:**

- `dataset` (string): What dataset were used in the MLModel?
- `motivation` (string?): Why was this dataset chosen?
- `preProcessing` (string[]?): How was the data preprocessed (e.g., tokenization of sentences, cropping of i...

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### MLMetric

Properties associated with an ML Metric

**Fields:**

- `name` (string): Name of the mlMetric
- `description` (string?): Documentation of the mlMetric
- `value` (string?): The value of the mlMetric
- `createdAt` (long?): Date when the mlMetric was developed

#### MLModelFactors

Factors affecting the performance of the MLModel.

**Fields:**

- `groups` (string[]?): Groups refers to distinct categories with similar characteristics that are pr...
- `instrumentation` (string[]?): The performance of a MLModel can vary depending on what instruments were used...
- `environment` (string[]?): A further factor affecting MLModel performance is the environment in which it...

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...

#### TimeStamp

A standard event timestamp

**Fields:**

- `time` (long): When did the event occur
- `actor` (string?): Optional: The actor urn involved in the event.

#### VersionTag

A resource-defined string representing the resource state for the purpose of concurrency control

**Fields:**

- `versionTag` (string?): 
- `metadataAttribution` (MetadataAttribution?): 


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TrainedBy

   - DataJob via `mlModelProperties.trainingJobs`
   - DataProcessInstance via `mlModelProperties.trainingJobs`
- UsedBy

   - DataJob via `mlModelProperties.downstreamJobs`
   - DataProcessInstance via `mlModelProperties.downstreamJobs`
- Consumes

   - MlFeature via `mlModelProperties.mlFeatures`
- DeployedTo

   - MlModelDeployment via `mlModelProperties.deployments`
- MemberOf

   - MlModelGroup via `mlModelProperties.groups`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- VersionOf

   - VersionSet via `versionProperties.versionSet`
- IsPartOf

   - Container via `container.container`
#### Incoming
These are the relationships stored in other entity's aspects
- Consumes

   - DataProcessInstance via `dataProcessInstanceInput.inputs`
- DataProcessInstanceConsumes

   - DataProcessInstance via `dataProcessInstanceInput.inputEdges`
- Produces

   - DataProcessInstance via `dataProcessInstanceOutput.outputs`
- DataProcessInstanceProduces

   - DataProcessInstance via `dataProcessInstanceOutput.outputEdges`
- RelatedAsset

   - Document via `documentInfo.relatedAssets.asset`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
