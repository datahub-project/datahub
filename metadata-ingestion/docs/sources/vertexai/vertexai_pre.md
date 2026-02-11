Ingesting metadata from VertexAI requires using the **Vertex AI** module.

#### Prerequisites

Please refer to the [Vertex AI documentation](https://cloud.google.com/vertex-ai/docs) for basic information on Vertex AI.

#### Credentials to access to GCP

Please read the section to understand how to set up application default Credentials to [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

##### Permissions

- Grant the following permissions to the Service Account on every project where you would like to extract metadata from

Default GCP Role which contains these permissions [roles/aiplatform.viewer](https://cloud.google.com/vertex-ai/docs/general/access-control#aiplatform.viewer)

| Permission                          | Description                                                          |
| ----------------------------------- | -------------------------------------------------------------------- |
| `aiplatform.models.list`            | Allows a user to view and list all ML models in a project            |
| `aiplatform.models.get`             | Allows a user to view details of a specific ML model                 |
| `aiplatform.endpoints.list`         | Allows a user to view and list all prediction endpoints in a project |
| `aiplatform.endpoints.get`          | Allows a user to view details of a specific prediction endpoint      |
| `aiplatform.trainingPipelines.list` | Allows a user to view and list all training pipelines in a project   |
| `aiplatform.trainingPipelines.get`  | Allows a user to view details of a specific training pipeline        |
| `aiplatform.customJobs.list`        | Allows a user to view and list all custom jobs in a project          |
| `aiplatform.customJobs.get`         | Allows a user to view details of a specific custom job               |
| `aiplatform.experiments.list`       | Allows a user to view and list all experiments in a project          |
| `laiplatform.experiments.get`       | Allows a user to view details of a specific experiment in a project  |
| `aiplatform.metadataStores.list`    | allows a user to view and list all metadata store in a project       |
| `aiplatform.metadataStores.get`     | allows a user to view details of a specific metadata store           |
| `aiplatform.executions.list`        | allows a user to view and list all executions in a project           |
| `aiplatform.executions.get`         | allows a user to view details of a specific execution                |
| `aiplatform.datasets.list`          | allows a user to view and list all datasets in a project             |
| `aiplatform.datasets.get`           | allows a user to view details of a specific dataset                  |
| `aiplatform.pipelineJobs.get`       | allows a user to view and list all pipeline jobs in a project        |
| `aiplatform.pipelineJobs.list`      | allows a user to view details of a specific pipeline job             |

#### Create a service account and assign roles

1. Setup a ServiceAccount as per [GCP docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and assign the previously created role to this service account.
2. Download a service account JSON keyfile.

   - Example credential file:

   ```json
   {
     "type": "service_account",
     "project_id": "project-id-1234567",
     "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
     "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
     "client_id": "113545814931671546333",
     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
     "token_uri": "https://oauth2.googleapis.com/token",
     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
   }
   ```

3. To provide credentials to the source, you can either:

- Set an environment variable:

  ```sh
  $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
  ```

  _or_

- Set credential config in your source based on the credential json file. For example:

  ```yml
  credential:
    private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
    private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
    client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
    client_id: "123456678890"
  ```

### Integration Details

Ingestion Job extracts Models, Datasets, Training Jobs, Endpoints, Experiments, Experiment Runs, Model Evaluations, and Pipelines from Vertex AI in a given project and region.

The source supports ingesting across multiple GCP projects by specifying `project_ids`, `project_labels`, or `project_id_pattern`. The optional `platform_instance` configuration field can be used to distinguish between different Vertex AI environments (e.g., dev, staging, prod). Model versions are organized under their model group folders in the DataHub UI for easier navigation.

#### New Features for CustomJob Lineage

The connector now supports extracting lineage and metrics from CustomJob training jobs using the **Vertex AI ML Metadata API**. This enables:

- **Full lineage tracking** for CustomJob: input datasets → training job → output models
- **Hyperparameters and metrics extraction** from training jobs that log to ML Metadata Executions
- **Model evaluation** ingestion with evaluation metrics and lineage to models

These features are controlled by new configuration options:

- `use_ml_metadata_for_lineage` (default: `true`) - Extracts lineage from ML Metadata for CustomJob and other non-AutoML training jobs
- `extract_execution_metrics` (default: `true`) - Extracts hyperparameters and metrics from ML Metadata Executions
- `include_evaluations` (default: `true`) - Ingests model evaluations and evaluation metrics

**Note**: For CustomJob lineage to work, your training jobs must log to Vertex AI ML Metadata. This happens automatically when using Vertex AI Experiments SDK or manually logging artifacts/executions to ML Metadata.

#### Concept Mapping

This ingestion source maps the following Vertex AI Concepts to DataHub Concepts:

|                                                       Source Concept                                                       |                                             DataHub Concept                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| :------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
|         [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)          |        [`MlModelGroup`](https://docs.datahub.com/docs/generated/metamodel/entities/mlmodelgroup/)        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  The name of a Model Group is the same as Model's name. Model serve as containers for multiple versions of the same model in Vertex AI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|                    [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                    |             [`MlModel`](https://docs.datahub.com/docs/generated/metamodel/entities/mlmodel/)             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   The name of a Model is `{model_name}_{model_version}` (e.g. my_vertexai_model_1 for model registered to Model Registry or Deployed to Endpoint. Each Model Version represents a specific iteration of a model with its own metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|                                                     Dataset <br/><br/>                                                     |             [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)              |                                                                                                                                                                                                                                                                                                                                                  A Managed Dataset resource in Vertex AI is mapped to Dataset in DataHub. <br></br> Supported types of datasets include ([`Text`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset), [`Tabular`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TabularDataset), [`Image Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset), [`Video`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset), [`TimeSeries`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TimeSeriesDataset))                                                                                                                                                                                                                                                                                                                                                   |
|                     [`Training Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)                     | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | A Training Job is mapped as DataProcessInstance in DataHub. <br></br> Supported types of training jobs include ([`AutoMLTextTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTextTrainingJob), [`AutoMLTabularTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTabularTrainingJob), [`AutoMLImageTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLImageTrainingJob), [`AutoMLVideoTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLVideoTrainingJob), [`AutoMLForecastingTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLForecastingTrainingJob), [`Custom Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob), [`Custom TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomTrainingJob), [`Custom Container TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomContainerTrainingJob), [`Custom Python Packaging Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomPythonPackageTrainingJob) ) |
|    [`Experiment`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Experiment)     |           [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/)           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         Experiments organize related runs and serve as logical groupings for model development iterations. Each Experiment is mapped to a Container in DataHub.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                An Experiment Run represents a single execution of a ML workflow. An Experiment Run tracks ML parameters, metricis, artifacts and metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|     [`Execution`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Execution)      | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Metadata Execution resource for Vertex AI. Metadata Execution is started in a experiment run and captures input and output artifacts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

Vertex AI Concept Diagram:

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/vertexai/concept-mapping.png"/>
</p>

#### Lineage

Lineage is emitted using Vertex AI API to capture the following relationships:

**AutoML Jobs** (existing functionality):

- A training job and a model (which training job produces a model)
- A dataset and a training job (which dataset was consumed by a training job to train a model)

**CustomJob and other training jobs** (new ML Metadata-based lineage):

- Input datasets → training job (extracted from ML Metadata Executions)
- Training job → output models (extracted from ML Metadata Executions)
- Requires `use_ml_metadata_for_lineage: true` (default)
- Training jobs must log to ML Metadata for lineage to be captured

**Other relationships**:

- Experiment runs and an experiment
- Metadata execution and an experiment run
- Model evaluations and models
- Cross-platform lineage from Execution and Job artifacts to external sources when URIs are present:
  - GCS artifacts (gs://...) are linked to `gcs` datasets
  - BigQuery artifacts (bq://project.dataset.table or projects/.../datasets/.../tables/...) are linked to `bigquery` datasets

**Note for CustomJob users**: To enable lineage tracking for CustomJob, ensure your training scripts log artifacts to Vertex AI ML Metadata using the Vertex AI SDK:

```python
from google.cloud import aiplatform

# Initialize Vertex AI
aiplatform.init(project="your-project", location="us-central1")

# Log input dataset
dataset_artifact = aiplatform.Artifact.create(
    schema_title="system.Dataset",
    uri="gs://your-bucket/data/train.csv",
    display_name="training-dataset"
)

# In your training job, link artifacts via Execution
with aiplatform.start_execution(
    schema_title="system.ContainerExecution",
    display_name=f"training-job-{job_name}"
) as execution:
    execution.assign_input_artifacts([dataset_artifact])
    # ... training logic ...
    model_artifact = aiplatform.Artifact.create(
        schema_title="system.Model",
        uri=model_uri,
        display_name="trained-model"
    )
    execution.assign_output_artifacts([model_artifact])
```
