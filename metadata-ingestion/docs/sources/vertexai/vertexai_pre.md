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
| `aiplatform.experiments.get`        | Allows a user to view details of a specific experiment in a project  |
| `aiplatform.metadataStores.list`    | allows a user to view and list all metadata store in a project       |
| `aiplatform.metadataStores.get`     | allows a user to view details of a specific metadata store           |
| `aiplatform.executions.list`        | allows a user to view and list all executions in a project           |
| `aiplatform.executions.get`         | allows a user to view details of a specific execution                |
| `aiplatform.datasets.list`          | allows a user to view and list all datasets in a project             |
| `aiplatform.datasets.get`           | allows a user to view details of a specific dataset                  |
| `aiplatform.pipelineJobs.get`       | allows a user to view and list all pipeline jobs in a project        |
| `aiplatform.pipelineJobs.list`      | allows a user to view details of a specific pipeline job             |

**Note**: ML Metadata extraction (enabled by default for enhanced lineage tracking) requires the `aiplatform.metadataStores.*` and `aiplatform.executions.*` permissions listed above. If your service account lacks these permissions, the connector will gracefully fall back with warnings. To disable ML Metadata features, set `use_ml_metadata_for_lineage: false`, `extract_execution_metrics: false`, and `include_evaluations: false`.

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

The source supports ingesting across multiple GCP projects by specifying `project_ids`, `project_labels`, or `project_id_pattern`. Use `env` (e.g., `PROD`, `DEV`, `STAGING`) to distinguish between environments. The optional `platform_instance` field namespaces resources to avoid URN collisions when ingesting from multiple Vertex AI setups.

**Performance**: Resources are fetched ordered by update time (most recently updated first). Limits like `max_training_jobs_per_type` cap how many resources are processed per run — for example, `max_training_jobs_per_type: 1000` will process only the 1000 most recently updated training jobs of each type.

Enabling `stateful_ingestion` has two effects: (1) resources not updated since the previous run are skipped, reducing redundant API calls on subsequent runs; and (2) entities deleted from Vertex AI are automatically soft-deleted in DataHub. Use `stateful_ingestion.ignore_old_state: true` to get soft-deletion only without the incremental skip behaviour.

For improved organization in the DataHub UI:

- Model versions are organized under their respective model group folders
- Pipeline tasks and task runs are nested under their parent pipeline folders

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
|   [`PipelineJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.PipelineJob)    |            [`DataFlow`](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)            |                                                                                                                                                                                                                                                                                             A Vertex AI Pipeline is mapped to a stable DataFlow entity in DataHub (one per pipeline template). The compiled pipeline spec name (`pipelineInfo.name`, i.e. the `@pipeline(name="...")` argument) is used as the stable identifier; non-Kubeflow pipelines fall back to `display_name` with any timestamp suffix stripped. Each pipeline run creates a DataProcessInstance, and pipeline tasks are modeled as DataJobs nested under the parent DataFlow. This enables proper incremental lineage aggregation across multiple pipeline runs. **Breaking Change (v1.4.0)**: Previously, each pipeline run created a separate DataFlow entity. Existing pipeline entities from earlier versions will appear as separate entities from new ingestion runs. Enable stateful ingestion with stale entity removal to clean up old pipeline entities.                                                                                                                                                                                                                                                                                              |
|       [`PipelineJob Task`](https://cloud.google.com/vertex-ai/docs/pipelines/build-pipeline#understanding-pipelines)       |             [`DataJob`](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             Each task within a Vertex AI pipeline is modeled as a DataJob in DataHub, nested under its parent pipeline DataFlow. Tasks represent individual steps in the pipeline workflow.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|     [`PipelineJob Task Run`](https://cloud.google.com/vertex-ai/docs/pipelines/build-pipeline#understanding-pipelines)     | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   Each execution of a pipeline task is modeled as a DataProcessInstance, linked to its DataJob (task definition). This captures runtime metadata, inputs/outputs, and lineage for each task execution.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

##### Vertex AI Concept Diagram

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/vertexai/concept-mapping.png"/>
</p>

#### Lineage

The connector captures comprehensive lineage relationships including cross-platform lineage to external data sources:

**Core Vertex AI Lineage**:

- Training job → Model (AutoML and CustomJob)
- Dataset → Training job (AutoML and ML Metadata-based)
- Training job → Output models (ML Metadata Executions)
- Model → Training datasets (direct upstream lineage via TrainingData aspect)
- Experiment run → Model (outputs)
- Model evaluation → Model and test datasets (inputs)
- Pipeline task runs → Models and datasets (inputs/outputs via DataProcessInstance aspects)

**Cross-Platform Lineage** (external data sources):

The connector links Vertex AI resources to external datasets when referenced in job configurations or ML Metadata artifacts. Supported platforms:

- **Google Cloud Storage** (gs://...) → `gcs` platform
- **BigQuery** (bq://project.dataset.table or projects/.../datasets/.../tables/...) → `bigquery` platform
- **Amazon S3** (s3://..., s3a://...) → `s3` platform
- **Azure Blob Storage** (wasbs://..., abfss://...) → `abs` platform
- **Snowflake** (snowflake://...) → `snowflake` platform

Use `platform_instance_map` to configure platform instances and environments for external platforms, ensuring URNs match those from native connectors for proper lineage connectivity.
