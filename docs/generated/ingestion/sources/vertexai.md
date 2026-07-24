


# Vertex AI

## Overview

Vertexai is a machine learning platform. Learn more in the [official Vertexai documentation](https://cloud.google.com/vertex-ai).

The DataHub integration for Vertexai covers ML entities such as models, features, and related lineage metadata. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

|                                                       Source Concept                                                       |                                             DataHub Concept                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| :------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
|         [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)          |        [`MlModelGroup`](/docs/generated/metamodel/entities/mlmodelgroup/)        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  The name of a Model Group is the same as Model's name. Model serve as containers for multiple versions of the same model in Vertex AI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|                    [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                    |             [`MlModel`](/docs/generated/metamodel/entities/mlmodel/)             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   The name of a Model is `{model_name}_{model_version}` (e.g. my_vertexai_model_1 for model registered to Model Registry or Deployed to Endpoint. Each Model Version represents a specific iteration of a model with its own metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|                                                     Dataset <br/><br/>                                                     |             [`Dataset`](/docs/generated/metamodel/entities/dataset)              |                                                                                                                                                                                                                                                                                                                                                  A Managed Dataset resource in Vertex AI is mapped to Dataset in DataHub. <br></br> Supported types of datasets include ([`Text`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset), [`Tabular`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TabularDataset), [`Image Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset), [`Video`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset), [`TimeSeries`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TimeSeriesDataset))                                                                                                                                                                                                                                                                                                                                                   |
|                     [`Training Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)                     | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) | A Training Job is mapped as DataProcessInstance in DataHub. <br></br> Supported types of training jobs include ([`AutoMLTextTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTextTrainingJob), [`AutoMLTabularTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTabularTrainingJob), [`AutoMLImageTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLImageTrainingJob), [`AutoMLVideoTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLVideoTrainingJob), [`AutoMLForecastingTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLForecastingTrainingJob), [`Custom Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob), [`Custom TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomTrainingJob), [`Custom Container TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomContainerTrainingJob), [`Custom Python Packaging Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomPythonPackageTrainingJob) ) |
|    [`Experiment`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Experiment)     |           [`Container`](/docs/generated/metamodel/entities/container/)           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         Experiments organize related runs and serve as logical groupings for model development iterations. Each Experiment is mapped to a Container in DataHub.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                An Experiment Run represents a single execution of a ML workflow. An Experiment Run tracks ML parameters, metricis, artifacts and metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|     [`Execution`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Execution)      | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Metadata Execution resource for Vertex AI. Metadata Execution is started in a experiment run and captures input and output artifacts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|   [`PipelineJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.PipelineJob)    |            [`DataFlow`](/docs/generated/metamodel/entities/dataflow/)            |                                                                                                                                                                                                                                                                                             A Vertex AI Pipeline is mapped to a stable DataFlow entity in DataHub (one per pipeline template). The compiled pipeline spec name (`pipelineInfo.name`, i.e. the `@pipeline(name="...")` argument) is used as the stable identifier; non-Kubeflow pipelines fall back to `display_name` with any timestamp suffix stripped. Each pipeline run creates a DataProcessInstance, and pipeline tasks are modeled as DataJobs nested under the parent DataFlow. This enables proper incremental lineage aggregation across multiple pipeline runs. **Breaking Change (v1.4.0)**: Previously, each pipeline run created a separate DataFlow entity. Existing pipeline entities from earlier versions will appear as separate entities from new ingestion runs. Enable stateful ingestion with stale entity removal to clean up old pipeline entities.                                                                                                                                                                                                                                                                                              |
|       [`PipelineJob Task`](https://cloud.google.com/vertex-ai/docs/pipelines/build-pipeline#understanding-pipelines)       |             [`DataJob`](/docs/generated/metamodel/entities/datajob/)             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             Each task within a Vertex AI pipeline is modeled as a DataJob in DataHub, nested under its parent pipeline DataFlow. Tasks represent individual steps in the pipeline workflow.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|     [`PipelineJob Task Run`](https://cloud.google.com/vertex-ai/docs/pipelines/build-pipeline#understanding-pipelines)     | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   Each execution of a pipeline task is modeled as a DataProcessInstance, linked to its DataJob (task definition). This captures runtime metadata, inputs/outputs, and lineage for each task execution.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

##### Vertex AI Concept Diagram

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/vertexai/concept-mapping.png"/>
</p>


## Module `vertexai`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default: Experiments are modeled as Container entities; model groups and pipeline templates create folder containers. |
| Descriptions | ✅ | Extract descriptions for Vertex AI Registered Models and Model Versions. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Optionally set via the `platform_instance` config field to namespace resources when ingesting from multiple Vertex AI setups. |
| Table-Level Lineage | ✅ | Enabled by default: training job → model, dataset → training job, and cross-platform lineage to GCS, BigQuery, S3, Snowflake, and Azure Blob Storage. |

### Overview

The `vertexai` module ingests metadata from Vertex AI into DataHub. It extracts Models, Datasets, Training Jobs, Endpoints, Experiments, Experiment Runs, Model Evaluations, and Pipelines from Vertex AI.

The source supports ingesting across multiple GCP projects by specifying `project_ids`, `project_labels`, or `project_id_pattern`. Use `env` (e.g., `PROD`, `DEV`, `STAGING`) to distinguish between environments. The optional `platform_instance` field namespaces resources to avoid URN collisions when ingesting from multiple Vertex AI setups.

> **Deprecation Notice**: The `project_id` (singular) configuration field is deprecated and will be removed in a future release. Use `project_ids` (list) instead:
>
> ```yaml
> # Deprecated
> project_id: my-project
>
> # Preferred
> project_ids:
>   - my-project
> ```
>
> **Migration behavior**: If `project_id` is set and `project_ids` is not, the value is automatically moved to `project_ids` and a deprecation warning is logged. If both are set with the same value, `project_id` is silently ignored. If both are set with conflicting values, ingestion fails with a validation error. No manual migration is required — update your recipe at your convenience to silence the warning.

**Performance**: Resources are fetched ordered by update time (most recently updated first). Limits like `max_training_jobs_per_type` cap how many resources are processed per run — for example, `max_training_jobs_per_type: 1000` will process only the 1000 most recently updated training jobs of each type.

**Rate limiting**: If you see `429 Quota Exceeded` errors, enable rate limiting with `rate_limit: true`. The default `requests_per_min: 600` matches Google's standard quota of 600 resource-management requests per minute per region. Lower this value (e.g. `300`) if you share quota with other workloads running in the same project and region.

Enabling `stateful_ingestion` has two effects: (1) resources not updated since the previous run are skipped, reducing redundant API calls on subsequent runs; and (2) entities deleted from Vertex AI are automatically soft-deleted in DataHub. Use `stateful_ingestion.ignore_old_state: true` to get soft-deletion only without the incremental skip behaviour.

For improved organization in the DataHub UI:

- Model versions are organized under their respective model group folders
- Pipeline tasks and task runs are nested under their parent pipeline folders

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Vertex AI setup

Refer to [Vertex AI documentation](https://cloud.google.com/vertex-ai/docs) for Vertex AI basics.

#### GCP Authentication

Set up Application Default Credentials (ADC) following [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

##### Permissions

Grant the following permissions to the service account on all target projects.

**Default GCP Role:** [roles/aiplatform.viewer](https://cloud.google.com/vertex-ai/docs/general/access-control#aiplatform.viewer)

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
| `aiplatform.metadataStores.list`    | Allows a user to view and list all metadata stores in a project      |
| `aiplatform.metadataStores.get`     | Allows a user to view details of a specific metadata store           |
| `aiplatform.executions.list`        | Allows a user to view and list all executions in a project           |
| `aiplatform.executions.get`         | Allows a user to view details of a specific execution                |
| `aiplatform.datasets.list`          | Allows a user to view and list all datasets in a project             |
| `aiplatform.datasets.get`           | Allows a user to view details of a specific dataset                  |
| `aiplatform.pipelineJobs.list`      | Allows a user to view and list all pipeline jobs in a project        |
| `aiplatform.pipelineJobs.get`       | Allows a user to view details of a specific pipeline job             |

:::note ML Metadata Permissions

ML Metadata extraction (enabled by default for enhanced lineage tracking) requires the `aiplatform.metadataStores.*` and `aiplatform.executions.*` permissions listed above. If your service account lacks these permissions, the connector will gracefully fall back with warnings. To disable ML Metadata features, set `use_ml_metadata_for_lineage: false`, `extract_execution_metrics: false`, and `include_evaluations: false`.

:::

:::note Project Auto-Discovery Permissions

When using `project_labels` or `project_id_pattern` to auto-discover projects, the service account also needs `resourcemanager.projects.get` (granted via [`roles/browser`](https://cloud.google.com/iam/docs/understanding-roles#browser)) on each candidate project. This is in addition to the Vertex AI permissions listed above and is required so the Cloud Resource Manager `search_projects` API can return the project. If you specify `project_ids` explicitly, no Resource Manager permissions are needed.

:::

#### Create a service account and assign roles

1. Create a service account following [GCP docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and assign the role above
2. Download the service account JSON keyfile

   Example credential file:

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

   Set an environment variable:

   ```sh
   $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
   ```

   _or_

   Set credential config in your source based on the credential json file. For example:

   ```yml
   credential:
     private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
     private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
     client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
     client_id: "123456678890"
   ```


### Install the Plugin
```shell
pip install 'acryl-datahub[vertexai]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: vertexai
  config:
    project_ids:
      - "acryl-poc"
    # project_id: "acryl-poc"  # [deprecated] Still works — auto-migrated to project_ids with a warning
    # region: "us-west2"  # [deprecated] Prefer 'regions' or 'discover_regions'
    # regions:
    #   - "us-west2"
    #   - "us-central1"
    # discover_regions: true  # Enumerate available regions and scan all
    # project_labels:
    #   - "env:prod"
    # project_id_pattern:
    #   allow:
    #     - ".*-prod"
# You must either set GOOGLE_APPLICATION_CREDENTIALS or provide credential as shown below
#   credential:
#      private_key: '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'
#      private_key_id: "project_key_id"
#      client_email: "client_email"
#      client_id: "client_id"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">bucket_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Bucket URI used in your project <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">discover_regions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If true, discover available Vertex AI regions per project and scan all. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_execution_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract hyperparameters and metrics from ML Metadata Executions. Useful for training jobs that don't use Experiments but log to ML Metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_evaluations</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest model evaluations and evaluation metrics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_experiments</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest experiments and experiment runs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_models</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest models and model versions from the registry. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest pipelines and tasks. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_training_jobs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest training jobs and related run events. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_evaluations_per_model</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum evaluations per model. Default: 10, Max: 100. Set to None for unlimited (not recommended). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">max_experiments</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of experiments to ingest. Experiments are ordered by update_time descending (most recently updated first). Default: 1000, Max: 10000. Set to None for unlimited (not recommended). <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">max_models</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of models to ingest. Models are ordered by update_time descending (most recently updated first). Default: 10000, Max: 50000. Set to None for unlimited (not recommended). <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">max_runs_per_experiment</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum experiment runs per experiment. Runs are ordered by update_time descending (most recently updated first). Default: 100, Max: 1000. Set to None for unlimited (not recommended). <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">max_training_jobs_per_type</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum training jobs per type (CustomJob, AutoML, etc.). Jobs are ordered by update_time descending (most recently updated first). Default: 1000, Max: 10000. Set to None for unlimited (not recommended). <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">ml_metadata_max_execution_search_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of ML Metadata executions to retrieve when searching for a training job. Ordered by LAST_UPDATE_TIME descending. Lower this if ingestion is slow or timing out. <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-main">normalize_external_dataset_paths</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Strip partition segments from external dataset paths (GCS/S3/ABS) to create stable dataset URNs. When enabled, 'gs://bucket/data/year=2024/month=01/' becomes 'gs://bucket/data/'. Partition-level information is captured via DataProcessInstance. Default is False for backward compatibility. Will default to True in a future major version. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">rate_limit</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Slow down ingestion to avoid hitting Vertex AI API quota limits. Enable if you see '429 Quota Exceeded' errors during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [deprecated] Single Vertex AI region. Prefer 'regions' or 'discover_regions'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">requests_per_min</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Max API requests per minute when rate_limit is enabled. 600 is Google's quota ceiling for resource management requests per project per region (see https://cloud.google.com/vertex-ai/docs/quotas). Lower this only if you share quota with other workloads running in the same project and region. <div className="default-line default-line-with-docs">Default: <span className="default-value">600</span></div> |
| <div className="path-line"><span className="path-main">use_ml_metadata_for_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract lineage from Vertex AI ML Metadata API for CustomJob and other training jobs. This enables input dataset → training job → output model lineage for non-AutoML jobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">vertexai_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | VertexUI URI <div className="default-line default-line-with-docs">Default: <span className="default-value">https://console.cloud.google.com/vertex-ai</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | GCP credential information <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_email</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client email  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client Id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_provider_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Auth provider x509 certificate url <div className="default-line default-line-with-docs">Default: <span className="default-value">https://www.googleapis.com/oauth2/v1/certs</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://accounts.google.com/o/oauth2/auth</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Project id to set the credentials <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">token_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Token uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://oauth2.googleapis.com/token</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication type <div className="default-line default-line-with-docs">Default: <span className="default-value">service&#95;account</span></div> |
| <div className="path-line"><span className="path-main">experiment_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">experiment_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">model_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">model_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">partition_pattern_rules</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Regex patterns to identify and strip partition segments from paths. Applied when normalize_external_dataset_paths is enabled. Patterns are applied in order. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;/&#91;^/&#93;+=(&#91;^/&#93;+)&#x27;, &#x27;/dt=\\d&#123;4&#125;-\\d&#123;2&#125;-\\d&#123;2&#125;&#x27;, &#x27;/\...</span></div> |
| <div className="path-line"><span className="path-prefix">partition_pattern_rules.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> | Platform instance configuration for external datasets referenced in Vertex AI lineage.  |
| <div className="path-line"><span className="path-prefix">platform_instance_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance for URN generation. If not set, no platform instance in URN. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">platform_instance_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Convert dataset names to lowercase. Set to true for Snowflake (which defaults to lowercase URNs). Leave false for GCS, BigQuery, S3, and ABS. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">platform_instance_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment for all assets from this platform <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">project_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Ingest specified GCP project ids. Overrides project_id_pattern.  |
| <div className="path-line"><span className="path-prefix">project_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">project_labels</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Ingest projects with these labels (key:value). Applied before project_id_pattern.  |
| <div className="path-line"><span className="path-prefix">project_labels.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">regions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of Vertex AI regions to scan. If empty and discover_regions is false, falls back to 'region'.  |
| <div className="path-line"><span className="path-prefix">regions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">training_job_type_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">training_job_type_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for tracking and removing stale metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "GCPCredential": {
      "additionalProperties": false,
      "properties": {
        "project_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Project id to set the credentials",
          "title": "Project Id"
        },
        "private_key_id": {
          "description": "Private key id",
          "title": "Private Key Id",
          "type": "string"
        },
        "private_key": {
          "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'",
          "format": "password",
          "title": "Private Key",
          "type": "string",
          "writeOnly": true
        },
        "client_email": {
          "description": "Client email",
          "title": "Client Email",
          "type": "string"
        },
        "client_id": {
          "description": "Client Id",
          "title": "Client Id",
          "type": "string"
        },
        "auth_uri": {
          "default": "https://accounts.google.com/o/oauth2/auth",
          "description": "Authentication uri",
          "title": "Auth Uri",
          "type": "string"
        },
        "token_uri": {
          "default": "https://oauth2.googleapis.com/token",
          "description": "Token uri",
          "title": "Token Uri",
          "type": "string"
        },
        "auth_provider_x509_cert_url": {
          "default": "https://www.googleapis.com/oauth2/v1/certs",
          "description": "Auth provider x509 certificate url",
          "title": "Auth Provider X509 Cert Url",
          "type": "string"
        },
        "type": {
          "default": "service_account",
          "description": "Authentication type",
          "title": "Type",
          "type": "string"
        },
        "client_x509_cert_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
          "title": "Client X509 Cert Url"
        }
      },
      "required": [
        "private_key_id",
        "private_key",
        "client_email",
        "client_id"
      ],
      "title": "GCPCredential",
      "type": "object"
    },
    "PlatformDetail": {
      "additionalProperties": false,
      "description": "Platform instance configuration for external datasets referenced in Vertex AI lineage.",
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Platform instance for URN generation. If not set, no platform instance in URN.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "Environment for all assets from this platform",
          "title": "Env",
          "type": "string"
        },
        "convert_urns_to_lowercase": {
          "default": false,
          "description": "Convert dataset names to lowercase. Set to true for Snowflake (which defaults to lowercase URNs). Leave false for GCS, BigQuery, S3, and ABS.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "PlatformDetail",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration for tracking and removing stale metadata."
    },
    "normalize_external_dataset_paths": {
      "default": false,
      "description": "Strip partition segments from external dataset paths (GCS/S3/ABS) to create stable dataset URNs. When enabled, 'gs://bucket/data/year=2024/month=01/' becomes 'gs://bucket/data/'. Partition-level information is captured via DataProcessInstance. Default is False for backward compatibility. Will default to True in a future major version.",
      "title": "Normalize External Dataset Paths",
      "type": "boolean"
    },
    "partition_pattern_rules": {
      "default": [
        "/[^/]+=([^/]+)",
        "/dt=\\d{4}-\\d{2}-\\d{2}",
        "/\\d{4}/\\d{2}/\\d{2}"
      ],
      "description": "Regex patterns to identify and strip partition segments from paths. Applied when normalize_external_dataset_paths is enabled. Patterns are applied in order.",
      "items": {
        "type": "string"
      },
      "title": "Partition Pattern Rules",
      "type": "array"
    },
    "credential": {
      "anyOf": [
        {
          "$ref": "#/$defs/GCPCredential"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "GCP credential information"
    },
    "region": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[deprecated] Single Vertex AI region. Prefer 'regions' or 'discover_regions'.",
      "title": "Region"
    },
    "include_models": {
      "default": true,
      "description": "Ingest models and model versions from the registry.",
      "title": "Include Models",
      "type": "boolean"
    },
    "include_training_jobs": {
      "default": true,
      "description": "Ingest training jobs and related run events.",
      "title": "Include Training Jobs",
      "type": "boolean"
    },
    "include_experiments": {
      "default": true,
      "description": "Ingest experiments and experiment runs.",
      "title": "Include Experiments",
      "type": "boolean"
    },
    "include_pipelines": {
      "default": true,
      "description": "Ingest pipelines and tasks.",
      "title": "Include Pipelines",
      "type": "boolean"
    },
    "include_evaluations": {
      "default": true,
      "description": "Ingest model evaluations and evaluation metrics.",
      "title": "Include Evaluations",
      "type": "boolean"
    },
    "use_ml_metadata_for_lineage": {
      "default": true,
      "description": "Extract lineage from Vertex AI ML Metadata API for CustomJob and other training jobs. This enables input dataset \u2192 training job \u2192 output model lineage for non-AutoML jobs.",
      "title": "Use Ml Metadata For Lineage",
      "type": "boolean"
    },
    "extract_execution_metrics": {
      "default": true,
      "description": "Extract hyperparameters and metrics from ML Metadata Executions. Useful for training jobs that don't use Experiments but log to ML Metadata.",
      "title": "Extract Execution Metrics",
      "type": "boolean"
    },
    "experiment_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny pattern for experiment names."
    },
    "training_job_type_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny pattern for training job class names (e.g., CustomJob)."
    },
    "model_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny pattern for model display names."
    },
    "max_models": {
      "anyOf": [
        {
          "maximum": 50000,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 10000,
      "description": "Maximum number of models to ingest. Models are ordered by update_time descending (most recently updated first). Default: 10000, Max: 50000. Set to None for unlimited (not recommended).",
      "title": "Max Models"
    },
    "max_training_jobs_per_type": {
      "anyOf": [
        {
          "maximum": 10000,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1000,
      "description": "Maximum training jobs per type (CustomJob, AutoML, etc.). Jobs are ordered by update_time descending (most recently updated first). Default: 1000, Max: 10000. Set to None for unlimited (not recommended).",
      "title": "Max Training Jobs Per Type"
    },
    "max_experiments": {
      "anyOf": [
        {
          "maximum": 10000,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1000,
      "description": "Maximum number of experiments to ingest. Experiments are ordered by update_time descending (most recently updated first). Default: 1000, Max: 10000. Set to None for unlimited (not recommended).",
      "title": "Max Experiments"
    },
    "max_runs_per_experiment": {
      "anyOf": [
        {
          "maximum": 1000,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 100,
      "description": "Maximum experiment runs per experiment. Runs are ordered by update_time descending (most recently updated first). Default: 100, Max: 1000. Set to None for unlimited (not recommended).",
      "title": "Max Runs Per Experiment"
    },
    "max_evaluations_per_model": {
      "anyOf": [
        {
          "maximum": 100,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 10,
      "description": "Maximum evaluations per model. Default: 10, Max: 100. Set to None for unlimited (not recommended).",
      "title": "Max Evaluations Per Model"
    },
    "ml_metadata_max_execution_search_limit": {
      "default": 500,
      "description": "Maximum number of ML Metadata executions to retrieve when searching for a training job. Ordered by LAST_UPDATE_TIME descending. Lower this if ingestion is slow or timing out.",
      "title": "Ml Metadata Max Execution Search Limit",
      "type": "integer"
    },
    "rate_limit": {
      "default": false,
      "description": "Slow down ingestion to avoid hitting Vertex AI API quota limits. Enable if you see '429 Quota Exceeded' errors during ingestion.",
      "title": "Rate Limit",
      "type": "boolean"
    },
    "requests_per_min": {
      "default": 600,
      "description": "Max API requests per minute when rate_limit is enabled. 600 is Google's quota ceiling for resource management requests per project per region (see https://cloud.google.com/vertex-ai/docs/quotas). Lower this only if you share quota with other workloads running in the same project and region.",
      "title": "Requests Per Min",
      "type": "integer"
    },
    "project_ids": {
      "description": "Ingest specified GCP project ids. Overrides project_id_pattern.",
      "items": {
        "type": "string"
      },
      "title": "Project Ids",
      "type": "array"
    },
    "project_labels": {
      "description": "Ingest projects with these labels (key:value). Applied before project_id_pattern.",
      "items": {
        "type": "string"
      },
      "title": "Project Labels",
      "type": "array"
    },
    "project_id_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for project ids to include/exclude."
    },
    "regions": {
      "description": "List of Vertex AI regions to scan. If empty and discover_regions is false, falls back to 'region'.",
      "items": {
        "type": "string"
      },
      "title": "Regions",
      "type": "array"
    },
    "discover_regions": {
      "default": false,
      "description": "If true, discover available Vertex AI regions per project and scan all.",
      "title": "Discover Regions",
      "type": "boolean"
    },
    "bucket_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Bucket URI used in your project",
      "title": "Bucket Uri"
    },
    "vertexai_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "https://console.cloud.google.com/vertex-ai",
      "description": "VertexUI URI",
      "title": "Vertexai Url"
    },
    "platform_instance_map": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "description": "Map external platform names (gcs, bigquery, s3, azure_blob_storage, snowflake) to their platform instance and env. Ensures URNs match native connectors for lineage connectivity.",
      "title": "Platform Instance Map",
      "type": "object"
    }
  },
  "title": "VertexAIConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

##### CustomJob Lineage

The connector supports extracting lineage and metrics from CustomJob training jobs using the **Vertex AI ML Metadata API**. This enables:

- **Full lineage tracking** for CustomJob: input datasets → training job → output models
- **Hyperparameters and metrics extraction** from training jobs that log to ML Metadata Executions
- **Model evaluation** ingestion with evaluation metrics and lineage to models

These features are controlled by the following configuration options:

- `use_ml_metadata_for_lineage` (default: `true`) — Extracts lineage from ML Metadata for CustomJob and other non-AutoML training jobs
- `extract_execution_metrics` (default: `true`) — Extracts hyperparameters and metrics from ML Metadata Executions
- `include_evaluations` (default: `true`) — Ingests model evaluations and evaluation metrics

:::tip "Log to Vertex AI ML Metadata"
For CustomJob lineage to work, your training jobs must log to Vertex AI ML Metadata. This happens automatically when using Vertex AI Experiments SDK or manually logging artifacts/executions to ML Metadata.
:::

```python
from google.cloud import aiplatform

aiplatform.init(project="your-project", location="us-central1")

dataset_artifact = aiplatform.Artifact.create(
    schema_title="system.Dataset",
    uri="gs://your-bucket/data/train.csv",
    display_name="training-dataset",
)

with aiplatform.start_execution(
    schema_title="system.ContainerExecution",
    display_name=f"training-job-{job_name}",
) as execution:
    execution.assign_input_artifacts([dataset_artifact])
    # ... training logic ...
    model_artifact = aiplatform.Artifact.create(
        schema_title="system.Model",
        uri=model_uri,
        display_name="trained-model",
    )
    execution.assign_output_artifacts([model_artifact])
```

##### Cross-Platform Lineage Configuration

To ensure external datasets are linked with the correct platform instances and environments (so URNs match those from native connectors), configure `platform_instance_map`:

```yaml
source:
  type: vertexai
  config:
    project_ids:
      - my-project
    platform_instance_map:
      gcs:
        platform_instance: prod-gcs
        env: PROD
      bigquery:
        platform_instance: prod-bq
        env: PROD
      s3:
        platform_instance: prod-s3
        env: PROD
      snowflake:
        platform_instance: prod-snowflake
        env: PROD
        convert_urns_to_lowercase: true # Required - Snowflake defaults to lowercase URNs
      abs:
        platform_instance: prod-abs
        env: PROD
```

**Platform-specific notes:**

- **Snowflake**: Must set `convert_urns_to_lowercase: true` to match the Snowflake connector's default behavior
- **All other platforms** (GCS, BigQuery, S3, ABS): Use the default `convert_urns_to_lowercase: false`

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.vertexai.vertexai.VertexAISource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/vertexai/vertexai.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Vertex AI, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
