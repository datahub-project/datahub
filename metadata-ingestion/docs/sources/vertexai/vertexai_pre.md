Ingesting metadata from VertexAI requires using the **Vertex AI** module.

#### Prerequisites
Please refer to the [Vertex AI documentation](https://cloud.google.com/vertex-ai/docs) for basic information on Vertex AI.

#### Credentials to access to GCP
Please read the section to understand how to set up application default Credentials to GCP [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

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

Ingestion Job extract Models, Datasets, Training Jobs, Endpoints, Experiment and Experiment Runs in a given project and region on Vertex AI. 

#### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

### Concept Mapping

This ingestion source maps the following Vertex AI Concepts to DataHub Concepts:

|                                                       Source Concept                                                       |                                                                   Source Concept Type                                                                    |                                                                                                                  DataHub Concept | Notes                                                                                                                                                                                                                                                      |
|:--------------------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------:|---------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|         [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)          |                                                                                                                                                          |                                      [`MlModelGroup`](https://datahubproject.io/docs/generated/metamodel/entities/mlmodelgroup/) | The name of a Model Group is the same as Model's name. Registered Models serve as containers for multiple versions of the same model in MLflow.                                                                                                            |
|                    [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                    |                                                                                                                                                          |                                                [`MlModel`](https://datahubproject.io/docs/generated/metamodel/entities/mlmodel/) | The name of a Model is `{registered_model_name}{model_name_separator}{model_version}` (e.g. my_vertexai_model_1 for registered model to Model Registry. Each Model Version represents a specific iteration of a model with its own artifacts and metadata. |
|     [`Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset)      |                  [`Text Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset)                  |                                                 [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset) | A managed text dataset resource for Vertex AI                                                                                                                                                                                                              |
|                                                                                                                            |               [`Tabular Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TabularDataset)               |                                                 [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset) | A managed tabular dataset resource for Vertex AI                                                                                                                                                                                                           |
|                                                                                                                            |                 [`Image Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset)                 |                                                 [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset) | A managed image dataset resource for Vertex AI                                                                                                                                                                                                             |
|                                                                                                                            |                 [`Video Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset)                 |                                                 [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset) | A managed video dataset resource for Vertex AI                                                                                                                                                                                                             |
|                                                                                                                            |            [`TimeSeries Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TimeSeriesDataset)            |                                                 [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset) | A managed time series dataset resource for Vertex AI                                                                                                                                                                                                       |
|                     [`Training Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)                     |        [`AutoML TextTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTextTrainingJob)        |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A AutoML Text Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                    |
|                                                                                                                            |     [`AutoML TabularTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTabularTrainingJob)     |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A AutoML Tabular Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                 |
|                                                                                                                            |       [`AutoML ImageTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLImageTrainingJob)       |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A AutoML Image Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                   |
|                                                                                                                            |       [`AutoML VideoTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLVideoTrainingJob)       |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A AutoML Video Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                   |
|                                                                                                                            | [`AutoML ForecastingTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLForecastingTrainingJob) |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A AutoML Forecasting Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                             |
|                                                                                                                            |                    [`Custom Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob)                    |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A Custom Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                                  |
|                                                                                                                            |            [`Custom TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomTrainingJob)            |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A Custom Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                         |
|                                                                                                                            |  [`Custom Container TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomContainerTrainingJob)   |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A Custom Container Training Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                               |
|                                                                                                                            | [`Custom Python Packaging Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomPythonPackageTrainingJob) |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | A Custom Python Packaging Job is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                 |
|         [`Experiment`](https://mlflow.org/docs/latest/tracking/#experiments)         |                                                                                                                                                          |                                            [`Container`](https://datahubproject.io/docs/generated/metamodel/entities/container/) | Each Experiment in MLflow is mapped to a Container in DataHub. Experiments organize related runs and serve as logical groupings for model development iterations, allowing tracking of parameters, metrics, and artifacts.                                                                       |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) |                                                                                                                                                          |                        [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) |                        Captures the run's execution details, parameters, metrics, and lineage to a model. |


#### Lineage

Lineage is emitted using Vertex AI API for

- Output Model training by AutoML Job  
- Input Dataset used by Auto ML Job 
- Endpoint to which a Model is deployed 
- Experiment runs executed in an Experiment 

