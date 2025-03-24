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

This ingestion source maps the following MLflow Concepts to DataHub Concepts:

|                                                       Source Concept                                                       |                                              DataHub Concept                                              | Notes                                                                                                                                                                                                                                                      |
|:--------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|                         [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)                         |        [`MlModelGroup`](https://datahubproject.io/docs/generated/metamodel/entities/mlmodelgroup/)        | The name of a Model Group is the same as Model's name. Registered Models serve as containers for multiple versions of the same model in MLflow.                                                                                                            |
|                      [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                       |             [`MlModel`](https://datahubproject.io/docs/generated/metamodel/entities/mlmodel/)             | The name of a Model is `{registered_model_name}{model_name_separator}{model_version}` (e.g. my_vertexai_model_1 for registered model to Model Registry. Each Model Version represents a specific iteration of a model with its own artifacts and metadata. |
|     [`Dataset`] (https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset)     |             [`Dataset`](https://datahubproject.io/docs/generated/metamodel/entities/dataset)              | Each Experiment in Vertex AI is mapped to a Container in DataHub. Experiments organize related runs and serve as logical groupings for model development iterations, allowing tracking of parameters, metrics, and artifacts.                              |
|  [`AutoML Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)   |      [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/)      | Each Training Job in VertexAI is mapped as DataProcessInstance in DataHub.                                                                                                                                                                                 |
|    [`Experiment`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Experiment)     |           [`Container`](https://datahubproject.io/docs/generated/metamodel/entities/container/)           | Each Experiment in Vertex AI is mapped to a Container in DataHub. Experiments organize related runs and serve as logical groupings for model development iterations, allowing tracking of parameters, metrics, and artifacts.                              |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) | [`DataProcessInstance`](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance/) | Captures the run's execution details, parameters, metrics, and lineage to a model.                                                                                                                                                                         |


#### Lineage

Lineage is emitted using Vertex AI API for

- Output Model training by AutoML Job  
- Input Dataset used by Auto ML Job 
- Endpoint to which a Model is deployed 
- Experiment runs executed in an Experiment 

