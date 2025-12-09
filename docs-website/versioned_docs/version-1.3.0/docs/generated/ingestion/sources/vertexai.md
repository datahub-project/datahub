---
sidebar_position: 75
title: Vertex AI
slug: /generated/ingestion/sources/vertexai
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/vertexai.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Vertex AI
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Extract descriptions for Vertex AI Registered Models and Model Versions. |


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

Ingestion Job extract Models, Datasets, Training Jobs, Endpoints, Experiment and Experiment Runs in a given project and region on Vertex AI.

#### Concept Mapping

This ingestion source maps the following Vertex AI Concepts to DataHub Concepts:

|                                                       Source Concept                                                       |                                             DataHub Concept                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| :------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
|         [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)          |        [`MlModelGroup`](/docs/generated/metamodel/entities/mlmodelgroup/)        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  The name of a Model Group is the same as Model's name. Model serve as containers for multiple versions of the same model in Vertex AI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|                    [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                    |             [`MlModel`](/docs/generated/metamodel/entities/mlmodel/)             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   The name of a Model is `{model_name}_{model_version}` (e.g. my_vertexai_model_1 for model registered to Model Registry or Deployed to Endpoint. Each Model Version represents a specific iteration of a model with its own metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|                                                     Dataset <br/><br/>                                                     |             [`Dataset`](/docs/generated/metamodel/entities/dataset)              |                                                                                                                                                                                                                                                                                                                                                  A Managed Dataset resource in Vertex AI is mapped to Dataset in DataHub. <br></br> Supported types of datasets include ([`Text`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset), [`Tabular`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TabularDataset), [`Image Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset), [`Video`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset), [`TimeSeries`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TimeSeriesDataset))                                                                                                                                                                                                                                                                                                                                                   |
|                     [`Training Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)                     | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) | A Training Job is mapped as DataProcessInstance in DataHub. <br></br> Supported types of training jobs include ([`AutoMLTextTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTextTrainingJob), [`AutoMLTabularTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTabularTrainingJob), [`AutoMLImageTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLImageTrainingJob), [`AutoMLVideoTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLVideoTrainingJob), [`AutoMLForecastingTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLForecastingTrainingJob), [`Custom Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob), [`Custom TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomTrainingJob), [`Custom Container TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomContainerTrainingJob), [`Custom Python Packaging Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomPythonPackageTrainingJob) ) |
|    [`Experiment`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Experiment)     |           [`Container`](/docs/generated/metamodel/entities/container/)           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         Experiments organize related runs and serve as logical groupings for model development iterations. Each Experiment is mapped to a Container in DataHub.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                An Experiment Run represents a single execution of a ML workflow. An Experiment Run tracks ML parameters, metricis, artifacts and metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|     [`Execution`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Execution)      | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  Metadata Execution resource for Vertex AI. Metadata Execution is started in a experiment run and captures input and output artifacts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

Vertex AI Concept Diagram:

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/vertexai/concept-mapping.png"/>
</p>

#### Lineage

Lineage is emitted using Vertex AI API to capture the following relationships:

- A training job and a model (which training job produce a model)
- A dataset and a training job (which dataset was consumed by a training job to train a model)
- Experiment runs and an experiment
- Metadata execution and an experiment run

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: vertexai
  config:
    project_id: "acryl-poc"
    region:  "us-west2"
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
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">project_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Project ID in Google Cloud Platform  |
| <div className="path-line"><span className="path-main">region</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Region of your project in Google Cloud Platform  |
| <div className="path-line"><span className="path-main">bucket_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Bucket URI used in your project <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">vertexai_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | VertexUI URI <div className="default-line default-line-with-docs">Default: <span className="default-value">https://console.cloud.google.com/vertex-ai</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of GCPCredential, null</span></div> | GCP credential information <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_email</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client email  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client Id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">private_key_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Private key id  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_provider_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Auth provider x509 certificate url <div className="default-line default-line-with-docs">Default: <span className="default-value">https://www.googleapis.com/oauth2/v1/certs</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">auth_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://accounts.google.com/o/oauth2/auth</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_x509_cert_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Project id to set the credentials <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">token_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Token uri <div className="default-line default-line-with-docs">Default: <span className="default-value">https://oauth2.googleapis.com/token</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authentication type <div className="default-line default-line-with-docs">Default: <span className="default-value">service&#95;account</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
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
          "title": "Private Key",
          "type": "string"
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
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
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
    "project_id": {
      "description": "Project ID in Google Cloud Platform",
      "title": "Project Id",
      "type": "string"
    },
    "region": {
      "description": "Region of your project in Google Cloud Platform",
      "title": "Region",
      "type": "string"
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
    }
  },
  "required": [
    "project_id",
    "region"
  ],
  "title": "VertexAIConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.vertexai.vertexai.VertexAISource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/vertexai/vertexai.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Vertex AI, feel free to ping us on [our Slack](https://datahub.com/slack).
