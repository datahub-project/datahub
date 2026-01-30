### Quick Start

```yaml
source:
  type: vertexai
  config:
    project_ids: [my-project] # Or omit for auto-discovery
    region: us-central1
    credential: # Or use GOOGLE_APPLICATION_CREDENTIALS env var
      private_key_id: "..."
      private_key: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
      client_email: "sa@project.iam.gserviceaccount.com"
      client_id: "123456789"
```

**Required role:** `roles/aiplatform.viewer` on each project.

---

### Prerequisites

- [Vertex AI basics](https://cloud.google.com/vertex-ai/docs)
- [GCP Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to)

### Permissions

Grant [roles/aiplatform.viewer](https://cloud.google.com/vertex-ai/docs/general/access-control#aiplatform.viewer) to your service account:

| Permission                          | Description                                                                                                       |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `resourcemanager.projects.list`     | Required only when using auto-discovery or `project_labels`. Not needed if `project_ids` is specified explicitly. |
| `aiplatform.models.list`            | Allows a user to view and list all ML models in a project                                                         |
| `aiplatform.models.get`             | Allows a user to view details of a specific ML model                                                              |
| `aiplatform.endpoints.list`         | Allows a user to view and list all prediction endpoints in a project                                              |
| `aiplatform.endpoints.get`          | Allows a user to view details of a specific prediction endpoint                                                   |
| `aiplatform.trainingPipelines.list` | Allows a user to view and list all training pipelines in a project                                                |
| `aiplatform.trainingPipelines.get`  | Allows a user to view details of a specific training pipeline                                                     |
| `aiplatform.customJobs.list`        | Allows a user to view and list all custom jobs in a project                                                       |
| `aiplatform.customJobs.get`         | Allows a user to view details of a specific custom job                                                            |
| `aiplatform.experiments.list`       | Allows a user to view and list all experiments in a project                                                       |
| `aiplatform.experiments.get`        | Allows a user to view details of a specific experiment in a project                                               |
| `aiplatform.metadataStores.list`    | Allows a user to view and list all metadata stores in a project                                                   |
| `aiplatform.metadataStores.get`     | Allows a user to view details of a specific metadata store                                                        |
| `aiplatform.executions.list`        | Allows a user to view and list all executions in a project                                                        |
| `aiplatform.executions.get`         | Allows a user to view details of a specific execution                                                             |
| `aiplatform.datasets.list`          | Allows a user to view and list all datasets in a project                                                          |
| `aiplatform.datasets.get`           | Allows a user to view details of a specific dataset                                                               |
| `aiplatform.pipelineJobs.list`      | Allows a user to view and list all pipeline jobs in a project                                                     |
| `aiplatform.pipelineJobs.get`       | Allows a user to view details of a specific pipeline job                                                          |

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

### Multi-Project Configuration

| Method              | Config                         | Use Case                   | Requires Org Permission |
| ------------------- | ------------------------------ | -------------------------- | ----------------------- |
| **Explicit list**   | `project_ids: [proj1, proj2]`  | Fixed list (3-10 projects) | No                      |
| **Label discovery** | `project_labels: ["env:prod"]` | Tagged projects (10-100+)  | Yes                     |
| **Auto-discovery**  | _(omit both)_                  | All accessible projects    | Yes                     |

Priority: `project_ids` > `project_labels` > auto-discovery. The `project_id_pattern` filter always applies after selection.

#### Examples

```yaml
# Explicit list (no org permissions needed)
project_ids: [project-prod, project-staging]

# Label-based discovery
project_labels: ["env:production", "team:ml"]

# Pattern filtering (regex, not glob!)
project_id_pattern:
  allow: ["prod-.*"] # Include projects matching regex
  deny: [".*-sandbox$"] # Exclude projects matching regex
```

#### Permissions

| Scope                               | Command                                                                                                            |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Org-level** (for discovery)       | `gcloud organizations add-iam-policy-binding ORG_ID --member="serviceAccount:SA" --role="roles/aiplatform.viewer"` |
| **Per-project** (for explicit list) | `gcloud projects add-iam-policy-binding PROJECT --member="serviceAccount:SA" --role="roles/aiplatform.viewer"`     |

#### Error Handling

| Scenario           | Behavior                                |
| ------------------ | --------------------------------------- |
| Some projects fail | Continues with remaining, logs warnings |
| All projects fail  | Run fails with error                    |
| Discovery fails    | Run fails immediately                   |
| Rate limits        | Auto-retry with exponential backoff     |

#### Migration from `project_id`

```yaml
# Old (deprecated, still works)     →    # New (recommended)
project_id: my-project                    project_ids: [my-project]
```

### Troubleshooting

| Error                                     | Cause                        | Fix                                                               |
| ----------------------------------------- | ---------------------------- | ----------------------------------------------------------------- |
| `Permission denied when listing projects` | Missing org-level permission | Use explicit `project_ids` or grant `roles/browser` at org level  |
| `Permission denied: {project}`            | Missing project permission   | Grant `roles/aiplatform.viewer` on project                        |
| `Rate limit exceeded`                     | API quota                    | Reduce scope with `project_id_pattern`, or request quota increase |
| `All projects excluded by pattern`        | Wrong pattern syntax         | Use regex (`prod-.*`) not glob (`prod-*`)                         |
| `Not found: {project}`                    | Typo or deleted project      | Verify with `gcloud projects describe PROJECT_ID`                 |

**Common pattern mistakes:**

```yaml
# ❌ Wrong (glob)          # ✅ Correct (regex)
allow: ["prod-*"]          allow: ["prod-.*"]

# ❌ Denies everything!
allow: ["prod-.*"]
deny: [".*"]               # Deny takes precedence
```

**Debug commands:**

```bash
gcloud projects list --filter='lifecycleState:ACTIVE'          # List accessible projects
gcloud projects list --filter='labels.env:prod'                # Find by label
gcloud services list --enabled --project=PROJECT | grep ai     # Check API enabled
```

### Integration Details

Ingestion Job extract Models, Datasets, Training Jobs, Endpoints, Experiment and Experiment Runs in a given project and region on Vertex AI.

#### Concept Mapping

This ingestion source maps the following Vertex AI Concepts to DataHub Concepts:

| Source Concept                                                                                                             | DataHub Concept                                                                                          | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| :------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`Model`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Model)                  | [`MlModelGroup`](https://docs.datahub.com/docs/generated/metamodel/entities/mlmodelgroup/)               | The name of a Model Group is the same as Model's name. Model serves as containers for multiple versions of the same model in Vertex AI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [`Model Version`](https://cloud.google.com/vertex-ai/docs/model-registry/versioning)                                       | [`MlModel`](https://docs.datahub.com/docs/generated/metamodel/entities/mlmodel/)                         | The name of a Model is `{model_name}_{model_version}` (e.g. my_vertexai_model_1) for model registered to Model Registry or Deployed to Endpoint. Each Model Version represents a specific iteration of a model with its own metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Dataset                                                                                                                    | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)                          | A Managed Dataset resource in Vertex AI is mapped to Dataset in DataHub. Supported types of datasets include [`Text`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TextDataset), [`Tabular`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TabularDataset), [`Image Dataset`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset), [`Video`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset), [`TimeSeries`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.TimeSeriesDataset).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [`Training Job`](https://cloud.google.com/vertex-ai/docs/beginner/beginners-guide)                                         | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | A Training Job is mapped as DataProcessInstance in DataHub. Supported types of training jobs include [`AutoMLTextTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTextTrainingJob), [`AutoMLTabularTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLTabularTrainingJob), [`AutoMLImageTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLImageTrainingJob), [`AutoMLVideoTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLVideoTrainingJob), [`AutoMLForecastingTrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.AutoMLForecastingTrainingJob), [`Custom Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomJob), [`Custom TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomTrainingJob), [`Custom Container TrainingJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomContainerTrainingJob), [`Custom Python Packaging Job`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.CustomPythonPackageTrainingJob). |
| [`Experiment`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Experiment)        | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/)                     | Experiments organize related runs and serve as logical groupings for model development iterations. Each Experiment is mapped to a Container in DataHub.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [`Experiment Run`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ExperimentRun) | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | An Experiment Run represents a single execution of a ML workflow. An Experiment Run tracks ML parameters, metrics, artifacts and metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [`Execution`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.Execution)          | [`DataProcessInstance`](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | Metadata Execution resource for Vertex AI. Metadata Execution is started in an experiment run and captures input and output artifacts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

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
