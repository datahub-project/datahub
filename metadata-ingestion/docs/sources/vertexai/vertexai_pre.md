Ingesting metadata from VertexAI requires using the **Vertex AI** module.

#### Prerequisites

Please refer to the [Vertex AI documentation](https://cloud.google.com/vertex-ai/docs) for basic information on Vertex AI.

#### Credentials to access to GCP

Please read the section to understand how to set up application default Credentials to [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

##### Permissions

- Grant the following permissions to the Service Account on every project where you would like to extract metadata from

Default GCP Role which contains these permissions [roles/aiplatform.viewer](https://cloud.google.com/vertex-ai/docs/general/access-control#aiplatform.viewer)

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

Scan Vertex AI resources across multiple GCP projects using one of three approaches:

**Project Selection Priority:**

1. `project_ids` (highest priority) - Explicit list of project IDs
2. `project_labels` - Discover projects by GCP labels
3. Auto-discovery - Find all accessible projects

The `project_id_pattern` filter is always applied after project selection.

#### Option 1: Explicit Project List

```yaml
source:
  type: vertexai
  config:
    project_ids:
      - project-prod
      - project-staging
      - project-dev
    region: us-central1
```

**Use when:** You have a fixed list of projects (3-10 projects).  
**Note:** Does not require org-level `resourcemanager.projects.list` permission.

#### Option 2: Label-Based Discovery

```yaml
source:
  type: vertexai
  config:
    project_labels:
      - "env:production"
      - "team:ml-platform"
    region: us-central1
```

**Use when:** Projects are tagged consistently (10-100+ projects).  
**Requirements:**

- `resourcemanager.projects.list` permission at organization/folder level
- Projects must have matching labels

**Label format:** `key:value` for exact match, or `key` to match any value.

**Debug:**

```bash
gcloud projects list --filter='labels.env:production'
```

#### Option 3: Auto-Discovery

```yaml
source:
  type: vertexai
  config:
    # No project_ids or project_labels = auto-discover all accessible projects
    region: us-central1
```

**Use when:** You want to scan all accessible projects.  
**Requirements:** `resourcemanager.projects.list` permission at organization/folder level.

#### Pattern Filtering

Filter projects using regex patterns with `project_id_pattern`:

```yaml
source:
  type: vertexai
  config:
    project_ids:
      - prod-ml-east
      - prod-ml-west
      - dev-ml-sandbox
    project_id_pattern:
      allow:
        - "prod-.*"
      deny:
        - ".*-sandbox$"
    region: us-central1
```

**Note:** When using auto-discovery, pattern filtering is applied at runtime. Invalid patterns that filter out all projects will cause ingestion to fail at runtime.

#### Combining Options

All three fields can be combined - results are deduplicated:

```yaml
project_ids: ["critical-project"]
project_labels: ["team:ml"]
project_id_pattern:
  allow: ["^ml-prod-.*"]
```

#### Permissions

Each project needs:

- `aiplatform.models.list`
- `aiplatform.endpoints.list`
- `aiplatform.featurestores.list`

**Grant across projects:**

**Option A: Organization-level (recommended for label-based discovery)**

```bash
gcloud organizations add-iam-policy-binding ORG_ID \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/aiplatform.viewer"
```

**Option B: Per-project (for explicit project_ids)**

```bash
for PROJECT in project1 project2; do
  gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:SA_EMAIL" \
    --role="roles/aiplatform.viewer"
done
```

#### Error Handling

- **Partial failures:** If some projects fail (e.g., permission denied), ingestion continues with remaining projects and logs warnings
- **Total failure:** If ALL projects fail, the ingestion run fails with an error
- **Discovery failure:** If auto-discovery fails, the run fails immediately
- **Rate limits:** API quota errors are automatically retried with exponential backoff

#### Migration from project_id

Old configs auto-migrate with a warning:

```yaml
# Old (still works)
project_id: my-project

# New (recommended)
project_ids: [my-project]
```

If both `project_id` and `project_ids` are specified, `project_id` is ignored with a warning.

### Troubleshooting

#### Permission Denied Errors

**Symptom:** `Permission denied when listing GCP projects` or `Permission denied: {project-id}`

**Solutions:**

1. **Missing project-level permissions**

   ```bash
   gcloud projects get-iam-policy PROJECT_ID \
     --flatten="bindings[].members" \
     --filter="bindings.members:SERVICE_ACCOUNT_EMAIL"
   ```

2. **Missing org-level permissions for label-based discovery**

   - Label-based discovery requires `resourcemanager.projects.list` at org/folder level
   - Solution: Use explicit `project_ids` instead, or grant `roles/browser` at org/folder level

3. **Vertex AI API not enabled**
   ```bash
   gcloud services enable aiplatform.googleapis.com --project=PROJECT_ID
   ```

#### Rate Limit Errors

**Symptom:** `Rate limit exceeded` or `Quota exceeded`

**Solutions:**

- Reduce scope using `project_id_pattern` or split into multiple runs
- Avoid running multiple Vertex AI ingestion jobs simultaneously
- Request quota increase in GCP Console

The source automatically retries with exponential backoff (1s → 2s → 4s... up to 60s max).

#### Pattern Configuration Issues

**Symptom:** `Found X projects but all were excluded by project_id_pattern`

**Common mistakes:**

1. **Glob syntax instead of regex**

   ```yaml
   # Wrong - glob syntax
   project_id_pattern:
     allow: ["prod-*"]

   # Correct - regex syntax
   project_id_pattern:
     allow: ["prod-.*"]
   ```

2. **Deny pattern overrides allow**

   ```yaml
   # This denies everything!
   project_id_pattern:
     allow: ["prod-.*"]
     deny: [".*"] # Deny takes precedence
   ```

3. **Validate patterns before running**
   ```bash
   echo "prod-ml-east" | grep -E "^prod-.*$"
   ```

#### Project Not Found Errors

**Symptom:** `Not found: {project-id}`

**Solutions:**

- Verify project ID spelling (check for typos)
- Confirm project exists and is active: `gcloud projects describe PROJECT_ID`
- Ensure service account has access to the project

#### Debug Commands

```bash
# List all accessible projects
gcloud projects list --filter='lifecycleState:ACTIVE'

# List projects by label
gcloud projects list --filter='labels.env:prod'

# Check service account permissions
gcloud projects get-iam-policy PROJECT_ID

# Verify Vertex AI API is enabled
gcloud services list --enabled --project=PROJECT_ID | grep aiplatform
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
