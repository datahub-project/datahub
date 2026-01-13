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

### Multi-Project Support

The Vertex AI source supports ingesting metadata from multiple GCP projects in a single run.

> **💡 Tip**: For production deployments with multiple projects, see the [Multi-Project Configuration Best Practices](#-multi-project-configuration-best-practices) section below for performance tuning guidance.

#### Project Selection Priority

Projects are selected using the following priority:

1. **`project_ids`** (highest priority) - Explicit list of project IDs
2. **`project_labels`** - Discover projects by GCP labels
3. **Auto-discovery** - Find all accessible projects

The `project_id_pattern` filter is always applied after project selection.

#### Option 1: Explicit Project List

Specify multiple projects directly in your configuration:

```yaml
source:
  type: vertexai
  config:
    project_ids:
      - project-alpha
      - project-beta
      - project-gamma
    region: us-central1
```

#### Option 2: Filter by GCP Labels

Discover projects using GCP labels (useful for environment-based filtering):

```yaml
source:
  type: vertexai
  config:
    project_labels:
      - "env:production"
      - "team:ml-platform"
    region: us-central1
```

Label format: `key:value` for exact match, or `key` to match any value.

#### Option 3: Auto-Discovery Mode

Leave both `project_ids` and `project_labels` empty to automatically discover all accessible projects:

```yaml
source:
  type: vertexai
  config:
    # No project_ids or project_labels = auto-discover all accessible projects
    region: us-central1
```

> **⚠️ Important: Auto-Discovery Permissions**
>
> Auto-discovery and label-based discovery require the `resourcemanager.projects.list` permission.
> This permission is included in:
>
> - `roles/browser` (organization/folder level)
> - `roles/viewer` (organization/folder level)
>
> If your service account only has project-level permissions, use the explicit `project_ids` list instead.

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
      - test-ml-ci
    project_id_pattern:
      allow:
        - "prod-.*" # Only production projects
      deny:
        - ".*-sandbox$" # Exclude sandboxes
    region: us-central1
```

> **Note:** When using auto-discovery (no `project_ids` or `project_labels`), pattern filtering is applied at runtime after projects are discovered. Invalid patterns that filter out all projects will cause the ingestion to fail at runtime, not at configuration validation time.

#### Backward Compatibility & Migration

The legacy `project_id` config is automatically converted to `project_ids`. No immediate changes are required.

**Migration Steps:**

1. **Single project** - Replace `project_id` with `project_ids`:

   ```yaml
   # Before
   project_id: my-project

   # After
   project_ids:
     - my-project
   ```

2. **Multiple projects** - Simply add more entries:

   ```yaml
   project_ids:
     - project-alpha
     - project-beta
   ```

3. **With filtering** - Add `project_id_pattern` for regex-based filtering:
   ```yaml
   project_ids:
     - prod-ml-east
     - prod-ml-west
     - dev-sandbox
   project_id_pattern:
     allow:
       - "prod-.*"
   ```

> **Note:** If both `project_id` and `project_ids` are specified, `project_id` is ignored with a warning.

#### Error Handling

- **Partial failures**: If some projects fail (e.g., permission denied), ingestion continues with remaining projects and logs warnings
- **Total failure**: If ALL projects fail, the ingestion run fails with an error
- **Discovery failure**: If auto-discovery fails, the run fails immediately
- **Rate limits**: API quota errors are automatically retried with exponential backoff

#### ⚙️ Multi-Project Configuration Best Practices

When enabling multi-project scanning, you're potentially going from ingesting 1 project to ingesting 10, 50, or even 100+ projects. This fundamentally changes the performance profile, quota usage, and operational characteristics of the connector.

**Performance Implications:**

- **API Call Multiplication**: For each project, the connector makes ~5-10 API calls to list Vertex AI resources. Scanning 50 projects results in 250-500 API calls per ingestion run.
- **Memory & Metadata Volume**: Each project's metadata is held in memory during ingestion. Large projects with hundreds of models can result in significant memory usage.
- **Ingestion Duration**:
  - 10 projects: ~2-5 minutes
  - 50 projects: ~10-20 minutes
  - 100+ projects: 30+ minutes (approaches DataHub ingestion timeout limits)

**GCP Rate Limiting & Quotas:**

Vertex AI API quotas (typical defaults):

- Read requests: 300 per minute per project
- List operations: 60 per minute per project

If scanning 50 projects in parallel, you could hit:

- 50 projects × 5 API calls = 250 requests
- Default quota: 300/min → easily exceeded if projects are busy

**Start Small and Scale Gradually:**

Recommended approach:

1. Start with 5-10 projects - Test performance and monitor quota usage
2. Monitor ingestion duration - Check DataHub logs for completion time
3. Scale to 20-50 projects - Adjust schedule frequency based on duration
4. For 100+ projects - Consider incremental ingestion or splitting into multiple connector instances

**Project Count Guidelines:**

| Project Count | Expected Duration | Recommended Schedule | Notes                            |
| ------------- | ----------------- | -------------------- | -------------------------------- |
| 1-10          | 2-5 minutes       | Hourly               | Good for real-time monitoring    |
| 10-50         | 10-20 minutes     | Every 2-4 hours      | Balance freshness vs load        |
| 50-100        | 20-40 minutes     | Every 6-12 hours     | Watch for quota issues           |
| 100+          | 40+ minutes       | Daily + incremental  | Split across multiple connectors |

**Use `project_labels` to Filter Relevant Projects:**

Instead of scanning all projects:

❌ **Bad**: Scans ALL projects in org (could be hundreds)

```yaml
# No project_ids or project_labels = auto-discover all
region: us-central1
```

✅ **Good**: Filter to ML/AI projects only

```yaml
project_labels:
  - "env:production"
  - "team:ml-platform"
region: us-central1
```

✅ **Better**: Explicit list for critical projects

```yaml
project_ids:
  - "prod-ml-project-1"
  - "prod-ml-project-2"
  - "prod-recommendations"
region: us-central1
```

**Monitor GCP API Quotas:**

Check your quota usage:

```bash
# View Vertex AI API quotas
gcloud services quotas list \
  --service=aiplatform.googleapis.com \
  --consumer=projects/YOUR_PROJECT \
  --filter="metric.type=aiplatform.googleapis.com/quota/read_requests"
```

If hitting quota limits:

1. Request quota increase from GCP (can take 24-48 hours)
2. Reduce scan frequency - Switch from hourly to every 4-6 hours
3. Split projects across connectors - Run multiple DataHub sources with different project subsets
4. Enable incremental ingestion - Only fetch changed resources (requires DataHub 0.10.0+)

**Performance Tuning:**

If ingestion is slow (>30 minutes):

1. Check project activity levels:
   - Projects with 1000+ models → slower
   - Empty/inactive projects → skip them with labels
2. Reduce parallelism if hitting rate limits (DataHub 0.11.0+):
   ```yaml
   max_workers: 5 # Limit concurrent API calls
   ```
3. Consider incremental mode:
   ```yaml
   stateful_ingestion:
     enabled: true
     # Only fetch resources modified in last 24 hours
   ```

**Service Account Permissions Planning:**

For multi-project scanning:

- ✅ **Org-level permissions** (when using `project_labels` or auto-discovery):
  - Requires `resourcemanager.projects.list` at org level
  - Requires `aiplatform.models.list` on each project
- ❌ **Project-level permissions won't work** with `project_labels`:
  - You'll get "no projects detected" errors
  - See troubleshooting section

**Cost Considerations:**

API costs (GCP charges for API calls):

- Vertex AI List operations: ~$0.0001 per call
- 50 projects × 10 calls × 24 runs/day = $0.12/day = $43/year
- Not significant, but scales with project count

**Error Handling Expectations:**

Common scenarios:

1. **Some projects fail**: Connector continues with other projects (logged as warnings)
2. **Permission denied on 1 project**: Other projects still ingest successfully
3. **Quota exceeded**: Entire run may fail - reduce frequency or request quota increase
4. **Timeout errors**: Reduce project count or enable incremental ingestion

**Monitoring & Alerting:**

Set up alerts for:

- Ingestion duration > 30 minutes (approaching timeout)
- GCP quota usage > 80% (via GCP Monitoring)
- Failed ingestion runs (via DataHub monitoring)
- Increasing error rates in connector logs

### Troubleshooting

#### Permission Denied Errors

**Symptom:** `Permission denied when listing GCP projects` or `Permission denied: {project-id}`

**Causes & Solutions:**

1. **Missing project-level permissions**

   ```bash
   # Verify service account has aiplatform.viewer role
   gcloud projects get-iam-policy PROJECT_ID \
     --flatten="bindings[].members" \
     --filter="bindings.members:SERVICE_ACCOUNT_EMAIL"
   ```

2. **Missing org-level permissions for auto-discovery**

   - Auto-discovery requires `resourcemanager.projects.list` permission
   - Solution: Use explicit `project_ids` instead, or grant `roles/browser` at org/folder level

3. **Vertex AI API not enabled**
   ```bash
   # Enable the API
   gcloud services enable aiplatform.googleapis.com --project=PROJECT_ID
   ```

#### Rate Limit Errors

**Symptom:** `Rate limit exceeded` or `Quota exceeded`

**Causes & Solutions:**

1. **Too many projects** - Reduce scope using `project_id_pattern` or split into multiple runs
2. **Concurrent ingestion** - Avoid running multiple Vertex AI ingestion jobs simultaneously
3. **Request quota increase** - Request higher quotas in GCP Console

The source automatically retries with exponential backoff (1s → 2s → 4s... up to 60s max).

#### Pattern Configuration Issues

**Symptom:** `Found X projects but all were excluded by project_id_pattern`

**Common mistakes:**

1. **Glob syntax instead of regex**

   ```yaml
   # Wrong - glob syntax
   project_id_pattern:
     allow:
       - "prod-*"      # Matches "prod-" followed by nothing or more dashes

   # Correct - regex syntax
   project_id_pattern:
     allow:
       - "prod-.*"     # Matches "prod-" followed by anything
   ```

2. **Deny pattern overrides allow**

   ```yaml
   # This denies everything!
   project_id_pattern:
     allow:
       - "prod-.*"
     deny:
       - ".*" # Deny takes precedence
   ```

3. **Validate patterns before running**
   ```bash
   # Test your pattern against project IDs
   echo "prod-ml-east" | grep -E "^prod-.*$"
   ```

#### Project Not Found Errors

**Symptom:** `Not found: {project-id}`

**Solutions:**

1. Verify project ID spelling (check for typos)
2. Confirm project exists and is active:
   ```bash
   gcloud projects describe PROJECT_ID
   ```
3. Ensure service account has access to the project

#### Debug Commands

Use these commands to diagnose issues:

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
