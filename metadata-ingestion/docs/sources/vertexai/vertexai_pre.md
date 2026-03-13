### Overview

The `vertexai` module ingests metadata from Vertex AI into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The ingestion job extracts Models, Datasets, Training Jobs, Endpoints, Experiments, Experiment Runs, Model Evaluations, and Pipelines from Vertex AI in a given project and region.

The source supports ingesting across multiple GCP projects by specifying `project_ids`, `project_labels`, or `project_id_pattern`. Use `env` (e.g., `PROD`, `DEV`, `STAGING`) to distinguish between environments. The optional `platform_instance` field namespaces resources to avoid URN collisions when ingesting from multiple Vertex AI setups.

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
