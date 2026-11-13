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

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then read the report: every problem names what it covers.

#### Failures and warnings

| Report entry | Run status                            | Soft-deletion                                                                                   |
| ------------ | ------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Failure      | Failed (exit code 1)                  | Paused for the whole run, in every project. Everything ingested earlier is kept.                |
| Warning      | Succeeded, "with at least N warnings" | Only the named scope is kept. Entities deleted from Vertex AI elsewhere are still soft-deleted. |

- A **failure** means a project listed in `project_ids` could not be read at all, or one of its four lists (training jobs, experiments, pipelines or models) could not be read in a region with an error that repeats on every run (a refusal, a not-found answer or another lasting error). The run still writes everything else it could read, models included, and the failure names the permission and the `switch` that turns that read off. The other projects are still ingested first, then the run ends as failed.
- A **warning** means one part could not be read: one item or one detail of an item, a list lost to a temporary error, or any lost part of a discovered project. The endpoint, dataset and evaluation reads are details, so losing one is always a warning. Repeated warnings merge into one entry that lists sample items and a total.
- A **temporary error** (429, 503, timeout, or a retry that ran out) keeps the previous version of what could not be read. Something never ingested before stays absent until a run completes without that error.
- A **refusal** (permission denied) writes the entity without the detail it could not read. This replaces stored values only if access was lost after they were ingested. An entity that cannot be written without that detail is kept as it was. Once a read has worked for one item in a region, a refusal costs only its own item, because one model can have its own access policy. Before that, 25 refusals in a row stop the read in that region for the rest of the run.
- A **not found** answer (404) is a part that could not be read. On a whole list, that resource type was not read in that region. On one item or one detail, only that item is affected, and the warning names it. A missing ML Metadata store is the exception: it means the project has never used ML Metadata, so it is reported as info and earlier experiments are soft-deleted.
- A training job or pipeline list lost to a temporary error, or one pipeline's task details, holds back that region's models, model groups, evaluations and deployments for the run, because their links cannot be known any other way; their earlier versions are kept. A temporary stop of one training job's lookups does not: the job keeps its previous version, and the links its own listing names (the output model and its training data) are still written. Experiments never hold models back, because their links do not reach the written models. A lasting loss writes the models without that read's links.

Read the context of each entry as `project=..., region=..., resource=..., item=...`. Parts that do not apply are left out. A refusal also names the `service` and the `permission` that was missing, and a read that depends on a setting names the `switch` that turns it off.

The report keeps only a sample of warnings and failures, so it also lists every lost or stopped read in full under `lost_reads`, by project, once per read and region. Each entry names the project, region and read, the `service`, `permission` and `switch` when known, and ends with `loss=temporary` or `loss=lasting`. It names no item, because it stands for the whole read in that region.

#### Listed and discovered projects

A project counts as listed only when `project_ids` is set. Projects picked by `project_labels` or `project_id_pattern` without `project_ids` are discovered.

| Project state                                                      | Listed in `project_ids` | Discovered                     |
| ------------------------------------------------------------------ | ----------------------- | ------------------------------ |
| Vertex AI API turned off, or project deleted                       | Failure                 | Warning, earlier entities kept |
| API on, but no Vertex AI role (every read refused)                 | Failure                 | Warning, earlier entities kept |
| One resource type lost to an error on every run                    | Failure                 | Warning, earlier entities kept |
| Any other error on the whole project (a timeout, quota, not found) | Failure                 | Warning, earlier entities kept |

To silence a warning for a discovered project you do not want, grant it a Vertex AI role such as `roles/aiplatform.viewer`, or exclude it with `project_id_pattern`. To fix a failure for a listed project, enable the API and grant the role, or remove it from `project_ids`. For one lost resource type, grant the permission the failure names, or set the `switch` it names, such as `include_experiments: false`.

When `discover_regions: true`, the connector lists each project's regions with `aiplatform.locations.list`. If that fails and the deprecated single-region field `region` is set, there is a warning, the connector uses `region`, and the project's earlier entities are kept. If `region` is not set, the project rules above apply. List `regions` in the recipe instead of using discovery to avoid this call.
