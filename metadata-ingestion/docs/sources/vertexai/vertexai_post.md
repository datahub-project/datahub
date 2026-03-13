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

##### ML Metadata Lineage

The connector extracts lineage and metrics from CustomJob training jobs using the **Vertex AI ML Metadata API**. This enables:

- **Full lineage tracking** for CustomJob: input datasets → training job → output models
- **Hyperparameters and metrics extraction** from training jobs that log to ML Metadata Executions
- **Model evaluation** ingestion with evaluation metrics and lineage to models

These features are controlled by the following configuration options:

- `use_ml_metadata_for_lineage` (default: `true`) — Extracts lineage from ML Metadata for CustomJob and other non-AutoML training jobs
- `extract_execution_metrics` (default: `true`) — Extracts hyperparameters and metrics from ML Metadata Executions
- `include_evaluations` (default: `true`) — Ingests model evaluations and evaluation metrics

For CustomJob lineage to work, your training scripts must log artifacts to Vertex AI ML Metadata. This happens automatically when using the Vertex AI Experiments SDK, or you can log manually:

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
    project_id: my-project
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
