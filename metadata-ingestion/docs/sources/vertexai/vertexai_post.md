### ML Metadata Lineage

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

### Cross-Platform Lineage Configuration

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
