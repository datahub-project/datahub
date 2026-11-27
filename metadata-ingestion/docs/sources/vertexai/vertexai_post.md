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

Experiment runs created during the Vertex Experiment Preview have no executions and no logged custom jobs (training-job links), so they are written without them. Their run times come from executions, so they are missing too. One shared info entry in the report covers all such runs, and nothing is kept for them.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then read the report: every problem names what it covers.

#### Failures and warnings

| Report entry | Run status                            | Soft-deletion                                                                                                                                                             |
| ------------ | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Failure      | Failed (exit code 1)                  | Paused for the whole run, in every project. Everything ingested earlier is kept.                                                                                          |
| Warning      | Succeeded, "with at least N warnings" | Only the named scope is kept, but a list lost in one region keeps that resource type in the whole project (see below). Entities deleted elsewhere are still soft-deleted. |

- A **failure** means a project listed in `project_ids` could not be read at all, or one of its four lists (training jobs, experiments, pipelines or models) could not be read in a region with an error that repeats on every run (a refusal, a not-found answer or another lasting error). The run still writes everything else it could read, models included, and the failure names the permission and the `switch` that turns that read off. The other projects are still ingested first, then the run ends as failed.
- A listed project whose every read fails is a **failure** even when each error is temporary, because nothing from it could be refreshed.
- A **warning** means one part could not be read: one item or one detail of an item, a list lost to a temporary error, or any lost part of a discovered project. The endpoint, dataset and evaluation reads are details, so losing one is always a warning. Repeated warnings merge into one entry that lists sample items and a total. Entity names do not carry their region, so a list lost in one region keeps that resource type's earlier entities in every region of the project.
- A **temporary error** (429, 503, timeout, or a retry that ran out) keeps the previous version of what could not be read. Something never ingested before stays absent until a run completes without that error.
- A **refusal** (permission denied) writes the entity without the detail it could not read. This replaces stored values only if access was lost after they were ingested. An entity that cannot be written without that detail is kept as it was. Once a read has worked for one item in a region, a refusal costs only its own item, because one model can have its own access policy. Before that, 25 refusals in a row stop the read in that region for the rest of the run.
- A **not found** answer (404) is a part that could not be read. On a whole list, that resource type was not read in that region. On one item or one detail, only that item is affected, and the warning names it. A missing ML Metadata store is the exception: it means the project has never used ML Metadata, so it is reported as info and earlier experiments are soft-deleted.
- A training job or pipeline list lost to a temporary error, or one pipeline's task details, holds back that region's models, model groups, evaluations and deployments for the run, because their links cannot be known any other way; their earlier versions are kept. Only that region is held back: a model in another region or project that these pipelines use is still written, and may lose its pipeline "used by" links until a run reads those pipelines again. A separate warning, "Models elsewhere may lose pipeline links", says so; it also appears when the whole project cannot be read or a pipeline read fails with a project-wide error. A temporary stop of one training job's lookups does not: the job keeps its previous version, and the links its own listing names (the output model and its training data) are still written. Experiments never hold models back, because their links do not reach the written models. A lasting loss writes the models without that read's links.

Pipeline and pipeline task URNs carry no project, and the URNs of pipeline, experiment and model group folders are hashes that do not show it. When a pipeline, experiment or model read is lost, or a whole project, in a project ingested before, the earlier ones of every project are kept this run, so their stale-entity removal pauses. The warning "Pipelines and containers kept in every project" names the project, region and read. A temporary loss in a discovered project keeps them the same way.

The connector removes entities only when Google says something you must act on, such as a refusal or the API turned off. When the error is not Google's, it points to a bug in the connector, which says nothing about the data, so everything is kept and the warning asks you to report it; a listed project still fails the run. A lasting loss in a discovered project (a refusal, the API turned off, or another Google error that repeats on every run) is not kept this way, so stale-entity removal goes on in every project: that project's earlier experiment folders are not kept, so stale-entity removal can soft-delete them. When the lost read covers its experiments, the warning "Experiment folders not kept while unreadable" names the project. They come back on the first run that reads them. Experiment runs are not removed; until then they show their experiment folder as removed. Its other earlier entities are kept. Pipelines are tied to their project through their folder in the last run's state, which keeps the pipeline, its tasks and its folder, and model group folders through the model group names, which carry the project. Experiment folders cannot be tied. A run that fails soft-deletes nothing.

Read the context of each entry as `project=..., region=..., resource=..., item=...`. Parts that do not apply are left out. A refusal also names the `service` and the `permission` that was missing, and a read that depends on a setting names the `switch` that turns it off.

The report keeps only a sample of warnings and failures, so it also lists every lost or stopped read in full under `lost_reads`, by project, once per read and region. Each entry names the project, region and read, the `service`, `permission` and `switch` when known, and ends with `loss=temporary` or `loss=lasting`. It names no item, because it stands for the whole read in that region.

#### Listed and discovered projects

A project counts as listed only when `project_ids` is set, or the deprecated `project_id`, which the connector moves into `project_ids`. Projects picked by `project_labels` or `project_id_pattern` without `project_ids` are discovered.

| Project state                                                      | Listed in `project_ids` | Discovered                     |
| ------------------------------------------------------------------ | ----------------------- | ------------------------------ |
| Vertex AI API turned off, or project deleted                       | Failure                 | Warning, earlier entities kept |
| API on, but no Vertex AI role (every read refused)                 | Failure                 | Warning, earlier entities kept |
| One resource type lost to an error on every run                    | Failure                 | Warning, earlier entities kept |
| Any other error on the whole project (a timeout, quota, not found) | Failure                 | Warning, earlier entities kept |

For a discovered project, the earlier entities kept leave out its experiment folders when Google's error lasts, here and in the region checks below, as described above.

To silence a warning for a discovered project you do not want, grant it a Vertex AI role such as `roles/aiplatform.viewer`, or exclude it with `project_id_pattern`. To fix a failure for a listed project, enable the API and grant the role, or remove it from `project_ids`. For one lost resource type, grant the permission the failure names, or set the `switch` it names, such as `include_experiments: false`.

When `discover_regions: true`, the connector lists each project's regions with `aiplatform.locations.list`. If that fails, the connector reads the regions in `regions` instead, or `region` if `regions` is not set. It reads only the listed regions the installed SDK can read, and never `global`. The "Region discovery failed" warning appears only when at least one listed region other than `global` can be read. With only an unreadable listed region, you get "Configured region could not be checked" and no discovery warning. Entity names do not carry their region, so the connector cannot tell which earlier entities belong to the listed regions: every earlier entity of the project is kept (in a discovered project with a lasting error, apart from experiment folders, as above), and stale-entity removal is paused for that project while discovery keeps failing. Pipelines, pipeline tasks and containers follow the rules above for a lost read in a listed or discovered project. An entity deleted from a listed region in that time is soft-deleted on the first run after discovery works again.

If a listed region is one the installed SDK does not know, nothing tells it from a misspelt one. That region gets a "Configured region could not be checked" failure for a listed project, or a warning for a discovered one. The context names the project, the service or permission, and the regions, and the discovery error is attached to it. The other listed regions are still read. If no listed region can be read, the project is not read. Earlier entities are kept either way. If nothing usable is listed (no `regions` or `region`, or only `global`), or the Vertex AI API is off in the project, the project rules above apply. To avoid the locations call, set `discover_regions: false` and list `regions` in the recipe.

The connector reads only the locations the installed Google Cloud SDK (`google-cloud-aiplatform`) accepts. Google lists some newer regions the SDK cannot read yet, multi-region locations such as `us` and `eu`, and the `global` location, which the connector does not read yet. These are skipped with one warning per project, "Vertex AI locations skipped", which names them under `newer_regions`, `multi_regions` and `unsupported`; nothing else in the project changes. A `global` set in `regions` is skipped the same way. With `discover_regions: false`, a region in `regions` (or `region`) that the SDK refuses is checked against Google's location list before any project is read. The first project is asked; if it refuses or has the API off, the next one is, until one answers: if Google lists it, it is skipped the same way; if not, the run fails with "Configured region is not a Vertex AI location", which names it, and no project is read. Entity names do not carry their region, so skipping a misspelt region would let stale-entity removal soft-delete the entities of the region meant. If no project answers, a temporary error stops the check, or the list is empty, the run fails with "Configured region could not be checked", which names the last project asked under `checked_in`: remove the region, or grant `aiplatform.locations.list`. In both cases earlier entities are kept. Full coverage of these locations is planned for a later release.

If one model's evaluation list cannot be read, earlier model evaluations in the project are kept, because evaluation names carry only the evaluation id and cannot be tied to a model. Evaluations deleted from other models are removed once that list reads again.
