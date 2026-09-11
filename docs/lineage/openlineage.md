---
description: "Ingest and display lineage from data processing frameworks in DataHub using the OpenLineage integration for end-to-end pipeline visibility."
---

# OpenLineage

DataHub, now supports [OpenLineage](https://openlineage.io/) integration. With this support, DataHub can ingest and display lineage information from various data processing frameworks, providing users with a comprehensive understanding of their data pipelines.

## Features

- **REST Endpoint Support**: DataHub now includes a REST endpoint that can understand OpenLineage events. This allows users to send lineage information directly to DataHub, enabling easy integration with various data processing frameworks.

- **[Spark Event Listener Plugin](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)**: DataHub provides a Spark Event Listener plugin that seamlessly integrates OpenLineage's Spark plugin. This plugin enhances DataHub's OpenLineage support by offering additional features such as PathSpec support, column-level lineage, patch support and more.

## OpenLineage Support with DataHub

### 1. REST Endpoint Support

DataHub's REST endpoint allows users to send OpenLineage events directly to DataHub. This enables easy integration with various data processing frameworks, providing users with a centralized location for viewing and managing data lineage information.

With Spark and Airflow we recommend using the Spark Lineage or DataHub's Airflow plugin for tighter integration with DataHub.

#### How to Use

To send OpenLineage messages to DataHub using the REST endpoint, simply make a POST request to the following endpoint:

```
POST GMS_SERVER_HOST:GMS_PORT/openapi/openlineage/api/v1/lineage
```

Include the OpenLineage message in the request body in JSON format.

The endpoint responds with:

| Status | Meaning                                                                                   |
| ------ | ----------------------------------------------------------------------------------------- |
| `201`  | The event was accepted and its metadata ingested                                          |
| `400`  | The payload is not a well-formed OpenLineage event                                        |
| `403`  | The caller lacks privileges on one or more of the entities the event would write          |
| `422`  | The event is well-formed but carries nothing DataHub can store (for example, no job name) |
| `500`  | A server-side failure                                                                     |

#### Sending a batch

A producer holding several events can send them in one request rather than one at a time:

```
POST GMS_SERVER_HOST:GMS_PORT/openapi/openlineage/api/v1/lineage/batch
```

The body is a JSON array of the same three event types. Each event is converted on its own, so one
unusable event does not reject the rest of the array; the events that did convert are then written
in a single transaction. A request that is accepted answers `200` and reports what happened:

```json
{
  "status": "partial_success",
  "summary": {
    "received": 3,
    "successful": 2,
    "failed": 1,
    "retriable": 0,
    "non_retriable": 1
  },
  "failed_events": [{ "index": 1, "reason": "...", "retriable": false }]
}
```

`index` is the event's position in the array you sent, which is the producer's handle on which
event to fix. `retriable` is always `false`, because every failure reported here is a conversion
failure and conversion is deterministic — the same bytes fail the same way. A failure a resend
could fix is a server fault, and those come back as a `500` for the whole request.

Two things are decided for the batch as a whole rather than per event:

| Concern       | Behaviour                                                                                                               |
| ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Authorization | A batch touching anything the caller may not write is refused whole with `403`, because privileges belong to the actor. |
| Batch size    | More than `datahub.openlineage.max-batch-size` events (default 1000) is a `400`.                                        |

A body that is not a JSON array is a `400`. An empty array is accepted and writes nothing.

### Supported event types

The endpoint accepts all three OpenLineage 2.0 event types:

| Event          | Carries                           | What DataHub creates                                |
| -------------- | --------------------------------- | --------------------------------------------------- |
| `RunEvent`     | A run, its job, and dataset edges | DataFlow, DataJob, DataProcessInstance, lineage     |
| `JobEvent`     | A job and dataset edges, no run   | DataFlow, DataJob, lineage — no DataProcessInstance |
| `DatasetEvent` | A single dataset, no job or run   | Dataset aspects only                                |

`JobEvent` and `DatasetEvent` are the spec's static-lineage path: a producer describing a job or a
table it did not just execute or write. The event type comes from `schemaURL`; when a producer omits
it, the event's shape decides — a `run` means a RunEvent, a top-level `dataset` means a DatasetEvent,
otherwise a JobEvent.

### What DataHub captures from an event

| OpenLineage facet                                                 | Where it lands                                                                   |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `schema`                                                          | Dataset `schemaMetadata`                                                         |
| `columnLineage`                                                   | Column-level lineage on the DataJob                                              |
| `symlinks`                                                        | Dataset URN resolution (Glue, Hive)                                              |
| `outputStatistics`, `lifecycleStateChange`                        | Dataset `Operation` — row counts, operation type, write history                  |
| `dataQualityMetrics`                                              | Dataset `DatasetProfile` — row count, size, per-column null and distinct counts  |
| `tags`, `ownership` (dataset)                                     | Dataset `GlobalTags`, `Ownership`                                                |
| `documentation`, `storage`, `datasetType`, `version`              | Dataset `DatasetProperties`                                                      |
| `tags`, `ownership` (job)                                         | DataJob `GlobalTags`, `Ownership`                                                |
| `documentation` (job)                                             | DataJob description                                                              |
| `jobType`                                                         | DataJob custom properties — `BATCH` vs `STREAMING`, and the emitting integration |
| `sourceCodeLocation`                                              | DataJob `InstitutionalMemory` link                                               |
| `nominalTime`, `externalQuery`, `extractionError`, `errorMessage` | DataProcessInstance custom properties                                            |
| `parent`                                                          | Job-to-job lineage edge                                                          |
| `processing_engine`                                               | Orchestrator name and custom properties                                          |

Facets outside this table are not stored. An `extractionError` facet also raises a warning in the GMS
log, because it is the producer reporting that its own lineage output is incomplete.

The caller needs `Edit Entity` and `Edit Lineage` privileges on the entity types the event
touches — DataFlow, DataJob, DataProcessInstance and Dataset.

Metadata reported across several events for one run accumulates rather than the last event
winning, so a producer that declares inputs at `START` and outputs at `COMPLETE` ends up with
both. Because lineage is applied additively, an edge a job no longer has is not removed
automatically; set `DATAHUB_OPENLINEAGE_USE_PATCH=false` for last-event-wins behaviour instead.

Example:

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes"
  },
  "inputs": [
    {
      "namespace": "postgres://workshop-db:None",
      "name": "workshop.public.taxes",
      "facets": {
        "dataSource": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
          "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/DataSourceDatasetFacet",
          "name": "postgres://workshop-db:None",
          "uri": "workshop-db"
        }
      }
    }
  ],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

##### How to set up Airflow

Follow the Airflow guide to setup the Airflow DAGs to send lineage information to DataHub. The guide can be found [here](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html).
The transport should look like this:

```json
{
  "type": "http",
  "url": "https://GMS_SERVER_HOST:GMS_PORT/openapi/openlineage/",
  "endpoint": "api/v1/lineage",
  "auth": {
    "type": "api_key",
    "api_key": "your-datahub-api-key"
  }
}
```

#### How to modify configurations

To modify the configurations for the OpenLineage REST endpoint, you can change it using environment variables. The following configurations are available:

##### DataHub OpenLineage Configuration

This document describes all available configuration options for the DataHub OpenLineage integration, including environment variables, application properties, and their usage.

##### Configuration Overview

The DataHub OpenLineage integration can be configured using environment variables, application properties files (`application.yml` or `application.properties`), or JVM system properties. All configuration options are prefixed with `datahub.openlineage`.

##### Environment Variables

| Environment Variable                                   | Property                                               | Type    | Default | Description                                                                                                       |
| ------------------------------------------------------ | ------------------------------------------------------ | ------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_OPENLINEAGE_ENV`                              | `datahub.openlineage.env`                              | String  | `PROD`  | Environment for DataFlow cluster and Dataset fabricType (see valid values below)                                  |
| `DATAHUB_OPENLINEAGE_ORCHESTRATOR`                     | `datahub.openlineage.orchestrator`                     | String  | `null`  | Orchestrator name for DataFlow entities. When set, takes precedence over processing_engine facet and producer URL |
| `DATAHUB_OPENLINEAGE_PLATFORM_INSTANCE`                | `datahub.openlineage.platform-instance`                | String  | `null`  | Override DataFlow cluster (defaults to env if not specified)                                                      |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_ENV`               | `datahub.openlineage.common-dataset-env`               | String  | `null`  | Override Dataset environment independently from DataFlow cluster                                                  |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_PLATFORM_INSTANCE` | `datahub.openlineage.common-dataset-platform-instance` | String  | `null`  | Common platform instance for dataset entities                                                                     |
| `DATAHUB_OPENLINEAGE_MATERIALIZE_DATASET`              | `datahub.openlineage.materialize-dataset`              | Boolean | `true`  | Whether to materialize dataset entities                                                                           |
| `DATAHUB_OPENLINEAGE_INCLUDE_SCHEMA_METADATA`          | `datahub.openlineage.include-schema-metadata`          | Boolean | `true`  | Whether to include schema metadata in lineage                                                                     |
| `DATAHUB_OPENLINEAGE_CAPTURE_COLUMN_LEVEL_LINEAGE`     | `datahub.openlineage.capture-column-level-lineage`     | Boolean | `true`  | Whether to capture column-level lineage information                                                               |
| `DATAHUB_OPENLINEAGE_USE_PATCH`                        | `datahub.openlineage.use-patch`                        | Boolean | `true`  | Apply lineage additively, so inputs and outputs reported across separate events for one run accumulate            |
| `DATAHUB_OPENLINEAGE_FILE_PARTITION_REGEXP_PATTERN`    | `datahub.openlineage.file-partition-regexp-pattern`    | String  | `null`  | Regular expression pattern for file partition detection                                                           |
| `DATAHUB_OPENLINEAGE_DOMAINS`                          | `datahub.openlineage.domains`                          | List    | `empty` | Comma-separated domain URNs (`urn:li:domain:<id>`) attached to the DataFlow and DataJob                           |
| `DATAHUB_OPENLINEAGE_MAX_BATCH_SIZE`                   | `datahub.openlineage.max-batch-size`                   | Integer | `1000`  | Most events accepted by a single `/lineage/batch` request                                                         |

> **Valid `env` values**: `PROD`, `DEV`, `TEST`, `QA`, `UAT`, `EI`, `PRE`, `STG`, `NON_PROD`, `CORP`, `RVW`, `PRD`, `TST`, `SIT`, `SBX`, `SANDBOX`, `CERT`
>
> **How `env` works**:
>
> - **By default**, `env` sets both the DataFlow cluster and Dataset fabricType for simplicity
> - **For advanced scenarios**, use `platform-instance` to override the DataFlow cluster or `common-dataset-env` to override the Dataset environment independently
>
> **Note**: The `env` property naming matches DataHub SDK conventions where `env` is the user-facing parameter that internally maps to the URN `cluster` field.

##### Assigning Domains

OpenLineage has no domain facet, so a domain cannot be carried on the event itself. Set it on the
endpoint instead — every DataFlow and DataJob created from events on this endpoint gets these domains:

```bash
DATAHUB_OPENLINEAGE_DOMAINS=urn:li:domain:finance,urn:li:domain:reporting
```

Values must be full domain URNs. A domain name such as `finance` cannot be resolved here and is
skipped with a warning. The remaining valid domains are written as the entity's complete domain
list, replacing any existing assignment — including one made in the UI. If no configured value
parses at all, nothing is emitted, so a bad value cannot silently clear domains.

##### Path specs and per-connection instances

Two settings take structured values and so cannot be set through environment variables. `path-specs`
is keyed by platform and nests a list of objects, which environment variable names cannot express;
`connections` is keyed by an OpenLineage namespace authority containing `:` and `/`, which they
cannot express either. Set both in `application.yml`:

```yaml
datahub:
  openlineage:
    # Per-platform path-to-URN rules for object-storage datasets.
    path-specs:
      s3:
        - alias: events
          platform: s3
          platform-instance: my_s3
          path-spec-list:
            - "s3://my-bucket/{table}"
    # Align URNs with what each platform's own connector stamps. Keys are OpenLineage namespace
    # authorities: arn:aws:glue:{region}:{account}, snowflake://{account}, postgres://{host}:{port}.
    connections:
      "snowflake://my-account":
        platform-instance: my_instance
        env: PROD
      "arn:aws:glue:us-east-1:111111111111":
        platform-instance: my_glue_catalog
```

Without `connections`, a deployment spanning two accounts emits URNs for one of them and the lineage
for the other dangles against datasets the platform's own connector ingested under a different
instance.

##### Usage Examples

**Setting Environment and Orchestrator**

_Simple Configuration (Recommended):_

For most use cases, set `env` to configure both DataFlow and Datasets:

```bash
# Development environment - sets DataFlow cluster to "dev" and Dataset fabricType to DEV
DATAHUB_OPENLINEAGE_ENV=DEV
DATAHUB_OPENLINEAGE_ORCHESTRATOR=my-orchestrator

# Production environment - sets DataFlow cluster to "prod" and Dataset fabricType to PROD
DATAHUB_OPENLINEAGE_ENV=PROD
DATAHUB_OPENLINEAGE_ORCHESTRATOR=dagster

# Staging environment
DATAHUB_OPENLINEAGE_ENV=STG
DATAHUB_OPENLINEAGE_ORCHESTRATOR=custom-pipeline
```

_Advanced Configuration (Multi-Region/Complex Deployments):_

Override DataFlow cluster or Dataset environment independently:

```bash
# DataFlow in specific regional cluster, but datasets marked as generic PROD
DATAHUB_OPENLINEAGE_ENV=PROD
DATAHUB_OPENLINEAGE_PLATFORM_INSTANCE=prod-us-west-2  # DataFlow cluster override

# Test pipeline against DEV data (cross-environment testing)
DATAHUB_OPENLINEAGE_ENV=PROD                    # DataFlow cluster: prod
DATAHUB_OPENLINEAGE_COMMON_DATASET_ENV=DEV      # Dataset fabricType: DEV

# Blue-green deployment
DATAHUB_OPENLINEAGE_ENV=PROD
DATAHUB_OPENLINEAGE_PLATFORM_INSTANCE=prod-blue  # or prod-green
```

**Using Application Properties**

Alternatively, configure via `application.yml`:

```yaml
datahub:
  openlineage:
    env: PROD
    orchestrator: my-custom-orchestrator
    platform-instance: us-west-2
    capture-column-level-lineage: true
```

**Priority Order for Orchestrator Determination**

The orchestrator name is determined in the following priority order:

1. `DATAHUB_OPENLINEAGE_ORCHESTRATOR` environment variable (highest priority)
2. `processing_engine` facet in the OpenLineage event
3. Parsing the `producer` URL field with known patterns (Airflow, etc.)

#### Known Limitations

With Spark and Airflow we recommend using the Spark Lineage or DataHub's Airflow plugin for tighter integration with DataHub.

- **[PathSpec](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage/#configuring-hdfs-based-dataset-urns) Support**: While the REST endpoint supports OpenLineage messages, full [PathSpec](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage/#configuring-hdfs-based-dataset-urns)) support is not yet available in the OpenLineage endpoint but it is available in the DataHub Cloud Spark Plugin.

etc...

### 2. Spark Event Listener Plugin

DataHub's Spark Event Listener plugin enhances OpenLineage support by providing additional features such as PathSpec support, column-level lineage, and more.

#### How to Use

Follow the guides of the Spark Lineage plugin page for more information on how to set up the Spark Lineage plugin. The guide can be found [here](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)

## References

- [OpenLineage](https://openlineage.io/)
- [DataHub OpenAPI Guide](../api/openapi/openapi-usage-guide.md)
- [DataHub Spark Lineage Plugin](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)
