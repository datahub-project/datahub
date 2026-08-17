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

Include the OpenLineage event object in the request body in JSON format. Successful asynchronous ingestion returns `202 Accepted`.

Example:

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
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
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json",
          "name": "postgres://workshop-db:None",
          "uri": "workshop-db"
        }
      }
    }
  ]
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

#### Spec compliance

The endpoint contract, event and facet mappings, custom-facet compatibility rules, validation behavior, and ingestion guarantees are defined in the [OpenLineage REST endpoint specification compliance RFC](../rfcs/active/17034-openlineage-spec-compliance.md).

#### How to modify configurations

To modify the configurations for the OpenLineage REST endpoint, you can change it using environment variables. The following configurations are available:

##### DataHub OpenLineage Configuration

This document describes all available configuration options for the DataHub OpenLineage integration, including environment variables, application properties, and their usage.

##### Configuration Overview

The DataHub OpenLineage integration can be configured using environment variables, application properties files (`application.yml` or `application.properties`), or JVM system properties. All configuration options are prefixed with `datahub.openlineage`.

##### Environment Variables

| Environment Variable                                   | Property                                               | Type    | Default | Description                                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------ | ------- | ------- | ------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_OPENLINEAGE_ENV`                              | `datahub.openlineage.env`                              | String  | `null`  | When set, environment for the DataFlow cluster and Dataset fabricType; Dataset fabric defaults to `PROD` when unset |
| `DATAHUB_OPENLINEAGE_ORCHESTRATOR`                     | `datahub.openlineage.orchestrator`                     | String  | `null`  | DataFlow platform override with precedence over OpenLineage facets                                                  |
| `DATAHUB_OPENLINEAGE_PIPELINE_NAME`                    | `datahub.openlineage.pipeline-name`                    | String  | `null`  | Override the DataFlow name for all ingested OpenLineage jobs                                                        |
| `DATAHUB_OPENLINEAGE_PLATFORM_INSTANCE`                | `datahub.openlineage.platform-instance`                | String  | `null`  | Override DataFlow cluster (defaults to env if not specified)                                                        |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_ENV`               | `datahub.openlineage.common-dataset-env`               | String  | `null`  | Override Dataset environment independently from DataFlow cluster                                                    |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_PLATFORM_INSTANCE` | `datahub.openlineage.common-dataset-platform-instance` | String  | `null`  | Common platform instance for dataset entities                                                                       |
| `DATAHUB_OPENLINEAGE_MATERIALIZE_DATASET`              | `datahub.openlineage.materialize-dataset`              | Boolean | `true`  | Whether to materialize dataset entities                                                                             |
| `DATAHUB_OPENLINEAGE_INCLUDE_SCHEMA_METADATA`          | `datahub.openlineage.include-schema-metadata`          | Boolean | `true`  | Whether to include schema metadata in lineage                                                                       |
| `DATAHUB_OPENLINEAGE_CAPTURE_COLUMN_LEVEL_LINEAGE`     | `datahub.openlineage.capture-column-level-lineage`     | Boolean | `true`  | Whether to capture column-level lineage information                                                                 |
| `DATAHUB_OPENLINEAGE_USE_PATCH`                        | `datahub.openlineage.use-patch`                        | Boolean | `false` | Whether to use patch operations for lineage/incremental lineage                                                     |
| `DATAHUB_OPENLINEAGE_FILE_PARTITION_REGEXP_PATTERN`    | `datahub.openlineage.file-partition-regexp-pattern`    | String  | `null`  | Regular expression pattern for file partition detection                                                             |

> **Valid `env` values**: `PROD`, `DEV`, `TEST`, `QA`, `UAT`, `EI`, `PRE`, `STG`, `NON_PROD`, `CORP`, `RVW`, `PRD`, `TST`, `SIT`, `SBX`, `SANDBOX`, `CERT`
>
> **How `env` works**:
>
> - **When set**, `env` sets both the DataFlow cluster and Dataset fabricType for simplicity
> - **When unset**, Dataset fabricType defaults to `PROD` and the DataFlow cluster remains unset
> - **For advanced scenarios**, use `platform-instance` to override the DataFlow cluster or `common-dataset-env` to override the Dataset environment independently
>
> **Note**: The `env` property naming matches DataHub SDK conventions where `env` is the user-facing parameter that internally maps to the URN `cluster` field.

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
    pipeline-name: warehouse-loads
    platform-instance: us-west-2
    capture-column-level-lineage: true
```

**Priority Order for Orchestrator Determination**

The orchestrator name is determined in the following priority order:

1. `DATAHUB_OPENLINEAGE_ORCHESTRATOR`
2. `JobTypeJobFacet.integration`
3. `ProcessingEngineRunFacet.name`
4. An explicit mapping for a known producer URI
5. The bootstrapped DataHub platform `unknown`

#### DataHub-native facet storage

OpenLineage job documentation, ownership, and standard job/run tags are stored on DataJob. OpenLineage has no DataFlow entity; DataHub infers DataFlow only to group DataJobs. OpenLineage source text is stored in `DataTransform.queryStatement` with an unknown query language, while the declared source language is preserved in DataJob custom properties. Output bytes and file count are stored as Operation custom properties because the native aspect only has a typed affected-row field. The latest OpenLineage dataset version is stored in `datasetProperties.customProperties["openlineage.datasetVersion"]`; it does not create DataHub version history.

For dotted job names, the prefix before the first dot identifies the DataFlow and the complete job name identifies the DataJob task. A job name without a dot is used for both. DatasetEvents always materialize a dataset key and status anchor. Filesystem datasets use the URI scheme as the DataHub platform and the URI authority/path as the dataset name; legacy Marquez events that place the filesystem URI in `name` receive the same normalization.

#### Known Limitations

With Spark and Airflow we recommend using the Spark Lineage or DataHub's Airflow plugin for tighter integration with DataHub.

- **PathSpec support**: The REST endpoint supports OpenLineage messages, but full PathSpec handling remains available through the DataHub Spark lineage plugin.
- **Dataset identity collisions**: OpenLineage identifies datasets by namespace and name; the event producer is not part of dataset identity. Events from independent producers that resolve to the same DataHub platform, dataset name, environment, and platform instance therefore update the same Dataset. Use distinct OpenLineage namespaces or DataHub platform instances when the datasets are not the same logical object.
- **Dangling DataFlow platform references**: DataHub does not verify that an orchestrator inferred from OpenLineage facets or supplied through `DATAHUB_OPENLINEAGE_ORCHESTRATOR` is a registered `dataPlatform` entity. Register custom orchestrator platforms before ingestion or configure the endpoint to use an existing platform.
- **Environment variables**: `EnvironmentVariablesRunFacet` values are stored as DataProcessInstance custom properties, with common sensitive names such as tokens, passwords, keys, credentials, and auth values redacted.
- **Custom facets**: Raw OpenLineage payloads are not retained. Unknown facet objects and unsupported producer/schema combinations remain opaque and are not mapped. Supported historical Airflow and Spark mappings are listed in the [OpenLineage specification compliance RFC](../rfcs/active/17034-openlineage-spec-compliance.md#custom-facet-compatibility).
- **Job dependencies**: `JobDependenciesRunFacet` upstream jobs are mapped to DataJob input-job edges using the same namespace, orchestrator, flow, and task identity rules as other OpenLineage jobs. Trigger and downstream dependency details are retained as DataJob custom properties; downstream entries do not create reverse lineage edges.
- **Dataset hierarchy**: `HierarchyDatasetFacet` levels are interpreted in their official highest-to-lowest order. DataHub materializes each non-terminal level as a Container with key, properties, status, and parent links, then attaches the Dataset to the nearest Container. The terminal level represents the Dataset and is not duplicated as a Container.

### 2. Spark Event Listener Plugin

DataHub's Spark Event Listener plugin enhances OpenLineage support by providing additional features such as PathSpec support, column-level lineage, and more.

#### How to Use

Follow the guides of the Spark Lineage plugin page for more information on how to set up the Spark Lineage plugin. The guide can be found [here](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)

## References

- [OpenLineage](https://openlineage.io/)
- [DataHub OpenAPI Guide](../api/openapi/openapi-usage-guide.md)
- [DataHub Spark Lineage Plugin](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage)
