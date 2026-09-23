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

| Environment Variable                                           | Property                                                       | Type    | Default | Description                                                                                                                     |
| -------------------------------------------------------------- | -------------------------------------------------------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_OPENLINEAGE_ENV`                                      | `datahub.openlineage.env`                                      | String  | `PROD`  | Environment for DataFlow cluster and Dataset fabricType (see valid values below)                                                |
| `DATAHUB_OPENLINEAGE_ORCHESTRATOR`                             | `datahub.openlineage.orchestrator`                             | String  | `null`  | Orchestrator name for DataFlow entities. When set, takes precedence over processing_engine facet and producer URL               |
| `DATAHUB_OPENLINEAGE_PLATFORM_INSTANCE`                        | `datahub.openlineage.platform-instance`                        | String  | `null`  | Override DataFlow cluster (defaults to env if not specified)                                                                    |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_ENV`                       | `datahub.openlineage.common-dataset-env`                       | String  | `null`  | Override Dataset environment independently from DataFlow cluster                                                                |
| `DATAHUB_OPENLINEAGE_COMMON_DATASET_PLATFORM_INSTANCE`         | `datahub.openlineage.common-dataset-platform-instance`         | String  | `null`  | Common platform instance for dataset entities                                                                                   |
| `DATAHUB_OPENLINEAGE_MATERIALIZE_DATASET`                      | `datahub.openlineage.materialize-dataset`                      | Boolean | `true`  | Whether to materialize dataset entities                                                                                         |
| `DATAHUB_OPENLINEAGE_INCLUDE_SCHEMA_METADATA`                  | `datahub.openlineage.include-schema-metadata`                  | Boolean | `true`  | Whether to include schema metadata in lineage                                                                                   |
| `DATAHUB_OPENLINEAGE_CAPTURE_COLUMN_LEVEL_LINEAGE`             | `datahub.openlineage.capture-column-level-lineage`             | Boolean | `true`  | Whether to capture column-level lineage information                                                                             |
| `DATAHUB_OPENLINEAGE_USE_PATCH`                                | `datahub.openlineage.use-patch`                                | Boolean | `false` | Whether to use patch operations for lineage/incremental lineage                                                                 |
| `DATAHUB_OPENLINEAGE_FILE_PARTITION_REGEXP_PATTERN`            | `datahub.openlineage.file-partition-regexp-pattern`            | String  | `null`  | Regular expression pattern for file partition detection                                                                         |
| `DATAHUB_OPENLINEAGE_DOMAINS`                                  | `datahub.openlineage.domains`                                  | List    | `empty` | Comma-separated domain URNs (`urn:li:domain:<id>`) attached to the DataFlow and DataJob                                         |
| `DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_ENABLED`                   | `datahub.openlineage.fabric-onelake-enabled`                   | Boolean | `false` | Map Fabric OneLake table paths to `fabric-onelake` URNs (opt-in; see [Microsoft Fabric (OneLake)](#microsoft-fabric-onelake))   |
| `DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_CONVERT_URNS_TO_LOWERCASE` | `datahub.openlineage.fabric-onelake-convert-urns-to-lowercase` | Boolean | `false` | Lowercase schema and table in `fabric-onelake` URNs; match the connector's `convert_urns_to_lowercase`                          |
| `DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_PLATFORM_INSTANCE`         | `datahub.openlineage.fabric-onelake-platform-instance`         | String  | `null`  | Platform instance for `fabric-onelake` URNs; match the connector's `platform_instance`                                          |
| `DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_ITEM_IDS`                  | `datahub.openlineage.fabric-onelake-item-ids`                  | String  | `null`  | Comma-separated `<workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/<itemGUID>` entries for friendly-name OneLake paths      |

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

#### Microsoft Fabric (OneLake)

Spark in Microsoft Fabric (notebooks and Spark job definitions) can send OpenLineage events to this
endpoint with the standard OpenLineage Spark listener and its `http` transport. Lakehouse tables live
in OneLake and show up in events as ABFS paths:

```text
abfss://<workspaceGUID>@onelake.dfs.fabric.microsoft.com/<itemGUID>/Tables/[<schema>/]<table>
abfss://<workspaceName>@onelake.dfs.fabric.microsoft.com/<itemName>.Lakehouse/Tables/[<schema>/]<table>
```

For example, add these Spark properties to a Fabric Environment:

```text
spark.extraListeners                     io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type         http
spark.openlineage.transport.url          https://GMS_SERVER_HOST:GMS_PORT/openapi/openlineage/
spark.openlineage.transport.endpoint     api/v1/lineage
spark.openlineage.transport.auth.type    api_key
spark.openlineage.transport.auth.apiKey  <datahub-access-token>
spark.openlineage.namespace              fabric
```

By default, OneLake paths are handled like any other ABFS path: tables land on `abs` path URNs (or
on the Spark catalog symlink, for example `hive.<lakehouse>.<table>`). Set
`DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_ENABLED=true` to map OneLake **table** paths to the
`fabric-onelake` platform instead. It uses the same name the
[Fabric OneLake source](https://docs.datahub.com/docs/generated/ingestion/sources/fabric-onelake)
uses, so runtime lineage attaches to the tables that source ingests:

```text
urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,<workspaceGUID>.<itemGUID>.<schema>.<table>,<env>)
```

- Schemas-disabled Lakehouses (`Tables/<table>`) use schema `dbo`, as the connector does.
- Workspace and item GUIDs are lowercased. Schema and table keep their case unless
  `fabric-onelake-convert-urns-to-lowercase` is set. Set it if the connector runs with
  `convert_urns_to_lowercase: true`.
- If the connector uses a `platform_instance`, set `fabric-onelake-platform-instance` to the same
  value. `common-dataset-platform-instance` is **not** applied to `fabric-onelake` URNs.
- Global (`onelake.dfs|blob.fabric.microsoft.com`), regional (`<region>-onelake...`),
  `[<region>-]api.onelake.fabric.microsoft.com` and workspace private-link hosts are recognized.
- A Delta table's location is more specific than its Spark catalog symlink, so the location wins.
  Without this, a symlink like `hive.<lakehouse>.<table>` would take over.
- `_delta_log`, `key=value` partition folders and data files below a table are ignored. Other
  shapes under `Tables/` aren't mapped and stay `abs` (logged once as a warning).
- Paths outside `Tables/` (for example `Files/`) aren't tables and stay on the `abs` platform.
- Friendly-name paths carry no GUIDs. They stay `abs` unless you add them to
  `fabric-onelake-item-ids`, for example
  `DATAHUB_OPENLINEAGE_FABRIC_ONELAKE_ITEM_IDS=Sales/bronze.Lakehouse=<wsGUID>/<itemGUID>`. Names
  are matched case-insensitively and in decoded form (`My%20Workspace` matches `My Workspace`).
  Entries are comma-separated, so names containing commas can't be mapped. Malformed entries are
  skipped with a warning at startup. Unmapped friendly-name tables are logged once as a warning.
- When enabled, the mapping takes precedence over `path_spec_list` for OneLake `Tables/` paths.
- Enabling the mapping changes the URNs of OneLake tables that were previously captured as `abs`
  or `hive` datasets. New lineage lands on the `fabric-onelake` URNs; the old entities and their
  lineage are not migrated. Soft-delete them if you no longer need them.

The same options exist in the [Spark agent](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage#configuration-instructions-microsoft-fabric)
as `spark.datahub.metadata.dataset.fabricOneLake.*`.

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
