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
| `DATAHUB_OPENLINEAGE_USE_PATCH`                        | `datahub.openlineage.use-patch`                        | Boolean | `false` | Whether to use patch operations for lineage/incremental lineage                                                   |
| `DATAHUB_OPENLINEAGE_FILE_PARTITION_REGEXP_PATTERN`    | `datahub.openlineage.file-partition-regexp-pattern`    | String  | `null`  | Regular expression pattern for file partition detection                                                           |
| `DATAHUB_OPENLINEAGE_DOMAINS`                          | `datahub.openlineage.domains`                          | List    | `empty` | Comma-separated domain URNs (`urn:li:domain:<id>`) attached to the DataFlow and DataJob                           |

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

Spark in Microsoft Fabric (notebooks and Spark job definitions) can send runtime lineage to this
endpoint through the OpenLineage Spark listener Fabric bundles. Lakehouse tables live in OneLake and
show up in events as ABFS paths:

```text
abfss://<workspaceGUID>@onelake.dfs.fabric.microsoft.com/<itemGUID>/Tables/[<schema>/]<table>
abfss://<workspaceName>@onelake.dfs.fabric.microsoft.com/<itemName>.Lakehouse/Tables/[<schema>/]<table>
```

Use **Fabric Runtime 2.0**. It bundles `openlineage-spark_2.13` 1.40.1, which reports column-level
lineage for `MERGE INTO`, `CREATE OR REPLACE TABLE ... AS SELECT`, `INSERT OVERWRITE` and DataFrame
`saveAsTable`. Runtime 1.3 bundles 1.26.0, which reports column-level lineage for `MERGE INTO` only.

1. The listener is registered but disabled. Turn it on in the Fabric Environment attached to the
   notebooks (Spark properties):

   ```text
   spark.openlineage.disabled  false
   ```

2. Fabric pins the listener's transport to a file in the notebook's default Lakehouse
   (`/lakehouse/default/Files/Lineage/lineage_<timestamp>`, one event per line). An `http`
   transport set in the Environment or with `%%configure` is overridden, so post the file to this
   endpoint from the last cell of the notebook (or a pipeline step that runs after it):

   ```python
   import requests, time

   time.sleep(20)  # let the listener write the last statement's events
   location = spark.sparkContext.getConf().get("spark.openlineage.transport.location")
   token = notebookutils.credentials.getSecret("https://<vault>.vault.azure.net/", "<secret-name>")
   with open(location) as events:
       for event in filter(None, map(str.strip, events)):
           requests.post(
               "https://GMS_SERVER_HOST:GMS_PORT/openapi/openlineage/api/v1/lineage"
               "?fabricOneLake=true&fabricNotebookFlowNames=true",
               data=event,
               headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
               timeout=60,
           ).raise_for_status()
   ```

   A notebook that fails before this cell sends no lineage for that run.

3. The query string turns on the OneLake mapping and notebook naming for these events (see
   [Request options](#request-options) below). Add `&fabricOneLakeConvertUrnsToLowercase=true` if
   the Fabric OneLake source runs with `convert_urns_to_lowercase: true`.

By default, OneLake paths are handled like any other ABFS path: tables land on `abs` path URNs (or
on the Spark catalog symlink, for example `hive.<lakehouse>.<table>`). Pass `fabricOneLake=true`
with a request to map its OneLake **table** paths to the `fabric-onelake` platform instead. It uses the same name the
[Fabric OneLake source](https://docs.datahub.com/docs/generated/ingestion/sources/fabric-onelake)
uses, so runtime lineage attaches to the tables that source ingests:

```text
urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,<workspaceGUID>.<itemGUID>.<schema>.<table>,<env>)
```

- Schemas-disabled Lakehouses (`Tables/<table>`) use schema `dbo`, as the connector does.
- Workspace and item GUIDs are lowercased. Schema, table and column names (the field paths in
  column-level lineage) keep their case unless `fabricOneLakeConvertUrnsToLowercase` is set.
  Set it if the connector runs with `convert_urns_to_lowercase: true`, which lowercases all three.
- If the connector uses a `platform_instance`, pass `fabricOneLakePlatformInstance` with the same
  value. `common-dataset-platform-instance` is **not** applied to `fabric-onelake` URNs.
- Global (`onelake.dfs|blob.fabric.microsoft.com`), regional (`<region>-onelake...`),
  `[<region>-]api.onelake.fabric.microsoft.com` and workspace private-link hosts are recognized.
- A Delta table's location is more specific than its Spark catalog symlink, so the location wins.
  Without this, a symlink like `hive.<lakehouse>.<table>` would take over.
- The event's schema facet is not written to `fabric-onelake` datasets, even with
  `include-schema-metadata`. The Fabric OneLake source owns their schema, and the facet is Spark's
  read schema (a Delta `MERGE` scan reports only the join key and the `_metadata` pseudo-column).
- `_delta_log`, `key=value` partition folders and data files below a table are ignored. Other
  shapes under `Tables/` aren't mapped and stay `abs` (logged once as a warning).
- Paths outside `Tables/` (for example `Files/`) aren't tables and stay on the `abs` platform.
- The Fabric OneLake source ingests Lakehouse and Warehouse items only. A GUID path doesn't say
  which item type it points to, so `Tables/` paths of other items (for example mirrored
  databases) also map to `fabric-onelake` URNs that no ingested entity backs.
- Friendly-name paths carry no GUIDs. They stay `abs` unless you pass them in
  `fabricOneLakeItemIds`, for example
  `fabricOneLakeItemIds=Sales/bronze.Lakehouse=<wsGUID>/<itemGUID>` (URL-encoded in a query
  string). Names are matched case-insensitively and in decoded form (`My%20Workspace` matches
  `My Workspace`). Entries are comma-separated, so names containing commas can't be mapped. A
  malformed entry rejects the request. Unmapped friendly-name tables are logged once as a warning.
- When enabled, the mapping takes precedence over `path_spec_list` for OneLake `Tables/` paths.
- Enabling the mapping for a producer changes the URNs of OneLake tables that producer previously
  sent as `abs` or `hive` datasets. New lineage lands on the `fabric-onelake` URNs; the old entities and their
  lineage are not migrated. Soft-delete them if you no longer need them.

Verified end to end from a Fabric Runtime 2.0 notebook (events forwarded as above), and against
events from the `openlineage-spark_2.12` 1.26.0 listener of Runtime 1.3:

- **Runtime 2.0 (1.40.1):** every write reports its inputs and output. The column lineage is on the
  `overwrite_by_expression_exec_v1` event for CTAS, `INSERT OVERWRITE` and `saveAsTable`; the
  `atomic_replace_table_as_select` and `command_result` events repeat the table-level lineage.
- **Runtime 1.3 (1.26.0):** `MERGE INTO` events list no inputs (the source table appears only in the
  column lineage), and `saveAsTable` / CTAS events carry table-level lineage only.

**Notebook names.** OpenLineage Spark names jobs `<spark.app.name>.<action>`, and in Fabric the app
name is `<notebook>_<session GUID>`, so every notebook run creates a new DataFlow and new DataJobs.
Pass `fabricNotebookFlowNames=true` to key the DataFlow on the notebook item instead: the flow id is the notebook item GUID (`trident.artifact.id`), its name is the notebook name
(`trident.artifact.name`), and job names drop the session prefix (for example
`execute_merge_into_command.customers`), so all runs of a notebook land on the same entities. A
configured `pipeline-name` still wins. Events without the `trident.artifact.*` Spark properties (the
application-level start/end events, which carry no lineage) keep the session name. It is opt-in because it renames the DataFlow and DataJob URNs of existing notebook lineage. The
[Fabric Data Factory source](https://docs.datahub.com/docs/generated/ingestion/sources/fabric-data-factory)
ingests a pipeline's Notebook activity as its own DataJob; it isn't linked to these flows.

##### Request options

The Fabric options are set per request, so each producer decides how its own events are mapped;
there is no GMS setting and no restart. Pass them as query parameters or headers (a query parameter
wins over the header of the same option). With none set, the endpoint maps events as before.

| Query parameter                       | Header                                               | Value                                                                               |
| ------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `fabricOneLake`                       | `X-DataHub-Fabric-OneLake`                           | `true` / `false`: map OneLake table paths to `fabric-onelake`                       |
| `fabricOneLakeConvertUrnsToLowercase` | `X-DataHub-Fabric-OneLake-Convert-Urns-To-Lowercase` | `true` / `false`: match the source's `convert_urns_to_lowercase`                    |
| `fabricOneLakePlatformInstance`       | `X-DataHub-Fabric-OneLake-Platform-Instance`         | The source's `platform_instance`                                                    |
| `fabricOneLakeItemIds`                | `X-DataHub-Fabric-OneLake-Item-Ids`                  | `<workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/<itemGUID>`, comma-separated |
| `fabricNotebookFlowNames`             | `X-DataHub-Fabric-Notebook-Flow-Names`               | `true` / `false`: one DataFlow per notebook                                         |

A malformed value (not `true`/`false`, empty, or an item-id entry that doesn't parse), or a OneLake
option without `fabricOneLake=true`, is rejected with `400 Bad Request`. Producers using the
OpenLineage `http` transport can send the headers with `spark.openlineage.transport.headers.<name>`.

The same options exist in the [Spark agent](https://docs.datahub.com/docs/metadata-integration/java/acryl-spark-lineage#configuration-instructions-microsoft-fabric)
as `spark.datahub.metadata.dataset.fabricOneLake.*` and `spark.datahub.metadata.fabricNotebookFlowNames`.

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
