


# dlt

## Overview

[dlt (data load tool)](https://dlthub.com) is an open-source Python ELT library for building data pipelines that load data from REST APIs, databases, and other sources into destinations like Postgres, BigQuery, Snowflake, and DuckDB.

The DataHub integration for dlt reads pipeline metadata from dlt's local state directory (`~/.dlt/pipelines/`) and emits DataFlow, DataJob, and lineage entities to DataHub. The connector also supports per-run history (DataProcessInstance) when the dlt package is installed and destination credentials are available, plus stateful deletion detection.

## Concept Mapping

| dlt                              | DataHub                                                                                                |
| -------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `Pipeline` (`pipeline_name`)     | [DataFlow](/docs/generated/metamodel/entities/dataflow/)                       |
| `Resource` / destination `Table` | [DataJob](/docs/generated/metamodel/entities/datajob/)                         |
| Destination table                | [Dataset](/docs/generated/metamodel/entities/dataset/) (DataJob output)        |
| User-configured upstream         | [Dataset](/docs/generated/metamodel/entities/dataset/) (DataJob input)         |
| `_dlt_loads` row                 | [DataProcessInstance](/docs/generated/metamodel/entities/dataprocessinstance/) |

Destination tables are mapped to Dataset URNs that match the destination platform's own DataHub connector (Postgres, BigQuery, etc.), enabling lineage stitching when both connectors run.


## Module `dlt`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| Extract Ownership | ❌ | Emitted when dlt pipeline state contains owner information. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Emits outlet lineage from dlt DataJobs to destination Dataset URNs. Configure destination_platform_map to match your destination connector's env/instance. |

### Overview

The `dlt` module ingests pipeline metadata from [dlt (data load tool)](https://dlthub.com) into DataHub. It reads dlt's local state directory directly — no live connection to dlt or the destination is required for basic metadata extraction. If the dlt Python package is installed, the connector uses the SDK for richer metadata; otherwise it falls back to parsing the YAML state files directly.

#### What is ingested

- **DataFlow** — one per dlt pipeline (`pipeline_name`)
- **DataJob** — one per destination table (including auto-unnested child tables like `orders__items`)
- **Outlet lineage** — DataJob → destination Dataset URNs (Postgres, BigQuery, Snowflake, etc.)
- **Inlet lineage** — user-configured upstream Dataset URNs (dlt does not record source connection info)
- **Column-level lineage** — for direct-copy pipelines with exactly one inlet and one outlet
- **DataProcessInstance** — per-run history from `_dlt_loads` (opt-in)

#### How dlt stores metadata

dlt writes pipeline state to a local directory after each `pipeline.run()` call:

```
~/.dlt/pipelines/
  <pipeline_name>/
    schemas/
      <schema_name>.schema.yaml   # Table definitions with columns and types
    state.json                    # Destination type, dataset name, pipeline state
```

### Prerequisites

- dlt pipeline(s) must have been run at least once (state files are created automatically)
- The `pipelines_dir` must be accessible from where DataHub ingestion runs
- For run history: the dlt package must be installed and destination credentials must be configured (see Capabilities → Run History below)

#### Where to find your `pipelines_dir`

##### Local / Quickstart

dlt's default location. Works out of the box:

```yaml
pipelines_dir: "~/.dlt/pipelines"
```

##### CI/CD (GitHub Actions, Airflow, Jenkins)

dlt runs in one job and DataHub ingestion runs in another. Both must use the same path or shared storage:

```yaml
pipelines_dir: "/data/dlt-pipelines"
```

Many dlt users already set a `PIPELINES_DIR` environment variable:

```yaml
pipelines_dir: "${PIPELINES_DIR:-~/.dlt/pipelines}"
```

##### Kubernetes / Docker

dlt runs in one pod and DataHub in another. Mount the same PersistentVolumeClaim in both pods:

```yaml
pipelines_dir: "/mnt/dlt-pipelines"
```

#### Required permissions

The connector reads local files only — no network permissions are needed for basic metadata extraction.

| Feature                                        | Requirement                                                              |
| ---------------------------------------------- | ------------------------------------------------------------------------ |
| Pipeline metadata (DataFlow, DataJob, lineage) | Filesystem read access to `pipelines_dir`                                |
| Run history (`_dlt_loads`)                     | dlt package installed + destination credentials in `~/.dlt/secrets.toml` |


### Install the Plugin
```shell
pip install 'acryl-datahub[dlt]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# DataHub dlt (data load tool) connector recipe.
# Reads pipeline metadata from dlt's local state directory and emits
# DataFlow, DataJob, and lineage metadata to DataHub.
# See: https://datahubproject.io/docs/generated/ingestion/sources/dlt

source:
  type: dlt
  config:
    # Path to the dlt pipelines directory (~/.dlt/pipelines by default).
    # Override for CI/CD or containerized environments — must point to the
    # same directory that dlt writes to.
    pipelines_dir: "~/.dlt/pipelines"

    # Filter pipelines by name. Matched against the pipeline_name passed to dlt.pipeline().
    pipeline_pattern:
      allow:
        - ".*"
      # deny:
      #   - "^test_.*"

    # Emit outlet lineage from DataJobs to destination Dataset URNs.
    include_lineage: true

    # Maps dlt destination type names to DataHub platform config for lineage stitching.
    # Must exactly match the env/platform_instance used by your destination connector.
    # The database field is required for SQL destinations (Postgres, Redshift, etc.)
    # that use 3-part URNs (database.schema.table) — dlt only stores the schema name.
    destination_platform_map:
      postgres:
        database: my_database
        platform_instance: null
        env: PROD

      # bigquery:
      #   platform_instance: "my-gcp-project"
      #   env: PROD

      # snowflake:
      #   platform_instance: "my-account"
      #   env: PROD

    # Optional: manually specify upstream Dataset URNs.
    # dlt does not store source connection info, so inlet lineage must be configured here.
    # Use source_dataset_urns for pipeline-level inlets (e.g. REST API sources):
    # source_dataset_urns:
    #   my_pipeline:
    #     - "urn:li:dataset:(urn:li:dataPlatform:salesforce,contacts,PROD)"
    #
    # Use source_table_dataset_urns for table-level inlets (e.g. sql_database sources):
    # source_table_dataset_urns:
    #   my_pipeline:
    #     my_table:
    #       - "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)"

    # Query _dlt_loads and emit DataProcessInstance run history. Disabled by default.
    # Requires dlt package + destination credentials in ~/.dlt/secrets.toml or env vars.
    include_run_history: false

    # Time window for run history (only used when include_run_history: true).
    run_history_config:
      start_time: "-7 days"
      # end_time: "now"

    # Remove DataFlow/DataJob entities when pipelines are deleted from pipelines_dir.
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

    # Optional: distinguish multiple independent dlt installations in DataHub.
    # platform_instance: "my-dlt-instance"

    env: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit outlet lineage from dlt DataJobs to destination Dataset URNs. Constructs URNs using destination_platform_map. For lineage to stitch in DataHub, destination_platform_map env/instance must match the destination connector's configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_run_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to query the destination's _dlt_loads table and emit DataProcessInstance run history. Requires the destination (e.g. Postgres, BigQuery) to be accessible and dlt credentials to be configured in ~/.dlt/secrets.toml. Disabled by default to avoid requiring destination access. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">pipelines_dir</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to the dlt pipelines directory. dlt stores all pipeline state, schemas, and load packages here. Defaults to ~/.dlt/pipelines/ which is dlt's standard location. Override when pipelines are stored in a non-default location (e.g. /data/dlt-pipelines in a Docker environment). <div className="default-line default-line-with-docs">Default: <span className="default-value">~/.dlt/pipelines</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">destination_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,DestinationPlatformConfig)</span></div> | Per-destination URN construction config. <br />  <br /> Maps a dlt destination type (e.g. "postgres", "bigquery") to a DataHub <br /> platform instance and environment so that outlet Dataset URNs produced by <br /> the dlt connector match those emitted by the destination's own DataHub <br /> connector — enabling lineage stitching. <br />  <br /> Example: <br />     postgres: <br />       platform_instance: null   # local dev, no instance <br />       env: "DEV" <br />     bigquery: <br />       platform_instance: "my-gcp-project" <br />       env: "PROD"  |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database name prefix for outlet Dataset URN construction. Required for SQL destinations where the DataHub connector uses a 3-part name (database.schema.table), e.g. Postgres uses 'chess.chess_data.players_games'. dlt only stores the schema (dataset_name), not the database name, so this must be supplied manually. Leave null for destinations like BigQuery where the project is captured in platform_instance instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform instance for this destination. Must exactly match the platform_instance used when ingesting the destination platform (e.g. your Snowflake or BigQuery connector). Leave null if no platform_instance was configured for that connector. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub environment for this destination (PROD, DEV, STAGING, etc.). Must match the env used by the destination platform's own connector. One of ['CERT', 'CORP', 'DEV', 'EI', 'NON_PROD', 'PRD', 'PRE', 'PROD', 'QA', 'RVW', 'SANDBOX', 'SBX', 'SIT', 'STG', 'TEST', 'TST', 'UAT']. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">pipeline_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">pipeline_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">run_history_config</span></div> <div className="type-name-line"><span className="type-name">DltRunHistoryConfig</span></div> | Time window config for querying _dlt_loads run history. <br />  <br /> Extends BaseTimeWindowConfig which provides start_time, end_time, and <br /> bucket_duration — the DataHub standard for all time-windowed queries. <br /> Use include_run_history on DltSourceConfig to enable/disable run history.  |
| <div className="path-line"><span className="path-prefix">run_history_config.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">run_history_config.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-prefix">run_history_config.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">source_dataset_urns</span></div> <div className="type-name-line"><span className="type-name">map(str,array)</span></div> |   |
| <div className="path-line"><span className="path-prefix">source_dataset_urns.`key`.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">source_table_dataset_urns</span></div> <div className="type-name-line"><span className="type-name">map(str,map)</span></div> |   |
| <div className="path-line"><span className="path-prefix">source_table_dataset_urns.`key`.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration. When enabled, automatically removes DataFlow/DataJob entities from DataHub if the corresponding dlt pipeline is deleted or no longer found in pipelines_dir. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "DestinationPlatformConfig": {
      "additionalProperties": false,
      "description": "Per-destination URN construction config.\n\nMaps a dlt destination type (e.g. \"postgres\", \"bigquery\") to a DataHub\nplatform instance and environment so that outlet Dataset URNs produced by\nthe dlt connector match those emitted by the destination's own DataHub\nconnector \u2014 enabling lineage stitching.\n\nExample:\n    postgres:\n      platform_instance: null   # local dev, no instance\n      env: \"DEV\"\n    bigquery:\n      platform_instance: \"my-gcp-project\"\n      env: \"PROD\"",
      "properties": {
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Database name prefix for outlet Dataset URN construction. Required for SQL destinations where the DataHub connector uses a 3-part name (database.schema.table), e.g. Postgres uses 'chess.chess_data.players_games'. dlt only stores the schema (dataset_name), not the database name, so this must be supplied manually. Leave null for destinations like BigQuery where the project is captured in platform_instance instead.",
          "title": "Database"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform instance for this destination. Must exactly match the platform_instance used when ingesting the destination platform (e.g. your Snowflake or BigQuery connector). Leave null if no platform_instance was configured for that connector.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "DataHub environment for this destination (PROD, DEV, STAGING, etc.). Must match the env used by the destination platform's own connector. One of ['CERT', 'CORP', 'DEV', 'EI', 'NON_PROD', 'PRD', 'PRE', 'PROD', 'QA', 'RVW', 'SANDBOX', 'SBX', 'SIT', 'STG', 'TEST', 'TST', 'UAT'].",
          "title": "Env",
          "type": "string"
        }
      },
      "title": "DestinationPlatformConfig",
      "type": "object"
    },
    "DltRunHistoryConfig": {
      "additionalProperties": false,
      "description": "Time window config for querying _dlt_loads run history.\n\nExtends BaseTimeWindowConfig which provides start_time, end_time, and\nbucket_duration \u2014 the DataHub standard for all time-windowed queries.\nUse include_run_history on DltSourceConfig to enable/disable run history.",
      "properties": {
        "bucket_duration": {
          "$ref": "#/$defs/BucketDuration",
          "default": "DAY",
          "description": "Size of the time window to aggregate usage stats."
        },
        "end_time": {
          "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
          "format": "date-time",
          "title": "End Time",
          "type": "string"
        },
        "start_time": {
          "default": null,
          "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
          "format": "date-time",
          "title": "Start Time",
          "type": "string"
        }
      },
      "title": "DltRunHistoryConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for the dlt DataHub source connector.\n\nReads pipeline metadata from dlt's local state directory\n(~/.dlt/pipelines/ by default) and emits DataFlow, DataJob, and lineage\nmetadata to DataHub.",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration. When enabled, automatically removes DataFlow/DataJob entities from DataHub if the corresponding dlt pipeline is deleted or no longer found in pipelines_dir."
    },
    "pipelines_dir": {
      "default": "~/.dlt/pipelines",
      "description": "Path to the dlt pipelines directory. dlt stores all pipeline state, schemas, and load packages here. Defaults to ~/.dlt/pipelines/ which is dlt's standard location. Override when pipelines are stored in a non-default location (e.g. /data/dlt-pipelines in a Docker environment).",
      "title": "Pipelines Dir",
      "type": "string"
    },
    "pipeline_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter pipelines by name. Matched against pipeline_name (the value passed to dlt.pipeline()). Example: allow ['^prod_.*'] to only ingest production pipelines."
    },
    "include_run_history": {
      "default": false,
      "description": "Whether to query the destination's _dlt_loads table and emit DataProcessInstance run history. Requires the destination (e.g. Postgres, BigQuery) to be accessible and dlt credentials to be configured in ~/.dlt/secrets.toml. Disabled by default to avoid requiring destination access.",
      "title": "Include Run History",
      "type": "boolean"
    },
    "run_history_config": {
      "$ref": "#/$defs/DltRunHistoryConfig",
      "description": "Time window for run history extraction. Uses standard DataHub BaseTimeWindowConfig \u2014 supports relative times like '-7 days' or absolute ISO timestamps."
    },
    "include_lineage": {
      "default": true,
      "description": "Whether to emit outlet lineage from dlt DataJobs to destination Dataset URNs. Constructs URNs using destination_platform_map. For lineage to stitch in DataHub, destination_platform_map env/instance must match the destination connector's configuration.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "destination_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/DestinationPlatformConfig"
      },
      "description": "Maps dlt destination type names to DataHub platform configuration. Used to construct correct Dataset URNs for lineage. The destination type is the value passed to dlt.pipeline(destination='...'). Example:\n  postgres:\n    platform_instance: null\n    env: DEV\n  bigquery:\n    platform_instance: my-gcp-project\n    env: PROD",
      "title": "Destination Platform Map",
      "type": "object"
    },
    "source_dataset_urns": {
      "additionalProperties": {
        "items": {
          "type": "string"
        },
        "type": "array"
      },
      "description": "Optional: manually specify inlet (upstream) Dataset URNs per pipeline. All listed URNs are applied as inlets to every DataJob in the pipeline. Use for REST API sources where every task shares the same upstream source. For SQL sources where each task reads from exactly one table, use source_table_dataset_urns instead. Key is the pipeline_name; value is a list of Dataset URN strings. Example:\n  crm_sync:\n    - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod.crm.customers,PROD)'",
      "title": "Source Dataset Urns",
      "type": "object"
    },
    "source_table_dataset_urns": {
      "additionalProperties": {
        "additionalProperties": {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        "type": "object"
      },
      "description": "Optional: manually specify inlet Dataset URNs per pipeline per table. Use for sql_database sources where each DataJob reads from exactly one source table and 1:1 lineage is desired. Outer key is pipeline_name; inner key is table_name; value is a list of URNs. Example:\n  my_pipeline:\n    my_table:\n      - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)'\n    other_table:\n      - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.other_table,PROD)'",
      "title": "Source Table Dataset Urns",
      "type": "object"
    }
  },
  "title": "DltSourceConfig",
  "type": "object"
}
```





### Capabilities

| Capability                        | Status                                   | Notes                                                                          |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------ |
| DataFlow / DataJob                | ✅ Always                                | One DataFlow per pipeline, one DataJob per destination table                   |
| Outlet lineage                    | ✅ Always (when `include_lineage: true`) | Requires `destination_platform_map` to match your destination connector        |
| Inlet lineage                     | ✅ User-configured                       | dlt does not store source identity; configure via `source_dataset_urns`        |
| Column-level lineage              | ✅ Partial                               | Only for tables with exactly one inlet and one outlet (unambiguous 1:1 copy)   |
| Run history (DataProcessInstance) | ⚙️ Opt-in                                | Requires `include_run_history: true` + dlt installed + destination credentials |
| Deletion detection                | ✅ Via stateful ingestion                | Removes DataFlow/DataJob when pipeline is deleted from `pipelines_dir`         |
| Ownership                         | ❌ Not supported                         | dlt state does not contain owner information                                   |

#### Modeling Notes — Why one DataJob per destination table?

A single `@dlt.resource` can produce more than one destination table when it returns nested data. dlt unnests JSON arrays into child tables using a double-underscore convention (for example `orders` plus `orders__items`). The connector emits **one DataJob per destination table** rather than one DataJob per `@dlt.resource`, so:

- Each destination table has a clean 1:1 outlet lineage entry (DataJob → Dataset URN on the destination).
- Column-level lineage stays at table granularity, which downstream lineage queries already expect.
- Browsing the dlt pipeline in DataHub shows every loaded table, not just the parent resource.

The trade-off is that the "this resource produced these tables" abstraction is not directly visible. To preserve that link:

- Each child-table DataJob carries a `parent_table` custom property pointing to its parent (for example, `parent_table: orders` on `orders__items`).
- All tables produced by the same resource share the same `resource_name` custom property.

The two are siblings in DataHub's lineage graph (both come from the same source rows at load time, not parent → child), so no synthetic upstream/downstream lineage is added between them — that would misrepresent the actual data flow.

#### Lineage Stitching

For outlet lineage to connect to your destination's Dataset URNs, `destination_platform_map` must match the environment and platform instance used by your destination connector.

If your Postgres connector uses `env: PROD` and no `platform_instance`:

```yaml
destination_platform_map:
  postgres:
    env: PROD
    platform_instance: null
    database: my_database # required for 3-part URN: database.schema.table
```

**Why `database` is needed for SQL destinations**: dlt stores the schema name (`dataset_name`) but not the database name. Postgres URNs in DataHub use a 3-part format (`database.schema.table`). Supply `database` to match those URNs.

Cloud warehouses (BigQuery, Snowflake) use the project or account as `platform_instance` instead:

```yaml
destination_platform_map:
  bigquery:
    platform_instance: "my-gcp-project"
    env: PROD
  snowflake:
    platform_instance: "my-account"
    env: PROD
```

#### Inlet Lineage (Upstream Sources)

dlt does not record where data came from — only where it went. To enable upstream lineage, manually configure Dataset URNs.

For REST API pipelines (all tables share the same source):

```yaml
source_dataset_urns:
  my_pipeline:
    - "urn:li:dataset:(urn:li:dataPlatform:salesforce,contacts,PROD)"
```

For `sql_database` pipelines (each table maps 1:1 to a source table):

```yaml
source_table_dataset_urns:
  my_pipeline:
    my_table:
      - "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)"
```

#### Run History

Run history requires the dlt package to be installed in the DataHub ingestion environment and destination credentials to be accessible:

```bash
pip install "dlt[postgres]"   # or dlt[bigquery], dlt[snowflake], etc.
```

Credentials are read from `~/.dlt/secrets.toml` (dlt's standard location) or environment variables:

```bash
export DESTINATION__POSTGRES__CREDENTIALS__HOST=localhost
export DESTINATION__POSTGRES__CREDENTIALS__DATABASE=my_db
export DESTINATION__POSTGRES__CREDENTIALS__USERNAME=dlt
export DESTINATION__POSTGRES__CREDENTIALS__PASSWORD=secret
```

The `run_history_config` time window is respected — configure `start_time` and `end_time` to limit which loads are ingested:

```yaml
include_run_history: true
run_history_config:
  start_time: "-7 days"
  end_time: "now"
```

### Limitations

#### Inlet lineage requires manual configuration

dlt's state files do not store source-system connection details. Inlet (upstream) Dataset URNs must be configured by the user via `source_dataset_urns` or `source_table_dataset_urns`.

#### Run history requires destination access

Querying `_dlt_loads` requires the dlt package and destination credentials. When dlt is not installed or credentials are missing, the connector still emits DataFlow / DataJob / outlet lineage but skips run history.

#### Ownership is not supported

dlt does not record pipeline owners.

### Troubleshooting

#### No entities emitted

- Check that `pipelines_dir` points to a directory containing subdirectories with `schemas/` inside them
- Run `datahub ingest -c recipe.yml --test-source-connection` to verify the path is readable

#### Lineage not stitching

- Verify `destination_platform_map` env/instance/database match exactly what your destination connector uses
- Check the destination Dataset URNs in DataHub and compare to what the dlt connector constructs
- For Postgres: ensure `database` is set in `destination_platform_map.postgres`

#### Run history empty

- Confirm `include_run_history: true` is set
- Confirm the dlt package is installed: `python -c "import dlt; print(dlt.__version__)"`
- Confirm destination credentials are in `~/.dlt/secrets.toml` or environment variables
- Check DataHub ingestion logs for warnings from the dlt connector

#### Nested child tables (e.g. `orders__items`)

dlt automatically unnests nested JSON into child tables using double-underscore naming. These appear as separate DataJobs with `parent_table` set in their custom properties. This is expected behavior.


### Code Coordinates
- Class Name: `datahub.ingestion.source.dlt.dlt.DltSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dlt/dlt.py)


:::tip Questions?

If you've got any questions on configuring ingestion for dlt, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
