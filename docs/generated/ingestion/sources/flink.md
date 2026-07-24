


# Flink

## Overview

Apache Flink is a distributed stream and batch processing framework. Learn more in the [official Flink documentation](https://flink.apache.org/).

The DataHub integration for Flink extracts job metadata, operator topology, and dataset lineage by connecting to the Flink JobManager REST API and optionally the SQL Gateway. It resolves table references to their actual platforms (Kafka, Postgres, Iceberg, etc.) via catalog introspection, and tracks job execution history as DataProcessInstances. Stateful ingestion is supported for stale entity removal.

## Concept Mapping

| Source Concept | DataHub Concept                                                                 | Notes                                                     |
| -------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Flink Job      | [DataFlow](docs/generated/metamodel/entities/dataFlow.md)                       | One DataFlow per Flink job                                |
| Flink Operator | [DataJob](docs/generated/metamodel/entities/dataJob.md)                         | Granularity depends on `operator_granularity`             |
| Job Execution  | [DataProcessInstance](docs/generated/metamodel/entities/dataProcessInstance.md) | When `include_run_history` is enabled                     |
| Kafka Topic    | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via lineage (DataStream or SQL/Table API)        |
| JDBC Table     | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via SQL Gateway catalog introspection            |
| Iceberg Table  | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via SQL Gateway or `catalog_platform_map` config |


## Module `flink`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Table-level lineage from Kafka DataStream sources/sinks and SQL/Table API via catalog resolution. |

### Overview

The `flink` module ingests metadata from Apache Flink into DataHub. It connects to the Flink JobManager REST API to extract jobs, execution plans, and run history. When a SQL Gateway URL is provided, it resolves SQL/Table API table references to their actual platforms (Kafka, Postgres, Iceberg, Paimon, etc.) via catalog introspection.

### Prerequisites

In order to ingest metadata from Apache Flink, you will need:

- Access to a Flink cluster with the **JobManager REST API** enabled (default port 8081)
- Flink version >= 1.16 (tested with 1.19; platform resolution via `DESCRIBE CATALOG` requires Flink 1.20+)
- For platform-resolved lineage of SQL/Table API jobs: access to a **Flink SQL Gateway** (default port 8083)

#### Required Permissions

| Capability                | API                                   | Required Access                    |
| ------------------------- | ------------------------------------- | ---------------------------------- |
| Job metadata, run history | JobManager REST API (`/v1/jobs`)      | Read access to the REST API        |
| Platform-resolved lineage | SQL Gateway REST API (`/v1/sessions`) | Session creation and SQL execution |

> **_NOTE:_** If your Flink cluster uses authentication (bearer token or basic auth), provide credentials in the `connection` config. The same credentials are used for both the JobManager and SQL Gateway APIs.


### Install the Plugin
```shell
pip install 'acryl-datahub[flink]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"

      # SQL Gateway enables platform resolution for SQL/Table API lineage.
      # Without it, only DataStream Kafka lineage is extracted.
      # sql_gateway_url: "http://localhost:8083"

      # Authentication (uncomment ONE method)
      # token: "${FLINK_API_TOKEN}"
      # username: "admin"
      # password: "${FLINK_PASSWORD}"

      # Advanced connection tuning
      # timeout_seconds: 30
      # max_retries: 3
      # verify_ssl: true

    # Filter jobs by name
    # job_name_pattern:
    #   allow:
    #     - "^prod_.*"

    # Filter by job state (defaults to RUNNING, FINISHED, FAILED, CANCELED)
    # include_job_states:
    #   - "RUNNING"
    #   - "FINISHED"

    # Per-catalog platform_instance overrides. Platform is auto-detected via
    # SQL Gateway for most catalog types; specify platform only when
    # auto-detection is unavailable (see documentation for details).
    # catalog_platform_map:
    #   pg_catalog:
    #     platform_instance: "prod-postgres"
    #   kafka_catalog:
    #     platform_instance: "prod-kafka"

    # DataJob granularity - "job" (default) or "vertex" (one per operator)
    # operator_granularity: "job"

    # include_lineage: true
    # include_run_history: true

    # Platform-wide fallback for platform_instance (used when catalog_platform_map
    # does not have an entry for the catalog).
    # platform_instance_map:
    #   kafka: "prod-kafka-cluster"

    # Parallel job processing
    # max_workers: 10

    # Stale entity removal
    # stateful_ingestion:
    #   enabled: true
    #   remove_stale_metadata: true

    env: "PROD"

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
| <div className="path-line"><span className="path-main">connection</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">FlinkConnectionConfig</span></div> | Connection configuration for Flink REST APIs.  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">rest_api_url</span>&nbsp;<abbr title="Required if connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | JobManager REST API endpoint (e.g., http://localhost:8081).  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum total attempts (initial + retries) for failed HTTP requests with exponential backoff. Default of 3 means 1 initial attempt plus up to 2 retries. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for HTTP Basic authentication. Must be paired with 'username'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">sql_gateway_operation_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum time in seconds to wait for a SQL Gateway operation (SHOW CATALOGS, DESCRIBE CATALOG, etc.) to complete. Increase for slow catalog backends. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">sql_gateway_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | SQL Gateway REST API endpoint (e.g., http://localhost:8083). Enables platform resolution for SQL/Table API lineage. When provided, the connector resolves table references to their actual platform (kafka, postgres, iceberg, etc.) via catalog introspection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Bearer token for authentication. Mutually exclusive with username/password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Username for HTTP Basic authentication. Must be paired with 'password'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Verify SSL certificates for HTTPS connections. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract source/sink lineage from Flink execution plans. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_run_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit DataProcessInstance entities for job execution tracking. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Max parallel threads for fetching job details from the Flink REST API. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">operator_granularity</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "job", "vertex" <div className="default-line default-line-with-docs">Default: <span className="default-value">job</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">catalog_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,CatalogPlatformDetail)</span></div> | Platform details for a Flink catalog, used in dataset URN construction. <br />  <br /> Provides two pieces of information for a given Flink catalog: <br />  <br /> - ``platform``: The DataHub platform name (e.g., "iceberg", "postgres"). <br />   On Flink 1.20+, this is auto-detected via DESCRIBE CATALOG and only needs <br />   to be specified for catalogs where auto-detection fails. On Flink < 1.20, <br />   this is required for Iceberg and Paimon catalogs (which don't expose a <br />   ``connector`` property in SHOW CREATE TABLE). <br />  <br /> - ``platform_instance``: The DataHub platform instance (e.g., "prod-postgres"). <br />   Used when a Flink cluster connects to multiple deployments of the same <br />   platform and you need distinct dataset URNs per deployment. <br />  <br /> Follows the same pattern as Fivetran's ``PlatformDetail`` and Looker's <br /> ``LookerConnectionDefinition``.  |
| <div className="path-line"><span className="path-prefix">catalog_platform_map.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform name for datasets in this catalog (e.g., 'iceberg', 'postgres', 'kafka'). When omitted, the connector auto-detects the platform via SQL Gateway. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">catalog_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform instance for datasets in this catalog (e.g., 'prod-postgres', 'us-east-kafka'). Used to distinguish multiple deployments of the same platform. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_job_states</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Flink job states to include in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;RUNNING&#x27;, &#x27;FINISHED&#x27;, &#x27;FAILED&#x27;, &#x27;CANCELED&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">include_job_states.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">job_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">job_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion for soft-deleting stale entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "CatalogPlatformDetail": {
      "additionalProperties": false,
      "description": "Platform details for a Flink catalog, used in dataset URN construction.\n\nProvides two pieces of information for a given Flink catalog:\n\n- ``platform``: The DataHub platform name (e.g., \"iceberg\", \"postgres\").\n  On Flink 1.20+, this is auto-detected via DESCRIBE CATALOG and only needs\n  to be specified for catalogs where auto-detection fails. On Flink < 1.20,\n  this is required for Iceberg and Paimon catalogs (which don't expose a\n  ``connector`` property in SHOW CREATE TABLE).\n\n- ``platform_instance``: The DataHub platform instance (e.g., \"prod-postgres\").\n  Used when a Flink cluster connects to multiple deployments of the same\n  platform and you need distinct dataset URNs per deployment.\n\nFollows the same pattern as Fivetran's ``PlatformDetail`` and Looker's\n``LookerConnectionDefinition``.",
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform name for datasets in this catalog (e.g., 'iceberg', 'postgres', 'kafka'). When omitted, the connector auto-detects the platform via SQL Gateway.",
          "title": "Platform"
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
          "description": "DataHub platform instance for datasets in this catalog (e.g., 'prod-postgres', 'us-east-kafka'). Used to distinguish multiple deployments of the same platform.",
          "title": "Platform Instance"
        }
      },
      "title": "CatalogPlatformDetail",
      "type": "object"
    },
    "FlinkConnectionConfig": {
      "additionalProperties": false,
      "description": "Connection configuration for Flink REST APIs.",
      "properties": {
        "rest_api_url": {
          "description": "JobManager REST API endpoint (e.g., http://localhost:8081).",
          "title": "Rest Api Url",
          "type": "string"
        },
        "sql_gateway_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "SQL Gateway REST API endpoint (e.g., http://localhost:8083). Enables platform resolution for SQL/Table API lineage. When provided, the connector resolves table references to their actual platform (kafka, postgres, iceberg, etc.) via catalog introspection.",
          "title": "Sql Gateway Url"
        },
        "token": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Bearer token for authentication. Mutually exclusive with username/password.",
          "title": "Token"
        },
        "username": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Username for HTTP Basic authentication. Must be paired with 'password'.",
          "title": "Username"
        },
        "password": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Password for HTTP Basic authentication. Must be paired with 'username'.",
          "title": "Password"
        },
        "timeout_seconds": {
          "default": 30,
          "description": "HTTP request timeout in seconds.",
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "max_retries": {
          "default": 3,
          "description": "Maximum total attempts (initial + retries) for failed HTTP requests with exponential backoff. Default of 3 means 1 initial attempt plus up to 2 retries.",
          "minimum": 0,
          "title": "Max Retries",
          "type": "integer"
        },
        "verify_ssl": {
          "default": true,
          "description": "Verify SSL certificates for HTTPS connections.",
          "title": "Verify Ssl",
          "type": "boolean"
        },
        "sql_gateway_operation_timeout_seconds": {
          "default": 60,
          "description": "Maximum time in seconds to wait for a SQL Gateway operation (SHOW CATALOGS, DESCRIBE CATALOG, etc.) to complete. Increase for slow catalog backends.",
          "minimum": 5,
          "title": "Sql Gateway Operation Timeout Seconds",
          "type": "integer"
        }
      },
      "required": [
        "rest_api_url"
      ],
      "title": "FlinkConnectionConfig",
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
  "description": "Source configuration for Flink connector.",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
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
      "description": "Stateful ingestion for soft-deleting stale entities."
    },
    "connection": {
      "$ref": "#/$defs/FlinkConnectionConfig",
      "description": "Flink REST API connection configuration."
    },
    "job_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Flink jobs by name."
    },
    "include_job_states": {
      "default": [
        "RUNNING",
        "FINISHED",
        "FAILED",
        "CANCELED"
      ],
      "description": "Flink job states to include in ingestion.",
      "items": {
        "type": "string"
      },
      "title": "Include Job States",
      "type": "array"
    },
    "include_lineage": {
      "default": true,
      "description": "Extract source/sink lineage from Flink execution plans.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "include_run_history": {
      "default": true,
      "description": "Emit DataProcessInstance entities for job execution tracking.",
      "title": "Include Run History",
      "type": "boolean"
    },
    "catalog_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/CatalogPlatformDetail"
      },
      "description": "Platform overrides for Flink catalogs, keyed by catalog name. Values take priority over SQL Gateway auto-detection. Example: {'ice_catalog': {'platform': 'iceberg', 'platform_instance': 'prod-iceberg'}, 'pg_catalog': {'platform_instance': 'prod-postgres'}}. The 'platform' field overrides auto-detection. Required for Iceberg/Paimon catalogs on Flink < 1.20 (DESCRIBE CATALOG unavailable). On Flink 1.20+, platform is auto-detected via SQL Gateway unless overridden here. The 'platform_instance' field takes priority over the inherited platform_instance_map (platform -> platform_instance) for catalogs listed here.",
      "title": "Catalog Platform Map",
      "type": "object"
    },
    "operator_granularity": {
      "default": "job",
      "description": "DataJob granularity: 'job' emits one coalesced DataJob per flow, 'vertex' emits one DataJob per operator/vertex in the execution plan.",
      "enum": [
        "job",
        "vertex"
      ],
      "title": "Operator Granularity",
      "type": "string"
    },
    "max_workers": {
      "default": 10,
      "description": "Max parallel threads for fetching job details from the Flink REST API.",
      "maximum": 50,
      "minimum": 1,
      "title": "Max Workers",
      "type": "integer"
    }
  },
  "required": [
    "connection"
  ],
  "title": "FlinkSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Lineage Extraction

The connector extracts table-level lineage by analyzing Flink execution plans. It handles two distinct cases:

**DataStream API (Kafka only):** The connector recognizes `KafkaSource-{topic}` and `KafkaSink-{topic}` patterns in operator descriptions. Platform is always `kafka`, and the topic name is extracted directly from the description. No SQL Gateway needed.

**SQL/Table API (all connectors):** The connector parses `TableSourceScan(table=[[catalog, db, table]])` and `Sink(table=[[catalog, db, table]])` patterns. These are generic Flink plan formats — the same for Kafka, JDBC, Iceberg, Paimon, and every other connector. The connector resolves the actual platform via SQL Gateway catalog introspection:

1. **`catalog_platform_map` config** — user-provided overrides; take priority over all auto-detection
2. **DESCRIBE CATALOG** (Flink 1.20+) — determines catalog type (jdbc, iceberg, paimon, hive, etc.)
3. **SHOW CREATE TABLE** — reads the `connector` property from the table DDL (for hive/generic_in_memory catalogs with mixed connector types)

#### Platform Resolution Examples

A Flink job reads from a Postgres JDBC catalog table `pg_catalog.mydb.public.users`:

```
Plan: TableSourceScan(table=[[pg_catalog, mydb, public.users]])
  → SQL Gateway: DESCRIBE CATALOG → type=jdbc, base-url=jdbc:postgresql://  (Flink 1.20+)
  → URN: urn:li:dataset:(urn:li:dataPlatform:postgres, mydb.public.users, PROD)
```

A Flink job reads from an Iceberg catalog table `ice_catalog.lake.events`:

```
Plan: TableSourceScan(table=[[ice_catalog, lake, events]])
  → SQL Gateway: DESCRIBE CATALOG → type=iceberg  (Flink 1.20+)
  → URN: urn:li:dataset:(urn:li:dataPlatform:iceberg, lake.events, PROD)
```

#### Platform Instance Mapping

If your datasets belong to specific platform instances (e.g., a particular Kafka cluster or Postgres deployment), use `catalog_platform_map` for per-catalog mapping or `platform_instance_map` as a platform-wide fallback:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      sql_gateway_url: "http://localhost:8083"
    # Per-catalog: takes priority
    catalog_platform_map:
      pg_us:
        platform_instance: "us-postgres"
      pg_eu:
        platform_instance: "eu-postgres"
    # Platform-wide fallback
    platform_instance_map:
      kafka: "prod-kafka-cluster"
```

#### Iceberg/Paimon on Flink < 1.20

On Flink versions before 1.20, `DESCRIBE CATALOG` is not available. The connector falls back to `SHOW CREATE TABLE`, but Iceberg and Paimon tables do not have a `connector` property in their DDL. In this case, provide the platform explicitly via `catalog_platform_map`:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      sql_gateway_url: "http://localhost:8083"
    catalog_platform_map:
      ice_catalog:
        platform: "iceberg"
      paimon_catalog:
        platform: "paimon"
```

On Flink 1.20+, this config is not needed — the platform is auto-detected from the catalog type.

#### Operator Granularity

By default (`operator_granularity: job`), the connector emits **one DataJob per Flink job** with all source and sink lineage coalesced into that single DataJob.

Set `operator_granularity: vertex` to emit **one DataJob per operator/vertex** in the execution plan. This gives finer-grained lineage at the cost of more entities.

#### Run History

When `include_run_history` is enabled (the default), the connector emits `DataProcessInstance` entities that track individual job executions:

- **Start and end timestamps** from the Flink job timeline
- **Run result**: `FINISHED` maps to SUCCESS, `FAILED` maps to FAILURE, `CANCELED` maps to SKIPPED
- **Process type**: STREAMING or BATCH, based on the Flink job type

Jobs in `RUNNING` state emit a start event only. Completed jobs emit both start and end events.

### Limitations

1. **SQL Gateway required for SQL/Table API lineage.** Without a SQL Gateway URL, the connector cannot resolve `TableSourceScan(table=[[catalog, db, table]])` references to their actual platform. DataStream Kafka lineage (`KafkaSource-{topic}`) works without SQL Gateway.

2. **Catalogs must be visible to the SQL Gateway session.** Catalogs registered programmatically in job code, via ephemeral SQL client sessions, or in a separate FileCatalogStore are invisible to the connector. Production deployments should use a persistent catalog (e.g., HiveCatalog backed by Hive Metastore) so that table definitions are visible across sessions.

3. **Iceberg/Paimon on Flink < 1.20 require config.** `DESCRIBE CATALOG` was introduced in Flink 1.20. On earlier versions, Iceberg and Paimon catalogs cannot be auto-detected because their tables don't have a `connector` property in `SHOW CREATE TABLE`. Use `catalog_platform_map` to specify the platform manually.

4. **Operator-chained sinks have no catalog info.** The `tableName[N]: Writer` pattern produced by Flink's operator chaining does not include catalog or database information. Only the bare table name is available. These sinks cannot be resolved to a platform and are reported as unclassified.

5. **Temporary tables are invisible to SQL Gateway.** `CREATE TEMPORARY TABLE` definitions are session-scoped and not persisted in any catalog. The SQL Gateway cannot look up their definitions, so temporary tables cannot be resolved to a platform and are reported as unclassified.

6. **DataStream non-Kafka connectors are not supported.** Only `KafkaSource-{topic}` and `KafkaSink-{topic}` DataStream patterns are recognized. Other DataStream connectors (Kinesis, Pulsar, RabbitMQ, custom) produce user-provided names with no platform information.

7. **No column-level lineage.** Only table-level (coarse) lineage is extracted from execution plans.

### Troubleshooting

**"Failed to connect to Flink cluster"**
Verify the `rest_api_url` is correct and reachable. Test manually: `curl http://<host>:8081/v1/config`

**Jobs appear but no lineage is extracted**
Check the ingestion report for "unclassified" nodes. Common causes:

- SQL/Table API jobs without `sql_gateway_url` configured — add the SQL Gateway URL
- Tables in `default_catalog` (GenericInMemoryCatalog) created in ephemeral sessions — use a persistent catalog like HiveCatalog
- DataStream jobs using non-Kafka connectors — not currently supported

**SQL Gateway configured but platform not resolved**
On Flink < 1.20, `DESCRIBE CATALOG` is unavailable. Check if the table's `SHOW CREATE TABLE` output includes a `connector` property. For Iceberg/Paimon catalogs, add `catalog_platform_map` config.

**Lineage URNs don't match other connectors (e.g., Kafka connector)**
Ensure `platform_instance_map` or `catalog_platform_map` produces the same platform instance as your other ingestion sources. For example, if the Kafka connector uses `platform_instance: "prod-cluster"`, configure:

```yml
platform_instance_map:
  kafka: "prod-cluster"
```


### Code Coordinates
- Class Name: `datahub.ingestion.source.flink.source.FlinkSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/flink/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Flink, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
