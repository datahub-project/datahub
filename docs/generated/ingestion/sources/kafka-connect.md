


# Kafka Connect

## Overview

Kafka Connect is a streaming or integration platform. Learn more in the [official Kafka Connect documentation](https://kafka.apache.org/documentation/#connect).

The DataHub integration for Kafka Connect covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

| Source Concept                                                                  | DataHub Concept                                                                           | Notes |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ----- |
| `"kafka-connect"`                                                               | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |       |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks) | [DataFlow](/docs/generated/metamodel/entities/dataflow/)          |       |
| Kafka Topic                                                                     | [Dataset](/docs/generated/metamodel/entities/dataset/)            |       |


## Module `kafka-connect`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

The `kafka-connect` module ingests metadata from Kafka Connect into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Java Runtime Dependency

This source requires Java to be installed and available on the system for transform pipeline support (RegexRouter, etc.). The Java runtime is accessed via JPype to enable Java regex pattern matching that's compatible with Kafka Connect transforms.

- **Python installations**: Install Java separately (e.g., `apt-get install openjdk-11-jre-headless` on Debian/Ubuntu)
- **Docker deployments**: Ensure your DataHub ingestion Docker image includes a Java runtime. The official DataHub images include Java by default.
- **Impact**: Without Java, transform pipeline features will be disabled and lineage accuracy may be reduced for connectors using transforms

**Note for Docker users**: If you're building custom Docker images for DataHub ingestion, ensure a Java Runtime Environment (JRE) is included in your image to support full transform pipeline functionality.


### Install the Plugin
```shell
pip install 'acryl-datahub[kafka-connect]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "kafka-connect"
  config:
    # Coordinates
    connect_uri: "http://localhost:8083"

    # Credentials
    username: admin
    password: password

    # Optional
    # Platform instance mapping to use when constructing URNs.
    # Use if single instance of platform is referred across connectors.
    platform_instance_map:
      mysql: mysql_platform_instance

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">cluster_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Cluster to ingest from. <div className="default-line default-line-with-docs">Default: <span className="default-value">connect-cluster</span></div> |
| <div className="path-line"><span className="path-main">confluent_cloud_cluster_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Confluent Cloud Kafka Connect cluster ID (e.g., 'lkc-abc123'). When specified along with confluent_cloud_environment_id, the connect_uri will be automatically constructed. This is the recommended approach for Confluent Cloud instead of manually constructing the full URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">confluent_cloud_environment_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Confluent Cloud environment ID (e.g., 'env-xyz123'). When specified along with confluent_cloud_cluster_id, the connect_uri will be automatically constructed. This is the recommended approach for Confluent Cloud instead of manually constructing the full URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance mapping when multiple instances for a platform is available. Entry for a platform should be in either `platform_instance_map` or `connect_to_platform_map`. e.g.`connect_to_platform_map: { "postgres-connector-finance-db": "postgres": "core_finance_instance" }` <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | URI to connect to. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:8083/</span></div> |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert the urns of ingested lineage dataset to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">kafka_api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Optional: Confluent Cloud Kafka API key for authenticating with Kafka REST API v3. If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. Only needed if you want to use separate credentials for the Kafka API. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">kafka_api_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Optional: Confluent Cloud Kafka API secret for authenticating with Kafka REST API v3. If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. Only needed if you want to use separate credentials for the Kafka API. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">kafka_rest_endpoint</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional: Confluent Cloud Kafka REST API endpoint for comprehensive topic retrieval. Format: https://pkc-xxxxx.region.provider.confluent.cloud If not specified, DataHub automatically derives the endpoint from connector configurations (kafka.endpoint). When available, enables getting all topics from Kafka cluster for improved transform pipeline accuracy. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Kafka Connect password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance mapping to use when constructing URNs. e.g.`platform_instance_map: { "hive": "warehouse" }` <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_resolver_expand_patterns</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Enable table pattern expansion using DataHub schema metadata. When use_schema_resolver=True, this controls whether to expand patterns like 'database.*' to actual table names by querying DataHub. Only applies when use_schema_resolver is enabled. Defaults to True when use_schema_resolver is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_resolver_finegrained_lineage</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Enable fine-grained (column-level) lineage extraction using DataHub schema metadata. When use_schema_resolver=True, this controls whether to generate column-level lineage by matching schemas between source tables and Kafka topics. Only applies when use_schema_resolver is enabled. Defaults to True when use_schema_resolver is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_connect_topics_api</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use Kafka Connect API for topic retrieval and validation. This flag controls the environment-specific topic retrieval strategy:  <br /> **When True (default):** - **Self-hosted environments:** Uses runtime `/connectors/{name}/topics` API for accurate topic information - **Confluent Cloud:** Uses comprehensive Kafka REST API v3 to get all topics for transform pipeline, with config-based fallback  <br /> **When False:** Disables all API-based topic retrieval for both environments. Returns empty topic lists. Useful for air-gapped environments or when topic validation isn't needed for performance optimization. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_schema_resolver</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use DataHub's schema metadata to enhance Kafka Connect connector lineage. When enabled (requires DataHub graph connection): 1) Expands table patterns (e.g., 'database.*') to actual tables using DataHub metadata 2) Generates fine-grained column-level lineage for Kafka Connect sources/sinks.  <br />  <br /> **Auto-enabled for Confluent Cloud:** This feature is automatically enabled for Confluent Cloud environments where DataHub graph connection is required. Set `use_schema_resolver: false` to disable.  <br />  <br /> **Prerequisite:** Source database tables must be ingested into DataHub before Kafka Connect ingestion for this feature to work. Without prior database ingestion, schema resolver will not find table metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Kafka Connect username. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connector_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">generic_connectors</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Provide lineage graph for sources connectors other than Confluent JDBC Source Connector, Debezium Source Connector, and Mongo Source Connector <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">generic_connectors.</span><span className="path-main">GenericConnectorConfig</span></div> <div className="type-name-line"><span className="type-name">GenericConnectorConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">generic_connectors.GenericConnectorConfig.</span><span className="path-main">connector_name</span>&nbsp;<abbr title="Required if GenericConnectorConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">generic_connectors.GenericConnectorConfig.</span><span className="path-main">source_dataset</span>&nbsp;<abbr title="Required if GenericConnectorConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">generic_connectors.GenericConnectorConfig.</span><span className="path-main">source_platform</span>&nbsp;<abbr title="Required if GenericConnectorConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">provided_configs</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Provided Configurations <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">provided_configs.</span><span className="path-main">ProvidedConfig</span></div> <div className="type-name-line"><span className="type-name">ProvidedConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">provided_configs.ProvidedConfig.</span><span className="path-main">path_key</span>&nbsp;<abbr title="Required if ProvidedConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">provided_configs.ProvidedConfig.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if ProvidedConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">provided_configs.ProvidedConfig.</span><span className="path-main">value</span>&nbsp;<abbr title="Required if ProvidedConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
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
    "GenericConnectorConfig": {
      "additionalProperties": false,
      "properties": {
        "connector_name": {
          "title": "Connector Name",
          "type": "string"
        },
        "source_dataset": {
          "title": "Source Dataset",
          "type": "string"
        },
        "source_platform": {
          "title": "Source Platform",
          "type": "string"
        }
      },
      "required": [
        "connector_name",
        "source_dataset",
        "source_platform"
      ],
      "title": "GenericConnectorConfig",
      "type": "object"
    },
    "ProvidedConfig": {
      "additionalProperties": false,
      "properties": {
        "provider": {
          "title": "Provider",
          "type": "string"
        },
        "path_key": {
          "title": "Path Key",
          "type": "string"
        },
        "value": {
          "title": "Value",
          "type": "string"
        }
      },
      "required": [
        "provider",
        "path_key",
        "value"
      ],
      "title": "ProvidedConfig",
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
  "properties": {
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
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
      "description": "Platform instance mapping to use when constructing URNs. e.g.`platform_instance_map: { \"hive\": \"warehouse\" }`",
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
    "connect_uri": {
      "default": "http://localhost:8083/",
      "description": "URI to connect to.",
      "title": "Connect Uri",
      "type": "string"
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
      "description": "Kafka Connect username.",
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
      "description": "Kafka Connect password.",
      "title": "Password"
    },
    "cluster_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "connect-cluster",
      "description": "Cluster to ingest from.",
      "title": "Cluster Name"
    },
    "convert_lineage_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert the urns of ingested lineage dataset to lowercase",
      "title": "Convert Lineage Urns To Lowercase",
      "type": "boolean"
    },
    "connector_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for connectors to filter for ingestion."
    },
    "provided_configs": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/ProvidedConfig"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Provided Configurations",
      "title": "Provided Configs"
    },
    "connect_to_platform_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "additionalProperties": {
              "type": "string"
            },
            "type": "object"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Platform instance mapping when multiple instances for a platform is available. Entry for a platform should be in either `platform_instance_map` or `connect_to_platform_map`. e.g.`connect_to_platform_map: { \"postgres-connector-finance-db\": \"postgres\": \"core_finance_instance\" }`",
      "title": "Connect To Platform Map"
    },
    "generic_connectors": {
      "default": [],
      "description": "Provide lineage graph for sources connectors other than Confluent JDBC Source Connector, Debezium Source Connector, and Mongo Source Connector",
      "items": {
        "$ref": "#/$defs/GenericConnectorConfig"
      },
      "title": "Generic Connectors",
      "type": "array"
    },
    "use_connect_topics_api": {
      "default": true,
      "description": "Whether to use Kafka Connect API for topic retrieval and validation. This flag controls the environment-specific topic retrieval strategy: \n**When True (default):** - **Self-hosted environments:** Uses runtime `/connectors/{name}/topics` API for accurate topic information - **Confluent Cloud:** Uses comprehensive Kafka REST API v3 to get all topics for transform pipeline, with config-based fallback \n**When False:** Disables all API-based topic retrieval for both environments. Returns empty topic lists. Useful for air-gapped environments or when topic validation isn't needed for performance optimization.",
      "title": "Use Connect Topics Api",
      "type": "boolean"
    },
    "kafka_rest_endpoint": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional: Confluent Cloud Kafka REST API endpoint for comprehensive topic retrieval. Format: https://pkc-xxxxx.region.provider.confluent.cloud If not specified, DataHub automatically derives the endpoint from connector configurations (kafka.endpoint). When available, enables getting all topics from Kafka cluster for improved transform pipeline accuracy.",
      "title": "Kafka Rest Endpoint"
    },
    "kafka_api_key": {
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
      "description": "Optional: Confluent Cloud Kafka API key for authenticating with Kafka REST API v3. If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. Only needed if you want to use separate credentials for the Kafka API.",
      "title": "Kafka Api Key"
    },
    "kafka_api_secret": {
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
      "description": "Optional: Confluent Cloud Kafka API secret for authenticating with Kafka REST API v3. If not specified, DataHub will reuse the Connect credentials (username/password) for Kafka API authentication. Only needed if you want to use separate credentials for the Kafka API.",
      "title": "Kafka Api Secret"
    },
    "confluent_cloud_environment_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Confluent Cloud environment ID (e.g., 'env-xyz123'). When specified along with confluent_cloud_cluster_id, the connect_uri will be automatically constructed. This is the recommended approach for Confluent Cloud instead of manually constructing the full URI.",
      "title": "Confluent Cloud Environment Id"
    },
    "confluent_cloud_cluster_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Confluent Cloud Kafka Connect cluster ID (e.g., 'lkc-abc123'). When specified along with confluent_cloud_environment_id, the connect_uri will be automatically constructed. This is the recommended approach for Confluent Cloud instead of manually constructing the full URI.",
      "title": "Confluent Cloud Cluster Id"
    },
    "use_schema_resolver": {
      "default": false,
      "description": "Use DataHub's schema metadata to enhance Kafka Connect connector lineage. When enabled (requires DataHub graph connection): 1) Expands table patterns (e.g., 'database.*') to actual tables using DataHub metadata 2) Generates fine-grained column-level lineage for Kafka Connect sources/sinks. \n\n**Auto-enabled for Confluent Cloud:** This feature is automatically enabled for Confluent Cloud environments where DataHub graph connection is required. Set `use_schema_resolver: false` to disable. \n\n**Prerequisite:** Source database tables must be ingested into DataHub before Kafka Connect ingestion for this feature to work. Without prior database ingestion, schema resolver will not find table metadata.",
      "title": "Use Schema Resolver",
      "type": "boolean"
    },
    "schema_resolver_expand_patterns": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Enable table pattern expansion using DataHub schema metadata. When use_schema_resolver=True, this controls whether to expand patterns like 'database.*' to actual table names by querying DataHub. Only applies when use_schema_resolver is enabled. Defaults to True when use_schema_resolver is enabled.",
      "title": "Schema Resolver Expand Patterns"
    },
    "schema_resolver_finegrained_lineage": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Enable fine-grained (column-level) lineage extraction using DataHub schema metadata. When use_schema_resolver=True, this controls whether to generate column-level lineage by matching schemas between source tables and Kafka topics. Only applies when use_schema_resolver is enabled. Defaults to True when use_schema_resolver is enabled.",
      "title": "Schema Resolver Finegrained Lineage"
    }
  },
  "title": "KafkaConnectSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Transform Pipeline Support

**✅ Fully Supported:**

- **Any combination of transforms**: RegexRouter, EventRouter, and non-routing transforms
- **Complex transform chains**: Multiple chained transforms automatically handled
- **Both environments**: Self-hosted and Confluent Cloud work identically
- **Future-proof**: New transform types automatically supported

:::warning Considerations

For connectors not listed in the supported connector table above, use the `generic_connectors` configuration to provide explicit lineage mappings.
Some advanced connector-specific features may not be fully supported
:::

#### Environment-Specific Behavior

**Environment Detection**: Automatically detects environment based on `connect_uri` patterns containing `confluent.cloud`.

##### Self-hosted Kafka Connect

- **Topic Discovery**: Uses runtime `/connectors/{name}/topics` API endpoint for maximum accuracy
- **Requirements**: Standard Kafka Connect REST API access
- **Fallback**: If runtime API fails, falls back to config-based derivation

```yml
source:
  type: kafka-connect
  config:
    # Self-hosted Kafka Connect cluster
    connect_uri: "http://localhost:8083"
    # use_connect_topics_api: true  # Default - enables runtime topic discovery
```

##### Confluent Cloud

- **Topic Discovery**: Uses comprehensive Kafka REST API v3 to get all topics, with automatic credential reuse
- **Transform Support**: Full support for all transform combinations via reverse pipeline strategy using actual cluster topics
- **Auto-derivation**: Automatically derives Kafka REST endpoint from connector configurations

**Recommended approach using environment and cluster IDs:**

```yml
source:
  type: kafka-connect
  config:
    # Auto-construct URI from environment and cluster IDs (recommended)
    confluent_cloud_environment_id: "env-xyz123" # Your Confluent Cloud environment ID
    confluent_cloud_cluster_id: "lkc-abc456" # Your Kafka Connect cluster ID

    # Standard credentials for Kafka Connect API
    username: "your-connect-api-key" # API key for Kafka Connect access
    password: "your-connect-api-secret" # API secret for Kafka Connect access

    # Optional: Separate credentials for Kafka REST API (if different from Connect API)
    kafka_api_key: "your-kafka-api-key" # API key for Kafka REST API access
    kafka_api_secret: "your-kafka-api-secret" # API secret for Kafka REST API access

    # Optional: Dedicated Kafka REST endpoint for comprehensive topic retrieval
    kafka_rest_endpoint: "https://pkc-xxxxx.region.provider.confluent.cloud"

    # use_connect_topics_api: true  # Default - enables comprehensive topic retrieval
```

**Alternative approach using full URI (legacy):**

```yml
source:
  type: kafka-connect
  config:
    # Confluent Cloud Connect URI - automatically detected
    connect_uri: "https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
    username: "your-connect-api-key" # API key for Kafka Connect
    password: "your-connect-api-secret" # API secret for Kafka Connect
    kafka_api_key: "your-kafka-api-key" # API key for Kafka REST API (if different)
    kafka_api_secret: "your-kafka-api-secret" # API secret for Kafka REST API (if different)

    # Optional: Dedicated Kafka REST endpoint for comprehensive topic retrieval
    kafka_rest_endpoint: "https://pkc-xxxxx.region.provider.confluent.cloud"
```

##### Configuration Control

The `use_connect_topics_api` flag controls topic retrieval behavior:

- **When `true` (default)**: Uses environment-specific topic discovery with full transform support
- **When `false`**: Disables all topic discovery for air-gapped environments or performance optimization

#### Advanced Scenarios: Complex Transform Chains

The new reverse transform pipeline strategy handles complex scenarios automatically:

```yaml
### Example: EventRouter + RegexRouter chain
transforms: EventRouter,RegexRouter
transforms.EventRouter.type: io.debezium.transforms.outbox.EventRouter
transforms.RegexRouter.type: org.apache.kafka.connect.transforms.RegexRouter
transforms.RegexRouter.regex: "outbox\\.event\\.(.*)"
transforms.RegexRouter.replacement: "events.$1"
```

#### Advanced Scenarios: Fallback Options

- If transform pipeline cannot determine mappings, DataHub falls back to simple topic-based lineage
- For unsupported connector types or complex custom scenarios, use `generic_connectors` configuration

#### Advanced Scenarios: Performance Optimization

- Set `use_connect_topics_api: false` to disable topic discovery in air-gapped environments
- Transform pipeline processing adds minimal overhead and improves lineage accuracy

#### Supported Source Connectors

| Connector Type                                                                   | Self-hosted Support | Confluent Cloud Support | Topic Discovery Method      | Lineage Extraction             |
| -------------------------------------------------------------------------------- | ------------------- | ----------------------- | --------------------------- | ------------------------------ |
| **Platform JDBC Source**<br/>`io.confluent.connect.jdbc.JdbcSourceConnector`     | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud PostgreSQL CDC**<br/>`PostgresCdcSource`                                 | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud PostgreSQL CDC V2**<br/>`PostgresCdcSourceV2`                            | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud MySQL Source**<br/>`MySqlSource`                                         | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud MySQL CDC**<br/>`MySqlCdcSource`                                         | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Debezium MySQL**<br/>`io.debezium.connector.mysql.MySqlConnector`              | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium PostgreSQL**<br/>`io.debezium.connector.postgresql.PostgresConnector` | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium SQL Server**<br/>`io.debezium.connector.sqlserver.SqlServerConnector` | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium Oracle**<br/>`io.debezium.connector.oracle.OracleConnector`           | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium DB2**<br/>`io.debezium.connector.db2.Db2Connector`                    | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium MongoDB**<br/>`io.debezium.connector.mongodb.MongoDbConnector`        | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Collection → Topic CDC mapping |
| **Debezium Vitess**<br/>`io.debezium.connector.vitess.VitessConnector`           | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Table → Topic CDC mapping      |
| **MongoDB Source**<br/>`com.mongodb.kafka.connect.MongoSourceConnector`          | ✅ Full             | 🔧 Config Required      | Runtime API / Manual config | Collection → Topic mapping     |
| **Generic Connectors**                                                           | 🔧 Config Required  | 🔧 Config Required      | User-defined mapping        | Custom lineage mapping         |

#### Supported Sink Connectors

| Connector Type                                                                 | Self-hosted Support | Confluent Cloud Support | Topic Discovery Method     | Lineage Extraction        |
| ------------------------------------------------------------------------------ | ------------------- | ----------------------- | -------------------------- | ------------------------- |
| **BigQuery Sink**<br/>`com.wepay.kafka.connect.bigquery.BigQuerySinkConnector` | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **S3 Sink**<br/>`io.confluent.connect.s3.S3SinkConnector`                      | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → S3 object mapping |
| **Snowflake Sink**<br/>`com.snowflake.kafka.connector.SnowflakeSinkConnector`  | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud PostgreSQL Sink**<br/>`PostgresSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud MySQL Sink**<br/>`MySqlSink`                                           | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud Snowflake Sink**<br/>`SnowflakeSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Debezium JDBC Sink**<br/>`io.debezium.connector.jdbc.JdbcSinkConnector`      | ✅ Full             | ✅ Partial              | Runtime API / Config-based | Topic → Table mapping     |
| **ClickHouse Sink**<br/>`com.clickhouse.kafka.connect.ClickHouseSinkConnector` | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Iceberg Sink**<br/>`org.apache.iceberg.connect.IcebergSinkConnector`         | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Confluent JDBC Sink**<br/>`io.confluent.connect.jdbc.JdbcSinkConnector`      | ✅ Full             | ✅ Partial              | Runtime API / Config-based | Topic → Table mapping     |

**Legend:**

- ✅ **Full**: Complete lineage extraction with accurate topic discovery
- ✅ **Partial**: Lineage extraction supported but topic discovery may be limited (config-based only)
- 🔧 **Config Required**: Requires `generic_connectors` configuration for lineage mapping
- ❌ **Not supported**: Connector class is not used in this environment

:::info
On JDBC Sink connectors in Confluent Cloud:\*\* `io.debezium.connector.jdbc.JdbcSinkConnector` and `io.confluent.connect.jdbc.JdbcSinkConnector` are not Confluent Cloud managed connectors — they can only appear as custom (self-managed) connectors deployed against a Confluent Cloud Kafka cluster. When present, DataHub supports lineage extraction for them, but with one limitation: the target platform (e.g. `postgres`, `mysql`, `oracle`, `mssql`) must be auto-detected from the `connection.url` field in the connector configuration. If `connection.url` is absent or uses an unrecognised JDBC scheme, platform detection will fail and a warning will be emitted. For Confluent Cloud managed JDBC sink connectors, use the dedicated `PostgresSink` or `MySqlSink` connector classes instead, which have explicit platform support.
:::

#### Supported Transforms

DataHub uses an **advanced transform pipeline strategy** that automatically handles complex transform chains by applying the complete pipeline to all topics and checking if results exist. This provides robust support for any combination of transforms.

##### Topic Routing Transforms

- **RegexRouter**: `org.apache.kafka.connect.transforms.RegexRouter`
- **Cloud RegexRouter**: `io.confluent.connect.cloud.transforms.TopicRegexRouter`
- **Debezium EventRouter**: `io.debezium.transforms.outbox.EventRouter` (Outbox pattern)

##### Non-Topic Routing Transforms

DataHub recognizes but passes through these transforms (they don't affect lineage):

- InsertField, ReplaceField, MaskField, ValueToKey, HoistField, ExtractField
- SetSchemaMetadata, Flatten, Cast, HeadersFrom, TimestampConverter
- Filter, InsertHeader, DropHeaders, Drop, TombstoneHandler

##### Transform Pipeline Strategy

DataHub uses an improved **reverse transform pipeline approach** that:

1. **Takes all actual topics** from the connector manifest/Kafka cluster
2. **Applies the complete transform pipeline** to each topic
3. **Checks if transformed results exist** in the actual topic list
4. **Creates lineage mappings** only for successful matches

**Benefits:**

- ✅ **Works with any transform combination** (single or chained transforms)
- ✅ **Handles complex scenarios** like EventRouter + RegexRouter chains
- ✅ **Uses actual topics as source of truth** (no prediction needed)
- ✅ **Future-proof** for new transform types
- ✅ **Works identically** for both self-hosted and Confluent Cloud environments

#### How Lineage Inference Works with Transform Pipelines

Kafka Connect connectors can apply transforms (like RegexRouter) that modify topic names before data reaches Kafka. DataHub's lineage inference analyzes these transform configurations to determine how topics are produced:

1. **Configuration Analysis** - Extracts source tables from connector configuration (`table.include.list`, `database.include.list`)
2. **Transform Application** - Applies configured transforms (RegexRouter, EventRouter, etc.) to predict final topic names
3. **Topic Validation** - Validates predicted topics against actual cluster topics using Kafka REST API v3
4. **Lineage Construction** - Maps source tables to validated topics, preserving schema information

This approach works for both self-hosted and Confluent Cloud environments:

- **Self-hosted**: Uses runtime `/connectors/{name}/topics` API for actual topics produced by each connector
- **Confluent Cloud**: Uses Kafka REST API v3 to get all cluster topics, then applies transform pipeline to match with connector config

**Key Benefits**:

- **90-95% accuracy** for Cloud connectors with transforms (significant improvement over previous config-only approach)
- **Full RegexRouter support** with Java regex compatibility
- **Complex transform chains** handled correctly
- **Schema preservation** maintains full table names with schema information

**Configuration Options:**

- **Environment/Cluster IDs (recommended)**: Use `confluent_cloud_environment_id` and `confluent_cloud_cluster_id` for automatic URI construction
- **Auto-derivation**: DataHub finds Kafka REST endpoint automatically from connector configs
- **Manual endpoint**: Specify `kafka_rest_endpoint` if auto-derivation doesn't work
- **Separate credentials (typical)**: Use `connect_api_key`/`connect_api_secret` for Connect API and `kafka_api_key`/`kafka_api_secret` for Kafka REST API
- **Legacy credentials**: Use `username`/`password` for Connect API (falls back for Kafka API if separate credentials not provided)

#### Enhanced Topic Resolution for Source and Sink Connectors

DataHub now provides intelligent topic resolution that works reliably across all environments, including Confluent Cloud where the Kafka Connect topics API is unavailable.

##### How It Works

**Source Connectors** (Debezium, Snowflake CDC, JDBC):

- Always derive expected topics from connector configuration (`table.include.list`, `database.include.list`)
- Apply configured transforms (RegexRouter, EventRouter, etc.) to predict final topic names
- When Kafka API is available: Filter to only topics that exist in Kafka
- When Kafka API is unavailable (Confluent Cloud): Create lineages for all configured tables without filtering

**Sink Connectors** (S3, Snowflake, BigQuery, ClickHouse, JDBC):

- Support both explicit topic lists (`topics` field) and regex patterns (`topics.regex` field)
- When `topics.regex` is used:
  - Priority 1: Match against `manifest.topic_names` from Kafka API (if available)
  - Priority 2: Query DataHub for Kafka topics and match pattern (if `use_schema_resolver` enabled)
  - Priority 3: Warn user that pattern cannot be expanded

##### Configuration Examples

**Source Connector with Pattern Expansion:**

```yml
# Debezium PostgreSQL source with wildcard tables
connector.config:
  table.include.list: "public.analytics_.*"
  # When Kafka API unavailable, DataHub will:
  # 1. Query DataHub for all PostgreSQL tables matching pattern
  # 2. Derive expected topic names (server.schema.table format)
  # 3. Apply transforms if configured
  # 4. Create lineages without Kafka validation
```

**Sink Connector with topics.regex (Confluent Cloud):**

```yml
# S3 sink connector consuming from pattern-matched topics
connector.config:
  topics.regex: "analytics\\..*" # Match topics like analytics.users, analytics.orders
  # When Kafka API unavailable, DataHub will:
  # 1. Query DataHub for all Kafka topics (requires use_schema_resolver: true)
  # 2. Match topics against the regex pattern
  # 3. Create lineages for matched topics
```

**Enable DataHub Topic Querying for Sink Connectors:**

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
    username: "your-connect-api-key"
    password: "your-connect-api-secret"

    # Enable DataHub schema resolver for topic pattern expansion
    use_schema_resolver: true # Required for topics.regex fallback

    # Configure graph connection for DataHub queries
    datahub_gms_url: "http://localhost:8080" # Your DataHub GMS endpoint
```

##### Key Benefits

1. **Confluent Cloud Support**: Both source and sink connectors work correctly with pattern-based configurations
2. **Config as Source of Truth**: Source connectors always derive topics from configuration, not from querying all tables in DataHub
3. **Smart Fallback**: Sink connectors can query DataHub for Kafka topics when Kafka API is unavailable
4. **Pattern Expansion**: Wildcards in `table.include.list` and `topics.regex` are properly expanded
5. **Transform Support**: All transforms (RegexRouter, EventRouter, etc.) are applied correctly

##### When DataHub Topic Querying is Used

DataHub will query for topics in these scenarios:

**Source Connectors:**

- When expanding wildcard patterns in `table.include.list` (e.g., `ANALYTICS.PUBLIC.*`)
- Queries source platform (PostgreSQL, MySQL, etc.) for tables matching the pattern

**Sink Connectors:**

- When `topics.regex` is used AND Kafka API is unavailable (Confluent Cloud)
- Queries DataHub's Kafka platform for topics matching the regex pattern
- Requires `use_schema_resolver: true` in configuration

**Important Notes:**

- DataHub never queries "all tables" to create lineages - config is always the source of truth
- Source connectors query source platforms (databases) to expand table patterns
- Sink connectors query Kafka platform to expand topic regex patterns
- Both require appropriate DataHub credentials and connectivity

#### Using DataHub Schema Resolver for Pattern Expansion and Column-Level Lineage

The Kafka Connect source can query DataHub for schema information to provide two capabilities:

1. **Pattern Expansion** - Converts wildcard patterns like `database.*` into actual table names by querying DataHub
2. **Column-Level Lineage** - Generates field-level lineage by matching schemas between source tables and Kafka topics

Both features require existing metadata in DataHub from your database and Kafka schema registry ingestion.

##### Auto-Enabled for Confluent Cloud

**Starting with the latest version**, `use_schema_resolver` is **automatically enabled** for Confluent Cloud environments to provide better defaults for enhanced lineage extraction. This gives you column-level lineage and pattern expansion out of the box!

**Confluent Cloud (Auto-Enabled):**

```yml
source:
  type: kafka-connect
  config:
    # Confluent Cloud environment
    confluent_cloud_environment_id: "env-xyz123"
    confluent_cloud_cluster_id: "lkc-abc456"
    username: "your-connect-api-key"
    password: "your-connect-api-secret"

    # Schema resolver automatically enabled! ✓
    # use_schema_resolver: true (auto-enabled)
    # schema_resolver_expand_patterns: true (auto-enabled)
    # schema_resolver_finegrained_lineage: true (auto-enabled)
```

**To disable** (if you don't need these features):

```yml
source:
  type: kafka-connect
  config:
    confluent_cloud_environment_id: "env-xyz123"
    confluent_cloud_cluster_id: "lkc-abc456"
    use_schema_resolver: false # Explicitly disable auto-enable
```

**Self-hosted (Manual Enable Required):**

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"

    # Must explicitly enable for self-hosted
    use_schema_resolver: true

    # DataHub connection
    datahub_api:
      server: "http://localhost:8080"
```

**Important Prerequisites:**

> **⚠️ Source database tables must be ingested into DataHub BEFORE running Kafka Connect ingestion**
>
> The schema resolver queries DataHub for existing table metadata. If your source databases haven't been ingested yet, the feature will have no effect. Run database ingestion first!

**Recommended Ingestion Order:**

1. Ingest source databases (PostgreSQL, MySQL, Snowflake, etc.) → DataHub
2. Ingest Kafka schema registry (optional, for topic schemas) → DataHub
3. Run Kafka Connect ingestion → Enjoy enhanced lineage!

##### Configuration Overview

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"

    # Enable DataHub schema querying (auto-enabled for Confluent Cloud)
    use_schema_resolver: true

    # Control which features to use (both default to true when schema resolver enabled)
    schema_resolver_expand_patterns: true # Expand wildcard patterns
    schema_resolver_finegrained_lineage: true # Generate column-level lineage

    # DataHub connection (required when use_schema_resolver=true)
    datahub_api:
      server: "http://localhost:8080"
      token: "your-datahub-token" # Optional
```

##### Pattern Expansion

Converts wildcard patterns in connector configurations into actual table names by querying DataHub.

**Example: MySQL Source with Wildcards**

```yml
# Connector config contains pattern
connector.config:
  table.include.list: "analytics.user_*" # Pattern: matches user_events, user_profiles, etc.

# DataHub config
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true
# Result: DataHub queries for MySQL tables matching "analytics.user_*"
# Finds: user_events, user_profiles, user_sessions
# Creates lineage:
#   mysql.analytics.user_events -> kafka.server.analytics.user_events
#   mysql.analytics.user_profiles -> kafka.server.analytics.user_profiles
#   mysql.analytics.user_sessions -> kafka.server.analytics.user_sessions
```

**When to use:**

- Connector configs have wildcard patterns (`database.*`, `schema.table_*`)
- You want accurate lineage without manually listing every table
- Source metadata exists in DataHub from database ingestion

**When to skip:**

- Connector configs use explicit table lists (no patterns)
- Source metadata not yet in DataHub
- Want faster ingestion without DataHub API calls

**Configuration:**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true # Enable pattern expansion


    # If you only want column-level lineage but NOT pattern expansion:
    # schema_resolver_expand_patterns: false
```

**Behavior without schema resolver:**
Patterns are treated as literal table names, resulting in potentially incorrect lineage.

##### Column-Level Lineage

Generates field-level lineage by matching column names through the Kafka Connect pipeline. It is supported for both directions:

- **Source connectors** (DB table → Kafka topic): field names are taken from the source table schema in DataHub
- **Sink connectors** (Kafka topic → DB table): field names are taken from the Kafka topic schema in DataHub

**Prerequisites:**

Both the source and target schemas must already be ingested into DataHub before running Kafka Connect ingestion:

- For source connectors: ingest the source database (e.g. run the Postgres source)
- For sink connectors: ingest both the Kafka topics (e.g. run the Kafka source) and the destination database

**Example: PostgreSQL to Kafka CDC (source direction)**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_finegrained_lineage: true
# Source table schema in DataHub:
# postgres.public.users: [user_id, email, created_at, updated_at]

# Kafka topic schema in DataHub:
# kafka.server.public.users: [user_id, email, created_at, updated_at]

# Result: Column-level lineage created:
#   postgres.public.users.user_id -> kafka.server.public.users.user_id
#   postgres.public.users.email -> kafka.server.public.users.email
#   postgres.public.users.created_at -> kafka.server.public.users.created_at
#   postgres.public.users.updated_at -> kafka.server.public.users.updated_at
```

Column matching is case-insensitive, so Kafka fields with lowercase names (e.g. `order_id`) will match Snowflake columns stored in uppercase (`ORDER_ID`).

Topic routing transforms (RegexRouter, EventRouter, etc.) work transparently — the transform pipeline correctly identifies which topic maps to which dataset, and column-level lineage operates on those resolved pairs.

**Field-Level Transform Support:**

The following field-level transforms are applied when building the column mapping:

| Transform                             | Effect on column lineage                                                                              |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `ReplaceField$Value`                  | include/exclude filter and field renames are respected                                                |
| `ExtractField$Value`                  | sub-fields of the extracted struct are promoted as top-level (e.g. Debezium `field=after` unwrapping) |
| `HoistField$Value`                    | all fields are nested under the new struct field (e.g. `id` → `data.id`)                              |
| `Flatten$Value`                       | nested struct paths are joined using the configured delimiter (default: `.`)                          |
| `MaskField`, `Cast`, `Filter`, `Drop` | field names unchanged — lineage is unaffected                                                         |

For example, a `ReplaceField` that excludes internal columns:

```yml
transforms: "removeFields"
transforms.removeFields.type: "org.apache.kafka.connect.transforms.ReplaceField$Value"
transforms.removeFields.exclude: "internal_id,temp_column"
# Source schema: [user_id, email, internal_id, temp_column]
# After transform: [user_id, email]
# Column lineage created only for: user_id, email
```

**Configuration:**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_finegrained_lineage: true # Enable column-level lineage


    # If you only want pattern expansion but NOT column-level lineage:
    # schema_resolver_finegrained_lineage: false
```

**Behavior without schema resolver:**
Only dataset-level lineage is created (e.g., `postgres.users -> kafka.users`), without field-level detail.

##### Complete Configuration Example

```yml
source:
  type: kafka-connect
  config:
    # Kafka Connect cluster
    connect_uri: "http://localhost:8083"
    cluster_name: "production-connect"

    # Enable schema resolver features
    use_schema_resolver: true
    schema_resolver_expand_patterns: true # Expand wildcard patterns
    schema_resolver_finegrained_lineage: true # Generate column-level lineage

    # DataHub connection
    datahub_api:
      server: "http://datahub.company.com"
      token: "${DATAHUB_TOKEN}"

    # Platform instances (if using multiple)
    platform_instance_map:
      postgres: "prod-postgres"
      kafka: "prod-kafka"
```

##### Performance Impact

**API Calls per Connector:**

- Pattern expansion: 1 GraphQL query per unique wildcard pattern
- Column-level lineage: 2 GraphQL queries (source schema + target schema)
- Results cached for ingestion run duration

**Optimization:**

```yml
# Minimal configuration - no schema resolver
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    # use_schema_resolver: false  # Default - no DataHub queries

# Pattern expansion only
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true
    schema_resolver_finegrained_lineage: false  # Skip column lineage for faster ingestion

# Column lineage only
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: false      # Skip pattern expansion
    schema_resolver_finegrained_lineage: true
```

**Best Practice:**
Run database and Kafka schema ingestion before Kafka Connect ingestion to pre-populate DataHub with schema metadata.

##### Troubleshooting

**"Pattern expansion found no matches for: analytics.\*"**

Causes:

- Source database metadata not in DataHub
- Pattern syntax doesn't match DataHub dataset names
- Platform instance mismatch

Solutions:

1. Run database ingestion first to populate DataHub
2. Verify pattern matches table naming in source system
3. Check `platform_instance_map` matches database ingestion config
4. Use explicit table list to bypass pattern expansion temporarily

**"SchemaResolver not available: DataHub graph connection is not available"**

Causes:

- Missing `datahub_api` configuration
- DataHub GMS not accessible

Solutions:

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    datahub_api:
      server: "http://localhost:8080" # Add DataHub GMS URL
      token: "your-token" # Add if authentication enabled
```

**Column-level lineage not appearing**

Check:

1. Source table schema exists: Search for table in DataHub UI
2. Kafka topic schema exists: Search for topic in DataHub UI
3. Column names match (case differences are handled automatically)
4. Check ingestion logs for warnings about missing schemas

**Slow ingestion with schema resolver enabled**

Profile:

- Check logs for "Schema resolver cache hits: X, misses: Y"
- High misses indicate missing metadata in DataHub

Temporarily disable to compare:

```yml
use_schema_resolver: false
```

#### Working with Platform Instances

If you've multiple instances of kafka OR source/sink systems that are referred in your `kafka-connect` setup, you'd need to configure platform instance for these systems in `kafka-connect` recipe to generate correct lineage edges. You must have already set `platform_instance` in recipes of original source/sink systems. Refer the document [Working with Platform Instances](/docs/platform-instances) to understand more about this.

There are two options available to declare source/sink system's `platform_instance` in `kafka-connect` recipe. If single instance of platform is used across all `kafka-connect` connectors, you can use `platform_instance_map` to specify platform_instance to use for a platform when constructing URNs for lineage.

Example:

```yml
# Map of platform name to platform instance
platform_instance_map:
  snowflake: snowflake_platform_instance
  mysql: mysql_platform_instance
```

If multiple instances of platform are used across `kafka-connect` connectors, you'd need to specify platform_instance to use for platform for every connector.

##### Example - Multiple MySQL Source Connectors each reading from different mysql instance

```yml
# Map of platform name to platform instance per connector
connect_to_platform_map:
  mysql_connector1:
    mysql: mysql_instance1

  mysql_connector2:
    mysql: mysql_instance2
```

Here mysql_connector1 and mysql_connector2 are names of MySQL source connectors as defined in `kafka-connect` connector config.

##### Example - Multiple MySQL Source Connectors each reading from difference mysql instance and writing to different kafka cluster

```yml
connect_to_platform_map:
  mysql_connector1:
    mysql: mysql_instance1
    kafka: kafka_instance1

  mysql_connector2:
    mysql: mysql_instance2
    kafka: kafka_instance2
```

You can also use combination of `platform_instance_map` and `connect_to_platform_map` in your recipe. Note that, the platform_instance specified for the connector in `connect_to_platform_map` will always take higher precedance even if platform_instance for same platform is set in `platform_instance_map`.

If you do not use `platform_instance` in original source/sink recipes, you do not need to specify them in above configurations.

Note that, you do not need to specify platform_instance for BigQuery.

##### Example - Multiple BigQuery Sink Connectors each writing to different kafka cluster

```yml
connect_to_platform_map:
  bigquery_connector1:
    kafka: kafka_instance1

  bigquery_connector2:
    kafka: kafka_instance2
```

#### Provided Configurations from External Sources

Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
# Optional mapping of provider configurations if using
provided_configs:
  - provider: env
    path_key: MYSQL_CONNECTION_URL
    value: jdbc:mysql://test_mysql:3306/librarydb
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Air-gapped or Performance-Optimized Environments

Disable topic discovery entirely for environments where API access is not available or not needed:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false # Disables all topic discovery API calls
```

**Note**: When `use_connect_topics_api` is `false`, topic information will not be extracted, which may impact lineage accuracy but improves performance and works in air-gapped environments.

#### Topic Discovery Issues

**Problem**: Missing or incomplete topic information in lineage

**Solutions**:

1. **Verify Environment Detection**:

   ```bash
   # Check logs for environment detection messages
   # Self-hosted: "Detected self-hosted Kafka Connect - using runtime topics API"
   # Confluent Cloud: "Detected Confluent Cloud - using comprehensive Kafka REST API topic retrieval"
   ```

2. **Test API Connectivity**:

   ```bash
   # For self-hosted - test topics API
   curl -X GET "http://localhost:8083/connectors/{connector-name}/topics"

   # For Confluent Cloud - test Kafka REST API v3
   curl -X GET "https://pkc-xxxxx.region.provider.confluent.cloud/kafka/v3/clusters/{cluster-id}/topics"
   ```

3. **Configuration Troubleshooting**:
   ```yml
   # Enable debug logging
   source:
     type: kafka-connect
     config:
       # ... other config ...
       use_connect_topics_api: true # Ensure this is enabled (default)
   ```

#### Environment-Specific Issues

**Self-hosted Issues**:

- **403/401 errors**: Check authentication credentials (`username`, `password`)
- **404 errors**: Verify Kafka Connect cluster is running and REST API is accessible
- **Empty topic lists**: Check if connectors are actually running and processing data

**Confluent Cloud Issues**:

- **Missing topics**: Verify connector configuration has proper source table fields (`table.include.list`, `query`)
- **Transform accuracy**: Check that RegexRouter patterns in connector config are valid Java regex
- **Complex transforms**: Now fully supported via forward transform pipeline with topic validation
- **Schema preservation**: Full schema information (e.g., `public.users`) is maintained through transform pipeline

#### Performance Optimization

If topic discovery is impacting performance:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false # Disable for better performance (no topic info)
```


### Code Coordinates
- Class Name: `datahub.ingestion.source.kafka_connect.kafka_connect.KafkaConnectSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/kafka_connect/kafka_connect.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Kafka Connect, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
