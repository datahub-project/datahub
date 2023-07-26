---
sidebar_position: 22
title: Kafka Connect
slug: /generated/ingestion/sources/kafka-connect
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/kafka-connect.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Kafka Connect

## Integration Details

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                                                                  | DataHub Concept                                                   | Notes |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ----- |
| `"kafka-connect"`                                                               | [Data Platform](/docs/generated/metamodel/entities/dataPlatform/) |       |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks) | [DataFlow](/docs/generated/metamodel/entities/dataflow/)          |       |
| Kafka Topic                                                                     | [Dataset](/docs/generated/metamodel/entities/dataset/)            |       |

## Current limitations

Works only for

- Source connectors: JDBC, Debezium, Mongo and Generic connectors with user-defined lineage graph
- Sink connectors: BigQuery
  ![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                          | Status | Notes              |
| --------------------------------------------------- | ------ | ------------------ |
| [Platform Instance](../../../platform-instances.md) | ✅     | Enabled by default |

### CLI based Ingestion

#### Install the Plugin

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
    platform_instance_map:
      bigquery: bigquery_platform_instance_id

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                          | Description                                                                                                                                                                                                                                                                            |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">cluster_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                               | Cluster to ingest from. <div className="default-line default-line-with-docs">Default: <span className="default-value">connect-cluster</span></div>                                                                                                                                     |
| <div className="path-line"><span className="path-main">connect_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,map)</span></div>                                                                                                              |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                | URI to connect to. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:8083/</span></div>                                                                                                                                   |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                         | Whether to convert the urns of ingested lineage dataset to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                  |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                   | Kafka Connect password.                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                          | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                                                                             |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                   | Kafka Connect username.                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                        | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                   |
| <div className="path-line"><span className="path-main">connector_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                               | regex patterns for connectors to filter for ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                       |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                        |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">connector_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                        | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                            |
| <div className="path-line"><span className="path-main">generic_connectors</span></div> <div className="type-name-line"><span className="type-name">array(object)</span></div>                                                                                                                  |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">generic_connectors.</span><span className="path-main">connector_name</span>&nbsp;<abbr title="Required if generic_connectors is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>  |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">generic_connectors.</span><span className="path-main">source_dataset</span>&nbsp;<abbr title="Required if generic_connectors is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>  |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">generic_connectors.</span><span className="path-main">source_platform</span>&nbsp;<abbr title="Required if generic_connectors is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">provided_configs</span></div> <div className="type-name-line"><span className="type-name">array(object)</span></div>                                                                                                                    |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">provided_configs.</span><span className="path-main">path_key</span>&nbsp;<abbr title="Required if provided_configs is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>            |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">provided_configs.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if provided_configs is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>            |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">provided_configs.</span><span className="path-main">value</span>&nbsp;<abbr title="Required if provided_configs is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>               |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                             | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                           | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                     |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                             | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                           |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "KafkaConnectSourceConfig",
  "description": "Any source that connects to a platform should inherit this class",
  "type": "object",
  "properties": {
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance_map": {
      "title": "Platform Instance Map",
      "description": "Platform instance mapping to use when constructing URNs. e.g.`platform_instance_map: { \"hive\": \"warehouse\" }`",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "connect_uri": {
      "title": "Connect Uri",
      "description": "URI to connect to.",
      "default": "http://localhost:8083/",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "Kafka Connect username.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Kafka Connect password.",
      "type": "string"
    },
    "cluster_name": {
      "title": "Cluster Name",
      "description": "Cluster to ingest from.",
      "default": "connect-cluster",
      "type": "string"
    },
    "convert_lineage_urns_to_lowercase": {
      "title": "Convert Lineage Urns To Lowercase",
      "description": "Whether to convert the urns of ingested lineage dataset to lowercase",
      "default": false,
      "type": "boolean"
    },
    "connector_patterns": {
      "title": "Connector Patterns",
      "description": "regex patterns for connectors to filter for ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "provided_configs": {
      "title": "Provided Configs",
      "description": "Provided Configurations",
      "type": "array",
      "items": {
        "$ref": "#/definitions/ProvidedConfig"
      }
    },
    "connect_to_platform_map": {
      "title": "Connect To Platform Map",
      "description": "Platform instance mapping when multiple instances for a platform is available. Entry for a platform should be in either `platform_instance_map` or `connect_to_platform_map`. e.g.`connect_to_platform_map: { \"postgres-connector-finance-db\": \"postgres\": \"core_finance_instance\" }`",
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "additionalProperties": {
          "type": "string"
        }
      }
    },
    "generic_connectors": {
      "title": "Generic Connectors",
      "description": "Provide lineage graph for sources connectors other than Confluent JDBC Source Connector, Debezium Source Connector, and Mongo Source Connector",
      "default": [],
      "type": "array",
      "items": {
        "$ref": "#/definitions/GenericConnectorConfig"
      }
    }
  },
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "ProvidedConfig": {
      "title": "ProvidedConfig",
      "type": "object",
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
      "additionalProperties": false
    },
    "GenericConnectorConfig": {
      "title": "GenericConnectorConfig",
      "type": "object",
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
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

## Advanced Configurations

Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
# Optional mapping of provider configurations if using
provided_configs:
  - provider: env
    path_key: MYSQL_CONNECTION_URL
    value: jdbc:mysql://test_mysql:3306/librarydb
```

### Code Coordinates

- Class Name: `datahub.ingestion.source.kafka_connect.KafkaConnectSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/kafka_connect.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Kafka Connect, feel free to ping us on [our Slack](https://slack.datahubproject.io).
