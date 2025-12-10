---
sidebar_position: 34
title: Kafka
slug: /generated/ingestion/sources/kafka
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/kafka.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Kafka
Extract Topics & Schemas from Apache Kafka or Confluent Cloud.
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ❌ | Not supported. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ❌ | Not supported. |
| Descriptions | ✅ | Set dataset description to top level doc field for Avro schema. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | For multiple Kafka clusters, use the platform_instance configuration. |
| Schema Metadata | ✅ | Schemas associated with each topic are extracted from the schema registry. Avro and Protobuf (certified), JSON (incubating). Schema references are supported. |
| Table-Level Lineage | ❌ | Not supported. If you use Kafka Connect, the kafka-connect source can generate lineage. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:
- Topics from the Kafka broker
- Schemas associated with each topic from the schema registry (Avro, Protobuf and JSON schemas are supported)


### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "kafka"
  config:
    platform_instance: "YOUR_CLUSTER_ID"
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081

sink:
  # sink configs


```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">disable_topic_record_naming_strategy</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Disables the utilization of the TopicRecordNameStrategy for Schema Registry subjects. For more information, visit: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#handling-differences-between-preregistered-and-client-derived-schemas:~:text=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, applies the mappings that are defined through the meta_mapping directives. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">external_url_base</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Base URL for external platform (e.g. Aiven) where topics can be viewed. The topic name will be appended to this base URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">field_meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | mapping rules that will be executed against field-level schema properties. Refer to the section below on meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">ignore_warnings_on_schema_type</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Disables warnings reported for non-AVRO/Protobuf value or key schemas if set. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_schemas_as_entities</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enables ingesting schemas from schema registry as separate entities, in addition to the topics <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">meta_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | mapping rules that will be executed against top-level schema properties. Refer to the section below on meta automated mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_registry_class</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The fully qualified implementation class(custom) that implements the KafkaSchemaRegistryBase interface. <div className="default-line default-line-with-docs">Default: <span className="default-value">datahub.ingestion.source.confluent&#95;schema&#95;registry...</span></div> |
| <div className="path-line"><span className="path-main">schema_tags_field</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The field name in the schema metadata that contains the tags to be added to the dataset. <div className="default-line default-line-with-docs">Default: <span className="default-value">tags</span></div> |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to strip email id while adding owners using meta mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Prefix added to tags during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">topic_subject_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection</span></div> <div className="type-name-line"><span className="type-name">KafkaConsumerConnectionConfig</span></div> | Configuration class for holding connectivity information for Kafka consumers  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">bootstrap</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">localhost:9092</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">client_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The request timeout used when interacting with the Kafka APIs. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">consumer_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Extra consumer config serialized as JSON. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">schema_registry_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Extra schema registry config serialized as JSON. These options will be passed into Kafka's SchemaRegistryClient. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient  |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">schema_registry_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Schema registry URL. Can be overridden with KAFKA_SCHEMAREGISTRY_URL environment variable, or will use DATAHUB_GMS_BASE_PATH if not set.  |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">topic_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

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
    "KafkaConsumerConnectionConfig": {
      "additionalProperties": false,
      "description": "Configuration class for holding connectivity information for Kafka consumers",
      "properties": {
        "bootstrap": {
          "default": "localhost:9092",
          "title": "Bootstrap",
          "type": "string"
        },
        "schema_registry_url": {
          "description": "Schema registry URL. Can be overridden with KAFKA_SCHEMAREGISTRY_URL environment variable, or will use DATAHUB_GMS_BASE_PATH if not set.",
          "title": "Schema Registry Url",
          "type": "string"
        },
        "schema_registry_config": {
          "additionalProperties": true,
          "description": "Extra schema registry config serialized as JSON. These options will be passed into Kafka's SchemaRegistryClient. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient",
          "title": "Schema Registry Config",
          "type": "object"
        },
        "client_timeout_seconds": {
          "default": 60,
          "description": "The request timeout used when interacting with the Kafka APIs.",
          "title": "Client Timeout Seconds",
          "type": "integer"
        },
        "consumer_config": {
          "additionalProperties": true,
          "description": "Extra consumer config serialized as JSON. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .",
          "title": "Consumer Config",
          "type": "object"
        }
      },
      "title": "KafkaConsumerConnectionConfig",
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
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
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
      "default": null
    },
    "connection": {
      "$ref": "#/$defs/KafkaConsumerConnectionConfig",
      "default": {
        "bootstrap": "localhost:9092",
        "schema_registry_url": "http://localhost:8080/schema-registry/api/",
        "schema_registry_config": {},
        "client_timeout_seconds": 60,
        "consumer_config": {}
      }
    },
    "topic_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*"
        ],
        "ignoreCase": true
      }
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "A map of domain names to allow deny patterns. Domains can be urn-based (`urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810`) or bare (`13ae4d85-d955-49fc-8474-9004c663a810`).",
      "title": "Domain",
      "type": "object"
    },
    "topic_subject_map": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Provides the mapping for the `key` and the `value` schemas of a topic to the corresponding schema registry subject name. Each entry of this map has the form `<topic_name>-key`:`<schema_registry_subject_name_for_key_schema>` and `<topic_name>-value`:`<schema_registry_subject_name_for_value_schema>` for the key and the value schemas associated with the topic, respectively. This parameter is mandatory when the [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work) is used as the subject naming strategy in the kafka schema registry. NOTE: When provided, this overrides the default subject name resolution even when the `TopicNameStrategy` or the `TopicRecordNameStrategy` are used.",
      "title": "Topic Subject Map",
      "type": "object"
    },
    "schema_registry_class": {
      "default": "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry",
      "description": "The fully qualified implementation class(custom) that implements the KafkaSchemaRegistryBase interface.",
      "title": "Schema Registry Class",
      "type": "string"
    },
    "schema_tags_field": {
      "default": "tags",
      "description": "The field name in the schema metadata that contains the tags to be added to the dataset.",
      "title": "Schema Tags Field",
      "type": "string"
    },
    "enable_meta_mapping": {
      "default": true,
      "description": "When enabled, applies the mappings that are defined through the meta_mapping directives.",
      "title": "Enable Meta Mapping",
      "type": "boolean"
    },
    "meta_mapping": {
      "additionalProperties": true,
      "default": {},
      "description": "mapping rules that will be executed against top-level schema properties. Refer to the section below on meta automated mappings.",
      "title": "Meta Mapping",
      "type": "object"
    },
    "field_meta_mapping": {
      "additionalProperties": true,
      "default": {},
      "description": "mapping rules that will be executed against field-level schema properties. Refer to the section below on meta automated mappings.",
      "title": "Field Meta Mapping",
      "type": "object"
    },
    "strip_user_ids_from_email": {
      "default": false,
      "description": "Whether or not to strip email id while adding owners using meta mappings.",
      "title": "Strip User Ids From Email",
      "type": "boolean"
    },
    "tag_prefix": {
      "default": "",
      "description": "Prefix added to tags during ingestion.",
      "title": "Tag Prefix",
      "type": "string"
    },
    "ignore_warnings_on_schema_type": {
      "default": false,
      "description": "Disables warnings reported for non-AVRO/Protobuf value or key schemas if set.",
      "title": "Ignore Warnings On Schema Type",
      "type": "boolean"
    },
    "disable_topic_record_naming_strategy": {
      "default": false,
      "description": "Disables the utilization of the TopicRecordNameStrategy for Schema Registry subjects. For more information, visit: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#handling-differences-between-preregistered-and-client-derived-schemas:~:text=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
      "title": "Disable Topic Record Naming Strategy",
      "type": "boolean"
    },
    "ingest_schemas_as_entities": {
      "default": false,
      "description": "Enables ingesting schemas from schema registry as separate entities, in addition to the topics",
      "title": "Ingest Schemas As Entities",
      "type": "boolean"
    },
    "external_url_base": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Base URL for external platform (e.g. Aiven) where topics can be viewed. The topic name will be appended to this base URL.",
      "title": "External Url Base"
    }
  },
  "title": "KafkaSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

:::note
Stateful Ingestion is available only when a Platform Instance is assigned to this source.
:::

### Connecting to Confluent Cloud

If using Confluent Cloud you can use a recipe like this. In this `consumer_config.sasl.username` and `consumer_config.sasl.password` are the API credentials that you get (in the Confluent UI) from your cluster -> Data Integration -> API Keys. `schema_registry_config.basic.auth.user.info` has API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.

When creating API Key for the cluster ensure that the ACLs associated with the key are set like below. This is required for DataHub to read topic metadata from topics in Confluent Cloud.

```
Topic Name = *
Permission = ALLOW
Operation = DESCRIBE
Pattern Type = LITERAL
```

```yml
source:
  type: "kafka"
  config:
    platform_instance: "YOUR_CLUSTER_ID"
    connection:
      bootstrap: "abc-defg.eu-west-1.aws.confluent.cloud:9092"
      consumer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "PLAIN"
        sasl.username: "${CLUSTER_API_KEY_ID}"
        sasl.password: "${CLUSTER_API_KEY_SECRET}"
      schema_registry_url: "https://abc-defgh.us-east-2.aws.confluent.cloud"
      schema_registry_config:
        basic.auth.user.info: "${REGISTRY_API_KEY_ID}:${REGISTRY_API_KEY_SECRET}"

sink:
  # sink configs
```

If you are trying to add domains to your topics you can use a configuration like below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    domain:
      "urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810":
        allow:
          - ".*"
      "urn:li:domain:d6ec9868-6736-4b1f-8aa6-fee4c5948f17":
        deny:
          - ".*"
```

Note that the `domain` in config above can be either an _urn_ or a domain _id_ (i.e. `urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810` or simply `13ae4d85-d955-49fc-8474-9004c663a810`). The Domain should exist in your DataHub instance before ingesting data into the Domain. To create a Domain on DataHub, check out the [Domains User Guide](/docs/domains/).

If you are using a non-default subject naming strategy in the schema registry, such as [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work), the mapping for the topic's key and value schemas to the schema registry subject names should be provided via `topic_subject_map` as shown in the configuration below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    # Defines the mapping for the key & value schemas associated with a topic & the subject name registered with the
    # kafka schema registry.
    topic_subject_map:
      # Defines both key & value schema for topic 'my_topic_1'
      "my_topic_1-key": "io.acryl.Schema1"
      "my_topic_1-value": "io.acryl.Schema2"
      # Defines only the value schema for topic 'my_topic_2' (the topic doesn't have a key schema).
      "my_topic_2-value": "io.acryl.Schema3"
```

### Custom Schema Registry

The Kafka Source uses the schema registry to figure out the schema associated with both `key` and `value` for the topic.
By default it uses the [Confluent's Kafka Schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
and supports the `AVRO` and `PROTOBUF` schema types.

If you're using a custom schema registry, or you are using schema type other than `AVRO` or `PROTOBUF`, then you can provide your own
custom implementation of the `KafkaSchemaRegistryBase` class, and implement the `get_schema_metadata(topic, platform_urn)` method that
given a topic name would return object of `SchemaMetadata` containing schema for that topic. Please refer
`datahub.ingestion.source.confluent_schema_registry::ConfluentSchemaRegistry` for sample implementation of this class.

```python
class KafkaSchemaRegistryBase(ABC):
    @abstractmethod
    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        pass
```

The custom schema registry class can be configured using the `schema_registry_class` config param of the `kafka` source as shown below.

```YAML
source:
  type: "kafka"
  config:
    # Set the custom schema registry implementation class
    schema_registry_class: "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry"
    # Coordinates
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081
```

### OAuth Callback

The OAuth callback function can be set up using `config.connection.consumer_config.oauth_cb`.

You need to specify a Python function reference in the format &lt;python-module&gt;:&lt;function-name&gt;.

For example, in the configuration `oauth:create_token`, `create_token` is a function defined in `oauth.py`, and `oauth.py` must be accessible in the PYTHONPATH.

```YAML
source:
  type: "kafka"
  config:
    # Set the custom schema registry implementation class
    schema_registry_class: "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry"
    # Coordinates
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081
      consumer_config:
        security.protocol: "SASL_PLAINTEXT"
        sasl.mechanism: "OAUTHBEARER"
        oauth_cb: "oauth:create_token"
# sink configs
```

### Limitations of `PROTOBUF` schema types implementation

The current implementation of the support for `PROTOBUF` schema type has the following limitations:

- Recursive types are not supported.
- If the schemas of different topics define a type in the same package, the source would raise an exception.

In addition to this, maps are represented as arrays of messages. The following message,

```
message MessageWithMap {
  map<int, string> map_1 = 1;
}
```

becomes:

```
message Map1Entry {
  int key = 1;
  string value = 2/
}
message MessageWithMap {
  repeated Map1Entry map_1 = 1;
}
```

### Enriching DataHub metadata with automated meta mapping

:::note
Meta mapping is currently only available for Avro schemas, and requires that those Avro schemas are pushed to the schema registry.
:::

Avro schemas are permitted to have additional attributes not defined by the specification as arbitrary metadata. A common pattern is to utilize this for business metadata. The Kafka source has the ability to transform this directly into DataHub Owners, Tags and Terms.

#### Simple tags

If you simply have a list of tags embedded into an Avro schema (either at the top-level or for an individual field), you can use the `schema_tags_field` config.

Example Avro schema:

```json
{
  "name": "sampleRecord",
  "type": "record",
  "tags": ["tag1", "tag2"],
  "fields": [
    {
      "name": "field_1",
      "type": "string",
      "tags": ["tag3", "tag4"]
    }
  ]
}
```

The name of the field containing a list of tags can be configured with the `schema_tags_field` property:

```yaml
config:
  schema_tags_field: tags
```

#### meta mapping

You can also map specific Avro fields into Owners, Tags and Terms using meta
mapping.

Example Avro schema:

```json
{
  "name": "sampleRecord",
  "type": "record",
  "owning_team": "@Data-Science",
  "data_tier": "Bronze",
  "fields": [
    {
      "name": "field_1",
      "type": "string",
      "gdpr": {
        "pii": true
      }
    }
  ]
}
```

This can be mapped to DataHub metadata with `meta_mapping` config:

```yaml
config:
  meta_mapping:
    owning_team:
      match: "^@(.*)"
      operation: "add_owner"
      config:
        owner_type: group
    data_tier:
      match: "Bronze|Silver|Gold"
      operation: "add_term"
      config:
        term: "{{ $match }}"
  field_meta_mapping:
    gdpr.pii:
      match: true
      operation: "add_tag"
      config:
        tag: "pii"
```

The underlying implementation is similar to [dbt meta mapping](/docs/generated/ingestion/sources/dbt#dbt-meta-automated-mappings), which has more detailed examples that can be used for reference.

### Code Coordinates
- Class Name: `datahub.ingestion.source.kafka.kafka.KafkaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Kafka, feel free to ping us on [our Slack](https://datahub.com/slack).
