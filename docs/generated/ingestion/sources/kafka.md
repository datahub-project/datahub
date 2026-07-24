


# Kafka

## Overview

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform used for high-throughput, fault-tolerant messaging and real-time data pipelines. Confluent Cloud provides a fully managed Kafka service with an integrated Schema Registry.

The DataHub Kafka connector extracts topic metadata and schemas from Kafka clusters and Confluent Schema Registry. It supports Avro, Protobuf, and JSON schemas, multi-stage schema resolution with automatic fallback and inference for topics not registered in the schema registry, and optional data profiling to generate field-level statistics and sample values from message content. The integration also captures stateful deletion detection.

## Concept Mapping

| Kafka Concept    | DataHub Concept                                                                                                                                                                  | Notes                                                                                                                                       |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Topic            | [Dataset](../../metamodel/entities/dataset.md)                                                                                                                                   | Subtype `Topic`                                                                                                                             |
| Schema (Subject) | [Schema Metadata](../../metamodel/entities/dataset.md#schema)                                                                                                                    | Avro, Protobuf, JSON schemas                                                                                                                |
| Message Fields   | [Dataset Fields](../../metamodel/entities/dataset.md#fields)                                                                                                                     | Extracted from schemas or inferred                                                                                                          |
| Kafka Cluster    | [Data Platform Instance](../../../platform-instances.md)                                                                                                                         | When `platform_instance` is configured                                                                                                      |
| Schema metadata  | Tags, [Glossary Terms](../../metamodel/entities/glossaryTerm.md), Owners ([CorpUser](../../metamodel/entities/corpuser.md) / [CorpGroup](../../metamodel/entities/corpGroup.md)) | Optional, Avro only: derived from schema properties when `enable_meta_mapping` is set with `meta_mapping` / `field_meta_mapping` directives |


## Module `kafka`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ❌ | Not supported. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration `profiling.enabled`. |
| Descriptions | ✅ | Set dataset description to top level doc field for Avro schema. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | For multiple Kafka clusters, use the platform_instance configuration. |
| Schema Metadata | ✅ | Schemas associated with each topic are extracted from the schema registry. Avro and Protobuf (certified), JSON (incubating). Schema references are supported. |
| Table-Level Lineage | ❌ | Not supported. If you use Kafka Connect, the kafka-connect source can generate lineage. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `kafka` module ingests metadata from Kafka into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extract Topics & Schemas from Apache Kafka or Confluent Cloud.

This plugin extracts the following:

- Topics from the Kafka broker
- Schemas associated with each topic from the schema registry (Avro, Protobuf and JSON schemas are supported)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[kafka]'
```

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

    # Optional: Enable data profiling
    profiling:
      enabled: true
      sample_size: 1000
      max_sample_time_seconds: 60
      sampling_strategy: "latest"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
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
| <div className="path-line"><span className="path-main">schema_resolution</span></div> <div className="type-name-line"><span className="type-name">SchemaResolutionFallback</span></div> |   |
| <div className="path-line"><span className="path-prefix">schema_resolution.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable comprehensive schema resolution with multiple fallback strategies for topics where schema registry lookup fails. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">schema_resolution.</span><span className="path-main">max_messages_per_topic</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of messages to sample per topic for record name extraction and schema inference. Must be positive. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">schema_resolution.</span><span className="path-main">offset_reset_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "earliest", "latest", "hybrid"  |
| <div className="path-line"><span className="path-prefix">schema_resolution.</span><span className="path-main">sample_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Maximum time to spend sampling messages from a single topic (in seconds) for record name extraction and schema inference. Must be positive. <div className="default-line default-line-with-docs">Default: <span className="default-value">2.0</span></div> |
| <div className="path-line"><span className="path-main">topic_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ProfilerConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of messages to fetch in a single batch (for more efficient reading). Must be positive. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_sample_time_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum time to spend sampling messages in seconds. Must be positive. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ge", "sqlalchemy" <div className="default-line default-line-with-docs">Default: <span className="default-value">sqlalchemy</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">nested_field_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of messages to sample for profiling. Higher values provide more accurate statistics but take longer to process. Must be positive. <div className="default-line default-line-with-docs">Default: <span className="default-value">200</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sampling_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "latest", "random", "stratified", "full"  |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
    "OffsetResetStrategy": {
      "enum": [
        "earliest",
        "latest",
        "hybrid"
      ],
      "title": "OffsetResetStrategy",
      "type": "string"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "ProfilerConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "default": "sqlalchemy",
          "description": "Profiling method to use. `sqlalchemy` (default) runs profiling queries directly against your source's existing SQLAlchemy connection. `ge` selects the legacy Great Expectations profiler, which is deprecated and requires `pip install 'acryl-datahub[profiling-ge]'`.",
          "enum": [
            "ge",
            "sqlalchemy"
          ],
          "title": "Method",
          "type": "string"
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 200,
          "description": "Number of messages to sample for profiling. Higher values provide more accurate statistics but take longer to process. Must be positive.",
          "exclusiveMinimum": 0,
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        },
        "max_sample_time_seconds": {
          "default": 60,
          "description": "Maximum time to spend sampling messages in seconds. Must be positive.",
          "exclusiveMinimum": 0,
          "title": "Max Sample Time Seconds",
          "type": "integer"
        },
        "sampling_strategy": {
          "$ref": "#/$defs/SamplingStrategy",
          "default": "latest",
          "description": "Strategy for sampling messages: 'latest' (from end of topic), 'random' (random offsets), 'stratified' (evenly distributed), 'full' (entire topic, respects sample_size)"
        },
        "batch_size": {
          "default": 100,
          "description": "Number of messages to fetch in a single batch (for more efficient reading). Must be positive.",
          "exclusiveMinimum": 0,
          "title": "Batch Size",
          "type": "integer"
        }
      },
      "title": "ProfilerConfig",
      "type": "object"
    },
    "SamplingStrategy": {
      "enum": [
        "latest",
        "random",
        "stratified",
        "full"
      ],
      "title": "SamplingStrategy",
      "type": "string"
    },
    "SchemaResolutionFallback": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Enable comprehensive schema resolution with multiple fallback strategies for topics where schema registry lookup fails.",
          "title": "Enabled",
          "type": "boolean"
        },
        "sample_timeout_seconds": {
          "default": 2.0,
          "description": "Maximum time to spend sampling messages from a single topic (in seconds) for record name extraction and schema inference. Must be positive.",
          "exclusiveMinimum": 0,
          "title": "Sample Timeout Seconds",
          "type": "number"
        },
        "offset_reset_strategy": {
          "$ref": "#/$defs/OffsetResetStrategy",
          "default": "hybrid",
          "description": "Where to start reading when sampling messages for schema inference: 'earliest' (scan from beginning), 'latest' (recent messages only), or 'hybrid' (try latest first, fallback to earliest). Distinct from the profiler's `sampling_strategy`."
        },
        "max_messages_per_topic": {
          "default": 10,
          "description": "Maximum number of messages to sample per topic for record name extraction and schema inference. Must be positive.",
          "exclusiveMinimum": 0,
          "title": "Max Messages Per Topic",
          "type": "integer"
        }
      },
      "title": "SchemaResolutionFallback",
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
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
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
      "$ref": "#/$defs/KafkaConsumerConnectionConfig"
    },
    "topic_patterns": {
      "$ref": "#/$defs/AllowDenyPattern"
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
    "schema_resolution": {
      "$ref": "#/$defs/SchemaResolutionFallback",
      "description": "Configuration for comprehensive schema resolution with multiple fallback strategies."
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
    },
    "profiling": {
      "$ref": "#/$defs/ProfilerConfig",
      "description": "Settings for message sampling and profiling"
    }
  },
  "title": "KafkaSourceConfig",
  "type": "object"
}
```





### Capabilities

:::note
Stateful Ingestion is available only when a Platform Instance is assigned to this source.
:::

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connecting to Confluent Cloud

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

#### Custom Schema Registry

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

```yaml
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

#### OAuth Callback

The OAuth callback function can be set up for both Kafka sources (consumers) and sinks (producers):

- For sources: `config.connection.consumer_config.oauth_cb`
- For sinks: `config.connection.producer_config.oauth_cb`

You need to specify a Python function reference in the format &lt;python-module&gt;:&lt;function-name&gt;.

For example, in the configuration `oauth:create_token`, `create_token` is a function defined in `oauth.py`, and `oauth.py` must be accessible in the PYTHONPATH.

##### Deploying Custom OAuth Callbacks

**For Built-in Callbacks (Recommended):**

DataHub includes pre-built OAuth callbacks for common use cases:

- **AWS MSK IAM**: `datahub_actions.utils.kafka_msk_iam:oauth_cb`
- **Azure Event Hubs**: `datahub_actions.utils.kafka_eventhubs_auth:oauth_cb`

**Important:** To use these built-in callbacks, you must install the `acryl-datahub-actions` package:

```bash
pip install acryl-datahub-actions>=1.3.1.2
```

**For Custom OAuth Callbacks:**

If you need to implement a custom OAuth callback, you must ensure your Python module is accessible to the DataHub process, e.g. adding it via `PYTHONPATH=/path/to/your/module:$PYTHONPATH` or `pip install my-oauth-package`.

**Example for Kafka Source:**

```yaml
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

**Example for Kafka Sink (e.g., MSK IAM authentication):**

```yaml
sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "b-1.msk.us-west-2.amazonaws.com:9098"
      schema_registry_url: "http://datahub-gms:8080/schema-registry/api/"
      producer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "OAUTHBEARER"
        sasl.oauthbearer.method: "default"
        oauth_cb: "datahub_actions.utils.kafka_msk_iam:oauth_cb"
```

#### Enriching DataHub metadata with automated meta mapping

:::note
Meta mapping is currently only available for Avro schemas, and requires that those Avro schemas are pushed to the schema registry.
:::

Avro schemas are permitted to have additional attributes not defined by the specification as arbitrary metadata. A common pattern is to utilize this for business metadata. The Kafka source has the ability to transform this directly into DataHub Owners, Tags and Terms.

##### Simple tags

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

##### meta mapping

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

#### Schema Resolution

DataHub provides multi-stage schema resolution for topics whose schemas are not registered in the schema registry, or that use non-default naming strategies. This feature is **independent** of data profiling.

When `schema_resolution.enabled` is true, DataHub attempts to resolve schemas using the following stages in order:

1. **TopicNameStrategy**: Direct lookup using `<topic>-key/value` pattern (most common)
2. **TopicSubjectMap**: User-defined topic-to-subject mappings via `topic_subject_map` config
3. **RecordNameStrategy**: Extract record names from message data and lookup `<record_name>-key/value`
4. **TopicRecordNameStrategy**: Combine topic + record name: `<topic>-<record_name>-key/value`
5. **Schema Inference**: As final fallback, infer schema from message data analysis

This ensures maximum compatibility with different Confluent Schema Registry [naming strategies](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work).

```yaml
source:
  type: kafka
  config:
    schema_resolution:
      enabled: true # disabled by default
      sample_timeout_seconds: 2.0
      offset_reset_strategy: "hybrid" # "earliest", "latest", or "hybrid"
      max_messages_per_topic: 10

    profiling:
      max_workers: 20 # controls parallelization for both profiling and schema resolution
      nested_field_max_depth: 5
```

**Sampling strategies for schema inference:**

- **`hybrid` (default)**: Tries `latest` first for speed, falls back to `earliest` if no recent messages found.
- **`latest`**: Only reads recent messages. Fastest but may fail on quiet topics.
- **`earliest`**: Scans from the beginning of topic history. Most comprehensive but slower on large topics.

**Performance notes:**

- Inferred schemas are cached for 60 minutes to avoid re-sampling.
- Worker count scales automatically based on CPU cores and topic count.
- Set `schema_resolution.enabled: false` to receive warnings for missing schemas instead of automatic resolution.

#### Data Profiling

The Kafka source supports data profiling of message content to generate field-level statistics and sample values. Profiling and schema resolution are enabled independently — either can be turned on without the other. Note, however, that both features draw their parallelism from the same `profiling.max_workers` setting.

```yaml
source:
  type: "kafka"
  config:
    profiling:
      enabled: true
      sample_size: 200 # messages to sample per topic
      max_sample_time_seconds: 60
      sampling_strategy: "latest" # latest, random, stratified, or full
      max_workers: 4
      batch_size: 100

      # Field-level statistics
      include_field_null_count: true
      include_field_distinct_count: true
      include_field_min_value: true
      include_field_max_value: true
      include_field_mean_value: true
      include_field_median_value: true
      include_field_stddev_value: true
      include_field_quantiles: false # expensive, disabled by default
      include_field_distinct_value_frequencies: false # expensive
      include_field_histogram: false # expensive
      include_field_sample_values: true

      # Nested field handling
      profile_nested_fields: true
      nested_field_max_depth: 10

      # Scheduled profiling (optional)
      operation_config:
        lower_freq_profile_enabled: false
        profile_day_of_week: 1 # Monday=0, Sunday=6
        profile_date_of_month: 15
```

**Sampling strategies:**

- **`latest`** (default): Samples the most recent messages from the end of each partition.
- **`random`**: Samples messages from random offsets across partitions.
- **`stratified`**: Evenly distributes samples across the topic timeline.
- **`full`**: Processes the entire topic (respects `sample_size` limit).

The `nested_field_max_depth` setting (default: 10) prevents recursion errors on deeply nested or circular JSON structures. Reduce it for topics with complex nested messages.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### `PROTOBUF` Schema Type Limitations

The current implementation of the support for `PROTOBUF` schema type has the following limitations:

- Recursive types are not supported.
- Protobuf compilation uses a **process-global descriptor pool**. The first topic that compiles a given fully-qualified message type succeeds; any later topic that embeds the same type name hits a `duplicate symbol` error and is left with no schema fields (DataHub logs a warning and continues). On estates where many topics share common proto types, most protobuf topics after the first will therefore have no schema fields. Enable `schema_resolution` so those topics fall back to schema inference from message data.

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

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Schema Parsing Errors

DataHub automatically handles schema parsing errors gracefully and continues processing.

##### Avro Binary Encoding Errors

**Error:** `avro.errors.InvalidAvroBinaryEncoding: Read 0 bytes, expected 1 bytes`

Enable schema resolution to automatically infer schemas:

```yaml
source:
  type: kafka
  config:
    schema_resolution:
      enabled: true
      offset_reset_strategy: "hybrid"
```

##### Protobuf Duplicate Symbol Errors

**Error:** `Couldn't build proto file into descriptor pool: duplicate symbol`

DataHub logs warnings and continues processing. Topics with schema conflicts will use inferred schemas if schema resolution is enabled.

##### Schema Registry Connection Issues

If schema registry is unavailable, enable schema resolution as a fallback:

```yaml
source:
  type: kafka
  config:
    connection:
      schema_registry_url: "http://localhost:8081"
    schema_resolution:
      enabled: true
```

#### Performance Optimization

For large Kafka clusters with many topics:

```yaml
source:
  type: kafka
  config:
    topic_patterns:
      allow: ["prod_.*", "analytics_.*"]
      deny: [".*_temp", ".*_test"]

    profiling:
      enabled: true
      max_workers: 20 # controls both profiling and schema resolution parallelization
      sample_size: 100
      nested_field_max_depth: 10

    schema_resolution:
      enabled: true
      sample_timeout_seconds: 1.0
```

#### Memory Issues with Large Topics

For topics with large messages or high volume, reduce the sample size and recursion depth:

```yaml
profiling:
  enabled: true
  sample_size: 50
  nested_field_max_depth: 2
```


### Code Coordinates
- Class Name: `datahub.ingestion.source.kafka.kafka.KafkaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Kafka, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
