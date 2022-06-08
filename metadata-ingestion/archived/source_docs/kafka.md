# Kafka Metadata

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[kafka]'`.

## Capabilities

This plugin extracts the following:

- Topics from the Kafka broker
- Schemas associated with each topic from the schema registry

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Platform Instance | ✔️ | [link](../../docs/platform-instances.md) |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |
| Stateful Ingestion | ✔️ | [link](./stateful_ingestion.md)

Stateful Ingestion is available only when a Platform Instance is assigned to this source.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
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

### Connecting to Confluent Cloud

If using Confluent Cloud you can use a recipe like this. In this `consumer_config.sasl.username` and `consumer_config.sasl.password` are the API credentials that you get (in the Confluent UI) from your cluster -> Data Integration -> API Keys. `schema_registry_config.basic.auth.user.info`  has API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.

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

Note that the `domain` in config above can be either an _urn_ or a domain _id_ (i.e. `urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810` or simply `13ae4d85-d955-49fc-8474-9004c663a810`). The Domain should exist in your DataHub instance before ingesting data into the Domain. To create a Domain on DataHub, check out the [Domains User Guide](https://datahubproject.io/docs/domains/).

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

## Custom Schema Registry

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

sink:
  # sink configs
```

### Limitations of `PROTOBUF` schema types implementation

The current implementation of the support for `PROTOBUF` schema type has the following limitations:

+ Recursive types are not supported.
+ If the schemas of different topics define a type in the same package, the source would raise an exception.

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

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                         | Required | Default                                                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|-----------------------------------------------|----------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `conection.bootstrap`                         |          | `"localhost:9092"`                                                            | Bootstrap servers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `connection.schema_registry_url`              |          | `http://localhost:8081"`                                                      | Schema registry location.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `connection.schema_registry_config.<option>`  |          |                                                                               | Extra schema registry config. These options will be passed into Kafka's SchemaRegistryClient. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `connection.consumer_config.<option>`         |          |                                                                               | Extra consumer config. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `connection.producer_config.<option>`         |          |                                                                               | Extra producer config. These options will be passed into Kafka's SerializingProducer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `topic_patterns.allow`                        |          |                                                                               | List of regex patterns for topics to include in ingestion.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `topic_patterns.deny`                         |          |                                                                               | List of regex patterns for topics to exclude from ingestion.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `topic_patterns.ignoreCase`                   |          | `True`                                                                        | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `domain.domain_urn.allow`                     |          |                                                                               | List of regex patterns for topics to set domain_urn domain key. There can be multiple domain key specified.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `domain.domain_urn.deny`                      |          |                                                                               | List of regex patterns for topics to not assign domain_urn. There can be multiple domain key specified.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `domain.domain_urn.ignoreCase`                |          | `True`                                                                        | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `platform_instance`                           |          | None                                                                          | The Platform instance to use while constructing URNs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `topic_subject_map`                           |          | `{}`                                                                          | Provides the mapping for the `key` and the `value` schemas of a topic to the corresponding schema registry subject name. Each entry of this map has the form `<topic_name>-key`:`<schema_registry_subject_name_for_key_schema>` and `<topic_name>-value`:`<schema_registry_subject_name_for_value_schema>` for the key and the value schemas associated with the topic, respectively. This parameter is mandatory when the [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work) is used as the subject naming strategy in the kafka schema registry. NOTE: When provided, this overrides the default subject name resolution even when the `TopicNameStrategy` or the `TopicRecordNameStrategy` are used. |
| `schema_registry_class`                       |          | `datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry`  | The Kafka schema registry class implementation that would be used for obtaining both the key schema and the value schema of the kafka topic.                                                                                                                                                                                                                                                                                                                                                                                                                           |  
| `ignore_warnings_on_schema_type`              |          | `False`                                                                       | Set to `True` to disable warnings reported for non-AVRO value or key schemas.


The options in the consumer config and schema registry config are passed to the Kafka DeserializingConsumer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.dhub.yml).

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
