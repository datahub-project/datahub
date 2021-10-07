# Kafka Metadata

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[kafka]'`.

## Capabilities

This plugin extracts the following:

- Topics from the Kafka broker
- Schemas associated with each topic from the schema registry

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "kafka"
  config:
    # Coordinates
    connection:
      bootstrap: "broker:9092"

      schema_registry_url: http://localhost:8081

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                        | Required | Default                  | Description                                                                                                                                                                                                                                                                          |
| -------------------------------------------- | -------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `conection.bootstrap`                        |          | `"localhost:9092"`       | Bootstrap servers.                                                                                                                                                                                                                                                                   |
| `connection.schema_registry_url`             |          | `http://localhost:8081"` | Schema registry location.                                                                                                                                                                                                                                                            |
| `connection.schema_registry_config.<option>` |          |                          | Extra schema registry config. These options will be passed into Kafka's SchemaRegistryClient. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient.                                                                   |
| `connection.consumer_config.<option>`        |          |                          | Extra consumer config. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md. |
| `connection.producer_config.<option>`        |          |                          | Extra producer config. These options will be passed into Kafka's SerializingProducer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.     |
| `topic_patterns.allow`                       |          |                          | List of regex patterns for topics to include in ingestion.                                                                                                                                                                                                                                    |
| `topic_patterns.deny`                        |          |                          | List of regex patterns for topics to exclude from ingestion.                                                                                                                                                                                                                                  |
| `topic_patterns.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |

The options in the consumer config and schema registry config are passed to the Kafka DeserializingConsumer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.yml).

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
