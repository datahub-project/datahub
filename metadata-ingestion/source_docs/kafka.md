# Kafka Metadata

## Setup

To install this plugin, run `pip install 'acryl-datahub[kafka]'`.

## Capabilities

This plugin extracts the following:

- List of topics - from the Kafka broker
- Schemas associated with each topic - from the schema registry

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: "kafka"
  config:
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081

      # Extra schema registry config.
      # These options will be passed into Kafka's SchemaRegistryClient.
      # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient
      schema_registry_config: {}

      # Extra consumer config.
      # These options will be passed into Kafka's DeserializingConsumer.
      # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer
      # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
      consumer_config: {}

      # Extra producer config.
      # These options will be passed into Kafka's SerializingProducer.
      # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer
      # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
      producer_config: {}
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                             | Required | Default | Description |
| --------------------------------- | -------- | ------- | ----------- |
| `bootstrap`                       |          |         |             |
| `schema_registry_url`             |          |         |             |
| `schema_registry_config.<option>` |          |         |             |
| `consumer_config`                 |          |         |             |
| `producer_config`                 |          |         |             |

The options in the consumer config and schema registry config are passed to the Kafka DeserializingConsumer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.yml).

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
