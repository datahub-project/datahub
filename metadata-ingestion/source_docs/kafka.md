# Kafka Metadata

To install this plugin, run `pip install 'acryl-datahub[kafka]'`.

This plugin extracts the following:

- List of topics - from the Kafka broker
- Schemas associated with each topic - from the schema registry

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

The options in the consumer config and schema registry config are passed to the Kafka DeserializingConsumer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.yml).

# Kafka Connect `kafka-connect`

This plugin extracts the following:

- Kafka Connect connector as individual `DataFlowSnapshotClass` entity
- Creating individual `DataJobSnapshotClass` entity using `{connector_name}:{source_dataset}` naming
- Lineage information between source database to Kafka topic

```yml
source:
  type: "kafka-connect"
  config:
    connect_uri: "http://localhost:8083"
    cluster_name: "connect-cluster"
    connector_patterns:
      deny:
        - ^denied-connector.*
      allow:
        - ^allowed-connector.*
```

Current limitations:

- Currently works only for Debezium source connectors.
