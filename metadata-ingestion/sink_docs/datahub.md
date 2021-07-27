# DataHub Rest

To install this plugin, run `pip install 'acryl-datahub[datahub-reset]'`.

Pushes metadata to DataHub using the GMA rest API. The advantage of the rest-based interface
is that any errors can immediately be reported.

```yml
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

# DataHub Kafka

To install this plugin, run `pip install 'acryl-datahub[datahub-kafka]'`.

Pushes metadata to DataHub by publishing messages to Kafka. The advantage of the Kafka-based
interface is that it's asynchronous and can handle higher throughput. This requires the
Datahub mce-consumer container to be running.

```yml
sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "localhost:9092"
      producer_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.SerializingProducer
      schema_registry_url: "http://localhost:8081"
      schema_registry_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient
```

The options in the producer config and schema registry config are passed to the Kafka SerializingProducer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.yml).
