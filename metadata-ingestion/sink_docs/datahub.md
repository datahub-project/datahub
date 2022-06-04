# DataHub

## DataHub Rest

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Setup

To install this plugin, run `pip install 'acryl-datahub[datahub-rest]'`.

### Capabilities

Pushes metadata to DataHub using the GMS REST API. The advantage of the REST-based interface
is that any errors can immediately be reported.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes). This should point to the GMS server.

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

If you are running the ingestion in a container in docker and your [GMS is also running in docker](../../docker/README.md) then you should use the internal docker hostname of the GMS pod. Usually it would look something like

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://datahub-gms:8080"
```

If GMS is running in a kubernetes pod [deployed through the helm charts](../../docs/deploy/kubernetes.md) and you are trying to connect to it from within the kubernetes cluster then you should use the Kubernetes service name of GMS. Usually it would look something like

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://datahub-datahub-gms.datahub.svc.cluster.local:8080"
```

If you are using [UI based ingestion](../../docs/ui-ingestion.md) then where GMS is deployed decides what hostname you should use.

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default              | Description                                                                                        |
|----------------------------|----------|----------------------|----------------------------------------------------------------------------------------------------|
| `server`                   | ✅        |                      | URL of DataHub GMS endpoint.                                                                       |
| `timeout_sec`              |          | 30                   | Per-HTTP request timeout.                                                                          |
| `retry_max_times`          |          | 1                    | Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially |
| `retry_status_codes`       |          | [429, 502, 503, 504] | Retry HTTP request also on these status codes                                                      |
| `token`                    |          |                      | Bearer token used for authentication.                                                              |
| `extra_headers`            |          |                      | Extra headers which will be added to the request.                                                  |
| `max_threads`              |          | `1`                  | Experimental: Max parallelism for REST API calls                                                   |
| `ca_certificate_path`      |          |                      | Path to CA certificate for HTTPS communications                                                    |
| `disable_ssl_verification` |          | false                | Disable ssl certificate validation                                                                 |

## DataHub Kafka

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Setup

To install this plugin, run `pip install 'acryl-datahub[datahub-kafka]'`.

### Capabilities

Pushes metadata to DataHub by publishing messages to Kafka. The advantage of the Kafka-based
interface is that it's asynchronous and can handle higher throughput.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  # source configs

sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "localhost:9092"
      schema_registry_url: "http://localhost:8081"
```

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                        | Required | Default | Description                                                                                                                                              |
| -------------------------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection.bootstrap`                       | ✅       |         | Kafka bootstrap URL.                                                                                                                                     |
| `connection.producer_config.<option>`        |          |         | Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.SerializingProducer                  |
| `connection.schema_registry_url`             | ✅       |         | URL of schema registry being used.                                                                                                                       |
| `connection.schema_registry_config.<option>` |          |         | Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient |
| `topic_routes.MetadataChangeEvent`           |          | MetadataChangeEvent     | Overridden Kafka topic name for the MetadataChangeEvent |
| `topic_routes.MetadataChangeProposal`        |          | MetadataChangeProposal  | Overridden Kafka topic name for the MetadataChangeProposal |

The options in the producer config and schema registry config are passed to the Kafka SerializingProducer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](../examples/recipes/secured_kafka.yml).

## Questions

If you've got any questions on configuring this sink, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
