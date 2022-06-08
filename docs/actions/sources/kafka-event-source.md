# Kafka Event Source

## Overview

The Kafka Event Source is the default Event Source used within the DataHub Actions Framework.

Under the hood, the Kafka Event Source uses a Kafka Consumer to subscribe to the topics streaming
out of DataHub (MetadataChangeLog_v1, PlatformEvent_v1). Each Action is automatically placed into a unique
[consumer group](https://docs.confluent.io/platform/current/clients/consumer.html#consumer-groups) based on
the unique `name` provided inside the Action configuration file.

This means that you can easily scale-out Actions processing by sharing the same Action configuration file across
multiple nodes or processes. As long as the `name` of the Action is the same, each instance of the Actions framework will subscribe as a member in the same Kafka Consumer Group, which allows for load balancing the
topic traffic across consumers which each consume independent [partitions](https://developer.confluent.io/learn-kafka/apache-kafka/partitions/#kafka-partitioning). 

Because the Kafka Event Source uses consumer groups by default, actions using this source will be **stateful**. 
This means that Actions will keep track of their processing offsets of the upstream Kafka topics. If you 
stop an Action and restart it sometime later, it will first "catch up" by processing the messages that the topic
has received since the Action last ran. Be mindful of this - if your Action is computationally expensive, it may be preferable to start consuming from the end of the log, instead of playing catch up. The easiest way to achieve this is to simply rename the Action inside the Action configuration file - this will create a new Kafka Consumer Group which will begin processing new messages at the end of the log (latest policy). 

### Processing Guarantees

This event source implements an "ack" function which is invoked if and only if an event is successfully processed
by the Actions framework, meaning that the event made it through the Transformers and into the Action without
any errors. Under the hood, the "ack" method synchronously commits Kafka Consumer Offsets on behalf of the Action. This means that by default, the framework provides *at-least once* processing semantics. That is, in the unusual case that a failure occurs when attempting to commit offsets back to Kafka, that event may be replayed on restart of the Action. 

If you've configured your Action pipeline `failure_mode` to be `CONTINUE` (the default), then events which
fail to be processed will simply be logged to a `failed_events.log` file for further investigation (dead letter queue). The Kafka Event Source will continue to make progress against the underlying topics and continue to commit offsets even in the case of failed messages. 

If you've configured your Action pipeline `failure_mode` to be `THROW`, then events which fail to be processed result in an Action Pipeline error. This in turn terminates the pipeline before committing offsets back to Kafka. Thus the message will not be marked as "processed" by the Action consumer.


## Supported Events

The Kafka Event Source produces

- [Entity Change Event V1](../events/entity-change-event.md)
- [Metadata Change Log V1](../events/metadata-change-log-event.md)


## Configure the Event Source

Use the following config(s) to get started with the Kafka Event Source. 

```yml
name: "pipeline-name"
source:
  type: "kafka"
  config:
    # Connection-related configuration
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
      # Dictionary of freeform consumer configs propagated to underlying Kafka Consumer 
      consumer_config: 
          #security.protocol: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-PLAINTEXT}
          #ssl.keystore.location: ${KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION:-/mnt/certs/keystore}
          #ssl.truststore.location: ${KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION:-/mnt/certs/truststore}
          #ssl.keystore.password: ${KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD:-keystore_password}
          #ssl.key.password: ${KAFKA_PROPERTIES_SSL_KEY_PASSWORD:-keystore_password}
          #ssl.truststore.password: ${KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD:-truststore_password}
    # Topic Routing - which topics to read from.
    topic_routes:
      mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1} # Topic name for MetadataChangeLog_v1 events. 
      pe: ${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1} # Topic name for PlatformEvent_v1 events. 
action:
  # action configs
```

<details>
  <summary>View All Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `connection.bootstrap` | ✅ | N/A | The Kafka bootstrap URI, e.g. `localhost:9092`. |
  | `connection.schema_registry_url` | ✅ | N/A | The URL for the Kafka schema registry, e.g. `http://localhost:8081` |
  | `connection.consumer_config` | ❌ | {} | A set of key-value pairs that represents arbitrary Kafka Consumer configs |
  | `topic_routes.mcl` | ❌  | `MetadataChangeLog_v1` | The name of the topic containing MetadataChangeLog events |
  | `topic_routes.pe` | ❌ | `PlatformEvent_v1` | The name of the topic containing PlatformEvent events |
</details>


## FAQ

1. Is there a way to always start processing from the end of the topics on Actions start?

Currently, the only way is to change the `name` of the Action in its configuration file. In the future,
we are hoping to add first-class support for configuring the action to be "stateless", ie only process
messages that are received while the Action is running.

2. Is there a way to asynchronously commit offsets back to Kafka?

Currently, all consumer offset commits are made synchronously for each message received. For now we've optimized for correctness over performance. If this commit policy does not accommodate your organization's needs, certainly reach out on [Slack](https://slack.datahubproject.io/). 