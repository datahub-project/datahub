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
any errors. Under the hood, the "ack" method synchronously commits Kafka Consumer Offsets on behalf of the Action. This means that by default, the framework provides _at-least once_ processing semantics. That is, in the unusual case that a failure occurs when attempting to commit offsets back to Kafka, that event may be replayed on restart of the Action.

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
      mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1} # Topic name for MetadataChangeLogEvent_v1 events.
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
  | `async_commit_enabled` | ❌ | `true` | Use async (periodic background) offset commits. Set to `false` for batch-processing actions. |
  | `async_commit_interval` | ❌ | `10000` | Milliseconds between background offset commits (only used when `async_commit_enabled` is `true`) |
  | `commit_retry_count` | ❌ | `5` | Number of retries on synchronous commit failure (only used when `async_commit_enabled` is `false`) |
  | `commit_retry_backoff` | ❌ | `10.0` | Seconds between synchronous commit retries (only used when `async_commit_enabled` is `false`) |
</details>

## Schema Registry Configuration

The Kafka Event Source requires a schema registry to deserialize events. There are several ways to configure the schema registry:

### Default Schema Registry

When using the default schema registry that comes with DataHub, you can use the internal URL:

```yml
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: "http://datahub-datahub-gms:8080/schema-registry/api/"
```

Note: If you're running this outside the DataHub cluster, you'll need to map this internal URL to an externally accessible URL.

### External Schema Registry

For external schema registries (like Confluent Cloud), you'll need to provide the full URL and any necessary authentication:

```yml
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: "https://your-schema-registry-url"
      schema_registry_config:
        basic.auth.user.info: "${REGISTRY_API_KEY_ID}:${REGISTRY_API_KEY_SECRET}"
```

### AWS Glue Schema Registry

If you're using AWS Glue Schema Registry, you'll need to configure it differently. See the [AWS deployment guide](../../deploy/aws.md/#aws-glue-schema-registry) for details.

## Offset Commit Modes

The Kafka Event Source supports two offset commit strategies, controlled by the `async_commit_enabled` configuration option.

### Async Commits (default)

```yml
source:
  type: "kafka"
  config:
    async_commit_enabled: true # default
    async_commit_interval: 10000 # ms between background commits (default: 10s)
```

After each event is processed, the offset is stored locally via `store_offsets()`. A background thread
in librdkafka periodically commits all stored offsets to Kafka at the configured interval. This is the
[manual store + auto commit](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#auto-offset-commit)
pattern.

**Tradeoff:** On consumer crash, up to `async_commit_interval` milliseconds of events may be
redelivered. For idempotent actions (all built-in actions), this is a non-issue.

**Performance:** Async commits eliminate the ~3ms synchronous Kafka round-trip per event, which can
yield up to 25x throughput improvement for high-volume pipelines.

### Sync Commits

```yml
source:
  type: "kafka"
  config:
    async_commit_enabled: false
    commit_retry_count: 5 # retries on commit failure (default: 5)
    commit_retry_backoff: 10.0 # seconds between retries (default: 10)
```

After each event is processed, the consumer performs a blocking synchronous commit to Kafka. This
provides tighter delivery guarantees at the cost of throughput (~326 events/sec per pod vs ~8,200
with async).

### Batch-Processing Actions

Batch-processing actions (where `act()` returns `False` to defer acknowledgment) work with both
commit modes. When an action returns `False`, no offset is stored or committed for that event.
The action later calls `ctx.event_source.ack(event, processed=True)` to commit the offset when
the batch is ready.

| Mode                | Commit Timing               | Throughput            | Use Case                              |
| ------------------- | --------------------------- | --------------------- | ------------------------------------- |
| **Async** (default) | Background thread, periodic | High (~8,200 evt/sec) | All actions, including batch          |
| **Sync**            | Immediate, per-event        | Lower (~326 evt/sec)  | When tighter offset control is needed |

## FAQ

1. Is there a way to always start processing from the end of the topics on Actions start?

Currently, the only way is to change the `name` of the Action in its configuration file. In the future,
we are hoping to add first-class support for configuring the action to be "stateless", ie only process
messages that are received while the Action is running.
