# MetadataChangeEvent (MCE) Consumer Job
MCE Consumer is a [Kafka Streams](https://kafka.apache.org/documentation/streams/) job. Its main function is to listen
`MetadataChangeEvent` Kafka topic for messages and process those messages and writes new metadata to `DataHub GMS`.
After every successful update of metadata, GMS fires a `MetadataAuditEvent` and this is consumed by 
[MAE Consumer Job](../mae-consumer-job).

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) installed on your machine to be
able to build `DataHub GMS`.

## Build
`MCE Consumer Job` is already built as part of top level build:

```
./gradlew build
```

However, if you only want to build `MCE Consumer Job` specifically:
```
./gradlew :metadata-jobs:mce-consumer-job:build
```

## Dependencies
Before starting `MCE Consumer Job`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../../docker/kafka-setup)
Docker containers are up and running. MCE Consumer Job runs as part of the `datahub-gms` container.

## Endpoints
`healthcheck`, `metrics` and `info` web endpoints are enabled by default.

`healthcheck` - http://localhost:9090/actuator/health

`metrics` - http://localhost:9090/actuator/metrics

To retrieve a specific metric - http://localhost:9090/actuator/metrics/kafka.consumer.records.consumed.total

