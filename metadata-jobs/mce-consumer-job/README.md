# MetadataChangeEvent (MAE) Consumer Job
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
Before starting `MCE Consumer Job`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../../docker/kafka)
and [DataHub GMS](../../docker/gms) Docker containers are up and running.

## Start via Docker image
Quickest way to try out `MCE Consumer Job` is running the [Docker image](../../docker/mce-consumer).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
./gradlew :metadata-jobs:mce-consumer-job:bootRun
```

## Debugging

To debug with an IDE (i.e. IntelliJ), run the `bootRun` task with the `--debug-jvm` flag. This will launch the app and
listen on port 5005 for a remote debugger.

```
./gradlew :metadata-jobs:mce-consumer-job:bootRun --debug-jvm
```

## Endpoints
Spring boot actuator has been enabled for MCE Application. 
`healthcheck`, `metrics` and `info` web endpoints are enabled by default.

`healthcheck` - http://localhost:9090/actuator/health

`metrics` - http://localhost:9090/actuator/metrics

To retrieve a specific metric - http://localhost:9090/actuator/metrics/kafka.consumer.records.consumed.total

