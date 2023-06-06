---
title: "metadata-jobs:mce-consumer-job"
---

# Metadata Change Event Consumer Job

The Metadata Change Event Consumer is a Spring job which can be deployed by itself, or as part of the Metadata Service.

Its main function is to listen to change proposal events emitted by clients of DataHub which request changes to the Metadata Graph. It then applies
these requests against DataHub's storage layer: the Metadata Service.

Today the job consumes from two topics:

1. `MetadataChangeProposal_v1`
2. (Deprecated) `MetadataChangeEvent_v4`

and produces to the following topics

1. `FailedMetadataChangeProposal_v1`
2. (Deprecated) `FailedMetadataChangeEvent_v4`

> Where does the misleading name **Metadata Change Event** come from? Well, history. Previously, this job consumed
> a single `MetadataChangeEvent` topic which has been deprecated and replaced by per-aspect Metadata Change Proposals. Hence, the name!

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) installed on your machine to be
able to build `DataHub Metadata Service`.

## Build
`Metadata Change Event Consumer Job` is already built as part of top level build:

```
./gradlew build
```

However, if you only want to build `Metadata Change Event Consumer Job` specifically:
```
./gradlew :metadata-jobs:mce-consumer-job:build
```

## Dependencies
Before starting `Metadata Change Event Consumer Job`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../../docker/kafka-setup)
and [DataHub GMS](../../docker/datahub-gms) Docker containers are up and running.

## Start via Docker image
Quickest way to try out `Metadata Change Event Consumer Job` is running the [Docker image](../../docker/datahub-mce-consumer).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
MCP_CONSUMER_ENABLED=true ./gradlew :metadata-jobs:mce-consumer-job:bootRun
```

## Debugging

To debug with an IDE (i.e. IntelliJ), run the `bootRun` task with the `--debug-jvm` flag. This will launch the app and
listen on port 5005 for a remote debugger.

```
MCP_CONSUMER_ENABLED=true ./gradlew :metadata-jobs:mce-consumer-job:bootRun --debug-jvm
```

## Endpoints
Spring boot actuator has been enabled for MCE Application.
`healthcheck`, `metrics` and `info` web endpoints are enabled by default.

`healthcheck` - http://localhost:9090/actuator/health

`metrics` - http://localhost:9090/actuator/metrics

To retrieve a specific metric - http://localhost:9090/actuator/metrics/kafka.consumer.records.consumed.total

## Standalone MCE Consumer Mode

In standalone consumer mode, the number of `CPU cores + 1` determines the parallelization of the MCE consumer's internal
processing. This determines the number of Kafka concurrent consumers, the number of threads available to the local
Restli service, and the maximum number SQL connections used by the MCE consumer container.

Note that the effective throughput is limited to the number of partitions configured for the source Kafka topics.
Allocating additional consumers, or CPU cores, beyond the number of topic partitions to the MCE standalone consumer(s)
will not increase ingestion performance.

### Additional Endpoints

`/*` - Restli service endpoints (standalone consumer mode)

The Restli service endpoints are exposed for use locally (not via a K8 service) by the MCE consumer job itself. This is
only true in standalone mode. When run within GMS, the Restli endpoints are already available and accessed via the K8
service.

