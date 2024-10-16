---
title: "metadata-jobs:mae-consumer-job"
---

# Metadata Audit Event Consumer Job

The Metadata Audit Event Consumer is a Spring job which can be deployed by itself, or as part of the Metadata Service.

Its main function is to listen to change log events emitted as a result of changes made to the Metadata Graph, converting changes in the metadata model into updates
against secondary search & graph indexes (among other things)

Today the job consumes from two important Kafka topics:

1. `MetadataChangeLog_Versioned_v1`
2. `MetadataChangeLog_Timeseries_v1`

> Where does the name **Metadata Audit Event** come from? Well, history. Previously, this job consumed
> a single `MetadataAuditEvent` topic which has been deprecated and removed from the critical path. Hence, the name!

## Prerequisites

Follow the [main developer guide](../../docs/developers.md) to set up your development environment and install the required dependencies.

## Build

`Metadata Audit Event Consumer Job` is already built as part of top level build:

```shell
./gradlew build
```

However, if you only want to build `MAE Consumer Job` specifically:

```shell
./gradlew :metadata-jobs:mae-consumer-job:build
```

## Dependencies

Before starting `Metadata Audit Event Consumer Job`, you need to make sure that all backend services, including Kafka and ElasticSearch, are up and running. If GMS is healthy, then Kafka and ElasticSearch should be healthy as well.

## Start via Docker image

The quickest way to try out `Metadata Audit Event Consumer Job` is running the [Docker image](../../docker/datahub-mae-consumer).

## Start via command line

If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):

```shell
MCL_CONSUMER_ENABLED=true ./gradlew :metadata-jobs:mae-consumer-job:bootRun
```

## Endpoints

Spring boot actuator has been enabled for MAE Application.
`healthcheck`, `metrics` and `info` web endpoints are enabled by default.

`healthcheck` - http://localhost:9091/actuator/health

`metrics` - http://localhost:9091/actuator/metrics

To retrieve a specific metric - http://localhost:9091/actuator/metrics/process.uptime
