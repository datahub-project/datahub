---
title: "metadata-jobs:mae-consumer-job"
---

# MetadataAuditEvent (MAE) Consumer Job
MAE Consumer is a [Kafka Streams](https://kafka.apache.org/documentation/streams/) job. Its main function is to listen
`MetadataAuditEvent` Kafka topic for messages and process those messages, converting changes in the metadata model into updates
against secondary search & graph indexes. 

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) 
installed on your machine to be able to build `DataHub GMS`.

## Build
`MAE Consumer Job` is already built as part of top level build:
```
./gradlew build
```
However, if you only want to build `MAE Consumer Job` specifically:
```
./gradlew :metadata-jobs:mae-consumer-job:build
```

## Dependencies
Before starting `MAE Consumer Job`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../../docker/kafka-setup),
[Elasticsearch](../../docker/elasticsearch), and [Neo4j](../../docker/neo4j) Docker containers are up and running.

## Start via Docker image
Quickest way to try out `MAE Consumer Job` is running the [Docker image](../../docker/datahub-mae-consumer).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
./gradlew :metadata-jobs:mae-consumer-job:bootRun
```

## Endpoints
Spring boot actuator has been enabled for MAE Application. 
`healthcheck`, `metrics` and `info` web endpoints are enabled by default.

`healthcheck` - http://localhost:9091/actuator/health

`metrics` - http://localhost:9091/actuator/metrics

To retrieve a specific metric - http://localhost:9091/actuator/metrics/process.uptime
