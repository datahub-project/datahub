## Overview

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform used for high-throughput, fault-tolerant messaging and real-time data pipelines. Confluent Cloud provides a fully managed Kafka service with an integrated Schema Registry.

The DataHub Kafka connector extracts topic metadata and schemas from Kafka clusters and Confluent Schema Registry. It supports Avro, Protobuf, and JSON schemas, multi-stage schema resolution with automatic fallback and inference for topics not registered in the schema registry, and optional data profiling to generate field-level statistics and sample values from message content. The integration also captures stateful deletion detection.

## Concept Mapping

| Kafka Concept    | DataHub Concept                                               | Notes                                  |
| ---------------- | ------------------------------------------------------------- | -------------------------------------- |
| Topic            | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `Topic`                        |
| Schema (Subject) | [Schema Metadata](../../metamodel/entities/dataset.md#schema) | Avro, Protobuf, JSON schemas           |
| Message Fields   | [Dataset Fields](../../metamodel/entities/dataset.md#fields)  | Extracted from schemas or inferred     |
| Kafka Cluster    | [Data Platform Instance](../../../platform-instances.md)      | When `platform_instance` is configured |
