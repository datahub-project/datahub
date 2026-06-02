## Overview

AWS Kinesis is a real-time streaming and data-delivery service. See the [official AWS Kinesis page](https://aws.amazon.com/kinesis/) for product details.

This connector covers both AWS streaming services with one recipe and one IAM policy:

- **Amazon Kinesis Data Streams (KDS)** — emitted under the `kinesis` platform (display name: _Amazon Kinesis Data Streams_) as **Datasets** (`Stream` subtype).
- **Amazon Data Firehose** (formerly _Amazon Kinesis Data Firehose / KDF_) — emitted under the `kinesis-firehose` platform (display name: _Amazon Data Firehose_) as **DataJobs** under one regional **DataFlow** per recipe, with cross-platform lineage edges to the destination (S3, Redshift, OpenSearch, Snowflake, Apache Iceberg, MongoDB).

AWS resource tags become DataHub tags and ownership. Glue Schema Registry can be opted in to attach Avro / JSON / Protobuf schemas to streams.

> **Looking specifically for Amazon Data Firehose?** Firehose delivery streams are ingested by this same connector — see the [Concept Mapping](#concept-mapping) table below or the [Limitations](#limitations) section for cross-platform lineage configuration.

## Concept Mapping

| Source Concept                                                               | DataHub Concept                                           | Notes                                                                                                                                         |
| ---------------------------------------------------------------------------- | --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `"kinesis"` / `"kinesis-firehose"`                                           | [Data Platform](../../metamodel/entities/dataPlatform.md) | Two platforms, mirroring AWS's own service split.                                                                                             |
| AWS Region                                                                   | [Container](../../metamodel/entities/container.md)        | Subtype `Region`. One per recipe.                                                                                                             |
| Kinesis Data Stream                                                          | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Stream`. Parent: the regional Container.                                                                                             |
| Glue Schema Registry schema (per stream)                                     | `SchemaMetadata` aspect                                   | Avro / JSON / Protobuf. Attached when `glue_schema_registry.enabled: true` and a schema is resolved for the stream.                           |
| AWS Region (for Firehose)                                                    | [DataFlow](../../metamodel/entities/dataFlow.md)          | Subtype `Firehose`. One per recipe.                                                                                                           |
| Firehose delivery stream                                                     | [DataJob](../../metamodel/entities/dataJob.md)            | Subtype `Firehose Delivery Stream`. Parent: the regional DataFlow.                                                                            |
| Firehose destination (S3, Redshift, OpenSearch, Snowflake, Iceberg, MongoDB) | Lineage edge                                              | Emitted via `dataJobInputOutput.outputDatasets`. Upstream is the source KDS stream when `DeliveryStreamType=KinesisStreamAsSource`.           |
| AWS resource tag (`Key=Value`)                                               | Tag                                                       | Tag URN form: `urn:li:tag:Key:Value`.                                                                                                         |
| AWS resource tag with the configured `owner_tag_key` (default `owner`)       | CorpUser                                                  | The tag value becomes a `urn:li:corpuser:<value>` owner URN. DataHub's identity layer handles group-membership mapping at the platform level. |

### Compatibility

Six Firehose destination platforms are supported: S3, Redshift, OpenSearch/Elasticsearch, Snowflake, Apache Iceberg, and MongoDB. Delivery streams targeting other destinations (HTTP, Datadog, Splunk, New Relic, etc.) emit as DataJobs without lineage output edges and surface a warning in the ingestion report. See [Limitations](#limitations) for the `destination_platform_map` configuration required when destination platforms were ingested with a non-default `platform_instance`.
