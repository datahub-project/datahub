## Overview

AWS Kinesis is a real-time streaming and data-delivery service. See the [official AWS Kinesis page](https://aws.amazon.com/kinesis/) for product details.

This connector covers both AWS streaming services with one recipe and one IAM policy:

- **Amazon Kinesis Data Streams (KDS)** — emitted under the `kinesis` platform (display name: _Amazon Kinesis Data Streams_) as **Datasets** (`Stream` subtype).
- **Amazon Data Firehose** (formerly _Amazon Kinesis Data Firehose / KDF_) — each Firehose stream is emitted under the `kinesis-firehose` platform (display name: _Amazon Data Firehose_) as its own **DataFlow** (`Firehose Stream` subtype) containing a single **DataJob** (`Delivery` subtype) that carries cross-platform lineage edges to the destination (S3, Redshift, OpenSearch, Snowflake, Apache Iceberg, MongoDB).

AWS resource tags become DataHub tags (and can be turned into ownership via the `extract_ownership_from_tags` transformer). Glue Schema Registry can be opted in to attach Avro / JSON / Protobuf schemas to streams.

:::info Looking specifically for Amazon Data Firehose?

Firehose streams are ingested by this same connector — see the [Concept Mapping](#concept-mapping) table below or the [Limitations](#limitations) section for cross-platform lineage configuration.
:::

## Concept Mapping

| Source Concept                                                               | DataHub Concept                                           | Notes                                                                                                                               |
| ---------------------------------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `"kinesis"` / `"kinesis-firehose"`                                           | [Data Platform](../../metamodel/entities/dataPlatform.md) | Two platforms, mirroring AWS's own service split.                                                                                   |
| AWS Region                                                                   | [Container](../../metamodel/entities/container.md)        | Subtype `Region`. One per recipe.                                                                                                   |
| Kinesis Data Stream                                                          | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Stream`. Parent: the regional Container.                                                                                   |
| Glue Schema Registry schema (per stream)                                     | `SchemaMetadata` aspect                                   | Avro / JSON / Protobuf. Attached when `glue_schema_registry.enabled: true` and a schema is resolved for the stream.                 |
| Firehose stream                                                              | [DataFlow](../../metamodel/entities/dataFlow.md)          | Subtype `Firehose Stream`. One DataFlow per Firehose stream (the pipeline).                                                         |
| Firehose delivery (the stream's data movement)                               | [DataJob](../../metamodel/entities/dataJob.md)            | Subtype `Delivery`. The single DataJob inside each Firehose stream's DataFlow; carries the lineage.                                 |
| Firehose destination (S3, Redshift, OpenSearch, Snowflake, Iceberg, MongoDB) | Lineage edge                                              | Emitted via `dataJobInputOutput.outputDatasets`. Upstream is the source KDS stream when `DeliveryStreamType=KinesisStreamAsSource`. |
| AWS resource tag (`Key=Value`)                                               | Tag                                                       | Tag URN form: `urn:li:tag:Key:Value`.                                                                                               |
| AWS resource tag (via the `extract_ownership_from_tags` transformer)         | Owner                                                     | Ownership is derived from the emitted tags by the transformer, not by this source directly.                                         |

### Compatibility

Six Firehose destination platforms are supported: S3, Redshift, OpenSearch/Elasticsearch, Snowflake, Apache Iceberg, and MongoDB. Firehose streams targeting other destinations (HTTP, Datadog, Splunk, New Relic, etc.) are still emitted (as a DataFlow + Delivery DataJob) but without lineage output edges, and surface a warning in the ingestion report. See [Limitations](#limitations) for the `destination_platform_map` configuration required when destination platforms were ingested with a non-default `platform_instance`.
