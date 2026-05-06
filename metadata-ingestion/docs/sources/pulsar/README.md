## Overview

Pulsar is a streaming or integration platform. Learn more in the [official Pulsar documentation](https://pulsar.apache.org/).

The DataHub integration for Pulsar covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                                    | Notes                                                                     |
| -------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pulsar`       | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |                                                                           |
| Pulsar Topic   | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `topic`                                                        |
| Pulsar Schema  | [SchemaField](docs/generated/metamodel/entities/schemaField.md)    | Maps to the fields defined within the `Avro` or `JSON` schema definition. |
