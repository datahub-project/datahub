## Overview

Apache NiFi is a streaming or integration platform. Learn more in the [official Apache NiFi documentation](https://nifi.apache.org/).

The DataHub integration for Apache NiFi covers streaming/integration entities such as topics, connectors, pipelines, or jobs. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept                    | DataHub Concept                                           | Notes                   |
| --------------------------------- | --------------------------------------------------------- | ----------------------- |
| `"Nifi"`                          | [Data Platform](../../metamodel/entities/dataPlatform.md) |                         |
| Nifi flow                         | [Data Flow](../../metamodel/entities/dataFlow.md)         |                         |
| Nifi Ingress / Egress Processor   | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Remote Port                  | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Port with remote connections | [Dataset](../../metamodel/entities/dataset.md)            |                         |
| Nifi Process Group                | [Container](../../metamodel/entities/container.md)        | Subtype `Process Group` |
