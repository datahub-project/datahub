## Overview

Snaplogic is a streaming or integration platform. Learn more in the [official Snaplogic documentation](https://www.snaplogic.com/).

The DataHub integration for Snaplogic covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures table- and column-level lineage.

## Concept Mapping

| Source Concept | DataHub Concept                                                    | Notes                                                                                                                                   |
| -------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Snap-pack      | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | Snap-packs are mapped to Data Platforms, either directly (e.g., Snowflake) or dynamically based on connection details (e.g., JDBC URL). |
| Table/Dataset  | [Dataset](docs/generated/metamodel/entities/dataset.md)            | May be different. It depends on snap type. For SQL databases it is a table, and for Kafka it is a topic.                                |
| Snap           | [Data Job](docs/generated/metamodel/entities/dataJob.md)           |                                                                                                                                         |
| Pipeline       | [Data Flow](docs/generated/metamodel/entities/dataFlow.md)         |                                                                                                                                         |
