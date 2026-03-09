## Overview

Fivetran is a streaming or integration platform. Learn more in the [official Fivetran documentation](https://www.fivetran.com/).

The DataHub integration for Fivetran covers streaming/integration entities such as topics, connectors, pipelines, or jobs. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Fivetran        | Datahub                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------- |
| `Connector`     | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                        |
| `Source`        | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Destination`   | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Connector Run` | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance) |

Source and destination are mapped to Dataset as an Input and Output of Connector.
