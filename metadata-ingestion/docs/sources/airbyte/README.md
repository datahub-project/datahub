## Overview

[Airbyte](https://airbyte.com/) is an open-source data integration platform that syncs data from sources to destinations through configurable connections. It supports hundreds of pre-built connectors and lets you build custom ones.

This integration extracts metadata from Airbyte to give DataHub visibility into your data pipelines — including connections, sources, destinations, streams, and job execution history. It captures lineage between source and destination datasets at both the table and column level.

## Concept Mapping

Here's a table for **Concept Mapping** between Airbyte and DataHub to provide a clear overview of how entities and concepts in Airbyte are mapped to corresponding entities in DataHub:

| Source Concept     | DataHub Concept       | Notes                                                                  |
| ------------------ | --------------------- | ---------------------------------------------------------------------- |
| **Workspace**      | `DataFlow`            | Top-level container for Airbyte resources                              |
| **Connection**     | `DataFlow`            | Represents an Airbyte connection between source and destination        |
| **Source**         | `Dataset`             | Source datasets are mapped to DataHub datasets                         |
| **Destination**    | `Dataset`             | Destination datasets are mapped to DataHub datasets                    |
| **Stream**         | `DataJob`             | Each stream is represented as a DataJob within the Connection DataFlow |
| **Connection Job** | `DataProcessInstance` | Execution information for a connection run                             |
| **Source Schema**  | `SchemaMetadata`      | Schema information from source datasets                                |
| **Column Mapping** | `FineGrainedLineage`  | Column-level lineage between source and destination                    |
