## Overview

Matillion Data Productivity Cloud (DPC) is a cloud-native data integration platform for building, orchestrating, and monitoring data pipelines. Learn more in the [official Matillion documentation](https://docs.matillion.com/data-productivity-cloud/).

The DataHub integration for Matillion DPC ingests pipelines, streaming pipelines, projects, and environments as DataHub entities. It captures table- and column-level lineage via the Matillion OpenLineage API, pipeline execution history as operational metadata, and child pipeline dependency relationships for end-to-end orchestration visibility.

## Concept Mapping

| Source Concept              | DataHub Concept     | Notes                                                                |
| --------------------------- | ------------------- | -------------------------------------------------------------------- |
| Project                     | Container           | Top-level grouping of pipelines within a Matillion account.          |
| Environment                 | Container           | Deployment environment within a project (e.g. Production, Staging).  |
| Pipeline                    | DataFlow            | An orchestration pipeline that transforms or moves data.             |
| Pipeline Component / Step   | DataJob             | An individual step within a pipeline.                                |
| Streaming Pipeline          | DataFlow            | A CDC or streaming pipeline, emitted with `pipeline_type=streaming`. |
| Pipeline Execution          | DataProcessInstance | A single run of a pipeline, including status and timing.             |
| OpenLineage table reference | Dataset             | Upstream or downstream dataset referenced via OpenLineage events.    |
| Table/column lineage edge   | Lineage edge        | Extracted from OpenLineage events; column-level via SQL parsing.     |
