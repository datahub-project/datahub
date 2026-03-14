## Overview

Apache Flink is a distributed stream and batch processing framework. Learn more in the [official Flink documentation](https://flink.apache.org/).

The DataHub integration for Flink extracts job metadata, operator-level lineage from Kafka sources and sinks, catalog table schemas via SQL Gateway, and job execution history as DataProcessInstances. It supports stateful ingestion for stale entity removal.

## Concept Mapping

| Source Concept | DataHub Concept                                                                 | Notes                                         |
| -------------- | ------------------------------------------------------------------------------- | --------------------------------------------- |
| Flink Job      | [DataFlow](docs/generated/metamodel/entities/dataFlow.md)                       | One DataFlow per Flink job                    |
| Flink Operator | [DataJob](docs/generated/metamodel/entities/dataJob.md)                         | Granularity depends on `operator_granularity` |
| Job Execution  | [DataProcessInstance](docs/generated/metamodel/entities/dataProcessInstance.md) | When `include_run_history` is enabled         |
| Kafka Topic    | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Via lineage (inlets/outlets)                  |
| Catalog Table  | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | When `include_catalog_metadata` is enabled    |
| Catalog        | [Container](docs/generated/metamodel/entities/container.md)                     | _subType_: `Catalog`                          |
| Database       | [Container](docs/generated/metamodel/entities/container.md)                     | _subType_: `Database`, nested under Catalog   |
