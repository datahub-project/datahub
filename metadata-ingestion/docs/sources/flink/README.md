## Overview

Apache Flink is a distributed stream and batch processing framework. Learn more in the [official Flink documentation](https://flink.apache.org/).

The DataHub integration for Flink extracts job metadata, operator topology, and dataset lineage by connecting to the Flink JobManager REST API and optionally the SQL Gateway. It resolves table references to their actual platforms (Kafka, Postgres, Iceberg, etc.) via catalog introspection, and tracks job execution history as DataProcessInstances. Stateful ingestion is supported for stale entity removal.

## Concept Mapping

| Source Concept | DataHub Concept                                                                 | Notes                                                     |
| -------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Flink Job      | [DataFlow](docs/generated/metamodel/entities/dataFlow.md)                       | One DataFlow per Flink job                                |
| Flink Operator | [DataJob](docs/generated/metamodel/entities/dataJob.md)                         | Granularity depends on `operator_granularity`             |
| Job Execution  | [DataProcessInstance](docs/generated/metamodel/entities/dataProcessInstance.md) | When `include_run_history` is enabled                     |
| Kafka Topic    | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via lineage (DataStream or SQL/Table API)        |
| JDBC Table     | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via SQL Gateway catalog introspection            |
| Iceberg Table  | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Resolved via SQL Gateway or `catalog_platform_map` config |
