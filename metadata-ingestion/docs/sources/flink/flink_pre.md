## Integration Details

This source extracts the following from an [Apache Flink](https://flink.apache.org/) cluster via the JobManager REST API:

- Flink jobs as Data Pipelines (DataFlow) with ownership and metadata.
- Coalesced operators from execution plans as Data Jobs (DataJob) with input/output lineage.
- Job executions as DataProcessInstances (runs) for operational monitoring.
- Source and sink lineage to Kafka topics extracted from Flink execution plans.

### Concept Mapping

| Flink Concept            | DataHub Concept                                                        | Notes                                                         |
| ------------------------ | ---------------------------------------------------------------------- | ------------------------------------------------------------- |
| `"flink"`                | [Data Platform](../../metamodel/entities/dataPlatform.md)              |                                                               |
| Flink Job                | [DataFlow](../../metamodel/entities/dataFlow.md)                       | One DataFlow per Flink job                                    |
| Coalesced Operators      | [DataJob](../../metamodel/entities/dataJob.md)                         | Operators from the execution plan, grouped into a single task |
| Job Execution            | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) | Represents a specific run of a job                            |
| Kafka Source/Sink Topics | [Dataset](../../metamodel/entities/dataset.md)                         | Connected via lineage edges on the DataJob                    |

## Prerequisites

1. **Flink Cluster**: A running Flink cluster (standalone, YARN, Kubernetes, or managed) with the JobManager REST API accessible.
2. **Network Access**: The machine running DataHub ingestion must be able to reach the Flink JobManager REST endpoint (default port `8081`).
3. **Authentication** (if applicable): If your Flink REST API is behind a gateway or proxy that requires auth, have a bearer token or basic auth credentials ready.

## Configuration Notes

### REST Endpoint

The `rest_endpoint` should point to the Flink JobManager's REST API — the same URL where you access the Flink Web UI. For example:

- Standalone: `http://flink-jobmanager:8081`
- Kubernetes (port-forwarded): `http://localhost:8081`
- Behind reverse proxy: `https://flink.internal.company.com`

### Kafka Lineage — `kafka_platform_instance`

This is the most important configuration for lineage accuracy. The connector extracts Kafka topic names from Flink execution plans and constructs Kafka dataset URNs. For lineage edges to connect to your existing Kafka datasets in DataHub:

- **`kafka_platform_instance`** must match the `platform_instance` used by your Kafka ingestion source.
- If unset, URNs are constructed using `env` only, which works if your Kafka source also does not set `platform_instance`.

**Example**: If your Kafka source recipe has `platform_instance: "prod-kafka"`, set `kafka_platform_instance: "prod-kafka"` in this connector.

### Job Filtering

Use `job_name_pattern` (allow/deny regex) and `include_job_states` to control which jobs are ingested. By default, jobs in `RUNNING`, `FINISHED`, `FAILED`, and `CANCELED` states are included.

### Stateful Ingestion

When `stateful_ingestion` is enabled, the connector tracks entities across runs. Jobs that no longer exist in the Flink cluster (e.g., deleted or renamed) are automatically soft-deleted in DataHub.

## Current Limitations

- **Kafka-only lineage**: The MVP extracts lineage for Kafka sources and sinks only. Other connectors (JDBC, filesystem, Iceberg, etc.) are not yet supported for automatic lineage extraction.
- **Coalesced operator mode**: Operators from the Flink execution plan are coalesced into a single DataJob per job. Per-operator granularity is not yet available.
- **Execution plan parsing**: Lineage is extracted by parsing the `description` field of execution plan nodes via regex. Flink jobs using custom or unusual connector class names may not be detected.
- **REST API only**: The connector communicates with the JobManager REST API. It does not read Flink's SQL catalog or access the Flink SQL Gateway.
