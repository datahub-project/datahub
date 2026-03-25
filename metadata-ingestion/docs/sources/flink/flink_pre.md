### Overview

The `flink` module ingests metadata from Apache Flink into DataHub. It connects to the Flink JobManager REST API to extract jobs, execution plans, and run history. When a SQL Gateway URL is provided, it resolves SQL/Table API table references to their actual platforms (Kafka, Postgres, Iceberg, Paimon, etc.) via catalog introspection.

### Prerequisites

In order to ingest metadata from Apache Flink, you will need:

- Access to a Flink cluster with the **JobManager REST API** enabled (default port 8081)
- Flink version >= 1.16 (tested with 1.19; platform resolution via `DESCRIBE CATALOG` requires Flink 1.20+)
- For platform-resolved lineage of SQL/Table API jobs: access to a **Flink SQL Gateway** (default port 8083)

#### Required Permissions

| Capability                | API                                   | Required Access                    |
| ------------------------- | ------------------------------------- | ---------------------------------- |
| Job metadata, run history | JobManager REST API (`/v1/jobs`)      | Read access to the REST API        |
| Platform-resolved lineage | SQL Gateway REST API (`/v1/sessions`) | Session creation and SQL execution |

> **_NOTE:_** If your Flink cluster uses authentication (bearer token or basic auth), provide credentials in the `connection` config. The same credentials are used for both the JobManager and SQL Gateway APIs.
