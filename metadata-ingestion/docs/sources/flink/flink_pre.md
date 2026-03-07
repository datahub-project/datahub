### Prerequisites

In order to ingest metadata from Apache Flink, you will need:

- Access to a Flink cluster with the **JobManager REST API** enabled (default port 8081)
- Flink version >= 1.19 (tested with 1.19)
- For catalog metadata extraction: access to a **Flink SQL Gateway** (default port 8083)

### Required Permissions

| Capability                | API                                   | Required Access                    |
| ------------------------- | ------------------------------------- | ---------------------------------- |
| Job metadata, lineage     | JobManager REST API (`/v1/jobs`)      | Read access to the REST API        |
| Catalog metadata, schemas | SQL Gateway REST API (`/v1/sessions`) | Session creation and SQL execution |

> **_NOTE:_** If your Flink cluster uses authentication (bearer token or basic auth), provide credentials in the `connection` config. The same credentials are used for both the JobManager and SQL Gateway APIs.
