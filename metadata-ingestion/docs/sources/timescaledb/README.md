## Overview

TimescaleDB is an open-source time-series database packaged as a PostgreSQL extension. See the [official TimescaleDB documentation](https://docs.tigerdata.com/).

This connector extends the Postgres source with TimescaleDB-specific metadata: hypertables, continuous aggregates, compression and retention policies, and optionally TimescaleDB background jobs. Both self-hosted TimescaleDB and Tiger Cloud are supported.

## Concept Mapping

| TimescaleDB Concept       | DataHub Entity (Subtype)                    | Notes                                                                                                                        |
| ------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Database                  | Container (Database)                        |                                                                                                                              |
| Schema                    | Container (Schema)                          |                                                                                                                              |
| Table                     | Dataset (Table)                             |                                                                                                                              |
| Hypertable                | Dataset (Table) + `hypertable` tag          | Dimensions, chunk count, compression flag, and retention policy are emitted as custom properties.                            |
| Continuous Aggregate      | Dataset (View) + `continuous_aggregate` tag | Refresh policy and source hypertable are emitted as custom properties. Upstream lineage to the source hypertable is emitted. |
| View / Materialized View  | Dataset (View)                              |                                                                                                                              |
| Stored Procedure          | DataJob (Stored Procedure)                  | Grouped into a `Procedures Container` DataFlow per schema. TimescaleDB policy procedures are filtered out.                   |
| Background Job (optional) | DataJob (Background Job)                    | Grouped into a `Background Jobs` DataFlow per schema. Requires `include_background_jobs: true`.                              |
| Job execution             | DataProcessInstance                         | Recent runs of a background job.                                                                                             |
| Column                    | SchemaField                                 |                                                                                                                              |
