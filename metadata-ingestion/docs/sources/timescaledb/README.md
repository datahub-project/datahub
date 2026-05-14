## Overview

TimescaleDB is an open-source time-series database built as a PostgreSQL extension. It adds first-class support for hypertables, continuous aggregates, compression, and data retention while preserving full SQL and the PostgreSQL ecosystem. Learn more in the [official TimescaleDB documentation](https://docs.tigerdata.com/).

The DataHub integration for TimescaleDB extends the Postgres connector with TimescaleDB-specific metadata. In addition to standard databases, schemas, tables, views, and stored procedures, it surfaces hypertables (with dimensions, chunks, and compression settings), continuous aggregates (with their refresh policies and source hypertables), and — optionally — TimescaleDB background jobs as DataJob/DataFlow entities. Both self-hosted TimescaleDB and Tiger Cloud (managed TimescaleDB) are supported with automatic environment detection.

## Concept Mapping

| TimescaleDB Concept       | DataHub Entity (Subtype)                    | Notes                                                                                                                                                                   |
| ------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database                  | Container (Database)                        | Top-level namespace. Multiple databases can be ingested in one run.                                                                                                     |
| Schema                    | Container (Schema)                          | Nested under its Database container.                                                                                                                                    |
| Table                     | Dataset (Table)                             | Regular PostgreSQL tables.                                                                                                                                              |
| Hypertable                | Dataset (Table) + `hypertable` tag          | Includes dimensions, chunk count, compression flag, and retention policy as custom properties.                                                                          |
| Continuous Aggregate      | Dataset (View) + `continuous_aggregate` tag | Materialized view backed by a hypertable. Includes refresh policy, source hypertable, and materialization settings.                                                     |
| View / Materialized View  | Dataset (View)                              | Standard PostgreSQL views. No subtype distinction between standard and materialized.                                                                                    |
| Stored Procedure          | DataJob (Stored Procedure)                  | Grouped into a DataFlow (Procedures Container) per schema. TimescaleDB policy procedures are filtered out and surfaced as background jobs instead.                      |
| Background Job (optional) | DataJob (Background Job)                    | Continuous aggregate refresh, compression, retention, and reorder jobs. Grouped into a DataFlow (Background Jobs) per schema. Requires `include_background_jobs: true`. |
| Job execution             | DataProcessInstance                         | Recent runs of a background job (success/failure timestamps and counts).                                                                                                |
| Column / field            | SchemaField                                 | Column type, nullability, and comments are extracted as on standard Postgres.                                                                                           |
| Upstream lineage          | Lineage edges                               | Continuous aggregates → their source hypertables; views → their referenced relations via `pg_depend`.                                                                   |
