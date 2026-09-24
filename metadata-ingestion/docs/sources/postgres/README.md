## Overview

Postgres is a data platform used to store and query analytical or operational data. Learn more in the [official Postgres documentation](https://www.postgresql.org/).

The DataHub integration for Postgres covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, data profiling, and stateful deletion detection.

## Concept Mapping

| PostgreSQL Concept                  | DataHub Entity (Subtype)          | Notes                                                                                                                                    |
| ----------------------------------- | --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Database                            | Container (DATABASE)              | Top-level namespace. Multiple databases can be ingested in one run.                                                                      |
| Schema                              | Container (SCHEMA)                | Nested under its Database container.                                                                                                     |
| Table                               | Dataset (TABLE)                   | Includes regular, foreign, temporary, and unlogged tables.                                                                               |
| View / Materialized View            | Dataset (VIEW)                    | View definition is captured. No subtype distinction between standard and materialized views.                                             |
| Stored Procedure                    | DataJob (STORED PROCEDURE)        | Grouped into a DataFlow (PROCEDURES CONTAINER) per schema. Extracted via `pg_proc`.                                                      |
| Column / field                      | SchemaField                       | Column type, nullability, and comments (as descriptions) are extracted.                                                                  |
| Query executor (pg_stat_statements) | CorpUser                          | User attribution for lineage and usage statistics. Requires `pg_stat_statements` extension.                                              |
| View / query lineage                | Lineage edges                     | View-based lineage via `pg_depend`; query-based lineage via `pg_stat_statements` (requires PostgreSQL 13+ and `pg_read_all_stats` role). |
| Query operations and usage          | DatasetUsageStatistics, Operation | From `pg_stat_statements`. Requires `include_query_lineage` and `include_usage_statistics` to be enabled.                                |
