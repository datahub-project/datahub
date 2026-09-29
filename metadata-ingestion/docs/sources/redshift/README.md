## Overview

Redshift is a data platform used to store and query analytical or operational data. Learn more in the [official Redshift documentation](https://aws.amazon.com/redshift/).

The DataHub integration for Redshift covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, ownership, and stateful deletion detection.

## Concept Mapping

| Redshift Concept            | DataHub Entity (Subtype)          | Notes                                                                                                               |
| --------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Cluster / Account           | Platform Instance                 | Top-level scope; all URNs include the configured platform instance.                                                 |
| Database                    | Container (DATABASE)              | Top-level namespace. Shared databases (datashares) are also ingested.                                               |
| Schema                      | Container (SCHEMA)                | Nested under its Database container. External schemas (Glue, Hive, PostgreSQL) include external platform metadata.  |
| Table                       | Dataset (TABLE)                   | Regular, foreign, and external tables (Redshift Spectrum). Custom properties include `dist_style` and `table_type`. |
| View                        | Dataset (VIEW)                    | View definition is captured.                                                                                        |
| Materialized View           | Dataset (VIEW)                    | `materialized=true` in ViewProperties.                                                                              |
| Late Binding View           | Dataset (VIEW)                    | Columns extracted from `pg_get_late_binding_view_cols()`.                                                           |
| External Table (Spectrum)   | Dataset (TABLE)                   | Includes location, input/output format, and SerDe parameters.                                                       |
| Column / field              | SchemaField                       | `dist_key` and `sort_key` columns are tagged accordingly.                                                           |
| User (schema / table owner) | CorpUser                          | Extracted when `extract_ownership` is enabled.                                                                      |
| Table / column lineage      | Lineage edges                     | From STL_SCAN, view dependencies, COPY/UNLOAD commands, ALTER TABLE RENAME, and datashare references.               |
| Query operations and usage  | DatasetUsageStatistics, Operation | From STL_SCAN (provisioned) or SYS_QUERY_DETAIL (serverless).                                                       |
