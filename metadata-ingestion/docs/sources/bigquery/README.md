## Overview

BigQuery is a data platform used to store and query analytical or operational data. Learn more in the [official BigQuery documentation](https://cloud.google.com/bigquery).

The DataHub integration for BigQuery covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, and stateful deletion detection.

## Concept Mapping

| BigQuery Concept           | DataHub Entity (Subtype)               | Notes                                                                                                                        |
| -------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| GCP Project                | Platform Instance, Container (PROJECT) | Project ID is used as both the platform instance and the top-level container.                                                |
| Dataset                    | Container (DATASET)                    | Nested under its Project container. Includes location and labels.                                                            |
| Table                      | Dataset (TABLE)                        | Regular and partitioned tables. Schema, descriptions, PKs/FKs, partition keys, clustering columns, and labels are extracted. |
| Sharded Table              | Dataset (SHARDED TABLE)                | Tables with a `_yyyymmdd` suffix pattern; grouped under a shared URN prefix.                                                 |
| External Table             | Dataset (EXTERNAL TABLE)               | Includes source format, URIs, and compression properties.                                                                    |
| Table Snapshot             | Dataset (BIGQUERY TABLE SNAPSHOT)      | Identified by name pattern `{table}@{timestamp_ms}`.                                                                         |
| View                       | Dataset (VIEW)                         | SQL definition captured in ViewProperties. `materialized=false`.                                                             |
| Materialized View          | Dataset (VIEW)                         | SQL definition captured. `materialized=true` in ViewProperties.                                                              |
| Column / field             | SchemaField                            | Includes partition key, clustering key, PKs, FKs, and policy tags where available.                                           |
| Label                      | Tag                                    | Table, view, and dataset labels captured as tags when `capture_*_label_as_tag` is enabled.                                   |
| Policy tag (Data Catalog)  | Tag                                    | Column-level policy tags extracted when `extract_policy_tags_from_catalog` is enabled.                                       |
| Table / column lineage     | Lineage edges                          | From view definitions and audit log query history.                                                                           |
| Query operations and usage | DatasetUsageStatistics, Operation      | Per-dataset and per-column access counts extracted from audit logs.                                                          |
