## Overview

Dataplex is a DataHub utility or metadata-focused integration. Learn more in the [official Dataplex documentation](https://cloud.google.com/dataplex).

The DataHub integration for Dataplex uses Universal Catalog entries as the source of truth and maps them into DataHub datasets and containers with provider-native URNs (for example `bigquery`, `cloudsql`, `spanner`, and `pubsub`). Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

Dataplex ingestion is entry-type driven: each Universal Catalog `entry_type` maps to a specific DataHub entity type and hierarchy behavior.

### Supported entry-type mapping

| Dataplex entry type short name | DataHub platform | Emitted entity                 | Parent relationship                                     |
| :----------------------------- | :--------------- | :----------------------------- | :------------------------------------------------------ |
| `bigquery-dataset`             | `bigquery`       | Container (`BigQuery Dataset`) | Parent is project container                             |
| `bigquery-table`               | `bigquery`       | Dataset (`Table`)              | Parent is BigQuery dataset container                    |
| `bigquery-view`                | `bigquery`       | Dataset (`View`)               | Parent is BigQuery dataset container                    |
| `cloudsql-mysql-instance`      | `cloudsql`       | Container (`Instance`)         | Parent is project container                             |
| `cloudsql-mysql-database`      | `cloudsql`       | Container (`Database`)         | Parent is Cloud SQL instance container                  |
| `cloudsql-mysql-table`         | `cloudsql`       | Dataset (`Table`)              | Parent is Cloud SQL database container                  |
| `cloud-spanner-instance`       | `spanner`        | Container (`Instance`)         | Parent is project container                             |
| `cloud-spanner-database`       | `spanner`        | Container (`Database`)         | Parent is Spanner instance container                    |
| `cloud-spanner-table`          | `spanner`        | Dataset (`Table`)              | Parent is Spanner database container                    |
| `pubsub-topic`                 | `pubsub`         | Dataset (`Topic`)              | No parent container from Dataplex entry hierarchy today |
