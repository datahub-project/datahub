## Overview

Dataplex is a DataHub utility or metadata-focused integration. Learn more in the [official Dataplex documentation](https://cloud.google.com/dataplex).

The DataHub integration for Dataplex covers metadata entities and operational objects relevant to this connector. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Dataplex Concept          | DataHub Concept                                                                     | Notes                                                                        |
| :------------------------ | :---------------------------------------------------------------------------------- | :--------------------------------------------------------------------------- |
| Entry (Universal Catalog) | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | From Universal Catalog. Uses source platform URNs (e.g., `bigquery`, `gcs`). |
| BigQuery Project/Dataset  | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container) | Created as containers to align with native BigQuery connector.               |

### System Type to Subtype Mapping

The connector automatically assigns DataHub subtypes to datasets based on the Google Cloud system type from `entry.entry_source.system`:

| Google Cloud System Type | DataHub Subtype | Notes                                |
| :----------------------- | :-------------- | :----------------------------------- |
| `CLOUD_PUBSUB`           | Pub/Sub         | Pub/Sub topics                       |
| `BIGQUERY`               | BigQuery        | BigQuery tables                      |
| `CLOUD_BIGTABLE`         | Bigtable        | Bigtable tables                      |
| `DATAPROC_METASTORE`     | Metastore       | Dataproc Metastore tables            |
| `CLOUD_SPANNER`          | Spanner         | Spanner tables                       |
| `CLOUD_STORAGE`          | (none)          | GCS files don't have a specific type |

This mapping helps distinguish different types of datasets in the DataHub UI and enables more precise filtering and search.
