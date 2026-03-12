## Overview

Dataplex is a DataHub utility or metadata-focused integration. Learn more in the [official Dataplex documentation](https://cloud.google.com/dataplex).

The DataHub integration for Dataplex covers metadata entities and operational objects relevant to this connector. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Dataplex Concept          | DataHub Concept                                                                     | Notes                                                                        |
| :------------------------ | :---------------------------------------------------------------------------------- | :--------------------------------------------------------------------------- |
| Entry (Universal Catalog) | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | From Universal Catalog. Uses source platform URNs (e.g., `bigquery`, `gcs`). |
| BigQuery Project/Dataset  | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container) | Created as containers to align with native BigQuery connector.               |
