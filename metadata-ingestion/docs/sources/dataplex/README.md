## Overview

Google Cloud Knowledge Catalog (Dataplex) is a is a fully managed service that automates the discovery and inventory of your distributed data and AI assets. Learn more in the [official Google Cloud Knowledge Catalog (Dataplex) documentation](https://cloud.google.com/dataplex).

The DataHub integration uses the Universal Catalog entries as the source of truth and maps them into DataHub datasets and containers with provider-native URNs (for example `bigquery`, `cloudsql`, `spanner`, `pubsub`, and `bigtable`). It also captures table-level lineage, Business Glossary ingestion and stateful deletion detection.

## Concept Mapping

The ingestion is entry-type driven: each Universal Catalog `entry_type` maps to a specific DataHub entity type and hierarchy behavior.

### Supported entry-type mapping

| Google Cloud Knowledge Catalog (Dataplex) entry type short name | DataHub platform | Emitted entity                 | Parent relationship                    |
| :-------------------------------------------------------------- | :--------------- | :----------------------------- | :------------------------------------- |
| `bigquery-dataset`                                              | `bigquery`       | Container (`BigQuery Dataset`) | Parent is project container            |
| `bigquery-table`                                                | `bigquery`       | Dataset (`Table`)              | Parent is BigQuery dataset container   |
| `bigquery-view`                                                 | `bigquery`       | Dataset (`View`)               | Parent is BigQuery dataset container   |
| `cloudsql-mysql-instance`                                       | `cloudsql`       | Container (`Instance`)         | Parent is project container            |
| `cloudsql-mysql-database`                                       | `cloudsql`       | Container (`Database`)         | Parent is Cloud SQL instance container |
| `cloudsql-mysql-table`                                          | `cloudsql`       | Dataset (`Table`)              | Parent is Cloud SQL database container |
| `cloud-spanner-instance`                                        | `spanner`        | Container (`Instance`)         | Parent is project container            |
| `cloud-spanner-database`                                        | `spanner`        | Container (`Database`)         | Parent is Spanner instance container   |
| `cloud-spanner-table`                                           | `spanner`        | Dataset (`Table`)              | Parent is Spanner database container   |
| `cloud-spanner-graph`                                           | `spanner`        | Dataset (`Graph`)              | Parent is Spanner database container   |
| `cloud-bigtable-instance`                                       | `bigtable`       | Container (`Instance`)         | Parent is project container            |
| `cloud-bigtable-table`                                          | `bigtable`       | Dataset (`Table`)              | Parent is Bigtable instance container  |
| `pubsub-topic`                                                  | `pubsub`         | Dataset (`Topic`)              | Parent is project container            |
| `vertexai-dataset`                                              | `vertexai`       | Dataset (`Table`)              | Parent is project container            |

### Business Glossary mapping

Dataplex [Business Glossaries](https://cloud.google.com/dataplex/docs/glossaries-overview) are ingested as a three-level hierarchy of DataHub Glossary entities.

| Dataplex entity | DataHub entity | URN pattern                                                    |
| :-------------- | :------------- | :------------------------------------------------------------- |
| Glossary        | `GlossaryNode` | `dataplex.{project_id}.{location}.{glossary_id}`               |
| Category        | `GlossaryNode` | `dataplex.{project_id}.{location}.{glossary_id}.{category_id}` |
| Term            | `GlossaryTerm` | `dataplex.{project_id}.{location}.{glossary_id}.{term_id}`     |

Terms are marked as `EXTERNAL` with a `source_url` pointing to the Dataplex console entry. When `include_glossary_term_associations` is enabled (default), the connector also resolves term-to-asset links via the Dataplex `lookupEntryLinks` API and attaches the corresponding `GlossaryTerm` to each linked DataHub dataset.
