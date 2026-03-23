## Overview

Dataplex is a DataHub utility or metadata-focused integration. Learn more in the [official Dataplex documentation](https://cloud.google.com/dataplex).

The DataHub integration for Dataplex uses Universal Catalog entries as the source of truth and maps them into DataHub datasets and containers with provider-native URNs (for example `bigquery`, `cloudsql`, `spanner`, and `pubsub`). Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

Dataplex ingestion is entry-type driven: each Universal Catalog `entry_type` maps to a specific DataHub entity type and hierarchy behavior.

| Dataplex concept                                                        | DataHub concept                                                                                                                                                        | Mapping details                                                                                                                                              |
| :---------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Entry group (`@bigquery`, `@spanner`, etc.)                             | Discovery scope only                                                                                                                                                   | Used to enumerate entries; not emitted as a DataHub entity.                                                                                                  |
| Entry (Universal Catalog)                                               | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset) or [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container) | The connector resolves `entry_type` short name and uses a static mapping to decide whether to emit a Dataset or Container.                                   |
| `fully_qualified_name` (FQN)                                            | URN identity                                                                                                                                                           | The connector validates FQNs using entry-type-specific regexes, then builds native-platform URNs so Dataplex-discovered assets align with native connectors. |
| `parent_entry`                                                          | Container parent relationship                                                                                                                                          | When available, `parent_entry` is parsed into a parent container URN and emitted through the Container relationship aspect.                                  |
| Project scope in FQN                                                    | Project-level Container                                                                                                                                                | The connector emits a project container per `(platform, project_id)` and uses it as parent for top-level containers.                                         |
| Dataplex metadata fields (`entry_id`, timestamps, description, aspects) | Dataset/Container properties and schema                                                                                                                                | Entry metadata is emitted as entity properties/custom properties, and schema is emitted for dataset-like entries when schema extraction is enabled.          |

### Supported entry-type mapping

| Dataplex entry type short name | DataHub platform | Emitted entity                 | Parent relationship                                     |
| :----------------------------- | :--------------- | :----------------------------- | :------------------------------------------------------ |
| `bigquery-dataset`             | `bigquery`       | Container (`BigQuery Dataset`) | Parent is project container                             |
| `bigquery-table`               | `bigquery`       | Dataset (`Table`)              | Parent is BigQuery dataset container                    |
| `cloudsql-mysql-instance`      | `cloudsql`       | Container (`Instance`)         | Parent is project container                             |
| `cloudsql-mysql-database`      | `cloudsql`       | Container (`Database`)         | Parent is Cloud SQL instance container                  |
| `cloudsql-mysql-table`         | `cloudsql`       | Dataset (`Table`)              | Parent is Cloud SQL database container                  |
| `cloud-spanner-instance`       | `spanner`        | Container (`Instance`)         | Parent is project container                             |
| `cloud-spanner-database`       | `spanner`        | Container (`Database`)         | Parent is Spanner instance container                    |
| `cloud-spanner-table`          | `spanner`        | Dataset (`Table`)              | Parent is Spanner database container                    |
| `pubsub-topic`                 | `pubsub`         | Dataset (`Topic`)              | No parent container from Dataplex entry hierarchy today |

:::note
The Spanner search workaround (`search_entries`) is merged into the same processing stream after entry-group traversal. Because those entries are not discovered through `list_entry_groups`, entry-group filters do not apply to that path; use entry-level filters (`filter_config.entries.pattern` / `filter_config.entries.fqn_pattern`) to control Spanner entry inclusion.
:::
