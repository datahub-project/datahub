## Overview

Csv Enricher is a DataHub utility or metadata-focused integration. Learn more in the [official Csv Enricher documentation](https://datahub.com/docs/).

The DataHub integration for Csv Enricher covers metadata entities and operational objects relevant to this connector. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

:::info Looking to ingest a CSV data file into DataHub, as an asset?

Use the Local File ingestion source. The CSV enricher is used for enriching entities already ingested into DataHub.
:::

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |
