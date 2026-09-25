## Overview

Mysql is a data platform used to store and query analytical or operational data. Learn more in the [official Mysql documentation](https://www.mysql.com/).

The DataHub integration for Mysql covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, data profiling, and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |
