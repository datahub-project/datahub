## Overview

Doris is a data platform used to store and query analytical or operational data. Learn more in the [official Doris documentation](https://doris.apache.org/).

The DataHub integration for Doris covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, data profiling, and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                          | DataHub Concept      | Notes                                                                     |
| --------------------------------------- | -------------------- | ------------------------------------------------------------------------- |
| Catalog (internal / Iceberg / Hive / …) | Platform Instance    | Set `catalog` (and usually `platform_instance`) for non-default catalogs. |
| Database                                | Container (Database) | Two-tier model; no schema layer.                                          |
| Table / View                            | Dataset              | Primary ingested technical asset.                                         |
| Columns                                 | SchemaField          | Via table reflection.                                                     |
| View SQL                                | Fine/coarse lineage  | Catalog prefixes stripped for URN matching.                               |
