## Overview

TiDB is an open-source, distributed SQL database that is compatible with the MySQL protocol and MySQL ecosystem. Learn more in the [official TiDB documentation](https://docs.pingcap.com/tidb/stable/overview/).

The DataHub integration for TiDB covers core relational metadata entities such as databases, schemas, tables, views, and schema fields. It also captures table- and column-level lineage, data profiling, and stateful deletion detection.

## Concept Mapping

| Source Concept              | DataHub Concept   | Notes                                         |
| --------------------------- | ----------------- | --------------------------------------------- |
| TiDB cluster or deployment  | Platform Instance | Configure using the `platform_instance` field |
| Database                    | Container         | Groups schemas and datasets                   |
| Schema                      | Container         | Groups tables and views                       |
| Table or view               | Dataset           | Primary ingested technical asset              |
| Column                      | SchemaField       | Included through schema extraction            |
| View and table dependencies | Lineage edges     | Available when lineage extraction is enabled  |
