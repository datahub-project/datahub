## Overview

[StarRocks](https://www.starrocks.io/) is a high-performance analytical database that supports real-time, multi-dimensional analytics. It features a multi-catalog architecture that enables federated queries across internal tables and external data sources such as Hive, Iceberg, Hudi, and Delta Lake.

This integration extracts metadata for databases, tables, and views across all catalogs (internal and external). It also supports optional data profiling and stateful ingestion for automatic stale entity removal.

## Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept                                           | Notes                                |
| -------------- | --------------------------------------------------------- | ------------------------------------ |
| `StarRocks`    | [Data Platform](../../metamodel/entities/dataPlatform.md) |                                      |
| Catalog        | [Container](../../metamodel/entities/container.md)        | Subtype `Catalog`                    |
| Database       | [Container](../../metamodel/entities/container.md)        | Subtype `Database`, child of Catalog |
| Table          | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`                      |
| View           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `View`                       |
