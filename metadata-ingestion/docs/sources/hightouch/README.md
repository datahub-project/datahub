## Overview

Hightouch is a Reverse ETL platform that syncs data from your data warehouse to business tools such as CRMs, marketing automation platforms, and analytics applications. Learn more in the [official Hightouch documentation](https://hightouch.com/docs).

The DataHub integration for Hightouch covers sources, models, syncs, destinations, and sync run execution history. It captures coarse-grained and column-level lineage from sources through models to destinations, and supports stateful deletion detection.

## Concept Mapping

| Hightouch Object | DataHub Entity                                                           | Description                                                   |
| ---------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `Source`         | [Dataset](../../metamodel/entities/dataset.md)                           | Source database/warehouse (referenced as input)               |
| `Model`          | [Dataset](../../metamodel/entities/dataset.md)                           | SQL query or transformation (optional, platform: "hightouch") |
| `Sync`           | [Data Job](../../metamodel/entities/dataJob.md)                          | Data pipeline that moves data from model to destination       |
| `Destination`    | [Dataset](../../metamodel/entities/dataset.md)                           | Target system (referenced as output)                          |
| `Sync Run`       | [Data Process Instance](../../metamodel/entities/dataProcessInstance.md) | Execution instance with statistics                            |
| `Workspace`      | Platform Instance                                                        | Hightouch workspace (optional grouping)                       |
