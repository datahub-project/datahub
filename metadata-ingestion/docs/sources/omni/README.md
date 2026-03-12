## Overview

Omni is a cloud-native business intelligence platform. Learn more in the [official Omni documentation](https://docs.omni.co/).

The DataHub integration for Omni covers BI entities such as dashboards, charts, semantic datasets, and related ownership context. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Omni Concept   | DataHub Concept                                               | Notes                                                                                  |
| -------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `Folder`       | [Container](../../metamodel/entities/container.md)            | SubType `"Folder"`                                                                     |
| `Dashboard`    | [Dashboard](../../metamodel/entities/dashboard.md)            | Published document with `hasDashboard=true`                                            |
| `Tile`         | [Chart](../../metamodel/entities/chart.md)                    | Each query presentation within a dashboard                                             |
| `Topic`        | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Topic"` — the semantic join graph entry point                                |
| `View`         | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"View"` — semantic layer table with dimensions and measures as schema fields  |
| `Workbook`     | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Workbook"` — unpublished personal exploration document                       |
| Warehouse table | [Dataset](../../metamodel/entities/dataset.md)               | Native platform entity (e.g. Snowflake, BigQuery); linked as upstream of Omni Views   |
| Document owner | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Propagated as `TECHNICAL_OWNER` to Dashboard and Chart entities                        |
