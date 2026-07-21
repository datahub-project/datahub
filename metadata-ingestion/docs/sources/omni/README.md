## Overview

Omni is a cloud-native business intelligence platform. Learn more in the [official Omni documentation](https://docs.omni.co/).

The DataHub integration for Omni covers BI entities such as dashboards, charts, semantic datasets, and related ownership context. It also captures table- and column-level lineage, ownership, and stateful deletion detection.

## Concept Mapping

| Omni Concept    | DataHub Concept                                    | Notes                                                                                                           |
| --------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `Folder`        | [Container](../../metamodel/entities/container.md) | SubType `"Folder"`                                                                                              |
| `Dashboard`     | [Dashboard](../../metamodel/entities/dashboard.md) | Published document with `hasDashboard=true`                                                                     |
| `Tile`          | [Chart](../../metamodel/entities/chart.md)         | Each query presentation within a dashboard                                                                      |
| `Topic`         | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"Topic"` — the semantic join graph entry point                                                         |
| `View`          | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"View"` — semantic layer table with dimensions and measures as schema fields                           |
| `Workbook`      | [Dataset](../../metamodel/entities/dataset.md)     | SubType `"Workbook"` — unpublished personal exploration document                                                |
| Warehouse table | Lineage reference only                             | Not emitted as entities; referenced via URN in upstream lineage of Omni Views (e.g. Snowflake, BigQuery tables) |
| Document owner  | Ownership relationship                             | URN reference only; propagated as `DATAOWNER` ownership aspect to Dashboard and Chart entities                  |
