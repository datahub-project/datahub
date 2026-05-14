## Overview

Grafana is a business intelligence and analytics platform. Learn more in the [official Grafana documentation](https://grafana.com/).

The DataHub integration for Grafana covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept              | DataHub Concept                                           | Notes                                                                                                      |
| --------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `"grafana"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md) |                                                                                                            |
| Grafana Folder              | [Container](../../metamodel/entities/container.md)        | Subtype `Folder`                                                                                           |
| Grafana Dashboard           | [Container](../../metamodel/entities/container.md)        | Subtype `Dashboard`                                                                                        |
| Grafana Panel/Visualization | [Chart](../../metamodel/entities/chart.md)                | Various types mapped based on panel type (e.g., graph → LINE, pie → PIE)                                   |
| Grafana Data Source         | [Dataset](../../metamodel/entities/dataset.md)            | Created for each panel's data source                                                                       |
| Dashboard Owner             | [Corp User](../../metamodel/entities/corpuser.md)         | Dashboard creator assigned as TECHNICAL_OWNER; email suffix removal configurable via `remove_email_suffix` |
| Dashboard Tags              | [Tag](../../metamodel/entities/tag.md)                    | Supports both simple tags and key:value tags                                                               |
