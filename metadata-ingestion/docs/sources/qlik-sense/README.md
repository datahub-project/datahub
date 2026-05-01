## Overview

Qlik Sense is a business intelligence and analytics platform. Learn more in the [official Qlik Sense documentation](https://www.qlik.com/us/products/qlik-sense).

The DataHub integration for Qlik Sense covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, ownership, and stateful deletion detection.

## Concept Mapping

| Qlik Sense | Datahub                                                     | Notes                    |
| ---------- | ----------------------------------------------------------- | ------------------------ |
| `Space`    | [Container](../../metamodel/entities/container.md)          | SubType `"Qlik Space"`   |
| `App`      | [Container](../../metamodel/entities/container.md)          | SubType `"Qlik App"`     |
| `Sheet`    | [Dashboard](../../metamodel/entities/dashboard.md)          |                          |
| `Chart`    | [Chart](../../metamodel/entities/chart.md)                  |                          |
| `Dataset`  | [Dataset](../../metamodel/entities/dataset.md)              | SubType `"Qlik Dataset"` |
| `User`     | [User (aka CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted     |
