## Overview

Qlik Sense is a business intelligence and analytics platform. Learn more in the [official Qlik Sense documentation](https://www.qlik.com/us/products/qlik-sense).

The DataHub integration for Qlik Sense covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, ownership, and stateful deletion detection.

## Concept Mapping

| Qlik Sense | Datahub                                                                                                | Notes                                                                                            |
| ---------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `Space`    | [Container](../../metamodel/entities/container.md)                                                     | SubType `"Qlik Space"`. On-prem streams too.                                                     |
| `App`      | [Container](../../metamodel/entities/container.md), [Dashboard](../../metamodel/entities/dashboard.md) | SubType `"Qlik App"`. The dashboard links to the app's sheets, so the app shows in lineage.      |
| `Sheet`    | [Dashboard](../../metamodel/entities/dashboard.md)                                                     |                                                                                                  |
| `Chart`    | [Chart](../../metamodel/entities/chart.md)                                                             |                                                                                                  |
| `Dataset`  | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"Qlik Dataset"`. Qlik Cloud catalog datasets (data files in a space).                   |
| App table  | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"Qlik Table"`. Tables in an app's data model and from its load script.                  |
| QVD        | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"QVD"`. Qlik Cloud QVD datasets, and on-prem QVD files that load scripts read or write. |
| `User`     | [User (aka CorpUser)](../../metamodel/entities/corpuser.md)                                            | Optionally Extracted                                                                             |
