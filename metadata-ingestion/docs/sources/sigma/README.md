## Overview

Sigma is a business intelligence and analytics platform. Learn more in the [official Sigma documentation](https://www.sigmacomputing.com/).

The DataHub integration for Sigma covers BI entities such as dashboards, charts, datasets, and related ownership context. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Sigma       | Datahub                                                       | Notes                       |
| ----------- | ------------------------------------------------------------- | --------------------------- |
| `Workspace` | [Container](../../metamodel/entities/container.md)            | SubType `"Sigma Workspace"` |
| `Workbook`  | [Dashboard](../../metamodel/entities/dashboard.md)            | SubType `"Sigma Workbook"`  |
| `Page`      | [Dashboard](../../metamodel/entities/dashboard.md)            |                             |
| `Element`   | [Chart](../../metamodel/entities/chart.md)                    |                             |
| `Dataset`   | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Sigma Dataset"`   |
| `User`      | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted        |
