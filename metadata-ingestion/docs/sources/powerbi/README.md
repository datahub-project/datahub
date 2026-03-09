## Overview

Microsoft Power BI is a business intelligence and analytics platform. Learn more in the [official Microsoft Power BI documentation](https://powerbi.microsoft.com/).

The DataHub integration for Microsoft Power BI covers BI entities such as dashboards, charts, datasets, and related ownership context. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| PowerBI           | Datahub             |
| ----------------- | ------------------- |
| `Dashboard`       | `Dashboard`         |
| `Dataset's Table` | `Dataset`           |
| `Tile`            | `Chart`             |
| `Report.webUrl`   | `Chart.externalUrl` |
| `Workspace`       | `Container`         |
| `Report`          | `Dashboard`         |
| `PaginatedReport` | `Dashboard`         |
| `Page`            | `Chart`             |
| `App`             | `Dashboard`         |

- If `Tile` is created from report then `Chart.externalUrl` is set to Report.webUrl.
- The `Page` is unavailable for PowerBI PaginatedReport.
