## Overview

Metabase is a business intelligence and analytics platform. Learn more in the [official Metabase documentation](https://www.metabase.com/).

The DataHub integration for Metabase covers BI entities such as dashboards, charts, datasets, and related ownership context. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                               | Notes                        |
| -------------- | ------------------------------------------------------------- | ---------------------------- |
| `"Metabase"`   | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                              |
| Dashboard      | [Dashboard](../../metamodel/entities/dashboard.md)            |                              |
| Card/Question  | [Chart](../../metamodel/entities/chart.md)                    |                              |
| Model          | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `["Model", "View"]` |
| Collection     | [Tag](../../metamodel/entities/tag.md)                        | Optionally extracted         |
| Database Table | [Dataset](../../metamodel/entities/dataset.md)                | From connected database      |
| User           | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Ownership information        |
