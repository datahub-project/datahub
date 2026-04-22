## Overview

Microsoft Fabric Data Factory is a cloud-based data integration service within the Microsoft Fabric platform. Learn more in the [official Microsoft Fabric Data Factory documentation](https://learn.microsoft.com/fabric/data-factory/data-factory-overview).

The DataHub integration for Fabric Data Factory covers pipeline and orchestration entities such as workspaces, data pipelines, and activities. Depending on module capabilities, it can also capture features such as lineage, execution history, platform instance mapping, and stateful deletion detection.

## Concept Mapping

| Fabric Data Factory Concept | DataHub Entity                            | Notes                                                         |
| --------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
| **Workspace**               | `Container` (subtype: `Fabric Workspace`) | Top-level organizational unit                                 |
| **Data Pipeline**           | `DataFlow`                                | Orchestration pipeline containing activities                  |
| **Activity**                | `DataJob`                                 | Individual task within a pipeline (Copy, Lookup, Spark, etc.) |
| **Pipeline Run**            | `DataProcessInstance`                     | Execution record for a pipeline run                           |
| **Activity Run**            | `DataProcessInstance`                     | Execution record for an individual activity within a pipeline |
| **Connection**              | _(resolved to external Dataset)_          | Used for lineage resolution to datasets on external platforms |

### Hierarchy Structure

```
Platform (fabric-data-factory)
└── Workspace (Container)
    └── Data Pipeline (DataFlow)
        └── Activity (DataJob)
            ├── Pipeline Run (DataProcessInstance)
            └── Activity Run (DataProcessInstance)
```
