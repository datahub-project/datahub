## Overview

Microsoft Fabric OneLake is a storage and lakehouse platform. Learn more in the [official Microsoft Fabric OneLake documentation](https://learn.microsoft.com/fabric/onelake/onelake-overview).

The DataHub integration for Microsoft Fabric OneLake covers file/lakehouse metadata entities such as datasets, paths, and containers. It also captures stateful deletion detection.

## Concept Mapping

| Microsoft Fabric | DataHub Entity                            | Notes                                       |
| ---------------- | ----------------------------------------- | ------------------------------------------- |
| **Workspace**    | `Container` (subtype: `Fabric Workspace`) | Top-level organizational unit               |
| **Lakehouse**    | `Container` (subtype: `Fabric Lakehouse`) | Contains schemas and tables                 |
| **Warehouse**    | `Container` (subtype: `Fabric Warehouse`) | Contains schemas and tables                 |
| **Schema**       | `Container` (subtype: `Fabric Schema`)    | Logical grouping within lakehouse/warehouse |
| **Table**        | `Dataset`                                 | Tables within schemas                       |

### Hierarchy Structure

```
Platform (fabric-onelake)
└── Workspace (Container)
    ├── Lakehouse (Container)
    │   └── Schema (Container)
    │       └── Table (Dataset)
    └── Warehouse (Container)
        └── Schema (Container)
            └── Table/View (Dataset)
```

### Platform Instance as Tenant

The Fabric REST API does not expose tenant-level endpoints. To represent tenant-level organization in DataHub, set the `platform_instance` configuration field to your tenant identifier (e.g., "contoso-tenant"). This will be included in all container and dataset URNs, effectively grouping all workspaces under the specified platform instance/tenant.
