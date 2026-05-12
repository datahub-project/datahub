## Overview

Hex is a business intelligence and analytics platform. Learn more in the [official Hex documentation](https://hex.tech/).

The DataHub integration for Hex covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures usage statistics, ownership, and stateful deletion detection.

## Concept Mapping

| Hex Concept | DataHub Concept                                                                           | Notes               |
| ----------- | ----------------------------------------------------------------------------------------- | ------------------- |
| `"hex"`     | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                     |
| Workspace   | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)        |                     |
| Project     | [Dashboard](https://docs.datahub.com/docs/generated/metamodel/entities/dashboard/)        | Subtype `Project`   |
| Component   | [Dashboard](https://docs.datahub.com/docs/generated/metamodel/entities/dashboard/)        | Subtype `Component` |
| Collection  | [Tag](https://docs.datahub.com/docs/generated/metamodel/entities/Tag/)                    |                     |

Other Hex concepts are not mapped to DataHub entities yet.
