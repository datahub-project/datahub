## Overview

[Hex](https://hex.tech/) is a collaborative data workspace where teams build interactive notebooks combining SQL, Python, and visualizations.

The DataHub integration emits Hex Projects (Dashboards) and Components (Charts) along with workspace containers, ownership, tags from Collections/Status/Categories, usage statistics, and upstream lineage to the warehouses Hex queries. It also emits per-project run history, and per-project context documents for AI agent retrieval (opt-in via `include_context_documents`). Upstream lineage is produced directly from Hex's own APIs (SQL parsing by default; Hex's `queriedTables` API can be enabled on Hex Enterprise workspaces) — no warehouse ingestion dependency is required.

## Concept Mapping

| Hex Concept | DataHub Concept                                                                           | Notes                                                                                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `"hex"`     | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                                                                                                                                                        |
| Workspace   | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)        | Parent container for all projects and components in the workspace.                                                                                     |
| Project     | [Dashboard](https://docs.datahub.com/docs/generated/metamodel/entities/dashboard/)        | Subtype `Project`. Carries usage statistics, last refresh time from run history, and upstream lineage edges to warehouse datasets.                     |
| Component   | [Chart](https://docs.datahub.com/docs/generated/metamodel/entities/chart/)                | Subtype `Component`. Reusable shared cell group with its own visualization; linked to importing projects via `DashboardInfo.charts`.                   |
| Collection  | [Tag](https://docs.datahub.com/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:collection:<name>` when `collections_as_tags` is enabled.                                                                              |
| Status      | [Tag](https://docs.datahub.com/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:status:<name>` when `status_as_tag` is enabled.                                                                                        |
| Category    | [Tag](https://docs.datahub.com/docs/generated/metamodel/entities/tag/)                    | Emitted as `hex:category:<name>` when `categories_as_tags` is enabled.                                                                                 |
| Project Doc | [Document](https://docs.datahub.com/docs/generated/metamodel/entities/document/)          | One per Project and per Component when `include_context_documents` is enabled. Hidden from global search; linked to the Dashboard/Chart for AI agents. |

Other Hex concepts are not mapped to DataHub entities yet.
