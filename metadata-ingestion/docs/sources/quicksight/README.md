## Overview

Amazon QuickSight is AWS's serverless, cloud-scale business intelligence service for building interactive dashboards and paginated reports. Learn more in the [official QuickSight documentation](https://docs.aws.amazon.com/quicksight/).

The DataHub integration for QuickSight covers BI entities such as dashboards, analyses, datasets, and data sources, along with the folder hierarchy that organizes them. It also stitches cross-platform table-level and column-level lineage from QuickSight datasets back to their upstream warehouse/database tables (Athena, Redshift, Snowflake, S3, and more), and optionally captures ownership, AWS resource tags, users/groups, and stateful deletion detection.

## Concept Mapping

| QuickSight    | DataHub                                                          | Notes                                                                         |
| ------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `Folder`      | [Container](../../metamodel/entities/container.md)               | SubType `"Folder"`; nests via folder membership (Enterprise edition)          |
| `Namespace`   | [Container](../../metamodel/entities/container.md)               | SubType `"Namespace"`; opt-in via `add_namespace_container`                   |
| `Data Source` | [Dataset](../../metamodel/entities/dataset.md)                   | SubType `"Data Source"`; carries connection parameters                        |
| `Dataset`     | [Dataset](../../metamodel/entities/dataset.md)                   | SubType `"Dataset"`; schema from `OutputColumns`, upstream lineage            |
| `Analysis`    | [Dashboard](../../metamodel/entities/dashboard.md)               | SubType `"Analysis"`                                                          |
| `Dashboard`   | [Dashboard](../../metamodel/entities/dashboard.md)               | SubType `"Dashboard"`; linked to its source Analysis                          |
| `Visual`      | [Chart](../../metamodel/entities/chart.md)                       | One Chart per visual; emitted when `extract_dashboard_definitions` is enabled |
| `User`        | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md)    | Optionally extracted via `extract_users_and_groups`                           |
| `Group`       | [Group (a.k.a CorpGroup)](../../metamodel/entities/corpGroup.md) | Optionally extracted via `extract_users_and_groups`                           |

Account separation is handled via `platform_instance` (the Glue / Redshift / PowerBI convention) rather than an account-level container.
