### Overview

The `quicksight` module ingests metadata from Amazon QuickSight into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- **Folders** (and optionally **Namespaces**) as Containers that organize the browse hierarchy.
- **Data sources** (Athena, Redshift, Snowflake, S3, etc.) as Datasets with the `Data Source` subtype, carrying their connection parameters.
- **QuickSight Datasets** as Datasets with the `Dataset` subtype, including `schemaMetadata` derived from `OutputColumns`.
- **Analyses** and **Dashboards** as Dashboard entities (subtypes `Analysis` and `Dashboard`), with each published Dashboard linked back to the Analysis it was built from.
- **Visuals** within a dashboard's sheets as Chart entities (when `extract_dashboard_definitions` is enabled).
- **Table-level lineage** from QuickSight Datasets to their upstream warehouse/database tables, plus **column-level lineage** for `CustomSql` datasets parsed via sqlglot.
- Optionally **ownership** (from resource permissions), **AWS resource tags**, and **users/groups**.

QuickSight is a regional service, so a single ingestion run targets one `aws_region`. Multi-region deployments run one recipe per region.

### Prerequisites

QuickSight enforces **three independent permission layers** — all three must be satisfied for ingestion to succeed.

#### Layer 1 — AWS IAM policy

There is no AWS-managed read-only policy for QuickSight, so attach the following custom policy to the ingesting principal:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:GetCallerIdentity",
        "quicksight:ListDashboards",
        "quicksight:ListAnalyses",
        "quicksight:ListDataSets",
        "quicksight:ListDataSources",
        "quicksight:ListFolders",
        "quicksight:ListFolderMembers",
        "quicksight:ListNamespaces",
        "quicksight:ListUsers",
        "quicksight:ListGroups",
        "quicksight:ListGroupMemberships",
        "quicksight:ListTagsForResource",
        "quicksight:DescribeDashboard",
        "quicksight:DescribeDashboardPermissions",
        "quicksight:DescribeAnalysis",
        "quicksight:DescribeAnalysisPermissions",
        "quicksight:DescribeDataSet",
        "quicksight:DescribeDataSetPermissions",
        "quicksight:DescribeDataSource",
        "quicksight:DescribeDataSourcePermissions",
        "quicksight:DescribeFolder",
        "quicksight:DescribeFolderPermissions"
      ],
      "Resource": "*"
    }
  ]
}
```

The API operations `DescribeDashboardDefinition` and `DescribeAnalysisDefinition` reuse the `quicksight:DescribeDashboard` / `quicksight:DescribeAnalysis` IAM actions — there is no separate `*Definition` action.

#### Layer 2 — QuickSight user role

The service user's QuickSight role must be **`AUTHOR`** (or `AUTHOR_PRO`) or higher. `READER` is **not** sufficient — it is denied `ListNamespaces`, `ListDataSources`, `ListAnalyses`, and some definition calls. Register or upgrade the user:

```bash
aws quicksight update-user \
  --aws-account-id <ACCOUNT_ID> --namespace default \
  --user-name <IAM_USER_NAME> --email <any-email> --role AUTHOR
```

`AUTHOR` is the least-privileged role that grants full read access; `ADMIN` works but is unnecessarily broad.

#### Layer 3 — Resource permissions

Each asset has its own "Share" permission list. The service user only sees assets it has been shared with. The recommended setup is to create one shared folder, grant the service user Read on it, and place all ingestable assets inside.

> QuickSight's `AccessDeniedException` messages **always blame IAM** ("no identity-based policy allows...") even when the real cause is Layer 2 or Layer 3. Check all three layers when you see this error.
