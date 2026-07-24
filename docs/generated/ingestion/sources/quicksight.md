


# QuickSight

## Overview

Amazon QuickSight is AWS's serverless, cloud-scale business intelligence service for building interactive dashboards and paginated reports. Learn more in the [official QuickSight documentation](https://docs.aws.amazon.com/quicksight/).

The DataHub integration for QuickSight covers BI entities such as dashboards, analyses, and datasets, along with the folder hierarchy that organizes them. It also stitches cross-platform table-level and column-level lineage from QuickSight datasets back to their upstream warehouse/database tables (Athena, Redshift, Snowflake, S3, and more), and optionally captures ownership, AWS resource tags, users/groups, and stateful deletion detection.

## Concept Mapping

| QuickSight  | DataHub                                                          | Notes                                                                         |
| ----------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `Folder`    | [Container](../../metamodel/entities/container.md)               | SubType `"Folder"`; nests via folder membership (Enterprise edition)          |
| `Namespace` | [Container](../../metamodel/entities/container.md)               | SubType `"Namespace"`; opt-in via `add_namespace_container`                   |
| `Dataset`   | [Dataset](../../metamodel/entities/dataset.md)                   | SubType `"Dataset"`; schema from `OutputColumns`, upstream lineage            |
| `Analysis`  | [Dashboard](../../metamodel/entities/dashboard.md)               | SubType `"Analysis"`                                                          |
| `Dashboard` | [Dashboard](../../metamodel/entities/dashboard.md)               | SubType `"Dashboard"`; linked to its source Analysis                          |
| `Visual`    | [Chart](../../metamodel/entities/chart.md)                       | One Chart per visual; emitted when `extract_dashboard_definitions` is enabled |
| `User`      | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md)    | Optionally extracted via `extract_users_and_groups`                           |
| `Group`     | [Group (a.k.a CorpGroup)](../../metamodel/entities/corpGroup.md) | Optionally extracted via `extract_users_and_groups`                           |

QuickSight **data sources** (the raw warehouse/database connections — Athena, Redshift, Snowflake, S3, etc.) are **not** modeled as their own entities. As with Tableau/Looker/PowerBI, the connection is used purely to resolve the upstream platform of each Dataset's tables for lineage; the lineage points directly at the warehouse table.

Account separation is handled via `platform_instance` (the Glue / Redshift / PowerBI convention) rather than an account-level container.


## Module `quicksight`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Enabled via `include_column_lineage`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled via `extract_ownership`. |
| Extract Tags | ✅ | Enabled via `extract_tags`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled via `extract_lineage`. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `quicksight` module ingests metadata from Amazon QuickSight into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- **Folders** (and optionally **Namespaces**) as Containers that organize the browse hierarchy.
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


### Install the Plugin
```shell
pip install 'acryl-datahub[quicksight]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: quicksight
  config:
    # QuickSight is regional — a single run targets one region.
    aws_region: us-east-1

    # AWS authentication (pick one). See AwsConnectionConfig for all options.
    # aws_profile: my-named-profile
    # -- or explicit keys --
    # aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    # aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    # -- or role assumption --
    # aws_role: arn:aws:iam::123456789012:role/datahub-quicksight-ingest

    # Optional - auto-detected via sts:GetCallerIdentity when omitted.
    # aws_account_id: "123456789012"

    # Lineage & enrichment (all default to true except usage / users-groups).
    extract_lineage: true
    include_column_lineage: true
    extract_ownership: true
    extract_tags: true
    # extract_users_and_groups: true   # opt-in; needs ListUsers/ListGroups perms

    # Container hierarchy
    # add_shared_folders_container: true  # synthesize a "Shared folders" root
    # add_namespace_container: true       # only for multi-namespace Enterprise accounts

    # Optional - cross-platform lineage stitching. Key by QuickSight DataSourceId
    # (UUID, preferred) or display name. Must mirror the upstream connector's
    # casing/env so URNs line up.
    # external_data_sources:
    #   "<data-source-uuid-or-name>":
    #     env: PROD
    #     platform_instance: prod-redshift
    #     convert_urns_to_lowercase: true
    #     default_database: analytics   # SQL-parser fallback for unqualified CustomSql tables
    #     default_schema: public

    # Optional - filters (allow/deny regex). Omit to ingest everything.
    # dashboard_pattern:
    #   allow:
    #     - ".*Sales.*"

    # Optional - automatic stale-entity (soft-delete) removal across runs.
    # stateful_ingestion:
    #   enabled: true

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">add_namespace_container</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, QuickSight namespaces are emitted as containers and appear as a level in the browse hierarchy. Most accounts have only the single built-in `default` namespace, so this is off by default (assets sit directly under the platform / platform_instance, or under their folder). Enable it for Enterprise accounts that use multiple namespaces. Mirrors Tableau's `add_site_container`. Account separation is handled via `platform_instance`, not a container — matching the Glue / Redshift / PowerBI / Informatica convention. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">add_shared_folders_container</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, a synthetic `Shared folders` container is emitted as the root of the folder hierarchy and every top-level QuickSight folder is nested beneath it — mirroring the `Shared folders` section in the QuickSight left-nav. (QuickSight's `Shared folders` is a UI category for SHARED-type folders, not a real folder, so it is only synthesized when this is on.) Off by default since we only ingest shared folders, making the level a constant prefix. Loose assets that belong to no folder are unaffected. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_account_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS account ID that owns the QuickSight assets. Auto-detected via `sts:GetCallerIdentity` when not provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">adaptive</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extract_analysis_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch full analysis definitions (sheets/visuals). These payloads are large. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboard_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to fetch full dashboard definitions (sheets/visuals). These payloads are large; disabling this skips Chart entity emission. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract upstream lineage from QuickSight Datasets to their backing warehouse/database tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract ownership from QuickSight resource permissions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract AWS resource tags on QuickSight assets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_users_and_groups</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract QuickSight users and groups (opt-in; often noisy and requires additional permissions). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract column-level lineage for CustomSql datasets via sqlglot. Requires `extract_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When extracting ownership, strip the email domain from user identities so the CorpUser URN uses the bare username (e.g. `jane@acme.com` -> `jane`). Matches Looker's `strip_user_ids_from_email`. For IAM/SSO-federated principals the QuickSight identity is the role-session name (typically the email), which is used regardless of this flag. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">analysis_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">analysis_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">data_source_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">data_source_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">dataset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dataset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">external_data_sources</span></div> <div className="type-name-line"><span className="type-name">map(str,ExternalDataSourceConfig)</span></div> | Per-data-source overrides used when stitching QuickSight Datasets to <br /> their upstream warehouse/database tables. <br />  <br /> Keyed in :class:`QuickSightSourceConfig.external_data_sources` by the stable <br /> QuickSight ``DataSourceId`` (UUID) — preferred because it survives renames in <br /> the QuickSight UI — with the data source display name accepted as a fallback. <br /> UUID matches take precedence over name matches.  |
| <div className="path-line"><span className="path-prefix">external_data_sources.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">external_data_sources.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lower-case identifiers when constructing upstream Dataset URNs. Must match the `convert_urns_to_lowercase` setting used by the corresponding upstream connector recipe (e.g. Snowflake / BigQuery typically preserve case). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">external_data_sources.`key`.</span><span className="path-main">default_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default database/catalog name used as the SQL-parser fallback when a CustomSql definition references unqualified tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">external_data_sources.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default schema name used as the SQL-parser fallback when a CustomSql definition references unqualified tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">external_data_sources.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">namespace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">namespace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">tag_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration (enables stale entity removal). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "AwsAssumeRoleConfig": {
      "additionalProperties": true,
      "properties": {
        "RoleArn": {
          "description": "ARN of the role to assume.",
          "title": "Rolearn",
          "type": "string"
        },
        "ExternalId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "External ID to use when assuming the role.",
          "title": "Externalid"
        }
      },
      "required": [
        "RoleArn"
      ],
      "title": "AwsAssumeRoleConfig",
      "type": "object"
    },
    "ExternalDataSourceConfig": {
      "additionalProperties": false,
      "description": "Per-data-source overrides used when stitching QuickSight Datasets to\ntheir upstream warehouse/database tables.\n\nKeyed in :class:`QuickSightSourceConfig.external_data_sources` by the stable\nQuickSight ``DataSourceId`` (UUID) \u2014 preferred because it survives renames in\nthe QuickSight UI \u2014 with the data source display name accepted as a fallback.\nUUID matches take precedence over name matches.",
      "properties": {
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by this connector belong to",
          "title": "Env",
          "type": "string"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
          "title": "Platform Instance"
        },
        "convert_urns_to_lowercase": {
          "default": false,
          "description": "Whether to lower-case identifiers when constructing upstream Dataset URNs. Must match the `convert_urns_to_lowercase` setting used by the corresponding upstream connector recipe (e.g. Snowflake / BigQuery typically preserve case).",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        },
        "default_database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default database/catalog name used as the SQL-parser fallback when a CustomSql definition references unqualified tables.",
          "title": "Default Database"
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default schema name used as the SQL-parser fallback when a CustomSql definition references unqualified tables.",
          "title": "Default Schema"
        }
      },
      "title": "ExternalDataSourceConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for the Amazon QuickSight ingestion source.\n\nReuses :class:`AwsConnectionConfig` for AWS authentication (explicit keys,\nnamed profile, IAM role assumption, or instance-role auto-detection).\nQuickSight is regional, so a single ingestion run targets one ``aws_region``.",
  "properties": {
    "aws_access_key_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Access Key Id"
    },
    "aws_secret_access_key": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Secret Access Key"
    },
    "aws_session_token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Session Token"
    },
    "aws_role": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "items": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "$ref": "#/$defs/AwsAssumeRoleConfig"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
      "title": "Aws Role"
    },
    "aws_profile": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
      "title": "Aws Profile"
    },
    "aws_region": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS region code.",
      "title": "Aws Region"
    },
    "aws_endpoint_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
      "title": "Aws Endpoint Url"
    },
    "aws_proxy": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
      "title": "Aws Proxy"
    },
    "aws_retry_num": {
      "default": 5,
      "description": "Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
      "title": "Aws Retry Num",
      "type": "integer"
    },
    "aws_retry_mode": {
      "default": "adaptive",
      "description": "Retry mode for failed AWS requests. Defaults to `adaptive` for QuickSight, whose per-API TPS throttling benefits from client-side rate limiting. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs.",
      "enum": [
        "legacy",
        "standard",
        "adaptive"
      ],
      "title": "Aws Retry Mode",
      "type": "string"
    },
    "read_timeout": {
      "default": 60,
      "description": "The timeout for reading from the connection (in seconds).",
      "title": "Read Timeout",
      "type": "number"
    },
    "aws_advanced_config": {
      "additionalProperties": true,
      "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
      "title": "Aws Advanced Config",
      "type": "object"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration (enables stale entity removal)."
    },
    "aws_account_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS account ID that owns the QuickSight assets. Auto-detected via `sts:GetCallerIdentity` when not provided.",
      "title": "Aws Account Id"
    },
    "extract_lineage": {
      "default": true,
      "description": "Whether to extract upstream lineage from QuickSight Datasets to their backing warehouse/database tables.",
      "title": "Extract Lineage",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Whether to extract column-level lineage for CustomSql datasets via sqlglot. Requires `extract_lineage` to be enabled.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "extract_ownership": {
      "default": true,
      "description": "Whether to extract ownership from QuickSight resource permissions.",
      "title": "Extract Ownership",
      "type": "boolean"
    },
    "strip_user_ids_from_email": {
      "default": false,
      "description": "When extracting ownership, strip the email domain from user identities so the CorpUser URN uses the bare username (e.g. `jane@acme.com` -> `jane`). Matches Looker's `strip_user_ids_from_email`. For IAM/SSO-federated principals the QuickSight identity is the role-session name (typically the email), which is used regardless of this flag.",
      "title": "Strip User Ids From Email",
      "type": "boolean"
    },
    "extract_tags": {
      "default": true,
      "description": "Whether to extract AWS resource tags on QuickSight assets.",
      "title": "Extract Tags",
      "type": "boolean"
    },
    "extract_dashboard_definitions": {
      "default": true,
      "description": "Whether to fetch full dashboard definitions (sheets/visuals). These payloads are large; disabling this skips Chart entity emission.",
      "title": "Extract Dashboard Definitions",
      "type": "boolean"
    },
    "extract_analysis_definitions": {
      "default": true,
      "description": "Whether to fetch full analysis definitions (sheets/visuals). These payloads are large.",
      "title": "Extract Analysis Definitions",
      "type": "boolean"
    },
    "extract_users_and_groups": {
      "default": false,
      "description": "Whether to extract QuickSight users and groups (opt-in; often noisy and requires additional permissions).",
      "title": "Extract Users And Groups",
      "type": "boolean"
    },
    "add_shared_folders_container": {
      "default": false,
      "description": "When enabled, a synthetic `Shared folders` container is emitted as the root of the folder hierarchy and every top-level QuickSight folder is nested beneath it \u2014 mirroring the `Shared folders` section in the QuickSight left-nav. (QuickSight's `Shared folders` is a UI category for SHARED-type folders, not a real folder, so it is only synthesized when this is on.) Off by default since we only ingest shared folders, making the level a constant prefix. Loose assets that belong to no folder are unaffected.",
      "title": "Add Shared Folders Container",
      "type": "boolean"
    },
    "add_namespace_container": {
      "default": false,
      "description": "When enabled, QuickSight namespaces are emitted as containers and appear as a level in the browse hierarchy. Most accounts have only the single built-in `default` namespace, so this is off by default (assets sit directly under the platform / platform_instance, or under their folder). Enable it for Enterprise accounts that use multiple namespaces. Mirrors Tableau's `add_site_container`. Account separation is handled via `platform_instance`, not a container \u2014 matching the Glue / Redshift / PowerBI / Informatica convention.",
      "title": "Add Namespace Container",
      "type": "boolean"
    },
    "namespace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight namespaces (only relevant when `add_namespace_container` is enabled)."
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight folders."
    },
    "dashboard_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight dashboards by name."
    },
    "analysis_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight analyses by name."
    },
    "dataset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight datasets by name."
    },
    "data_source_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter QuickSight data sources by name. Data sources are not emitted as entities; denying one suppresses upstream lineage resolution for the datasets backed by it."
    },
    "tag_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter assets by their AWS resource tags (formatted as `key=value`)."
    },
    "external_data_sources": {
      "additionalProperties": {
        "$ref": "#/$defs/ExternalDataSourceConfig"
      },
      "description": "Per-data-source overrides for cross-platform lineage stitching, keyed by QuickSight `DataSourceId` (UUID, preferred) or display name (fallback). Provides the upstream platform_instance, env, URN casing, and SQL-parser default database/schema.",
      "title": "External Data Sources",
      "type": "object"
    }
  },
  "title": "QuickSightSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Cross-platform lineage

QuickSight Datasets are stitched to their upstream warehouse/database tables so lineage spans from a dashboard down to the source table. The connector resolves the upstream platform from the data source's connection parameters (Athena, Redshift, Snowflake, RDS variants, and more). S3-backed datasets are an exception — see the limitation below.

For the upstream Dataset URNs to line up with the platform's own ingested tables, the env, `platform_instance`, and URN casing must match the upstream connector's recipe. Configure these per data source via `external_data_sources`, keyed by the QuickSight `DataSourceId` (UUID, preferred — it survives renames) with the display name accepted as a fallback.

##### Column-level lineage

`CustomSql` datasets carry a SQL definition that is parsed with sqlglot to derive column-level lineage. Unqualified table references are resolved using the `default_database` / `default_schema` configured for that data source in `external_data_sources`. Column-level lineage requires both `extract_lineage` and `include_column_lineage` to be enabled.

#### Ownership, tags, users and groups

With `extract_ownership` enabled, owners are derived from each asset's QuickSight resource permissions. IAM/SSO-federated principals are normalized to the role-session name (typically the user's email) so the resulting `CorpUser` URN matches DataHub's `urn:li:corpuser:<email>` convention; set `strip_user_ids_from_email` to use the bare username instead. `extract_tags` maps AWS resource tags to DataHub tags (filterable via `tag_pattern`). `extract_users_and_groups` (opt-in) emits `CorpUser` / `CorpGroup` entities and their memberships.

#### Stateful ingestion

Enable `stateful_ingestion.enabled` to automatically soft-delete entities that disappear from QuickSight between runs (stale entity removal).

### Limitations

#### Regional scope

QuickSight is a regional service, so a single ingestion run only sees assets in one `aws_region`. Run one recipe per region for multi-region deployments.

#### Folder hierarchy requires Enterprise edition

Folders are a QuickSight Enterprise-edition feature. On Standard-edition accounts no folders exist, so assets are emitted directly under the platform / `platform_instance`.

#### Only account-level folders are ingested (not personal "My folders")

The connector ingests every folder returned by the `ListFolders` API — i.e. both `SHARED` and `RESTRICTED` folder types — and these are filterable by name via `folder_pattern`. Personal **"My folders"** are private to each user and [are not exposed by any QuickSight API](https://community.amazonquicksight.com/t/list-folders-api-does-not-return-user-folders/8981), so they cannot be ingested. Assets that live only in a user's "My folders" (and in no shared/restricted folder) are still ingested as entities — they simply attach to the namespace container (if enabled) or the platform root rather than to a folder.

#### Definition payloads

Chart (visual) entities are only emitted when `extract_dashboard_definitions` is enabled. Definition payloads are large; disable `extract_dashboard_definitions` / `extract_analysis_definitions` to reduce API cost at the expense of visual-level detail.

#### No upstream lineage for S3-backed datasets

QuickSight only exposes the **manifest file location** for S3 data sources (`DescribeDataSource → S3Parameters.ManifestFileLocation`); neither the data source nor the dataset's `PhysicalTableMap.S3Source` carries the underlying data file/prefix paths. Because DataHub's S3 source keys datasets by the data path/prefix, a URN built from the manifest key (`bucket/manifest.json`) would never match an S3-ingested dataset. The connector therefore **skips** upstream lineage for S3-backed datasets rather than emit a dangling edge (the skip count is surfaced in the ingestion report). Relational (Athena/Redshift/Snowflake/…) and CustomSql lineage are unaffected.

### Troubleshooting

#### AccessDeniedException despite a correct IAM policy

QuickSight enforces three independent permission layers (AWS IAM policy, the QuickSight user role, and per-resource share permissions — see Prerequisites). Its `AccessDeniedException` messages **always** point at IAM ("no identity-based policy allows...") even when the real cause is the user's role being `READER` instead of `AUTHOR`, or the asset simply not being shared with the service user. When you hit this error, verify all three layers, not just the IAM policy.

#### Throttling / TPS errors

QuickSight applies per-API transactions-per-second limits. The connector uses adaptive retry mode, but very large accounts may still see throttling — re-run the ingestion, optionally narrowing scope with the `*_pattern` filters.

#### Unresolved upstream lineage

If dashboard-to-table lineage is missing, the upstream Dataset URN produced by QuickSight likely does not match the URN the upstream connector emits. Confirm the env, `platform_instance`, and `convert_urns_to_lowercase` in `external_data_sources` match the upstream recipe exactly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.quicksight.quicksight.QuickSightSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/quicksight/quicksight.py)


:::tip Questions?

If you've got any questions on configuring ingestion for QuickSight, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
