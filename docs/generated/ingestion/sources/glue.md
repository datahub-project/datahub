


# Glue

## Overview

Glue is a data platform used to store and query analytical or operational data. Learn more in the [official Glue documentation](https://aws.amazon.com/glue/).

The DataHub integration for Glue covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage and stateful deletion detection.

:::tip
If you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See here for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.
:::

## Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |

### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

### JDBC Lineage

DataHub extracts upstream lineage for Glue job nodes that read from JDBC databases. Two node styles are supported:

#### Named Glue Connections (Visual Editor)

Glue Studio's visual editor stores connection references as `connection_options.connectionName`. DataHub calls the `GetConnection` API to resolve the connection and determine the platform and database.

Supported connection types:

| Glue `ConnectionType` | DataHub Platform                 |
| --------------------- | -------------------------------- |
| `JDBC`                | Parsed from JDBC URL (see below) |
| `POSTGRESQL`          | `postgres`                       |
| `MYSQL`               | `mysql`                          |
| `REDSHIFT`            | `redshift`                       |
| `ORACLE`              | `oracle`                         |
| `SQLSERVER`           | `mssql`                          |

The table is read from `connection_options.dbtable`. If `dbtable` is absent, DataHub falls back to parsing `connection_options.query` (see [SQL Query Lineage](#sql-query-lineage) below).

#### Inline JDBC Nodes (Script Style)

Script-style nodes set `connection_type` to the database protocol and pass the JDBC URL inline via `connection_options.url`. Supported protocols:

| `connection_type` | DataHub Platform | Default schema |
| ----------------- | ---------------- | -------------- |
| `postgresql`      | `postgres`       | `public`       |
| `mysql`           | `mysql`          | —              |
| `mariadb`         | `mysql`          | —              |
| `redshift`        | `redshift`       | `public`       |
| `oracle`          | `oracle`         | —              |
| `sqlserver`       | `mssql`          | `dbo`          |

Example job script args that DataHub can parse:

```python
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://myhost:5432/mydb",
        "dbtable": "public.orders",
        # or: "query": "SELECT * FROM public.orders WHERE region = 'US'"
    },
)
```

#### Dataset Name Construction

Given a `dbtable` value and the resolved `(platform, database)`:

- `dbtable = "schema.table"` → `database.schema.table`
- `dbtable = "table"` (no schema) → `database.<default_schema>.table` if the platform has a default schema, otherwise `database.table`

#### SQL Query Lineage

When `dbtable` is absent and `connection_options.query` is set, DataHub uses [sqlglot](https://github.com/tobymao/sqlglot) to extract table references from the SQL string.

**Supported:** Single-table queries, JOINs, CTEs, subqueries — all referenced tables are emitted as upstream datasets.

```sql
-- All three tables become upstream lineage inputs
SELECT o.id, c.name, p.price
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
```

**Not supported:** Queries that fail to parse, or queries with no table references (e.g. `SELECT 1`). These produce a warning and the node is skipped.

> **Note:** `query`-based lineage reflects the tables referenced in the SQL at ingestion time. Dynamic SQL, parameterized queries, or queries built at runtime cannot be statically analyzed.


## Module `glue`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database. |
| Column-level Lineage | ✅ | Support via the `emit_storage_lineage` config field. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Enabled by default from Glue table created and last modified timestamps. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

The `glue` module ingests metadata from Glue into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Tables in the Glue catalog
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks
- Upstream lineage from JDBC sources (e.g. PostgreSQL, MySQL, Redshift) referenced by Glue jobs

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### IAM permissions

For ingesting datasets, the following IAM permissions are required:

```
{
    "Effect": "Allow",
    "Action": [
        "glue:GetDatabases",
        "glue:GetTables"
    ],
    "Resource": [
        "arn:aws:glue:$region-id:$account-id:catalog",
        "arn:aws:glue:$region-id:$account-id:database/*",
        "arn:aws:glue:$region-id:$account-id:table/*"
    ]
}
```

For ingesting jobs (extract_transforms: True), the following additional permissions are required:

```
{
    "Effect": "Allow",
    "Action": [
        "glue:GetDataflowGraph",
        "glue:GetJobs",
        "glue:GetConnection",
        "s3:GetObject",
    ],
    "Resource": "*"
}
```

The `glue:GetConnection` permission is required when Glue jobs reference named connections (e.g. JDBC connections configured in the Glue console). If your jobs only use inline connection parameters, this permission is not needed.

For profiling datasets, the following additional permissions are required:

```
    {
    "Effect": "Allow",
    "Action": [
        "glue:GetPartitions",
    ],
    "Resource": "*"
}
```

#### Cross-Account Access

The Glue connector supports cross-account access via AWS STS AssumeRole. This allows DataHub running in one AWS account to ingest Glue metadata from a catalog in a different AWS account.

**Setup steps:**

1. **In the target account** (where the Glue catalog lives), create an IAM role with:
   - The Glue permissions policy shown above
   - A trust policy allowing the source account to assume the role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::SOURCE-ACCOUNT-ID:role/DataHubExecutionRole"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "your-unique-external-id"
        }
      }
    }
  ]
}
```

2. **In the ingestion recipe**, configure `aws_config.aws_role` with the target role ARN:

**Simple ARN format:**

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
```

**With External ID** (recommended for security):

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role:
        RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
        ExternalId: "your-unique-external-id"
```

**Role chaining** (assume multiple roles in sequence):

```yaml
source:
  type: glue
  config:
    aws_config:
      aws_role:
        - "arn:aws:iam::INTERMEDIARY-ACCOUNT-ID:role/IntermediateRole"
        - RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubGlueReadRole"
          ExternalId: "your-unique-external-id"
```

The connector uses [boto3's assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.assume_role), so additional parameters like `RoleSessionName`, `DurationSeconds`, and `Policy` are also supported.

**Cross-account catalog access:**

For accessing a specific Glue catalog in another account (without assuming a role), use the `catalog_id` parameter:

```yaml
source:
  type: glue
  config:
    catalog_id: "123456789012" # Target account's AWS account ID
```

This is useful when Account A has shared its Glue catalog with Account B. If you're running ingestion from Account B and want to access Account A's catalog, specify Account A's ID in `catalog_id`.

**Platform instance considerations:**

- **Without platform instance**: If you ingest the same Glue catalog from different accounts without setting `platform_instance`, DataHub recognizes them as the same entities and creates a single dataset.
- **With platform instance**: Using different `platform_instance` values creates separate dataset entities with distinct URNs, useful for tracking the same data through different access paths.

**Mapping catalogs to platform instances:**

A single `platform_instance` stamps every table with the same instance. That is a problem when one ingestion run sees tables owned by **different** accounts — for example a cross-account `catalog_id` ingestion, or Lake Formation tables shared into your catalog. Those tables then get the ingestion account's instance, so their URNs do not match the ones the owning account's own Glue ingestion produces, and the same table appears twice.

Use `catalog_to_platform_instance` to map each owning catalog to the `platform_instance` (and optionally `env`) that the owner uses. Each table is stamped according to its own catalog. The key is the catalog's ARN authority `arn:aws:glue:{region}:{account-id}` — account **and** region, since the same account in two regions is two distinct catalogs:

```yaml
source:
  type: glue
  config:
    aws_region: us-east-1
    platform_instance: ingestion_acct # fallback for tables with no catalog mapping
    catalog_to_platform_instance:
      "arn:aws:glue:us-east-1:111122223333":
        platform_instance: domain_a
        env: PROD
      "arn:aws:glue:us-east-1:444455556666":
        platform_instance: domain_b
```

A table whose catalog is not listed falls back to the source's own `platform_instance`/`env`. For Lake Formation resource links, the connector also emits a table-level upstream lineage edge to the owning table's URN (resolved through this same map), so the shared table stitches back to its source instead of looking like a duplicate.

A resource link is only a pointer, so it carries no schema of its own in the catalog. By default (`resolve_resource_link_schema: true`) the connector populates the link's schema from the owning table so its columns are visible on the ingested dataset — read from **DataHub first** (when the owning account has already been ingested, so no extra AWS call is made) and otherwise from a **cross-account `glue:GetTable`** on the target (requires `glue:GetTable` permission on the shared table). When neither source is available the link is left schemaless and its columns remain reachable through the upstream lineage edge. Set `resolve_resource_link_schema: false` to always keep resource links schemaless.


### Install the Plugin
```shell
pip install 'acryl-datahub[glue]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: glue
  config:
    # Coordinates
    aws_region: "my-aws-region"

sink:
  # sink configs
```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">catalog_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The aws account id where the target glue catalog lives. If None, datahub will ingest glue in aws caller's account. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_storage_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit storage-to-Glue lineage. When enabled, creates lineage relationships between Glue tables and their underlying storage locations (S3 or Iceberg). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_column_parameters</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, column-level Parameters from Glue are ingested as structured properties on each schemaField entity. A StructuredPropertyDefinition is upserted once per unique parameter key per recipe run; subsequent columns reuse the cached definition. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_delta_schema_from_parameters</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | If enabled, delta schemas can be alternatively fetched from table parameters. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lakeformation_column_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True (requires `extract_lakeformation_tags`), Lake Formation tags assigned directly to columns are applied to the corresponding schema fields. If `propagate_lakeformation_tags` is also enabled, the table's and database's tags are inherited by every column as well (matching Lake Formation's inheritance), with a column's own tag taking precedence for the same key. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_lakeformation_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | When True, extracts Lake Formation tags directly assigned to Glue tables/databases. Tags inherited from databases are excluded by AWS from the per-table response; enable `propagate_lakeformation_tags` to apply database tags to their tables as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | When enabled, extracts ownership from Glue table property and overwrites existing owners (DATAOWNER). When disabled, ownership is left empty for datasets. Expects a corpGroup urn, a corpuser urn or only the identifier part for the latter. Not used in the normal course of AWS Glue operations. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_transforms</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to extract Glue transform jobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">glue_storage_lineage_direction</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "upstream", "downstream" <div className="default-line default-line-with-docs">Default: <span className="default-value">upstream</span></div> |
| <div className="path-line"><span className="path-main">ignore_resource_links</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | If set to True, ignore database resource links. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ignore_unsupported_connectors</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore unsupported connectors. If disabled, an error will be raised. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, column-level lineage will be extracted between Glue table columns and storage location fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, the SQL definition of each Glue view (TableType VIRTUAL_VIEW, e.g. Athena/Presto views) is parsed to extract upstream table/view lineage, including column-level lineage. Referenced tables must also be ingested for the lineage to resolve to DataHub entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The platform to use for the dataset URNs. Must be one of ['glue', 'athena']. <div className="default-line default-line-with-docs">Default: <span className="default-value">glue</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">propagate_lakeformation_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True (requires `extract_lakeformation_tags`), Lake Formation tags assigned to a Glue database are inherited by every table in that database, and (when `extract_lakeformation_column_tags` is also enabled) by every column as well — matching Lake Formation's own inheritance. AWS returns the database's tags alongside each table's own tags, so no extra API calls are made. A more-specific assignment overrides an inherited value for the same key (table over database, column over table/database). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">resolve_resource_link_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, populate a Lake Formation resource link's schema so its columns are visible on the ingested dataset — a resource link is schemaless in the catalog because it is only a pointer to the owning account's table. The owning table's schema is read from DataHub first (when that account has already been ingested, so no AWS call is made) and otherwise from a cross-account `glue:GetTable` on the target (requires `glue:GetTable` permission on the shared table). When neither is available the link is left schemaless and its columns remain reachable through its upstream lineage edge. Set to False to always keep resource links schemaless. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_s3_bucket_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_s3_object_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | If an S3 Objects Tags should be created for the Tables ingested by Glue. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">catalog_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,TargetPlatformConfig)</span></div> | Config for aligning dataset URNs with a separately ingested platform.  |
| <div className="path-line"><span className="path-prefix">catalog_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance used by the separate ingestion of this platform. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">catalog_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment used by the separate ingestion of this platform. Defaults to the Glue source env. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">target_platform_configs</span></div> <div className="type-name-line"><span className="type-name">map(str,TargetPlatformConfig)</span></div> | Config for aligning dataset URNs with a separately ingested platform.  |
| <div className="path-line"><span className="path-prefix">target_platform_configs.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance used by the separate ingestion of this platform. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">target_platform_configs.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment used by the separate ingestion of this platform. Defaults to the Glue source env. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GlueProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">column_count</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for column count in glue table. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the max value of a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">mean</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the mean value of a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">median</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the median value of a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">min</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the min value of a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">null_count</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the count of null values in a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">null_proportion</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the proportion of null values in a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">row_count</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for row count in glue table. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">stdev</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the standard deviation of a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">unique_count</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the count of unique value in a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">unique_proportion</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The parameter name for the proportion of unique values in a column. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
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
    "GlueProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "row_count": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for row count in glue table.",
          "title": "Row Count"
        },
        "column_count": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for column count in glue table.",
          "title": "Column Count"
        },
        "unique_count": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the count of unique value in a column.",
          "title": "Unique Count"
        },
        "unique_proportion": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the proportion of unique values in a column.",
          "title": "Unique Proportion"
        },
        "null_count": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the count of null values in a column.",
          "title": "Null Count"
        },
        "null_proportion": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the proportion of null values in a column.",
          "title": "Null Proportion"
        },
        "min": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the min value of a column.",
          "title": "Min"
        },
        "max": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the max value of a column.",
          "title": "Max"
        },
        "mean": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the mean value of a column.",
          "title": "Mean"
        },
        "median": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the median value of a column.",
          "title": "Median"
        },
        "stdev": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The parameter name for the standard deviation of a column.",
          "title": "Stdev"
        },
        "partition_patterns": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns for filtering partitions for profile. The pattern should be a string like: \"{'key':'value'}\"."
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        }
      },
      "title": "GlueProfilingConfig",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
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
    },
    "TargetPlatformConfig": {
      "additionalProperties": false,
      "description": "Config for aligning dataset URNs with a separately ingested platform.",
      "properties": {
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
          "description": "Platform instance used by the separate ingestion of this platform.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Environment used by the separate ingestion of this platform. Defaults to the Glue source env.",
          "title": "Env"
        }
      },
      "title": "TargetPlatformConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_properties": {
      "default": false,
      "description": "When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run.",
      "title": "Incremental Properties",
      "type": "boolean"
    },
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
      "default": "standard",
      "description": "Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
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
    "database_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for databases to filter in ingestion."
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for tables to filter in ingestion."
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
      "description": ""
    },
    "platform": {
      "default": "glue",
      "description": "The platform to use for the dataset URNs. Must be one of ['glue', 'athena'].",
      "title": "Platform",
      "type": "string"
    },
    "extract_owners": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "When enabled, extracts ownership from Glue table property and overwrites existing owners (DATAOWNER). When disabled, ownership is left empty for datasets. Expects a corpGroup urn, a corpuser urn or only the identifier part for the latter. Not used in the normal course of AWS Glue operations.",
      "title": "Extract Owners"
    },
    "extract_transforms": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract Glue transform jobs.",
      "title": "Extract Transforms"
    },
    "ignore_unsupported_connectors": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to ignore unsupported connectors. If disabled, an error will be raised.",
      "title": "Ignore Unsupported Connectors"
    },
    "emit_storage_lineage": {
      "default": false,
      "description": "Whether to emit storage-to-Glue lineage. When enabled, creates lineage relationships between Glue tables and their underlying storage locations (S3 or Iceberg).",
      "title": "Emit Storage Lineage",
      "type": "boolean"
    },
    "glue_storage_lineage_direction": {
      "default": "upstream",
      "description": "If `upstream`, storage locations are upstream to Glue. If `downstream`, they are downstream to Glue.",
      "enum": [
        "upstream",
        "downstream"
      ],
      "title": "Glue Storage Lineage Direction",
      "type": "string"
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "regex patterns for tables to filter to assign domain_key. ",
      "title": "Domain",
      "type": "object"
    },
    "catalog_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The aws account id where the target glue catalog lives. If None, datahub will ingest glue in aws caller's account.",
      "title": "Catalog Id"
    },
    "ignore_resource_links": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "If set to True, ignore database resource links.",
      "title": "Ignore Resource Links"
    },
    "use_s3_bucket_tags": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files.",
      "title": "Use S3 Bucket Tags"
    },
    "use_s3_object_tags": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "If an S3 Objects Tags should be created for the Tables ingested by Glue.",
      "title": "Use S3 Object Tags"
    },
    "extract_lakeformation_tags": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "When True, extracts Lake Formation tags directly assigned to Glue tables/databases. Tags inherited from databases are excluded by AWS from the per-table response; enable `propagate_lakeformation_tags` to apply database tags to their tables as well.",
      "title": "Extract Lakeformation Tags"
    },
    "propagate_lakeformation_tags": {
      "default": false,
      "description": "When True (requires `extract_lakeformation_tags`), Lake Formation tags assigned to a Glue database are inherited by every table in that database, and (when `extract_lakeformation_column_tags` is also enabled) by every column as well \u2014 matching Lake Formation's own inheritance. AWS returns the database's tags alongside each table's own tags, so no extra API calls are made. A more-specific assignment overrides an inherited value for the same key (table over database, column over table/database).",
      "title": "Propagate Lakeformation Tags",
      "type": "boolean"
    },
    "extract_lakeformation_column_tags": {
      "default": true,
      "description": "When True (requires `extract_lakeformation_tags`), Lake Formation tags assigned directly to columns are applied to the corresponding schema fields. If `propagate_lakeformation_tags` is also enabled, the table's and database's tags are inherited by every column as well (matching Lake Formation's inheritance), with a column's own tag taking precedence for the same key.",
      "title": "Extract Lakeformation Column Tags",
      "type": "boolean"
    },
    "profiling": {
      "$ref": "#/$defs/GlueProfilingConfig",
      "description": "Configs to ingest data profiles from glue table"
    },
    "extract_delta_schema_from_parameters": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "If enabled, delta schemas can be alternatively fetched from table parameters.",
      "title": "Extract Delta Schema From Parameters"
    },
    "include_column_lineage": {
      "default": true,
      "description": "When enabled, column-level lineage will be extracted between Glue table columns and storage location fields.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "resolve_resource_link_schema": {
      "default": true,
      "description": "When enabled, populate a Lake Formation resource link's schema so its columns are visible on the ingested dataset \u2014 a resource link is schemaless in the catalog because it is only a pointer to the owning account's table. The owning table's schema is read from DataHub first (when that account has already been ingested, so no AWS call is made) and otherwise from a cross-account `glue:GetTable` on the target (requires `glue:GetTable` permission on the shared table). When neither is available the link is left schemaless and its columns remain reachable through its upstream lineage edge. Set to False to always keep resource links schemaless.",
      "title": "Resolve Resource Link Schema",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "When enabled, the SQL definition of each Glue view (TableType VIRTUAL_VIEW, e.g. Athena/Presto views) is parsed to extract upstream table/view lineage, including column-level lineage. Referenced tables must also be ingested for the lineage to resolve to DataHub entities.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "extract_column_parameters": {
      "default": false,
      "description": "When enabled, column-level Parameters from Glue are ingested as structured properties on each schemaField entity. A StructuredPropertyDefinition is upserted once per unique parameter key per recipe run; subsequent columns reuse the cached definition.",
      "title": "Extract Column Parameters",
      "type": "boolean"
    },
    "target_platform_configs": {
      "additionalProperties": {
        "$ref": "#/$defs/TargetPlatformConfig"
      },
      "description": "Optional per-platform config for aligning dataset URNs with separately ingested platforms. Keys are DataHub platform names (e.g. 'postgres', 'mysql', 'redshift'). When provided, the platform_instance and env are applied to dataset URNs so they match the URNs produced by the platform's own connector. Only needed when the target platform's connector uses a platform_instance or a different env.",
      "title": "Target Platform Configs",
      "type": "object"
    },
    "catalog_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/TargetPlatformConfig"
      },
      "description": "Maps a Glue catalog's ARN authority `arn:{partition}:glue:{region}:{account}` to the platform_instance (and optionally env) to stamp on tables owned by that catalog. Use this for cross-account catalogs (e.g. ingesting another account via `catalog_id`, or Lake Formation shared tables) so each table's URN matches the one the owning account's own Glue ingestion produces, instead of the ingestion account's instance. The key is account + region \u2014 account alone is not unique across regions \u2014 and matches the key used by the Spark/OpenLineage `connections` map. The partition must match the region as resolved by boto3 \u2014 `aws` for commercial, `aws-us-gov` for GovCloud, `aws-cn` for China, and the `aws-iso*` partitions for isolated regions.",
      "title": "Catalog To Platform Instance",
      "type": "object"
    }
  },
  "title": "GlueSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Cross-Account Lineage

When ingesting Glue metadata from different AWS accounts, the connector can establish lineage relationships across account boundaries. This is particularly useful for tracking data flows in multi-account AWS environments.

**How it works:**

When you ingest a Glue catalog that references storage (e.g., S3 buckets) or other resources in different accounts, the connector creates lineage edges to those resources. For example:

- Account A has a Glue table pointing to an S3 bucket in Account A
- Account B has shared access to Account A's Glue catalog (using `catalog_id`)
- When you ingest from Account B, DataHub creates lineage from the Glue table to the S3 bucket

**URN resolution and entity merging:**

The connector's behavior depends on your `platform_instance` configuration:

- **Without platform instance**: If you ingest the same Glue catalog from different accounts without setting `platform_instance`, DataHub recognizes them as the same entities and creates a single dataset. Lineage to S3 (or other storage) is preserved regardless of which account performed the ingestion.

  ```yaml
  # Account A ingestion
  source:
    type: glue
    config:
      aws_config:
        aws_region: us-east-1
  ```

  ```yaml
  # Account B ingestion (using catalog_id)
  source:
    type: glue
    config:
      catalog_id: "111111111111" # Account A's ID
      aws_config:
        aws_region: us-east-1
  ```

  Result: Single Glue table entity with lineage to S3.

- **With platform instance**: Using different `platform_instance` values creates separate dataset entities with distinct URNs. This is useful for tracking the same data through different access paths or maintaining separate views per account.

  ```yaml
  # Account A ingestion
  source:
    type: glue
    config:
      platform_instance: account-a
      aws_config:
        aws_region: us-east-1
  ```

  ```yaml
  # Account B ingestion
  source:
    type: glue
    config:
      catalog_id: "111111111111" # Account A's ID
      platform_instance: account-b
      aws_config:
        aws_region: us-east-1
  ```

  Result: Two separate Glue table entities (one per platform instance), each with its own lineage edges.

**Storage lineage:**

To see lineage between Glue tables and their underlying S3 locations, you must:

1. Set `emit_storage_lineage: true` in the Glue connector config
2. Ingest S3 separately using the [S3 connector](/docs/generated/ingestion/sources/s3)

The S3 connector creates dataset entities for the storage locations, and the Glue connector creates lineage edges connecting Glue tables to those S3 datasets. This works seamlessly across account boundaries when both connectors are configured correctly.

For cross-account S3 access, refer to the [S3 connector's cross-account documentation](/docs/generated/ingestion/sources/s3#cross-account-access).

#### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

#### JDBC Upstream Lineage

When a Glue job reads from a JDBC source (e.g. PostgreSQL, MySQL, Redshift, Oracle, SQL Server), the plugin automatically extracts upstream lineage to the referenced tables. This works for both:

- **Direct JDBC connections** specified inline in the job script (via `connection_type` and `connection_options`)
- **Named Glue connections** configured in the Glue console and referenced by `connectionName`

Supported JDBC platforms: PostgreSQL, MySQL, MariaDB, Redshift, Oracle, SQL Server.

The plugin resolves table references from either the `dbtable` parameter or by parsing SQL from the `query` parameter.

#### Aligning URNs with Target Platform Connectors

If you also ingest the JDBC source separately (e.g. using the `postgres` or `mysql` connector) and that connector uses a `platform_instance` or a different `env`, you should configure `target_platform_configs` so the URNs match:

```yaml
source:
  type: glue
  config:
    target_platform_configs:
      postgres:
        platform_instance: prod-postgres
        env: PROD
      mysql:
        platform_instance: prod-mysql
```

When this is configured, dataset URNs produced by the Glue connector will include the same `platform_instance` and `env` as the target platform's connector, ensuring entities merge correctly in DataHub. If the target platform connector does not use a `platform_instance`, no configuration is needed — URNs will match by default.

#### Incremental Dataset Properties (PATCH Mode)

When `incremental_properties: true` is set, the Glue connector emits dataset properties as incremental PATCH operations rather than full overwrites. This means custom properties added to a dataset via the DataHub API, SDK, or other ingestion sources are preserved across Glue ingestion runs.

This is useful when you enrich datasets with additional metadata (e.g., ownership annotations, data quality scores, or business context) outside of the Glue catalog and want those properties to survive re-ingestion.

```yaml
source:
  type: glue
  config:
    incremental_properties: true
```

#### Column Parameters as Structured Properties

When `extract_column_parameters` is enabled, column-level `Parameters` from the Glue catalog are
ingested as structured properties on each `schemaField` entity. This surfaces Glue metadata such as Iceberg field IDs directly in the
DataHub Properties tab.

```yaml
source:
  type: glue
  config:
    extract_column_parameters: true
```

**How it works:**

- For each column that has a non-empty `Parameters` map, a `StructuredProperties` aspect is emitted
  on the corresponding `schemaField` entity.
- Each unique parameter key produces one `StructuredPropertyDefinition` per recipe run. Subsequent
  columns reusing the same key share the existing definition (no duplicate upserts).
- Property URNs follow the pattern
  `urn:li:structuredProperty:io.datahubproject.glue.column.<sanitized-key>`, where non-alphanumeric
  characters (except `.`) are replaced with `_`.
- Partition key columns are also processed.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

JDBC upstream lineage from SQL queries (`query` parameter) does not currently apply `target_platform_configs`. Only the `dbtable` code path uses the configured `platform_instance` and `env`.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.aws.glue.GlueSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aws/glue.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Glue, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
