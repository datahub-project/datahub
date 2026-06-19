### Overview

The `mysql` module ingests metadata from Mysql into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Grant the following privileges to the ingestion user:

- `grant select on DATABASE.* to 'USERNAME'@'%'` (required for metadata and profiling)
- `grant show view on DATABASE.* to 'USERNAME'@'%'` (required for view definitions)

#### Usage Statistics

Set `include_usage_statistics: true` to derive usage statistics and query-based lineage from
`performance_schema.events_statements_summary_by_digest` instead of the general query log. This
requires the `statements_digest` consumer to be enabled (the `performance_schema` default) and
`grant select on performance_schema.* to 'USERNAME'@'%'`. Usage counts are aggregated across users
and accumulate until the digest table is reset; see the Limitations section for details.

#### AWS RDS IAM Authentication

AWS RDS MySQL supports IAM authentication instead of username/password.

**Setup:**

Follow [AWS RDS IAM Database Authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) to:

1. Enable IAM database authentication on your RDS instance
2. Create database users with IAM authentication
3. Configure IAM policies with `rds-db:connect` permissions

**Configuration:**

Set `auth_mode: "AWS_IAM"` in your recipe. Optionally configure `aws_config` for credentials and region (defaults to boto3's credential chain).
