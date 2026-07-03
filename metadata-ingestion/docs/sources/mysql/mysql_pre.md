### Overview

The `mysql` module ingests metadata from Mysql into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Grant the following privileges to the ingestion user:

- `grant select on DATABASE.* to 'USERNAME'@'%'` (required for metadata and profiling)
- `grant show view on DATABASE.* to 'USERNAME'@'%'` (required for view definitions)

#### Usage Statistics

Set `include_usage_statistics: true` to derive usage statistics and query-based lineage from query
history. The `usage_source` config selects the history source. This query-based table-level lineage
is emitted whenever usage is enabled and is independent of `include_view_lineage` (which only
controls view-definition lineage):

- **`performance_schema`** (default): reads normalized digests from
  `events_statements_summary_by_digest`. Requires the `statements_digest` consumer (the
  `performance_schema` default) and `grant select on performance_schema.* to 'USERNAME'@'%'`. Usage
  counts are aggregated across users and accumulate until the digest table is reset.
- **`general_log`**: reads literal statements with user and timestamp from `mysql.general_log`.
  Requires `general_log=ON`, `log_output=TABLE`, and
  `grant select on mysql.general_log to 'USERNAME'@'%'`. Adds general-log overhead but provides
  per-user attribution and exact query text. If logins are LDAP/database usernames rather than
  emails, set `email_domain` (e.g. `corp.com`) so usage maps to the correct user.

#### AWS RDS IAM Authentication

AWS RDS MySQL supports IAM authentication instead of username/password.

**Setup:**

Follow [AWS RDS IAM Database Authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) to:

1. Enable IAM database authentication on your RDS instance
2. Create database users with IAM authentication
3. Configure IAM policies with `rds-db:connect` permissions

**Configuration:**

Set `auth_mode: "AWS_IAM"` in your recipe. Optionally configure `aws_config` for credentials and region (defaults to boto3's credential chain).
