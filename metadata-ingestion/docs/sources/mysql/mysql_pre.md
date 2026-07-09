### Overview

The `mysql` module ingests metadata from Mysql into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Grant the following privileges to the ingestion user:

- `grant select on DATABASE.* to 'USERNAME'@'%'` (required for metadata and profiling)
- `grant show view on DATABASE.* to 'USERNAME'@'%'` (required for view definitions)

#### Connection Usage and Limits

With profiling enabled the source opens connections concurrently. While a database is being profiled
it can use up to `pool_size` (default `5`) + `max_overflow` connections, and `max_overflow` defaults
to `profiling.max_workers` (which itself defaults to `5 × CPU count`). On a database replica with a
low per-user cap this can trip an error such as
`User 'USERNAME' has exceeded the 'max_user_connections' resource`.

To stay within a `max_user_connections` limit:

- Lower `profiling.max_workers` (for example to `5`) to cap concurrent profiling connections.
- Or size the SQLAlchemy pool directly via `options`, e.g. `options: {pool_size: 2, max_overflow: 5}`.
- Or disable profiling (`profiling.enabled: false`) if you only need schema and lineage metadata.

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
