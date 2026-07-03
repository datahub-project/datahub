### Overview

Use the MariaDB source to ingest relational metadata from MariaDB databases, including tables, views, and optional lineage/profiling signals.

### Prerequisites

- Network access to the MariaDB host and port (for example, `localhost:3306`).
- A MariaDB user with metadata access to the target schemas (typically `SELECT` and `SHOW VIEW`).
- Authentication details configured in the recipe (`username` and `password`).
- If SSL is required, provide MariaDB TLS connect arguments (`ssl_ca`, `ssl_cert`, `ssl_key`) under `options.connect_args`.

#### Usage Statistics Prerequisites

Set `include_usage_statistics: true` to derive usage statistics and query-based lineage from query
history. The `usage_source` config selects where that history is read from. This query-based
table-level lineage is emitted whenever usage is enabled and is independent of `include_view_lineage`
(which only controls view-definition lineage):

**`performance_schema` (default)** — reads normalized digests from
`events_statements_summary_by_digest`. No extra setup and no overhead, but query text is normalized
(literals replaced with `?`) and there is no per-user attribution.

- `performance_schema` must be enabled on the server. It is **off by default** in MariaDB, so start
  the server with `performance_schema = ON` (for example `--performance-schema=ON` on the command
  line, or `performance_schema = ON` in `my.cnf`). Once it is enabled the `statements_digest`
  consumer is on by default. Verify with:
  ```sql
  SELECT @@performance_schema;
  SELECT * FROM performance_schema.setup_consumers WHERE NAME = 'statements_digest';
  ```
- Grant the ingestion user read access: `GRANT SELECT ON performance_schema.* TO 'USERNAME'@'%'`.

**`general_log`** — reads literal statements with the executing user and timestamp from
`mysql.general_log`. Use this when you need per-user attribution and exact query text, and accept
the logging overhead.

- Enable the general query log to a table:
  ```sql
  SET GLOBAL log_output = 'TABLE';
  SET GLOBAL general_log = 1;
  ```
- Grant the ingestion user read access: `GRANT SELECT ON mysql.general_log TO 'USERNAME'@'%'`.
- If logins are LDAP/database usernames rather than emails, set `email_domain` so usage attributes
  to the correct user (e.g. `email_domain: corp.com` maps `jdoe` to `jdoe@corp.com`).
