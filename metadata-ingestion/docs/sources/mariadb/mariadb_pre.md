### Overview

Use the MariaDB source to ingest relational metadata from MariaDB databases, including tables, views, and optional lineage/profiling signals.

### Prerequisites

- Network access to the MariaDB host and port (for example, `localhost:3306`).
- A MariaDB user with metadata access to the target schemas (typically `SELECT` and `SHOW VIEW`).
- Authentication details configured in the recipe (`username` and `password`).
- If SSL is required, provide MariaDB TLS connect arguments (`ssl_ca`, `ssl_cert`, `ssl_key`) under `options.connect_args`.

#### Usage Statistics Prerequisites

Set `include_usage_statistics: true` to derive usage statistics and query-based lineage from
the `performance_schema.events_statements_summary_by_digest` table. This avoids enabling the
general query log.

- The `statements_digest` consumer must be enabled. It is on by default in MariaDB 10.11
  (`performance_schema = ON`). Verify with:
  ```sql
  SELECT * FROM performance_schema.setup_consumers WHERE NAME = 'statements_digest';
  ```
- Grant the ingestion user read access: `GRANT SELECT ON performance_schema.* TO 'USERNAME'@'%'`.
