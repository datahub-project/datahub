### Overview

Use the MariaDB source to ingest relational metadata from MariaDB databases, including tables, views, and optional lineage/profiling signals.

### Prerequisites

- Network access to the MariaDB host and port (for example, `localhost:3306`).
- A MariaDB user with metadata access to the target schemas (typically `SELECT` and `SHOW VIEW`).
- Authentication details configured in the recipe (`username` and `password`).
- If SSL is required, provide MariaDB TLS connect arguments (`ssl_ca`, `ssl_cert`, `ssl_key`) under `options.connect_args`.

### Capabilities

- Ingests MariaDB tables and views with schema and column metadata.
- Supports optional profiling and SQL-based lineage extraction when enabled.
- Supports secure connections through SSL client configuration.
