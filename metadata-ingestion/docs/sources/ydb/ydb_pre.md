### Overview

The `ydb` module ingests relational metadata from a YDB database into DataHub, including tables, views, and column schemas, with optional SQL-based profiling.

### Prerequisites

- Network access to the YDB endpoint (for example, `localhost:2136`).
- The YDB database path to ingest (for example, `/local` or `/Root/my-db`). A YDB connection is bound to a single database, so this is required.
- A principal with read access to the target database's schema metadata.
- For secured clusters, configure authentication via the options below (static credentials, an access token, or service-account credentials); see the [`ydb-sqlalchemy` connection docs](https://ydb-platform.github.io/ydb-sqlalchemy/connection.html) for the underlying details.
