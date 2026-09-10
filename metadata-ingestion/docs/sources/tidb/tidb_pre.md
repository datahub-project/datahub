### Overview

Use the TiDB source to ingest relational metadata from TiDB clusters, including tables, views, and optional lineage/profiling signals.

The source reuses DataHub's MySQL-compatible SQLAlchemy extraction path and emits assets under the TiDB platform (`urn:li:dataPlatform:tidb`).

### Prerequisites

- Network access to the TiDB host and port (for example, `localhost:4000`).
- A TiDB user with metadata access to the target databases and schemas (typically `SELECT` and `SHOW VIEW`).
- Authentication details configured in the recipe (`username` and `password`).
- If TLS is required, provide TiDB TLS connect arguments (`ssl_ca`, `ssl_cert`, `ssl_key`) under `options.connect_args`.
