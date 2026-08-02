## Overview

IBM Informix is a relational database management system used for transactional and analytical workloads. Learn more in the [official Informix documentation](https://www.ibm.com/docs/en/informix-servers).

The DataHub integration for Informix covers tables, views, schema fields, and containers (database/schema hierarchy). It also extracts foreign-key relationships, table- and column-level lineage for views, ownership, and approximate row counts, and supports stateful deletion detection. It connects via JDBC, so provisioning the IBM Informix JDBC driver is required before ingestion can run.

## Concept Mapping

| Source Concept | DataHub Concept         | Notes                                                            |
| -------------- | ----------------------- | ---------------------------------------------------------------- |
| Database       | Container               | Top-level container.                                             |
| Owner (schema) | Container               | Nested under the database container.                             |
| Table / View   | Dataset                 | `tabtype` `'T'`/`'V'` in `systables`.                            |
| Column         | SchemaField             | Mapped from `syscolumns.coltype`.                                |
| `owner`        | Ownership (`DATAOWNER`) | The creating database user, applied to schemas and datasets.     |
| Foreign key    | ForeignKeyConstraint    | From `sysconstraints` / `sysreferences`; tables only.            |
| View text      | Upstream lineage        | `sysviews.viewtext`, parsed for table- and column-level lineage. |
| `nrows`        | Dataset profile         | Approximate row count from `systables`, not a row scan.          |
