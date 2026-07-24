## Overview

IBM Informix is a relational database management system used for transactional and analytical workloads. Learn more in the [official Informix documentation](https://www.ibm.com/docs/en/informix-servers).

The DataHub integration for Informix covers tables, views, schema fields, and containers (database/schema hierarchy), with support for stateful deletion detection. It connects via JDBC, so provisioning the IBM Informix JDBC driver is required before ingestion can run.

## Concept Mapping

| Source Concept | DataHub Concept | Notes                                 |
| -------------- | --------------- | ------------------------------------- |
| Database       | Container       | Top-level container.                  |
| Owner (schema) | Container       | Nested under the database container.  |
| Table / View   | Dataset         | `tabtype` `'T'`/`'V'` in `systables`. |
| Column         | SchemaField     | Mapped from `syscolumns.coltype`.     |
