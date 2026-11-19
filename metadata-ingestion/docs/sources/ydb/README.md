## Overview

YDB is an open-source distributed SQL database that supports both transactional (OLTP) and analytical (OLAP) workloads. Learn more in the [official YDB documentation](https://ydb.tech/docs/en/).

The DataHub integration for YDB extracts core metadata entities — tables, views, schema fields, and the database container — over the [`ydb-sqlalchemy`](https://github.com/ydb-platform/ydb-sqlalchemy) dialect. It also supports optional SQL-based data profiling and stateful deletion detection.

## Concept Mapping

| Source Concept             | DataHub Concept              | Notes                                                                 |
| -------------------------- | ---------------------------- | --------------------------------------------------------------------- |
| Database (connection path) | Container, Platform Instance | The database a YDB connection is bound to (for example `/local`).     |
| Table                      | Dataset                      | YDB has no schema layer — directory paths are part of the table name. |
| View                       | Dataset (`View` subtype)     | View definitions are captured when available.                         |
| Column                     | SchemaField                  | Column types are mapped from the YDB SQLAlchemy types.                |
