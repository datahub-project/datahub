### Overview

The `clickhouse` module ingests metadata from Clickhouse into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure the DataHub host can reach the ClickHouse endpoint (HTTP `8123` / `8443`, or native `9000` / `9440`) and that you have a dedicated ClickHouse user for DataHub.

These grants assume ClickHouse's SQL-driven access control is enabled (the default on ClickHouse Cloud and on modern self-hosted installs — corresponds to `access_management = 1` on the user profile). Under that model, `system.tables`, `system.columns`, and `system.databases` return only rows the user has some privilege on, so the per-database `SELECT` grant below is what actually makes assets visible.

The connector reads three system tables and issues live `SELECT`s against your data:

| Capability                               | Required grants                                                                                                                                                                                                                                                                                                                                         |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Assets (tables, views, schemas, columns) | `GRANT SELECT ON <database>.* TO datahub` for each database you want to ingest. This is the grant that actually controls what appears in DataHub — without it, tables in that database silently do not get ingested. `GRANT SELECT ON system.tables TO datahub` and `GRANT SELECT ON system.columns TO datahub` are recommended as belt-and-suspenders. |
| Structural lineage (views, MVs, dicts)   | Covered by the asset grants above — lineage for `View`, `MaterializedView`, `Distributed`, and `Dictionary` engines is derived from `system.tables.create_table_query` / `engine_full`.                                                                                                                                                                 |
| Query-log lineage and usage statistics   | `GRANT SELECT ON system.query_log TO datahub` (or on your custom view if you set `query_log_table`). This grant is strictly required — without it the query-log fetch fails loudly. Enable via `include_query_log_lineage: true` and/or `include_usage_statistics: true`.                                                                               |
| Table- and column-level profiling        | `GRANT SELECT ON <database>.* TO datahub` for every database you want to profile — profiling runs live `SELECT` queries against the target tables.                                                                                                                                                                                                      |

Minimum grants for the full set of capabilities (assets + lineage + usage + profiling):

```sql
CREATE USER datahub IDENTIFIED WITH sha256_password BY '<password>';

-- Strictly required for query-log lineage and usage statistics.
GRANT SELECT ON system.query_log TO datahub;

-- Recommended (belt-and-suspenders — the per-database grants below already
-- give the user implicit SHOW visibility of these tables via ClickHouse's
-- access model, but explicit grants make the intent clear).
GRANT SELECT ON system.tables  TO datahub;
GRANT SELECT ON system.columns TO datahub;

-- Repeat per database you want to ingest and/or profile.
-- This is the grant that actually gates whether assets show up in DataHub.
GRANT SELECT ON <database>.* TO datahub;
```

Notes:

- `SHOW DATABASES` / `SHOW TABLES` do not need to be granted separately — they are implied by the `SELECT` grants above.
- No write privileges (`INSERT`, `ALTER`, `CREATE`) are required.
- If you route lineage/usage through a custom query-log view (e.g. for a cluster where you union `clusterAllReplicas('cluster', system.query_log)` into a view), grant `SELECT` on that view instead of `system.query_log` and set `query_log_table` in the recipe.
- ClickHouse Cloud uses the same GRANT syntax; run the statements as a user with `GRANT OPTION` on the target objects.
