### Overview

The `clickhouse` module ingests metadata from Clickhouse into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure the DataHub host can reach the ClickHouse endpoint (HTTP `8123` / `8443`, or native `9000` / `9440`) and that you have a dedicated ClickHouse user for DataHub.

These grants assume ClickHouse's SQL-driven access control is enabled (the default on ClickHouse Cloud and on modern self-hosted installs — corresponds to `access_management = 1` on the user profile). Under that model, `system.tables`, `system.columns`, and `system.databases` return only rows for objects the user has some privilege on.

`GRANT SELECT ON <database>.*` grants read access to the actual table data. Asset and structural-lineage extraction themselves only read ClickHouse metadata (`system.tables` / `system.columns` / view definitions). Profiling is the capability that runs live `SELECT`s against your tables.

| Capability                               | Required grants                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Assets (tables, views, schemas, columns) | Some privilege on each database you want to ingest, so those objects appear in `system.tables` / `system.columns`. Prefer metadata-only grants when you do not need profiling: `GRANT SHOW TABLES, SHOW COLUMNS ON <database>.* TO datahub`. `GRANT SELECT ON <database>.* TO datahub` also works (and is required for profiling), but it does grant read access to the underlying data. Explicit `GRANT SELECT ON system.tables TO datahub` and `GRANT SELECT ON system.columns TO datahub` are recommended as belt-and-suspenders. |
| Structural lineage (views, MVs, dicts)   | Covered by the asset grants above — lineage for `View`, `MaterializedView`, `Distributed`, and `Dictionary` engines is derived from `system.tables.create_table_query` / `engine_full`, not from reading table rows.                                                                                                                                                                                                                                                                                                                 |
| Query-log lineage and usage statistics   | `GRANT SELECT ON system.query_log TO datahub` (or on your custom view if you set `query_log_table`). Strictly required — without it the query-log fetch fails loudly. Enable via `include_query_log_lineage: true` and/or `include_usage_statistics: true`. No `SELECT` on user databases is needed for this path.                                                                                                                                                                                                                   |
| Table- and column-level profiling        | `GRANT SELECT ON <database>.* TO datahub` for every database you want to profile — profiling runs live `SELECT` queries against the target tables and therefore needs read access to the actual data.                                                                                                                                                                                                                                                                                                                                |

Minimum grants for the full set of capabilities (assets + lineage + usage + profiling):

```sql
CREATE USER datahub IDENTIFIED WITH sha256_password BY '<password>';

-- Strictly required for query-log lineage and usage statistics.
GRANT SELECT ON system.query_log TO datahub;

-- Recommended (belt-and-suspenders — object-level grants below already
-- make these tables visible via ClickHouse's access model, but explicit
-- grants make the intent clear).
GRANT SELECT ON system.tables  TO datahub;
GRANT SELECT ON system.columns TO datahub;

-- Metadata visibility for asset / structural-lineage ingestion.
-- Prefer SHOW when you do not need profiling (no data read).
GRANT SHOW TABLES, SHOW COLUMNS ON <database>.* TO datahub;

-- Required only when profiling is enabled (grants read access to data).
-- Repeat per database you want to profile.
GRANT SELECT ON <database>.* TO datahub;
```

If you enable profiling, `SELECT` alone is enough for both visibility and profiling — you can omit the separate `SHOW TABLES, SHOW COLUMNS` grant.

Notes:

- `SHOW DATABASES` does not need to be granted separately when the grants above are present.
- No write privileges (`INSERT`, `ALTER`, `CREATE`) are required.
- If you route lineage/usage through a custom query-log view (e.g. for a cluster where you union `clusterAllReplicas('cluster', system.query_log)` into a view), grant `SELECT` on that view instead of `system.query_log` and set `query_log_table` in the recipe.
- ClickHouse Cloud uses the same GRANT syntax; run the statements as a user with `GRANT OPTION` on the target objects.
