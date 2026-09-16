### Overview

The `clickhouse-usage` module ingests metadata from Clickhouse into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin has the below functionalities:

- For a specific dataset this plugin ingests the following statistics -
  - top n queries.
  - top users.
  - usage of each column in the dataset.
- Aggregation of these statistics into buckets, by day or hour granularity.
- Usage information is computed by querying the `system.query_log` table. In case you have a cluster or need to apply additional
  transformation/filters you can create a view and put to the `query_log_table` setting.

### Prerequisites

Before running ingestion, ensure the DataHub host can reach the ClickHouse endpoint (HTTP `8123` / `8443`, or native `9000` / `9440`) and that the DataHub user has read access to the query-log source. These grants assume ClickHouse's SQL-driven access control is enabled (`access_management = 1`).

| Capability                 | Required grants                                                                                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Usage statistics (default) | `GRANT SELECT ON system.query_log TO datahub` (or on the custom view referenced by `query_log_table`). Strictly required — the fetch fails loudly without it. |

This module does **not** need `SELECT` (or any other privilege) on your user databases. It only reads query text from the query log and runs SQL parsing locally; schema enrichment, when available, comes from the DataHub graph rather than from live reads against ClickHouse tables. Use the main `clickhouse` source (with profiling enabled) when you need table/column profiling.

Example:

```sql
CREATE USER datahub IDENTIFIED WITH sha256_password BY '<password>';

GRANT SELECT ON system.query_log TO datahub;
```

If your query log lives in a custom view (for example on a clustered deployment where you wrap `clusterAllReplicas('cluster', system.query_log)` in a view), grant `SELECT` on that view instead of `system.query_log` and set `query_log_table` in the recipe. No write privileges are required.
