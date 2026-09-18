---
title: Metadata Ingestion Recipes
description: "Reference ingestion recipes that emit the query entities and query usage statistics the Context Platform depends on."
visible-if:
  showContextHub: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Metadata Ingestion Recipes for Context Generation

<FeatureAvailability saasOnly />

:::caution Public Beta
Supported sources for context generation in Public Beta are **Snowflake**, **Databricks**, **BigQuery**, **Redshift**, **Looker**, and **dbt**.
:::

Context Intelligence derives semantic meaning from your query history. If your ingestion recipes are not emitting query history, context generation has little to work from. Use the recipes on this page as a reference when configuring your sources.

## What Gets Emitted

Two distinct things are emitted when you ingest query history:

- **Query entities** (the `queryProperties` aspect) — the statement text plus the datasets it reads and writes.
- **Query usage statistics** (the `queryUsageStatistics` aspect) — how often each query ran, bucketed over time.

Every platform reads query history for the `start_time` to `end_time` window, so set those according to your query volumes. Typically the window should be large enough to cover a representative corpus of analytical queries. Usage counts are grouped by `bucket_duration` (daily by default).

:::note
All recipes below must run on CLI version **1.6.0.9** or later. See [Prerequisites](overview.md#prerequisites).
:::

## Snowflake

```yaml
source:
  type: snowflake
  config:
    account_id: "<account>"
    warehouse: "<warehouse>"
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASS}"
    role: datahub_role
    start_time: "-7 days"

    include_queries: true # default true -> Query entities
    include_query_usage_statistics: true # default true -> queryUsageStatistics
    include_operational_stats: false # default true; off = no DDL/write operations
```

:::note
Grant `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` and use Snowflake Enterprise Edition. Query extraction reads `ACCOUNT_USAGE.QUERY_HISTORY` and `ACCESS_HISTORY`; without those grants you get no queries.

Account usage views also lag real time by roughly 45 minutes to 3 hours, so the most recent queries may not show up in a run.
:::

## Databricks

```yaml
source:
  type: unity-catalog
  config:
    workspace_url: https://<workspace>.cloud.databricks.com
    token: "${DATABRICKS_TOKEN}"
    warehouse_id: "<sql_warehouse_id>" # recommended
    start_time: "-7 days"

    include_usage_statistics: true # default true -> runs query extraction
    include_queries: true # default true -> Query entities
    include_query_usage_statistics: true # default true -> queryUsageStatistics
    include_operational_stats: false # default true; off = no DDL/write operations
```

:::note
With a Databricks warehouse set, the service principal needs `CAN_USE` on it, plus `SELECT` on `system.query.history` and `system.access.table_lineage`.

The system tables path keeps about 365 days of history; the REST API fallback only goes back about 30 days.
:::

## BigQuery

```yaml
source:
  type: bigquery
  config:
    project_ids: ["<project>"]

    # Query extraction reads the time window from these top-level keys
    start_time: "-7 days"

    include_queries: true # default true -> Query entities
    include_query_usage_statistics: true # default true -> queryUsageStatistics

    # Query extraction runs only if lineage or usage is on (both default true)
    include_table_lineage: true
    include_usage_statistics: true

    usage:
      include_operational_stats: false # default true; nested under usage on BigQuery
```

:::note
On BigQuery, `include_operational_stats` is nested under `usage` rather than set at the top level.
:::

## Redshift

```yaml
source:
  type: redshift
  config:
    host_port: <cluster>.<region>.redshift.amazonaws.com:5439
    database: <database>
    username: datahub
    password: "${REDSHIFT_PASS}"
    email_domain: example.com # required when include_usage_statistics is on
    start_time: "-7 days"

    lineage_generate_queries: true # default true -> Query entities
    include_usage_statistics: true # default FALSE -> enables the usage path
    include_query_usage_statistics: true # default true; sub-flag of include_usage_statistics -> queryUsageStatistics
    include_operational_stats: false # default true; off = no DDL/write operations
```

:::caution
Redshift system tables keep at most about seven days of query history, often closer to two to five. A `start_time` older than that returns nothing.

Each database needs its own recipe, and the ingestion user needs `SYSLOG ACCESS UNRESTRICTED` to read other users' query text.
:::

## Enriching Context with dbt and Looker

Query history tells the system how your data is used; dbt and Looker tell it what the data means. Configure the **dbt** and **Looker** connectors for the datasets in any domain scoped for context generation. When context generation detects associated dbt models or downstream Looker charts and dashboards, it incorporates that business context to improve the quality of the generated semantic anchors.

### Related Features

- [Metadata Ingestion](https://docs.datahub.com/docs/metadata-ingestion)
- [Dataset Usage and Query History](https://docs.datahub.com/docs/features/dataset-usage-and-query-history)

## Next Steps

With your sources emitting query history, return to [Configure Context Generation](configure-context-generation.md) to scope a domain and run your first job.
