---
title: Configure Context Generation
description: "Configure which domains have context generated, define metadata eval questions, and control when generated context becomes visible to AI agents."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Configure Context Generation

<FeatureAvailability saasOnly />

:::caution Public Beta
DataHub Context is currently in Public Beta. Features, UI, and configuration options are subject to change.
:::

**Role: Admin (Platform Engineer)**

Admins configure which data domains should have context generated, specify metadata eval questions to assess quality, and control when generated context becomes visible to AI agents.

This guide walks you through:

1. Opening the Context Generation settings
2. Creating a context generation source
3. Creating metadata eval questions

## Prerequisites

In order to configure context generation, you must have the `MANAGE_DOCUMENTS`, `MANAGE_ALL_DOCUMENT_PROPOSALS`, and `MANAGE_AGENTS` privileges — granted by default with the Admin role.

You must also have completed the setup described in [Prerequisites](overview.md#prerequisites), including pinning CLI version 1.6.0.9 and configuring source connectors with query ingestion enabled.

## Step 1: Open the Context Generation Settings

From the DataHub sidebar, head to **Settings** > **Context**.

You will see a list of context generation jobs with their status, last run, and other configured settings. From this list you can:

- **Run** — manually trigger context generation using the Run icon
- **Stop** — stop a running context generation job using the Stop icon
- **Edit or Delete** — modify or remove a context generation source from the menu icon
- **Run History** — view the last run's schedule, duration, and status. Select a run for more detail, and work with the Context Curator agent to resolve run issues.

## Step 2: Create a Context Generation Source

Click **Create** to configure a context generation job. You can either complete the context generation form on the left, or work with the **Context Curator** agent to configure the job conversationally.

Context generation is scoped by **domain** or **container** to focus on data assets that have owners (SMEs) and to keep costs manageable.

1. **Name:** Give your context generation job a clear, descriptive name.
2. **Scope:** At least one domain or container selection is required in order to run context generation.
   Ensure each selected domain or container (database / schema) has datasets ingested from Private Beta supported sources: **Snowflake, Databricks, BigQuery**, and BI tools (**Looker, dbt**). See the reference [Metadata Ingestion Recipes](#reference-metadata-ingestion-recipes).
3. **Publishing settings:** By default, **Auto-publish documents** is disabled. DataHub-generated context is not visible until a Data Expert has validated and published the context documents.
4. **Save:** Choose **Save as Draft** to preserve the configuration only, or **Save & Run** to preserve the configuration and execute it immediately.

The system will process datasets in the selected domains, analyze query history and schema metadata, and produce context documents (also called **semantic anchors**). If it detects associated dbt models or downstream Looker charts or dashboards, it will also incorporate that business context to enrich the quality of the semantic anchors.

:::tip
By default auto-publish is disabled, so generated context documents are unpublished — not visible to agents or humans via search. When auto-publish is enabled, context documents are published **without** SME review.

We recommend creating metadata evals before enabling auto-publish, so that evals run against context documents daily. This gives your team full control over what context the AI can use.
:::

## Reference Metadata Ingestion Recipes

Context Intelligence derives semantic meaning from your query history. If your ingestion recipes are not emitting query history, context generation has little to work from. Use the recipes on this page as a reference when configuring your sources.

### What Gets Emitted

Two distinct things are emitted when you ingest query history:

- **Query entities** (the `queryProperties` aspect) — the statement text plus the datasets it reads and writes.
- **Query usage statistics** (the `queryUsageStatistics` aspect) — how often each query ran, bucketed over time.

Every platform reads query history for the `start_time` to `end_time` window, so set those according to your query volumes. Typically the window should be large enough to cover a representative corpus of analytical queries. Usage counts are grouped by `bucket_duration` (daily by default).

:::note
All recipes below must run on CLI version **1.6.0.9** or later. See [Prerequisites](overview.md#prerequisites).
:::

### Snowflake

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

### Databricks

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

### BigQuery

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

### Redshift

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

## What Happens Next

After generation completes, unpublished context documents are surfaced as **proposals** in **Task Center** > **Requests** for Data Experts and SMEs (domain owners). They validate and publish context from there — see [Review Context Proposals](review-context-proposals.md).

As an Admin, you can also view generated documents directly at **Context** > **Documents**.

## Related Features

- [Domains](https://docs.datahub.com/docs/domains)
- [Metadata Ingestion](https://docs.datahub.com/docs/metadata-ingestion)
- [Roles and Privileges](https://docs.datahub.com/docs/authorization/roles)

## Next Steps

Now that context generation is configured, you're ready to [Review Context Proposals](review-context-proposals.md).
