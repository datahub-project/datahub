---
title: Context Generation
description: "Context Generation turns your team's analytics activity into context documents that show AI agents how your business answers its questions."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Generation

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Generation is part of the DataHub Cloud **Context** add-on and is in Public Beta. Details on this page may change as the feature evolves.
:::

Your team has already answered most of its analytics questions. The answers live in your warehouse query history, BI tools, and semantic models, but rarely in any documentation an agent can use.

**Context Generation** turns that analytics exhaust into **context documents**. Each document captures a business question your team answers repeatedly, and shows an agent how to answer it: which tables to use, how they connect, and what the business calls the result. DataHub calls these generated documents **Semantic Anchors**.

## Why it matters

Hand-written documentation covers the questions everyone asks. It rarely covers the long tail: the hundreds of questions that are each asked by a few people, across many domains, and that no one will ever document by hand.

Context Generation solves this cold-start problem. It gives your agents a map of how your organization actually uses its data, so they can answer long-tail questions correctly and consistently, and your experts can refine a draft instead of starting from a blank page.

## How it works

Context Generation works from metadata that DataHub has already collected. It never reads the data in your tables.

1. **It reads your analytics activity** for the domains, databases, or schemas you choose: warehouse query history, BI and semantic definitions such as Looker explores and Snowflake semantic views, and the descriptions, owners, and usage DataHub already knows.
2. **It finds recurring patterns,** the analyses people run again and again, and sets aside one-off queries.
3. **It writes a context document for each pattern,** describing the business questions it answers and the general shape of the SQL behind it. Raw query logs, which can contain literal values, aren't sent to a language model.

Generated documents are ordinary context documents. You can read, edit, and publish them like any other, and they're refreshed as your data and usage change.

## Where to find it

Go to **Settings > Context** to create and manage Context Generation jobs. Each job defines a scope (the domains, databases, or schemas to cover), where generated documents go, whether they publish automatically, and how often the job runs. Setting up jobs requires the **Manage Platform Settings** privilege.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-generation-create.png"/>
</p>

_Screenshot: configuring a Context Generation job in Settings > Context._

## How it fits

Context Generation is step 3 of [Build a Data Agent](../../../managed-datahub/build-a-data-agent/generate-context.md). A few principles apply:

- **Validate with evals.** Publishing generated documents automatically, then checking them against your [evals](./context-evals.md), is the fastest path to value. For sensitive domains, you can require a person to approve documents first. See [Reviewing Context Changes](./context-review.md).
- **Scope by domain.** Generate context for one domain at a time, and assign the documents to that domain so domain agents can see them.
- **Bring in your BI and semantic models.** Query history shows what people run; BI and semantic definitions show what they meant. Results are best with both.

## Editing generated documents

Generated documents are yours to edit. Once you edit one, it's **detached** from Context Generation: later runs never overwrite it, it isn't removed during cleanup, and your version is the one agents use. Documents nobody has edited continue to update as your data and usage change.

## Prerequisites

Context Generation depends on warehouse query history. Make sure your warehouse's data source collects queries (SQL text) and query usage statistics (how often each query runs), over a window that covers a representative period of analytical work. Connecting your BI tools and dbt improves the business language in generated documents.

### Reference recipes

Context Generation depends on two things your warehouse ingestion emits:

- **Query entities** (`queryProperties`): the SQL text and the datasets it reads and writes
- **Query usage statistics** (`queryUsageStatistics`): how often each query ran

Set `start_time` so the window covers a representative stretch of analytical work. Use CLI version **1.6.0.9** or later. In UI ingestion, set it under **Advanced Settings**.

<details>
<summary>Snowflake</summary>

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

Requires Snowflake Enterprise Edition and `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`. Query extraction reads `ACCOUNT_USAGE.QUERY_HISTORY` and `ACCESS_HISTORY`, which lag real time by roughly 45 minutes to 3 hours.

</details>

<details>
<summary>Databricks</summary>

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

The service principal needs `CAN_USE` on the warehouse and `SELECT` on `system.query.history` and `system.access.table_lineage`. System tables keep about 365 days of history; the REST API fallback keeps about 30.

</details>

<details>
<summary>BigQuery</summary>

```yaml
source:
  type: bigquery
  config:
    project_ids: ["<project>"]
    start_time: "-7 days"

    include_queries: true # default true -> Query entities
    include_query_usage_statistics: true # default true -> queryUsageStatistics

    # Query extraction runs only if lineage or usage is on (both default true)
    include_table_lineage: true
    include_usage_statistics: true

    usage:
      include_operational_stats: false # nested under usage on BigQuery
```

</details>

<details>
<summary>Redshift</summary>

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
    include_query_usage_statistics: true # sub-flag of include_usage_statistics
    include_operational_stats: false # default true; off = no DDL/write operations
```

Redshift keeps only a few days of query history (often two to five), so a `start_time` older than that returns nothing. Each database needs its own recipe, and the ingestion user needs `SYSLOG ACCESS UNRESTRICTED` to read other users' queries.

</details>

## FAQ

**Does Context Generation read our data?**
No. It works from metadata DataHub has already collected, such as query history, schemas, and BI and semantic definitions. It never reads the data in your tables, and raw query logs aren't sent to a language model.

**Will my edits survive the next run?**
Yes. Edited documents are detached from Context Generation, and your version becomes the one agents use. See [Editing generated documents](#editing-generated-documents).

**Does Context Generation use AI Credits?**
Yes. Context Generation runs consume AI Credits, as do evals, Ask DataHub, and custom agents. Scoping jobs to the domains you need keeps usage focused.

**A run created fewer documents than expected.**
Context Generation needs recurring query patterns to work from. Check that query history is being ingested for the tables in scope, over a long enough window. The assistant in **Settings > Context** can help diagnose a run.

## Related

- [Build a Data Agent: Generate context](../../../managed-datahub/build-a-data-agent/generate-context.md)
- [Context Documents](./context-documents.md)
- [Context Evals](./context-evals.md)
- [Reviewing Context Changes](./context-review.md)
