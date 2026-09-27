---
title: Context Generation
description: "Generate context documents from query history, Looker explores, and Snowflake semantic views so AI agents know how your business actually answers its questions."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Generation

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Generation is part of the DataHub Cloud **Context** add-on and is in Public Beta. Screens and settings may change as we learn from you.
:::

Your team has already answered most of its analytics questions. The answers are sitting in query history, in Looker explores, and in semantic views. Nobody wrote them down as documentation, and nobody is going to.

Context Generation writes it down for you. It mines your analytics exhaust (the query history, BI models, and semantic definitions DataHub has already collected), finds the analyses people run again and again, and turns each one into a **context document** that tells an AI agent which tables to use, how they join, and what the business calls the result.

Generated documents have the subtype **Semantic Anchor**. They solve the cold-start problem for business context. Think of each one as a map for a recurring question: "Here's how analysts figure out trial conversion. Start here, join this, filter that."

Hand-curated documentation is great for the questions everyone asks. Context Generation is for the long tail: the four hundred other questions that are each asked by three people, and that nobody will ever write a wiki page for.

## What it looks at

Context Generation only reads metadata that's already in DataHub. It never reads the data in your tables or runs queries against your warehouse. Raw query logs, which can contain literal values, aren't sent to a language model: queries are first grouped deterministically by parsing their SQL, and documents describe the general query pattern.

| Input                                                         | How it's used                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Query history** (Snowflake, Databricks, BigQuery, Redshift) | The main source. For the tables in your scope, it keeps analytical queries, groups the ones that answer the same kind of question, and focuses on patterns that recur and that several people use. Requires queries ingested by your warehouse's data source. |
| **BI assets**                                                 | When queries came from a dashboard or notebook, its title and description help explain what the analysis is for. Looker explores each become a document, built from their declared views, joins, and fields.                                                  |
| **Snowflake semantic views**                                  | One document per semantic view. The curated definition is the evidence, so no query history is needed.                                                                                                                                                        |
| **Existing metadata**                                         | Table and column descriptions, dbt models, glossary terms, owners, and usage enrich the documents and link them to related assets.                                                                                                                            |

Query history is what people actually run. BI and semantic definitions are what people meant. The best results come from having both.

## What you get

Each generated document captures one recurring analysis:

- **Title and description** in business language, like "Trial Company Conversion Metrics"
- **Business questions**: several phrasings of the questions this analysis answers. This is what agents match against, so a question can land on the right document without naming a single table.
- **Query pattern cards**: the tables, join predicates, metrics, dimensions, filters, and grain behind the analysis. The original SQL isn't copied into the document.
- **Related assets**: datasets, dbt models, Looker views, and glossary terms
- **Usage and experts**: how often the pattern runs, and who owns the underlying data

Documents land in **Context > Documents**, in a shared **Semantic Anchor** folder by default.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/semantic-anchor-document.png"/>
</p>

_Screenshot: a generated Semantic Anchor document, with business questions and query pattern cards._

## Before you start

**1. Ingest query history.** No query history, no context. Make sure your warehouse's data source collects queries and how often they run. See [Reference recipes](#reference-recipes) below.

**2. Ingest your BI and dbt metadata.** Connect Looker, dbt, and other BI tools for the tables you plan to cover. It's optional, but it noticeably improves the business language in the output.

**3. Set up domains.** Scope jobs by domain, and assign the documents they write to that domain, so domain agents can see them.

**4. Check privileges.** Creating jobs requires the **Manage Platform Settings** privilege.

## Create a job

1. Go to **Settings > Context** and click **Create** (or **Generate context** if this is your first job).
2. Give it a **Name**, such as `Finance - Revenue`.
3. **Scope:** pick at least one **Domain** or **Container** (database or schema).
4. Open the optional sections:

| Setting                      | What it does                                                                                                                                                            |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Assign generated docs to** | Assigns every document the job writes to a domain. Use it so domain agents and domain MCP servers see the documents.                                                    |
| **Folder**                   | Sends documents to a folder of your choice in **Documents**. By default, they go to a shared **Semantic Anchor** folder.                                                |
| **Auto-publish**             | Publishes new documents right away. We recommend turning it on and validating with evals. See [Publishing: auto-publish or review](#publishing-auto-publish-or-review). |
| **Reviewers**                | With auto-publish off, the users and groups who review new documents. If empty, DataHub picks the owners of the tables and domains involved, plus admins.               |
| **Schedule**                 | Off by default, so the job runs only when you start it. Turn it on to keep documents fresh as query behavior changes.                                                   |

5. Click **Save & run**, or **Save draft** if you're not ready.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-generation-create.png"/>
</p>

_Screenshot: creating a context generation job, with the Context Curator panel on the right._

The **Ask Context Curator** panel on the right can fill in the form for you, suggest a scope based on recent activity, and check whether your warehouse is capturing queries. It's also handy on a failed run: open the run and ask what went wrong.

:::tip Start with one domain
Scope your first job to one domain or schema that has a real owner and real questions. Check the results with your evals, then expand.
:::

### Manage jobs

**Settings > Context** lists your jobs with their scope, schedule, last run, and status. From there you can run or stop a job, toggle auto-publish inline, and edit or delete it from the **⋮** menu. The **Run History** tab shows each run's duration and logs.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-generation-sources.png"/>
</p>

_Screenshot: the context generation jobs list in Settings > Context._

When a run finishes, you'll see how many documents it created, with a shortcut to review them.

### Advanced settings (YAML)

The **YAML** tab exposes settings the form doesn't. You'll rarely need them, but these come up:

| Setting                                        | Default        | Use it when                                                                                    |
| ---------------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------- |
| `scope.allow_datasets` / `allow_data_products` | `[]`           | You want to scope to specific datasets or data products.                                       |
| `scope.allow_platforms`                        | `[]`           | You want to keep only certain platforms after scope expansion.                                 |
| `must_exclude_queries_from_users`              | `[]`           | Service accounts or scheduled jobs are drowning out real analyst behavior.                     |
| `must_include_queries_from_users`              | ingestion user | Some users' queries should always count, regardless of thresholds.                             |
| `looker_anchors.selection`                     | `everything`   | You only want documents for certain Looker projects. Looker selection ignores the job's scope. |
| `anchor_groups_tuning_mode`                    | `auto`         | You want to set `min_count` and `min_distinct_users` yourself (`manual`).                      |

## Publishing: auto-publish or review

**We recommend auto-publish for most teams.** A single run can produce dozens or hundreds of documents, and reviewing each one by hand before publishing slows you down for weeks. Publish right away, and let your [evals](./context-evals.md) tell you whether the documents help:

1. After a run, click **Run all DataHub evals** on the Evals page.
2. Open any eval that got worse or still fails, and check which documents the agent found.
3. Edit or unpublish documents that are wrong.

Keep daily eval runs on, so a scheduled refresh that makes things worse shows up the next morning.

### When you want human review

For a sensitive domain, turn auto-publish off. New documents are created **unpublished** and grouped into **proposals** for the **Reviewers** you set, or the table and domain owners plus admins if you didn't. Reviewers find proposals in **Tasks > Proposals** and get a Slack or email digest when a run finishes.

In a proposal, reviewers can:

- **Edit** a document's title, description, business questions, and query patterns
- **Comment** for other reviewers. Comments are never read by AI agents.
- Flip each document between **Will be published** and **Will be unpublished**
- Add evals under **Impact on Evals** and run them to see what the proposal fixes or breaks, as if it were already published
- **Try in Ask DataHub** to chat with the proposal applied
- **Apply Changes** to finalize, or **Reject** to close the proposal without changes

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-proposal-review.png"/>
</p>

_Screenshot: reviewing a context proposal, with the Impact on Evals section._

With review on, later runs can also open proposals to edit or unpublish documents, or to resolve conflicts. When two documents answer the same question differently, DataHub flags a conflict, and a person decides which one is right. See [Reviewing Context Changes](./context-review.md).

## Editing generated documents

Generated documents are yours to edit. Fix a join, sharpen a business question, or add a caveat the job couldn't know about.

Once you edit a document, it's **detached** from Context Generation:

- Your edits stay. Later runs never rewrite a document a person has changed.
- Your version is authoritative. Agents use what you wrote.
- Cleanup leaves it alone. If the job stops seeing the query pattern behind it, the document isn't removed.

Documents nobody has edited keep updating as query behavior changes.

## Using generated context

Published documents are ordinary context documents. They show up in search, in [Ask DataHub](../ask-datahub.md), in [agents](../agents.md), and through the [MCP server](../mcp.md). The [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow) skill teaches external agents to look for them first when writing SQL.

You can also turn them into tests: **Generate Evals** on the Evals page drafts eval questions from your published documents' business questions.

## Limits

- A job can cover up to **1,000 tables**, counting related tables DataHub adds automatically. Bigger scopes fail before any processing. Narrow the scope or split it into several jobs.
- A run stops if it would create more than **5,000** documents from query history. It aborts rather than truncating, so you never get a random half.

## Reference recipes

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

## Troubleshooting

**The run succeeded but created zero documents. Why?**
Usually the scope has too little query history: the window is too short, queries weren't ingested, or most queries come from a service account. Open the run and ask the Context Curator. It can see which stage filtered things out.

**Will my edits survive the next run?**
Yes. Edited documents are detached from the job, and your version becomes the one agents use. See [Editing generated documents](#editing-generated-documents).

**Does Context Generation see my data?**
No. It reads metadata already in DataHub: query logs, schemas, and BI and semantic definitions. Raw query logs aren't sent to a language model; queries are grouped deterministically by parsing their SQL, and generated documents describe general query patterns.

**Does Context Generation use AI Credits?**
Yes. Context Generation runs consume AI Credits, as do evals, Ask DataHub, and custom agents. Scoping jobs to the domains you need keeps usage focused.

## Related

- [Build a Data Agent: Generate context](../../../managed-datahub/build-a-data-agent/generate-context.md), the step-by-step walkthrough
- [Context Evals](./context-evals.md)
- [Context Documents](./context-documents.md)
