

# Step 2: Ingest Context

> **Availability:** DataHub Cloud only

Your baseline shows where your agent stands. The fastest way to improve it is with context your team has already written: semantic models, BI definitions, and documentation. Today, that knowledge is usually fragmented across tools. This step activates it by centralizing it in DataHub, in one place your agent can search.

Connecting your warehouse has already given your agent a lot: tables, columns, lineage, owners, usage, and past queries. These are the [trust signals](./key-concepts.md#technical-context) it uses to choose the right data. What it still lacks is meaning: what your metrics are, and how your team thinks about its data.

:::tip Where to focus
**If you have a semantic layer,** start with [Semantic models and metrics](#semantic-models-and-metrics). It's the most precise context you own.

**If you don't,** go straight to [Documentation](#documentation) and [Strengthen the basics](#strengthen-the-basics). Step 3 will generate much of what's missing.
:::

## Semantic models and metrics

Semantic models and metrics are precise, reviewed definitions. In DataHub, they're assets just like tables: your agent can find them, and your evals can reference them.

| Source                       | What your agent learns                                  | How to enable it                                                                                                   |
| ---------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Snowflake semantic views** | Metric definitions and the tables behind them           | In your [Snowflake](../../generated/ingestion/sources/snowflake.md) connection, set `semantic_views.enabled: true` |
| **dbt semantic models**      | Entities, dimensions, measures, and the models they use | Enabled by default in the [dbt](../../generated/ingestion/sources/dbt.md) connection (dbt 1.6 and later)           |
| **Databricks metric views**  | Dimensions, measures, and the tables behind them        | In your [Databricks](../../generated/ingestion/sources/databricks.md) connection, set `include_metric_views: true` |
| **Looker**                   | Explores, fields, and how tables join                   | Connect [Looker](../../generated/ingestion/sources/looker.md)                                                      |
| **Cube**                     | Cubes, views, and metric definitions                    | In your [Cube](../../generated/ingestion/sources/cube.md) connection, set `emit_semantic_model_entities: true`     |

For other tools, add definitions with the [Python SDK](../../api/tutorials/semantic-models.md). See [Metrics & Semantic Models](../../features/feature-guides/metrics-and-semantic-models.md) for details. Support for standalone dbt metrics (MetricFlow) is coming soon.

## Documentation

Next, bring in the pages where your team explains how things work: metric definitions, data dictionaries, calculation guides, and known issues. In DataHub, they become [context documents](../../features/feature-guides/context/context-documents.md) that your agent can search in plain language.

Go to **Documents > Import** and choose [Notion](../../features/feature-guides/context/import-notion.md), [Confluence](../../features/feature-guides/context/import-confluence.md), [GitHub](../../features/feature-guides/context/import-github.md), or **Upload files**. You can import once or keep documents in sync on a schedule. You can also write documents directly in DataHub.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-document-import.png"/>
</p>

_Screenshot: choosing a source to import documents from._

:::tip Import selectively
Agents trust what they read, so outdated pages do more harm than good. Import the spaces your team actively maintains, and leave the archives behind.
:::

## Strengthen the basics

A few hours here pays off in every answer. You can do all of this from a table's page in DataHub.

- **Deprecate look-alike tables.** If people keep querying an outdated copy by mistake, mark it deprecated. Your agent will avoid it.
- **Document guardrails.** For your most error-prone tables, write a short document with the rules people learn the hard way ("filter out `is_test`", "amounts are in cents"), and relate it to those tables. Your agent sees it whenever it looks them up.
- **Describe your key tables.** Start with the ten or twenty that matter most. A sentence each is enough.
- **Assign owners,** so DataHub knows who reviews changes.
- **Define key terms,** such as "active customer," in the [Business Glossary](../../glossary/business-glossary.md), and link them to the columns that implement them.
- **Assign everything to your domain,** including documents, so you can point your agent at this area later.

<!-- TODO(certification): When certification ships, add a bullet near the top, e.g.
"**Certify what you trust.** Certify the tables and docs your team stands behind. Certified context gets a boost when agents search." Link to the certification feature guide. -->

Each of these is a signal DataHub uses to rank what your agent sees. See [Technical context](./key-concepts.md#technical-context).

## Re-run your evals

Click **Run all DataHub evals**. Failures caused by missing definitions or the wrong table should start passing. What still fails is usually the long tail: questions no one has documented. Step 3 addresses those.

## Check your work

- Your semantic models and metrics, if you have them, appear in DataHub.
- Your team's most useful documentation is imported, and assigned to your domain.
- Your key tables have owners and descriptions, and outdated copies are deprecated.
- Your pass rate has improved on the baseline.

**Next:** [Step 3: Generate context](./generate-context.md)
