

# Metrics & Semantic Models — Best Practices

> **Availability:** DataHub Core (OSS) & DataHub Cloud

:::note Who this guide is for
Data platform owners, data engineers, and governance leads who have (or are about to turn on) [Metrics & Semantic Models](./metrics-and-semantic-models.md) and want an opinionated starting point for modeling, governing, and scaling the catalog. If you're new to the feature, read the [feature guide](./metrics-and-semantic-models.md) first — this doc assumes you know what the entities are and focuses on how to use them well.
:::

This guide helps you answer the structural questions that come up when adopting Metrics & Semantic Models in DataHub:

- How do Glossary Terms, Metrics, Semantic Models, and Data Products fit together, and when do you use each?
- Should every metric have a matching Glossary Term?
- How do you represent the same business concept when it's materialized across Snowflake, dbt, Databricks, and BI tools?
- How does metric lineage work upstream (back to source columns) and downstream (into dashboards and consumers)?
- What gets ingested automatically from Snowflake and dbt, and where does the Python SDK come in?
- How do you ground AI agents on your metrics?

Our modeling direction aligns with the emerging [Apache Ossie](https://ossie.apache.org/) spec for semantic interchange, so the patterns here translate cleanly to other Ossie-aligned platforms.

---

## Core entity types

Glossary Terms, Metrics, Semantic Models, Data Products, and Physical Datasets each serve a distinct purpose and are designed to work together to give you a detailed mapping from business language to the SQL and tables that produce the number.

| Primitive            | What it represents                                                                                                                     | Example                                                                                       | When it's the right tool                                                                                                   |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Glossary Term**    | A business concept — the shared meaning your organization agrees on. No calculation logic, no SQL.                                     | "Monthly Recurring Revenue"                                                                   | You want one durable business definition that outlives any specific implementation.                                        |
| **Metric**           | An executable implementation of a business concept — one or more SQL expressions, upstream datasets, column-level lineage, AI context. | `total_mrr` with Snowflake SQL and dbt-semantic-layer SQL                                     | You want the catalog to know how a measurement is actually computed, where it comes from, and what consumes it downstream. |
| **Semantic Model**   | A scoped grouping of datasets and their join logic, plus the metrics derivable from that context. Query-time plumbing.                 | `subscription_metrics_sm` grouping Subscriptions + Customers + Plans with their allowed joins | A metric or set of metrics only makes sense when a specific set of tables are joined a specific way.                       |
| **Data Product**     | A curated, governed bundle for a business outcome — can include datasets, metrics, dashboards, and docs.                               | "Subscription Health" data product owned by Growth Analytics                                  | You want to package what a business consumer needs to accomplish an outcome, not just document individual assets.          |
| **Physical Dataset** | The warehouse table, view, or BI column that produces the raw data.                                                                    | `raw.billing.subscriptions`                                                                   | Always — it's where lineage lands.                                                                                         |

### When each primitive is the right choice

| If you want to…                                                                                      | Use                      |
| ---------------------------------------------------------------------------------------------------- | ------------------------ |
| Define what "Monthly Recurring Revenue" means at your company                                        | Glossary Term            |
| Capture the SQL + lineage + ownership for a specific MRR calculation                                 | Metric                   |
| Document the join logic between Subscriptions, Customers, and Plans so downstream metrics inherit it | Semantic Model           |
| Package the datasets, metrics, and dashboards a Finance team needs to close the books each month     | Data Product             |
| Encode what makes an answer "correct" for an AI agent asked about MRR                                | AI Context on the metric |

:::note Standard governance primitives apply
Metrics and Semantic Models are first-class DataHub entities, so the usual governance primitives work on them — [ownership](../../ownership/ownership-types.md), [structured properties](../../api/tutorials/structured-properties.md), [deprecation](../../api/tutorials/deprecation.md), tags, and glossary terms. This guide focuses on _how_ to use them well for metrics; the linked guides cover the mechanics.
:::

---

## Modeling business concepts

### One concept, one implementation (the simple case)

Many metrics are 1:1 — a single platform, a single SQL definition, a single semantic model that provides the join context. Model it as:

- One **Glossary Term** for the business concept
- One **Semantic Model** scoping the datasets and joins
- One **Metric** inside the semantic model with its SQL and upstream dataset edge
- The metric linked to the glossary term

For most teams, this shape comes out of native ingestion automatically — see [How metrics get into DataHub](#how-metrics-get-into-datahub) below.

### One concept, many implementations

The pattern we see work best for cross-platform metrics is to treat the **Glossary Term as the business concept** and attach **one Metric per implementation**. The duplication is intentional: it makes cross-platform drift visible instead of hiding it.

```text
Glossary Term: "Monthly Recurring Revenue"
├─ Metric: Total MRR (Snowflake)   ─── Semantic Model: finance_billing_sm
│                                       Nightly batch; billing-authoritative; used by Finance
├─ Metric: Total MRR (dbt)         ─── Semantic Model: analytics_core_semantic_layer
│                                       Semantic-layer source of truth; used by BI/analytics
└─ Metric: Total MRR (Databricks)  ─── Semantic Model: ops_realtime_sm
                                        Real-time; used by Customer Success ops dashboard
```

Each metric carries its own SQL, lineage, owners, and status. The glossary term carries the business definition and acts as the entry point consumers browse to when asking "where do we calculate MRR?"

Browsing starts at the term and expands into the implementations. The duplication is a feature — it provokes the "why are we calculating this three times?" conversation rather than hiding it.

:::tip Why not one metric with multiple SQL dialects?
`MetricExpression` is modeled as an array of dialect-specific SQL expressions, so a single metric can hold Snowflake, ANSI SQL, Databricks, and MDX variants. That's the right shape when one implementation is materialized by multiple engines against the same source — for example, an Iceberg file queried from both Snowflake and Databricks against identical schema. It's the wrong shape when the two platforms carry genuinely different pipelines, grains, or refresh cadences — in that case, separate metrics tied to a shared term tell a truer story and let drift show.
:::

### Metrics derived from other metrics

Metrics can be built from other metrics via `derivedFrom`. Use this when a metric's definition is literally a function of other named metrics — for example, `arpu = total_mrr / active_customer_count` — not just a different SQL projection over the same datasets.

`derivedFrom` is a lineage edge, so the derived metric appears downstream of each base metric: impact analysis on `total_mrr` or `active_customer_count` includes `arpu`. The dbt source emits `derivedFrom` edges automatically for dbt `ratio`, `derived`, and `conversion` metrics.

---

## Lineage: tracing a metric

### Upstream — from a metric back to its source columns

The canonical chain is `Metric → Semantic Model Dataset → Physical Dataset`. The Semantic Model Dataset is a logical dataset that sits between the metric and the warehouse table it reads from; it carries the per-field semantic annotations (type, aggregation function, dimension vs. measure) and its own column-level lineage back to the physical table.

For column-level upstream edges directly on the metric — "this metric reads these specific columns" — DataHub supports `MetricUpstreams.fieldUpstreams`. Every field upstream must have its parent dataset also present in `datasetUpstreams`; the validator enforces this so column edges can't be orphaned.

Use column-level upstream lineage when:

- You want to answer "which metrics are impacted if a specific column changes?"
- A metric's calculation reads from a specific subset of columns on a dataset and the rest are irrelevant
- You're preparing for agent/AI consumers who need to know exactly which columns to show the model

### Downstream — from a metric to the dashboards and tables that consume it

Downstream-of-metric lineage lives on the **consumer**, not the metric. A dashboard, chart, or pre-aggregated dataset that reads a metric declares that edge via its own `upstreamMetrics` aspect.

Use this to answer "what breaks if I change this metric?" and "where does this metric show up?":

- **Dashboard → Metric** — a BI dashboard consuming the metric, for example the Finance board dashboard reading `total_mrr`
- **Chart → Metric** — a specific chart within a dashboard, for example an MRR trend chart on the Executive dashboard
- **Dataset → Metric** — a pre-aggregated / rollup table materialized from a metric calculation, for example a nightly `daily_mrr_snapshot` table that materializes `total_mrr` for performance

Ingestion from BI tools will populate these edges automatically as those connectors ship.

---

## How metrics get into DataHub

**The source of truth for a metric's SQL stays in the platform that computes it.** DataHub represents those definitions so they're discoverable, governable, and traceable through lineage — it does not push changes back to the source. Treat the UI as the governance surface, not the authoring surface: a SQL edit in DataHub exists only in the catalog, not in Snowflake, dbt, or any other source.

The order of preference, in descending order:

1. **Native semantic-layer ingestion — the primary path.** Supported today for Snowflake Semantic Views and dbt, with Cube and MicroStrategy in alpha. Databricks metric views, AtScale, and LookML are on the roadmap.
2. **Python SDK — only when ingestion doesn't yet cover your source.** For in-house semantic layers, custom YAML registries, or sources we don't ingest yet. Treat this as a stopgap that lets you run ahead of native connector coverage, not as the long-term home for your definitions. See the [Python SDK section of the feature guide](./metrics-and-semantic-models.md#from-the-datahub-python-sdk) and the full [SDK tutorial](../../api/tutorials/semantic-models.md) for code.
3. **DataHub UI edits — only for business metadata.** Owners, descriptions, tags, glossary term links, structured property values, deprecation.

### From Snowflake Semantic Views

The Snowflake connector reads `SEMANTIC VIEW` objects defined in-warehouse (requires Snowflake Enterprise Edition) and emits:

- One **Semantic Model** per view
- One **Semantic Model Dataset** per logical table in the view, subtyped `Semantic Model Dataset`
- **Per-field semantic annotations** on each dataset — type, aggregation function, whether the column is a dimension or a measure, whether it's a time dimension
- One **Metric** per metric column in the view, with:
  - The SQL expression preserved from the DDL
  - A link to its parent Semantic Model
  - Dataset upstreams pointing at the Semantic Model Dataset URNs
- **AI Context** carried over from Snowflake synonyms where present
- **Column-level lineage** from the physical table to the Semantic Model Dataset

Enable it in the recipe with `semantic_views.enabled: true`. On DataHub Cloud 2.1.0 or later with Metrics enabled, that's all you need. On DataHub Core (OSS), also set `semantic_views.emit_semantic_model_entities: true`; without it, semantic views are ingested as legacy `Semantic View` datasets instead. The full recipe shape lives in the [feature guide's Snowflake section](./metrics-and-semantic-models.md#from-snowflake-semantic-views).

### From dbt (semantic layer and metrics)

If your organization defines metrics in [dbt's semantic layer](https://docs.getdbt.com/docs/build/about-metricflow), the dbt source ingests the semantic layer natively — this is the lowest-friction path to a populated metrics catalog for most dbt-centric teams.

**What gets emitted:**

- **One `semanticModel` per dbt project**, holding the join relationships between your semantic models. Its URN is `urn:li:semanticModel:(urn:li:dataPlatform:dbt,<project>,semantic_layer)`.
- **Per-column semantic annotations** on each semantic model's underlying dataset — dimension vs. measure, aggregation function, whether a column is a time dimension.
- **One `metric` per measure** declared with `create_metric: true`, and one per top-level `metrics:` entry. The metric subtype names dbt's own metric type (`Simple`, `Ratio`, `Derived`, `Cumulative`, `Conversion`), and `derivedFrom` edges connect metrics that are built from other metrics.

**Enabling it:**

```yaml
source:
  type: dbt
  config:
    emit_semantic_model_entities: true
```

The flag is tri-state — unset means "follow the server" (enabled on recent DataHub Cloud, off on OSS), `true` always requests emission, `false` always disables it. Turning it on only adds metadata: a semantic-model reference on each semantic model's dataset and per-field semantic annotations. Dataset URNs and everything the connector already emits for those datasets stay the same, so no migration is needed.

**Join semantics** follow dbt's own MetricFlow conventions: a semantic model declaring an entity as `foreign`, `unique`, or `natural` joins to every semantic model declaring an entity of the **same name** as `primary`, `unique`, or `natural`. The source re-emits those join relationships on the `semanticModel` entity.

**Limitations to know about:**

- **dbt Cloud has no top-level `metrics:` block.** Only measure-level metrics (`create_metric: true`) are ingested from dbt Cloud. Use the dbt Core source if you depend on the `metrics:` block.
- **Metric type, filters, and cumulative windows don't have dedicated fields on `metricInfo`.** The type is captured as a subtype; filters are folded into the expression as a `FILTER (WHERE ...)` clause (or left as a trailing SQL comment where the expression can't carry one — ratios, author-written `expr` cases); cumulative windows become a trailing comment such as `/* cumulative over 7 day */`.
- **Time-dimension granularity** is recorded as "this is a time dimension" without the grain itself.

**Project name identity:** `semanticModel` and `metric` URNs are keyed by the dbt project name (`manifest.metadata.project_name` for dbt Core, package name for dbt Cloud). Pin it in the recipe with `semantic_model_project_name` if it might change — a rename reissues URNs as if the project were new.

### From an in-house framework via the Python SDK

Teams with semantic metadata outside of our supported connectors — custom YAML, in-house tooling, MetricFlow-equivalent frameworks — can emit Metric and Semantic Model entities programmatically. See the [Python SDK section of the feature guide](./metrics-and-semantic-models.md#from-the-datahub-python-sdk) and the full [SDK tutorial](../../api/tutorials/semantic-models.md) for the code.

Keep the YAML/SDK calls in the same repo as the pipeline that produces the data so the metric definition and the SQL it describes stay in sync. `client.entities.upsert` writes each aspect the script sets in full, so set only the fields the script owns: if the script sets owners or glossary terms, every run replaces the ones added in the DataHub UI.

:::caution Field annotations don't round-trip through the SDK
Per-field semantic annotations (`semanticFieldAnnotation`) and field-level AI Context are stored on each field's `schemaField` URN, not on the dataset. `client.entities.get(...)` returns a Semantic Model Dataset as a plain `Dataset` without them, so to change one, build a fresh `SemanticModelDataset` with its fields instead of fetching, editing, and re-upserting the dataset.
:::

### Don't fabricate what the source doesn't tell you

If the source system doesn't give you a SQL expression for a metric, omit it. A fabricated `expression` is worse than no expression — it tells consumers (and agents) that a definition exists when it doesn't. `MetricInfo.expression` is optional for exactly this reason.

The dbt source follows this convention: it folds dbt filters and cumulative windows into the expression where it can, and leaves them as trailing comments where it can't, rather than inventing SQL.

---

## Grounding metrics for AI agents

Every Metric and Semantic Model carries an `AiContext` aspect — the field that makes DataHub metrics usable by AI agents and natural-language interfaces. The AI Context schema maps directly to the `ai_context` field in the Apache Ossie spec.

| AI Context field      | What to put there                                                                                                                         |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `synonyms`            | How business users refer to this metric in natural language — "subscription revenue," "recurring revenue," "ARR run-rate" for `total_mrr` |
| `instructions`        | Short natural-language guidance for an agent — currency, filtering rules, which join to prefer                                            |
| `examples`            | Sample natural-language questions this metric answers — "What was MRR last quarter?", "MRR by plan tier"                                  |
| `custom_instructions` | A sample SQL query, dialect-specific guidance, or any other free-form content that helps an agent ground                                  |

### The glossary + AI Context + context document stack

For agent-ready metrics, these three sources work together:

1. **AI Context on the metric** — the structured, metric-scoped layer. Short, machine-friendly. Co-located with the SQL.
2. **Glossary Term** — the business-vocabulary layer. Written as prose. Agents resolve ambiguous business language back to a term and then to the metric linked from it.
3. **Context Documents** — longer-form, human-written guidance that can span multiple assets (metric + semantic model + related datasets). Use for nuance that doesn't fit on a single entity: cross-platform conventions, regulatory context, known gotchas.

Fill in from top to bottom in that order. Most agents will do well with just AI Context + Glossary Terms; context documents are the escape hatch for the nuanced cases.

---

## Anti-patterns to avoid

### Hacking SQL into Glossary Terms

The single most common pattern we're replacing. If your organization has been attaching SQL definitions or materialized views to glossary terms because there was nowhere better to put them, that content belongs on a Metric. Migrate it:

- Create a Metric for each glossary term that currently carries SQL
- Move the SQL to `metricInfo.expression`
- Move upstream dataset links to `metricUpstreams.datasetUpstreams`
- Keep the business definition on the Glossary Term and link the Metric to it

The term goes back to being what it was meant to be — business vocabulary. The metric owns the technical implementation.

### Treating DataHub as a semantic-layer authoring tool

DataHub reads from Snowflake Semantic Views, dbt, and in-house semantic layers via the SDK. It does **not** push metric definitions back to the source. If your team wants to define metrics in DataHub and have them appear in Snowflake Cortex Analyst, dbt, or Cube.dev, the authoring still has to happen in the source tool — DataHub catalogs it after the fact.

### Duplicate metrics for the same implementation

The cross-platform pattern — multiple metrics tied to one term, one per platform — is intentional. Within a single platform, two metrics with similar names but different SQL is a drift problem. Resolve it by:

1. Picking one implementation to be the production version
2. Marking the others deprecated with the production URN as the replacement

---

## Common questions

**Should every metric have a matching Glossary Term?**
Not every metric needs one. Add a Glossary Term when a business user would recognize the metric by name and might search for it — things like "Monthly Recurring Revenue" or "Active Customers." Skip the term for technical intermediates that only exist inside a derived metric calculation (for example, an unaggregated count used solely to build `arpu`).

**My team uses dbt. Does dbt metrics work today?**
Yes. The dbt source emits one `semanticModel` per project, per-column semantic annotations on each semantic model dataset, and one `metric` per measure declared with `create_metric: true` plus one per top-level `metrics:` entry. See [From dbt](#from-dbt-semantic-layer-and-metrics) for the specifics and known limitations.

**My team uses Looker. When can we ingest LookML into Metrics & Semantic Models?**
LookML ingestion into the semantic-model path is on the roadmap. Until it ships, use the Python SDK to emit Metric and Semantic Model entities from your LookML YAML — the ingestion-side mapping will land before the full source does.

**What's the difference between a Data Product and a Semantic Model?**
A Semantic Model is query-time plumbing — join logic and the metrics derivable from it. A Data Product is a packaging layer — "here's the metric + the dataset + the dashboard you need to answer this business question." You can (and should) put metrics into both.

**How do I handle the same metric calculated differently per region or business unit?**
Treat each regional implementation as its own metric. Link all of them to the same Glossary Term. Use a Structured Property to carry the region/BU key so you can filter and search.

**Does DataHub compute metric values?**
No — DataHub catalogs the definition. Value computation and visualization stay in your BI tool, semantic layer, or warehouse.

**How does column-to-metric lineage show up today?**
Column-level upstream edges live on `MetricUpstreams.fieldUpstreams` and are rendered in the lineage graph. See [Upstream — from a metric back to its source columns](#upstream--from-a-metric-back-to-its-source-columns).

---

## Related

- [Metrics & Semantic Models — Feature Guide](./metrics-and-semantic-models.md) — the what and the turn-on steps
- [Semantic Models & Metrics SDK Tutorial](../../api/tutorials/semantic-models.md) — the how, with code
- [dbt ingestion source](/docs/generated/ingestion/sources/dbt) — how dbt semantic models and metrics are ingested
- [Apache Ossie spec](https://ossie.apache.org/) — the open semantic interchange standard DataHub's model aligns with
- [Business Glossary](../../glossary/business-glossary.md) — the business vocabulary primitive
- [Data Products](../../dataproducts.md) — the packaging primitive
