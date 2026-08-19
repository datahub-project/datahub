---
title: Metrics & Semantic Models
description: "Turn business metric definitions into first-class, governed, lineage-aware entities in DataHub — discoverable in search, explorable in lineage, and grounded with rich AI context."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Metrics & Semantic Models

<FeatureAvailability />

:::caution Beta
Metrics & Semantic Models is currently in **Beta**. The underlying entity model is stable, but the UI experience, lineage visualization, and ingestion coverage are actively evolving. Expect ongoing improvements to sidebar navigation, entity page layouts, and lineage rendering across the next few releases.
:::

## Why Use Metrics & Semantic Models?

DataHub Metrics is a **catalog for metric definitions**, transforming how your team documents, discovers, and governs business measurements:

- **Single source of truth for definitions** — End the "which revenue number is right?" debate by cataloging each metric once, with its owner, description, calculation, and dimensional context.
- **Lineage from KPI to source column** — Trace every metric through its Semantic Model and logical datasets down to the physical tables and columns that feed it.
- **AI-ready context** — Attach synonyms, natural-language instructions, and example questions to every metric so agents and Ask DataHub can resolve natural language questions against cataloged definitions instead of guessing.
- **Governed like everything else** — Metrics and Semantic Models are full DataHub entities: owners, domains, tags, glossary terms, structured properties, and documentation all attach to them the same way they attach to Datasets.
- **Portable, tool-agnostic model** — The model is designed to accept metrics from any semantic layer (Snowflake Semantic Views today; dbt Semantic Layer, Databricks metric views, and BI-tool metrics on the roadmap), so investing in the catalog is not a bet on one tool.

## What's Included

Two new entity types ship with this feature:

### Semantic Model

A Semantic Model defines a group of datasets, how those datasets relate to one another (i.e. allowed join patterns), and which dimensions and metrics can be derived from those datasets. It serves as the backing dimensional context for the metrics calculated over it.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metrics-and-semantic-models/semantic-model-entity.png" alt="Semantic Model entity page"/>
</p>

Each Semantic Model page includes:

- **Governance** — name, description, owners, domain, tags, structured properties, and glossary terms.
- **Datasets** — the logical datasets exposed by the model (each a full-featured DataHub Dataset with the `Semantic Model Dataset` subtype).
- **Relationships** — join paths between the model's logical datasets, with cardinality and optional AI-context hints.
- **Related Metrics** — every metric backed by this model.
- **Definition** — the source-platform DDL / YAML (e.g. the Snowflake `CREATE SEMANTIC VIEW` statement) preserved verbatim for round-tripping and debugging.
- **AI Context** — synonyms, natural-language instructions, few-shot examples for LLM-assisted exploration.
- **Lineage** — flow from the model down through its logical datasets to the physical source tables.

### Metric

A Metric explicitly defines the SQL calculation of a named business measurement — `total_revenue`, `daily_active_users`, `conversion_rate` — and optionally carries **AI Context** to help agents understand synonyms, example questions the metric should answer, and sample queries.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metrics-and-semantic-models/metric-entity.png" alt="Metric entity page"/>
</p>

Each Metric page includes:

- **Expression** — the SQL that computes the metric, in one or more dialects (`ANSI_SQL`, `SNOWFLAKE`, `MDX`, `TABLEAU`, `DATABRICKS`, `MAQL`, `OTHER`).
- **Semantic Model** — the model this metric is calculated within.
- **Derived From / Related Metrics** — parent metrics and semantic peers (e.g. `revenue_per_customer` derived from `total_revenue` and `customer_count`).
- **AI Context** — synonyms (`revenue`, `topline`, `gross`), instructions, sample questions.
- **Governance** — name, description, owners, domain, tags, structured properties, and glossary terms.
- **Lineage** — Metric → Semantic Model → Logical Dataset → Physical Dataset chain.

### Semantic Model Datasets

Every logical dataset a Semantic Model exposes is itself a full **Dataset** entity, subtyped `Semantic Model Dataset`. That means each logical dataset gets its own schema, its own governance surface, its own lineage graph, and its own search results — exactly like dbt Sources, Looker Views, and Snowflake Dynamic Tables.

Each field carries a **semantic annotation** identifying it as a `DIMENSION` (grouping / filtering attribute), `MEASURE` (aggregatable numeric value), or `FILTER` (named boolean predicate), plus its underlying SQL expression and — for measures — the aggregation function.

## Prerequisites

**For DataHub Cloud customers**: Reach out to your DataHub representative to enable this feature for your organization. Metrics & Semantic Models require **DataHub Cloud 2.1.0 or later**, and the feature is gated behind a per-tenant flag while it's in Beta.

**For DataHub Core (OSS) deployments**: Available in **DataHub Core v1.7.0 or later**. Set the environment variable `METRICS_ENABLED=true` on the GMS service before starting DataHub. For local docker-compose deployments:

```bash
# Add METRICS_ENABLED=true to your GMS environment
METRICS_ENABLED=true datahub docker quickstart
```

Verify the flag is on via the GraphQL app-config query:

```graphql
{
  appConfig {
    featureFlags {
      metricsEnabled
    }
  }
}
```

Once enabled, the **Metrics** section appears in the left navigation sidebar with a `Beta` badge.

## Ingesting Metrics

### From Snowflake Semantic Views

DataHub's Snowflake connector emits Semantic Models and Metrics directly from **Snowflake Semantic Views**.

Enable in the Snowflake recipe:

```yaml
source:
  type: snowflake
  config:
    account_id: ...
    username: ...
    include_technical_schema: true

    semantic_views:
      enabled: true # ingest Semantic Views at all
      emit_semantic_model_entities: null # tri-state — see below
      column_lineage: true # physical→logical column lineage
```

The `emit_semantic_model_entities` control is tri-state:

| Value            | Behavior                                                                                                                                                                                                                         |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `null` (default) | Auto-detect. On DataHub Cloud ≥ 2.1.0 with Metrics enabled, emits SM/Metric entities. Elsewhere (older Cloud, OSS/self-hosted without an explicit opt-in, or connectionless runs) falls back to legacy `Semantic View` datasets. |
| `true`           | Request SM/Metric emission. Honored on Cloud when the server + flag support it; on OSS, requires the operator to run a server that registers these entities.                                                                     |
| `false`          | Force legacy `Semantic View` dataset behavior even if the server supports the new entities.                                                                                                                                      |

Snowflake Semantic Views are a native Snowflake capability. If you also use the Cortex Analyst integration on top of Semantic Views, that integration is gated to Snowflake Enterprise Edition and above — cataloging the Semantic Views into DataHub does not depend on Cortex.

### From the DataHub Python SDK

Emit metrics and semantic models programmatically from any script or connector using the high-level SDK builders (`datahub.sdk.SemanticModel`, `.Metric`, and `.SemanticModelDataset`):

```python
from datahub.sdk import (
    DataHubClient,
    DialectExpressionInput,
    Metric,
    SemanticModel,
)

client = DataHubClient(server="...", token="...")

model = SemanticModel(
    platform="snowflake",
    path="analytics",
    id="orders_model",
    name="Orders Model",
    description="Sales analytics semantic model",
    datasets=[...],
    relationships=[...],
)

metric = Metric(
    platform="snowflake",
    path="analytics.orders_model",
    id="total_revenue",
    name="Total Revenue",
    description="Sum of order amounts.",
    semantic_model=model.urn,
    expression=DialectExpressionInput(
        dialect="SNOWFLAKE",
        expression="SUM(orders.amount)",
    ),
)

for mcp in [*model.as_mcps(), *metric.as_mcps()]:
    client.entities.upsert(mcp)
```

Full tutorial and reference: [Semantic Models & Metrics SDK Guide](../../api/tutorials/semantic-models.md).

## Exploring Metrics

### Browse

The **Metrics** left-nav item opens `/metrics`, a landing page with a browse tree grouped by Semantic Model. Expand a model to see the metrics it backs. Filter and sort by owner, domain, tag, and platform.

<!-- SCREENSHOT: Metrics landing page with the browse tree expanded and a Semantic Model highlighted. -->

### Search

Discovery today is centered on the `/metrics` landing page and its browse tree. Surfacing Metrics and Semantic Models as first-class entity types in universal Search and Browse — plus retrieval via DataHub MCP for agentic workflows — is on the near-term roadmap.

### Lineage

The canonical lineage chain — `Metric → Semantic Model → Logical Dataset → Physical Dataset` — is visible on every metric's Lineage tab. Future releases will introduce robust impact analysis works both ways: clicking a physical column shows every downstream metric that reads from it, and clicking a metric shows every upstream field it depends on.

## Governance

Metrics and Semantic Models reuse DataHub's standard governance surface:

- **Owners** — assign ownership by user, group, or role.
- **Domains** — group metrics under business domains (Sales, Marketing, Finance, etc.).
- **Tags & Glossary terms** — apply the same taxonomy you use for datasets. `Certified`, `Deprecated`, and PII tags work as expected.
- **Structured properties** — extend the model with typed metadata (metric additivity, metric kind, measure shape, etc.).
- **Documentation & links** — long-form docs and institutional memory.

## AI Context

Every Semantic Model, Metric, and semantic-annotated field can carry an **AI Context** record:

- **Synonyms** — alternate names the metric is known by (`ARPU` ↔ `revenue_per_customer`).
- **Instructions** — natural-language guidance for LLMs interpreting or querying the metric.
- **Examples** — sample questions or SQL snippets showing the metric in action.
- **Custom instructions** — arbitrary structured hints.

Future releases will ensure AI Context feeds directly into Ask DataHub and any agent grounded on your catalog, so metric definitions become resolvable by natural-language reference rather than URN.

## What's Coming Next

We're actively investing in the Metrics experience. Near-term work includes:

- **Metrics & Semantic Models in universal Search and Browse** and available via **DataHub MCP** so agents can retrieve them during agentic workflows.
- **Additional ingestion sources.** Expanding beyond Snowflake Semantic Views to cover dbt Semantic Layer / MetricFlow, Databricks Unity Catalog metric views, Sigma, and BI-tool measures.
- **Column-level lineage from source columns through to metrics.** Full column-to-metric impact analysis — see which KPIs are affected by a change to a raw column.
- **Lineage visualization improvements.** Cleaner node labels, collapsed intermediate nodes, and a Semantic Model container view that reduces graph noise.
- **AI-native discovery.** Ask DataHub resolving natural-language questions to cataloged metric definitions using synonyms and expression-aware SQL.

## FAQ

**Can DataHub query my metric values?**
No. DataHub is a catalog for metric _definitions_ — the calculation, dimensional context, lineage, and governance. Value computation and visualization stay in your BI tool or semantic layer.

**Do I need Snowflake to use Metrics?**
No. The Python SDK lets you emit Semantic Models and Metrics from any source. Snowflake Semantic Views is the first turnkey ingestion path; more are in progress.

**What happens to my existing Snowflake Semantic Views ingested before this feature launched?**
They stay in DataHub as legacy `Semantic View` datasets. When you're ready to move to the new model, the `datahub migrate snowflake-semantic-views` CLI copies governance (owners, domains, tags, glossary terms, documentation, deprecation, applications, column-level tags/terms) onto the new Semantic Model and Metric URNs. The migration is not automatic — reach out to your DataHub representative to plan the cutover. Lineage and policies are not migrated by the CLI; run Snowflake ingestion with `emit_semantic_model_entities: true` afterward to fill structural aspects.

**Are metrics environment-specific?**
No. Metric URNs deliberately omit the environment qualifier (`PROD`, `STAGING`, etc.) — `total_revenue` in PROD and STAGING resolve to the same metric entity. Cross-platform metrics (e.g. the same measure defined in both dbt and Snowflake) remain distinct because the platform is encoded in the URN.

**Can I add custom properties (e.g. additivity, measure shape) to metrics?**
Yes. Metrics support Structured Properties — typed, governance-controlled, search-facet-enabled custom fields. Use them for platform-specific metadata that does not yet warrant a first-class schema field.

---

## Related

- [Semantic Models & Metrics SDK Tutorial](../../api/tutorials/semantic-models.md)
