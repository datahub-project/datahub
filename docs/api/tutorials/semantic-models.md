---
description: "Tutorial for emitting Semantic Models, Metrics, and Logical Datasets in DataHub using the Python SDK (datahub.sdk)."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Semantic Models & Metrics

## Why Would You Use Semantic Models and Metrics?

Semantic Models and Metrics let you describe a **logical layer** over your physical data: a
`semanticModel` groups one or more logical `dataset`s (each a "Semantic Model Dataset" subtype),
exposes dimensions and measures via schema-field-anchored `semanticFieldAnnotation`s, and
serves as the backing model for `metric` entities. Lineage flows
`Metric → Logical Dataset → Physical Dataset` (the SemanticModel is a container of its
members, not a lineage hop), giving consumers a stable, source-agnostic surface for
analytics, governance, and AI-assisted exploration.

This mirrors modeling that already exists inside the Snowflake connector, lifted into the
high-level SDK so any producer (connector or direct SDK user) can emit the same entities
without re-implementing the aspect wiring.

### Goal Of This Guide

This guide will show you how to:

- Build a `SemanticModel` with two logical datasets, schema fields, and a relationship.
- Emit `metric` entities backed by the model, including one metric derived from another.
- Serialize every emitted MCP to a file and inspect the resulting aspect shapes.

## Prerequisites

For this tutorial, you need DataHub SDK v2 (`datahub.sdk.*`) installed. If you are running
the example from the `metadata-ingestion` package, the venv is set up by
`../gradlew :metadata-ingestion:installDev`.

## Build a Semantic Model with Logical Datasets and Metrics

The example below builds the full lineage chain
`Metric -> Logical Dataset -> Physical Dataset` (with SemanticModel as a container of
its datasets and metrics) using the high-level `datahub.sdk` builders, then writes
every emitted MCP to a JSON file so the resulting aspect shapes can be inspected.

```python
{{ inline /metadata-ingestion/examples/library/semantic_model_create.py show_path_as_comment }}
```

### What the SDK emits for you

When you call `entity.as_mcps()` on each builder, the SDK produces the full aspect set and
wires the lineage chain automatically:

- **`semanticModel`**: a `Status`, a `SemanticModelInfo` (name, description, optional
  `relationships` — membership is **not** listed here), and a model-level `AiContext`
  **only when non-empty**.
- **Logical `dataset`s**: each gets `SubTypes([SEMANTIC_MODEL_DATASET])`, a
  `SemanticModelProperties(alias, semanticModel=<model urn>)` membership pointer
  (`IsPartOf`), a `SchemaMetadata` with the declared fields, and — when `upstreams` is
  provided — an `UpstreamLineage` to the physical datasets. For every field, the SDK
  emits a `schemaField`-anchored `semanticFieldAnnotation` (with `expression`
  auto-synthesized as `f"{alias}.{field_path}"` when not provided) and, when non-empty,
  a field-anchored `aiContext`.
- **`metric`s**: each gets `Status`, `MetricInfo` (with `semanticModel=<model urn>`
  membership (`ModeledBy`) and an optional `expression`; the expression is **never**
  fabricated when omitted), `MetricRelationships` (**always** emitted, even with empty
  `derivedFrom`, so `hasParentMetric` indexes as false), optional
  `MetricUpstreams.datasetUpstreams` pointing at the Semantic Model Dataset URN(s) the
  metric reads from, and an `AiContext` only when non-empty.

Lineage is `Metric → Logical Dataset → Physical Dataset` via `metricUpstreams` and each
logical dataset's `upstreamLineage`. The SemanticModel is a container (bounding box) of its
members, not a lineage hop.

### Expected Output

Running the example writes `semantic_model_create.json` in the working directory. Open it
and verify the aspect shapes match the producer contract:

- URN patterns: `urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)`,
  `urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,total_revenue)`, and
  `urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)`.
- Logical datasets carry the `Semantic Model Dataset` subtype.
- Each logical dataset's `semanticModelProperties` points back at the model URN with the
  right `alias`.
- `semanticFieldAnnotation` MCPs are anchored on `schemaField` URNs and the `expression`
  falls back to `ORDERS.order_id` when not explicitly provided.
- `aiContext` is only present on fields/entities that had non-empty inputs.
- `total_revenue` emits `metricUpstreams.datasetUpstreams` pointing at the `orders_ds`
  Semantic Model Dataset URN (from `upstream_datasets=[orders_ds.urn]`).

## API Reference

For the full surface area of each builder, see the
[SDK Entities Reference](../../../python-sdk/sdk-v2/entities.mdx).

- `SemanticModel` — `datahub.sdk.semantic_model.SemanticModel`
- `SemanticModelDataset` — `datahub.sdk.semantic_model.SemanticModelDataset`
- `Metric` — `datahub.sdk.metric.Metric`

## Server compatibility

The `semanticModel`, `metric`, and logical-`dataset` entities require a server
build that registers the semantic-model metadata model. Emitting to a server
that does not register these aspects fails loudly — the server rejects the
unregistered aspect and `emit_mcps` raises.

For a clear, actionable error instead of a server-side rejection, call the
opt-in preflight helper before emitting:

```python
from datahub.sdk import DataHubClient, require_metrics_support

client = DataHubClient(server="...", token="...")
require_metrics_support(client)  # raises if the server version is too old
```

The helper delegates to `RestServiceConfig.supports_feature`: it raises when the
server reports a version that does not support these entities, and fails open
when there is no version signal to check (the operator is then responsible for
running a build that includes the model). It is **not** wired into
`DataHubClient.upsert` automatically — call it explicitly when you want the
preflight.

### Read-modify-write caveat for logical datasets

Per-field `semanticFieldAnnotation` and field-level `aiContext` on a
`SemanticModelDataset` are **create-only**. A logical dataset shares the
`dataset` entity type, so `client.entities.get(<dataset urn>)` hydrates it as a
base `Dataset` — the field-anchored annotations live on `schemaField` URNs, not
in the dataset's aspect bag, and are not carried back on a read. To update a
logical dataset, rebuild a fresh `SemanticModelDataset` and re-attach its fields
via the `schema` constructor kwarg rather than read-modify-writing the fetched
`Dataset`.
