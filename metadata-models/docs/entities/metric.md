# Metric

The metric entity represents a named, reusable business measurement defined in a semantic
layer or metric store.

## Identity

Metrics are identified by three fields:

- **`platform`** — the DataPlatform URN that owns this metric
  (e.g. `urn:li:dataPlatform:dbt`, `urn:li:dataPlatform:snowflake`). Searchable as a URN field
  with autocomplete and a "Platform" filter pill.
- **`path`** — the namespace path that scopes this metric within its platform, preventing
  name collisions when two teams define metrics with the same `id` on the same platform.
- **`id`** — the metric name within that platform and path
  (e.g. `total_revenue`, `daily_active_users`).

An example URN: `urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,revenue)`.

## Important Capabilities

### Metric Info

Core metadata is stored in the `metricInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the metric measures.
- **`created`** -- `AuditStamp` (`time` + `actor`) capturing when the metric was created
  and by whom. Search-indexed as `createdAt` (DATETIME).
- **`lastModified`** -- `AuditStamp` capturing the most recent modification.
  Search-indexed as `lastModifiedAt` (DATETIME).
- **`expression`** — the metric formula expressed in one or more SQL dialects. Each `DialectExpression` pairs
  a `Dialect` enum value with the raw SQL string.
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.
- **`semanticModel`** -- URN of the `semanticModel` entity that defines this metric's
  dimensional context. Optional: null when the metric was ingested without a semantic model
  context (e.g. thin catalog-only metrics from BI tools like Tableau) or is a native /
  SDK-authored metric awaiting a model. The `ModeledBy` relationship on this field carries
  `isLineage: true`, so when `semanticModel` is populated the metric automatically appears as a
  downstream node in the semantic model's lineage explorer — no additional lineage MCPs are needed.

### Metric Relationships

Hierarchical and derivation relationships are stored in the `metricRelationships` aspect:

- **`parentMetric`** -- URN of the parent metric. Used to build a hierarchical tree of
  metrics. Stored as an `IsPartOf` graph edge.
- **`derivedFrom`** — array of `DerivedMetricInput` records pointing to source metrics.
  `DerivedMetricInput includes Edge`, so the payload is Edge-shaped
  (`destinationUrn`, audit stamps, `properties` bag). Edges are flagged
  `isLineage: true` so they appear in the DataHub lineage graph.
- **`relatedMetrics`** — array of `Edge` records pointing to semantically related metrics (no
  lineage flag).

### Governance and Lineage

The metric entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`.

## Relationships with Other Entities

| Relationship           | Direction | Target entity   | Aspect / edge name                 | Lineage? |
| ---------------------- | --------- | --------------- | ---------------------------------- | -------- |
| ModeledBy              | outbound  | `semanticModel` | `metricInfo`                       | yes      |
| IsPartOf               | outbound  | `metric`        | `metricRelationships`              | no       |
| DerivedFrom            | outbound  | `metric`        | `metricRelationships`              | yes      |
| RelatedTo              | outbound  | `metric`        | `metricRelationships`              | no       |
| Consumes (dataset)     | outbound  | `dataset`       | `metricUpstreams.datasetUpstreams` | yes      |
| Consumes (schemaField) | outbound  | `schemaField`   | `metricUpstreams.fieldUpstreams`   | yes      |

Metric-to-dataset and metric-to-column lineage are carried by the dedicated `metricUpstreams`
aspect. `datasetUpstreams` and `fieldUpstreams` are independently optional so ingestion sources
can populate whichever granularity they can extract. Metric-to-metric derivation lineage lives on
`metricRelationships.derivedFrom` and is not folded into `metricUpstreams`.

## Notable Exceptions

### Environment-independent identity

Metrics encode `platform` (a DataPlatform URN) in the key but deliberately do NOT encode
`FabricType` (unlike datasets, which include PROD/STAGING in their URN). Metrics are modeled
as environment-independent business definitions: `total_revenue` in PROD and STAGING is the
same concept, so both resolve to the same URN. Cross-platform metrics (e.g. the same measure in both dbt and
Snowflake) remain as separate entities because `platform` is part of the URN.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `metric` entity. Structured properties support typed values, governance controls, search
facets, and PATCH semantics — they are the recommended mechanism for platform-specific metadata
such as `additivity`, `filters`, `metricKind`, and `measureShape` that does not yet warrant a
first-class PDL field. These fields can be promoted to the core schema in a future revision when
usage patterns across platforms become clear.
