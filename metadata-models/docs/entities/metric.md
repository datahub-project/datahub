# Metric

The metric entity represents a named, reusable business measurement defined in a semantic
layer or metric store. Each metric URN includes the DataPlatform that owns the metric
(e.g. dbt, Snowflake, Databricks), so the same logical measure defined in two different
platforms yields two distinct metric entities. Environment (PROD vs. STAGING) is not part
of the URN, however -- those copies reconcile into a single entity and are distinguished
by the `dataPlatformInstance` aspect. See the "Environment-independent identity" note
below for details.

## Identity

Metrics are identified by two fields:

- **`platform`** — the DataPlatform URN that owns this metric
  (e.g. `urn:li:dataPlatform:dbt`, `urn:li:dataPlatform:snowflake`). Searchable as a URN field
  with autocomplete and a "Platform" filter pill.
- **`id`** — the metric name within that platform (e.g. `total_revenue`, `daily_active_users`).

An example URN: `urn:li:metric:(urn:li:dataPlatform:dbt,total_revenue)`.

## Important Capabilities

### Metric Info

Core metadata is stored in the `metricInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the metric measures.
- **`created`** -- `AuditStamp` (`time` + `actor`) capturing when the metric was created
  and by whom. Search-indexed as `createdAt` (DATETIME).
- **`lastModified`** -- `AuditStamp` capturing the most recent modification.
  Search-indexed as `lastModifiedAt` (DATETIME).
- **`expression`** — the metric formula expressed in one or more SQL dialects (SNOWFLAKE,
  DATABRICKS, DBT, ANSI_SQL, DATAHUB, UNKNOWN). Each `DialectExpression` pairs a `Dialect` enum
  value with the raw SQL string.
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.
- **`recoverability`** — searchable enum (FULL / PARTIAL / NONE) indicating whether the metric
  value can be recomputed from raw data.
- **`semanticModel`** -- URN of the `semanticModel` entity that defines this metric's
  dimensional context. Stored as a `ModeledBy` graph edge. Optional: null when the metric
  was ingested without a semantic model context (e.g. thin catalog-only metrics from BI
  tools like Tableau) or is a native / SDK-authored metric awaiting a model.

### Metric Relationships

Hierarchical and derivation relationships are stored in the `metricRelationships` aspect:

- **`parentMetric`** -- URN of the parent metric. Used to build a hierarchical tree of
  metrics. Stored as an `IsPartOf` graph edge.
- **`derivedFrom`** — array of `Edge` records pointing to source metrics; edges are flagged
  `isLineage: true` so they appear in the DataHub lineage graph.
- **`relatedMetrics`** — array of `Edge` records pointing to semantically related metrics (no
  lineage flag).

### Governance and Lineage

The metric entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`.

## Relationships with Other Entities

| Relationship           | Direction | Target entity   | Aspect / edge name                 |
| ---------------------- | --------- | --------------- | ---------------------------------- |
| ModeledBy              | outbound  | `semanticModel` | `metricInfo`                       |
| IsPartOf               | outbound  | `metric`        | `metricRelationships`              |
| DerivedFrom            | outbound  | `metric`        | `metricRelationships`              |
| RelatedTo              | outbound  | `metric`        | `metricRelationships`              |
| Consumes (dataset)     | outbound  | `dataset`       | `metricUpstreams.datasetUpstreams` |
| Consumes (schemaField) | outbound  | `schemaField`   | `metricUpstreams.fieldUpstreams`   |

Metric-to-dataset and metric-to-column lineage are carried by the dedicated
`metricUpstreams` aspect. `datasetUpstreams` and `fieldUpstreams` are independently
optional so ingestion sources can populate whichever granularity they can extract.
Metric-to-metric derivation lineage lives on `metricRelationships.derivedFrom` and is
not folded into `metricUpstreams`.

## Notable Exceptions

### Environment-independent identity

Metrics encode `platform` (a DataPlatform URN) in the key but deliberately do NOT encode
`FabricType` (unlike datasets, which include PROD/STAGING in their URN). This means PROD and
STAGING copies of the same logical metric resolve to the same entity and are distinguished by the
`dataPlatformInstance` aspect instead. Cross-environment deduplication is therefore automatic,
while cross-platform metrics (e.g. the same measure defined in both dbt and Snowflake) remain as
separate entities.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `metric` entity. Structured properties support typed values, governance controls, search
facets, and PATCH semantics — they are the recommended mechanism for platform-specific metadata
such as `additivity`, `filters`, `metricKind`, and `measureShape` that does not yet warrant a
first-class PDL field. These fields can be promoted to the core schema in a future revision when
usage patterns across platforms become clear.
