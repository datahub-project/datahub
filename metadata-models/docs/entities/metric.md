# Metric

The metric entity represents a named, reusable business measurement defined in a semantic layer or
metric store. Metrics are platform-independent: the same logical measure (e.g. `total_revenue`) can
be ingested from dbt, Snowflake, Databricks, or any other platform as a separate entity, with
platform identity carried by the `dataPlatformInstance` aspect.

## Identity

Metrics are identified by two fields:

- **`namespace`** — typically the platform or project name (e.g. `dbt`, `snowflake`, `my_project`).
  Searchable as a keyword so the sidebar can group metrics by platform.
- **`id`** — the metric name within that namespace (e.g. `total_revenue`, `daily_active_users`).

An example URN: `urn:li:metric:(dbt,total_revenue)`.

## Important Capabilities

### Metric Info

Core metadata is stored in the `metricInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the metric measures.
- **`expression`** — the metric formula expressed in one or more SQL dialects (SNOWFLAKE,
  DATABRICKS, DBT, ANSI_SQL, DATAHUB, UNKNOWN). Each `DialectExpression` pairs a `Dialect` enum
  value with the raw SQL string.
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.
- **`recoverability`** — searchable enum (FULL / PARTIAL / NONE) indicating whether the metric
  value can be recomputed from raw data.
- **`semanticModel`** — URN of the `semanticModel` entity that defines this metric's dimensional
  context. Stored as a `ModeledBy` graph edge.
- **`customExtensions`** — array of vendor-namespaced JSON blobs for platform-specific fields not
  covered by the core schema (e.g. `additivity`, `filters`, `metricKind`).

### Metric Relationships

Hierarchical and derivation relationships are stored in the `metricRelationships` aspect:

- **`parentMetric`** — URN of the parent metric for tree-style organisation (sidebar roots are
  metrics where `hasParentMetric=false`). Stored as an `IsPartOf` graph edge.
- **`derivedFrom`** — array of `Edge` records pointing to source metrics; edges are flagged
  `isLineage: true` so they appear in the DataHub lineage graph.
- **`relatedMetrics`** — array of `Edge` records pointing to semantically related metrics (no
  lineage flag).

### Governance and Lineage

The metric entity reuses the full set of standard governance aspects:

`ownership`, `domains`, `globalTags`, `glossaryTerms`, `institutionalMemory`,
`structuredProperties`, `status`, `deprecation`, `dataPlatformInstance`,
`subTypes`, `forms`, `testResults`, `documentation`, `browsePaths`, `browsePathsV2`,
`applications`, `container`, `incidentsSummary`, `displayProperties`, `assetSettings`,
`versionProperties`, `access`, `upstreamLineage`.

## Relationships with Other Entities

| Relationship  | Direction | Target entity   | Aspect / edge name    |
| ------------- | --------- | --------------- | --------------------- |
| ModeledBy     | outbound  | `semanticModel` | `metricInfo`          |
| IsPartOf      | outbound  | `metric`        | `metricRelationships` |
| DerivedFrom   | outbound  | `metric`        | `metricRelationships` |
| RelatedTo     | outbound  | `metric`        | `metricRelationships` |
| Upstream data | outbound  | `dataset`       | `upstreamLineage`     |

## Notable Exceptions

### Platform-independent identity

Unlike datasets, metrics do not encode a platform or environment in the key. Platform identity is
carried by the `dataPlatformInstance` aspect. This means duplicate logical metrics across platforms
remain as separate entities — cross-platform deduplication is out of scope for this release.

### Extensibility via customExtensions

Fields such as `additivity`, `filters`, `derivation`, `rowShaping`, `valueType`, `metricKind`, and
`measureShape` are deliberately absent from the core schema. They live inside `customExtensions` as
vendor-namespaced JSON strings, following the OSI-aligned Proposal B approach. They can be
promoted to first-class fields in a future version without requiring an aspect version bump.
