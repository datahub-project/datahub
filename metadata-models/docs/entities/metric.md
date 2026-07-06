# Metric

The metric entity represents a named, reusable business measurement defined in a semantic layer or
metric store. Metrics are platform-independent: the same logical measure (e.g. `total_revenue`) can
be ingested from dbt, Snowflake, Databricks, or any other platform as a separate entity, with
platform identity carried by the `dataPlatformInstance` aspect.

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
- **`expression`** — the metric formula expressed in one or more SQL dialects (SNOWFLAKE,
  DATABRICKS, DBT, ANSI_SQL, DATAHUB, UNKNOWN). Each `DialectExpression` pairs a `Dialect` enum
  value with the raw SQL string.
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.
- **`recoverability`** — searchable enum (FULL / PARTIAL / NONE) indicating whether the metric
  value can be recomputed from raw data.
- **`semanticModel`** — URN of the `semanticModel` entity that defines this metric's dimensional
  context. Stored as a `ModeledBy` graph edge.

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

| Relationship                 | Direction | Target entity   | Aspect / edge name                    |
| ---------------------------- | --------- | --------------- | ------------------------------------- |
| ModeledBy                    | outbound  | `semanticModel` | `metricInfo`                          |
| IsPartOf                     | outbound  | `metric`        | `metricRelationships`                 |
| DerivedFrom                  | outbound  | `metric`        | `metricRelationships`                 |
| RelatedTo                    | outbound  | `metric`        | `metricRelationships`                 |
| Upstream data (table-level)  | outbound  | `dataset`       | `upstreamLineage.upstreams`           |
| Upstream data (column-level) | outbound  | `schemaField`   | `upstreamLineage.fineGrainedLineages` |

Both granularities live in the same standard `upstreamLineage` aspect. `upstreams[]` carries
`metric → dataset` edges (table-level), while `fineGrainedLineages[]` carries
`metric → schemaField` edges and supports `transformOperation` and `confidenceScore` metadata.
Note that derived metrics reach their upstream datasets through
`metricRelationships.derivedFrom` (with `isLineage: true`) rather than populating
`upstreamLineage` directly; see RFC section 5.4 for details.

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
