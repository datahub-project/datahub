# Semantic Model

The semantic model entity represents a logical data model that groups datasets and defines the
dimensional context (fields, dimensions, relationships) used by metrics. It serves as the bridge
between raw datasets and the business metrics calculated over them.

## Identity

Semantic models are identified by three fields:

- **`platform`** — the DataPlatform URN that owns this semantic model
  (e.g. `urn:li:dataPlatform:dbt`, `urn:li:dataPlatform:snowflake`). Searchable as a URN field
  with autocomplete and a "Platform" filter pill.
- **`path`** — the namespace path that scopes this semantic model within its platform, preventing
  name collisions when two teams define models with the same `id` on the same platform.
- **`id`** — the model name within that platform and path
  (e.g. `orders_model`, `customer_360`).

An example URN: `urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics,orders_model)`.

## Important Capabilities

### Semantic Model Info

Core metadata is stored in the `semanticModelInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the model represents.
- **`created`** -- `AuditStamp` (`time` + `actor`) capturing when the semantic model was created
  and by whom. Search-indexed as `createdAt` (DATETIME).
- **`lastModified`** -- `AuditStamp` capturing the most recent modification.
  Search-indexed as `lastModifiedAt` (DATETIME).
- **`nativeDefinition`** — optional verbatim source definition (e.g. the Snowflake
  `CREATE SEMANTIC VIEW` DDL, the dbt `semantic_model` YAML, or the Databricks
  `CREATE METRIC VIEW` DDL). Preserved as-is for round-tripping and debugging; not
  parsed by DataHub.
- **`datasets`** — array of `ModelDataset` records, each linking a logical dataset name to a
  source URN. The source is normally a `dataset` URN, but may be a `query` URN when the semantic
  model uses an inline SQL query with no backing table.
- **`relationships`** — optional array of `SemanticModelRelationship` records describing join
  paths between the logical datasets in this model (from-table, to-table, join columns, optional
  name, and AI context).
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.

### Inline Query Sources

Some semantic-layer platforms allow a metric view or model to source from an inline SQL query
rather than a persistent table (e.g. Databricks metric views with `source: <SELECT ...>`).
DataHub represents these using the `query` entity:

1. **Emit the `query` entity.** The ingestion source constructs a `query` URN whose id is a
   content-hash of the normalized SQL
2. **Point `ModelDataset.source` at the query URN.** No other change is needed on the
   SemanticModel side; the `SourcedBy` edge is emitted the same way for both target kinds.

### Fields and Dimensions

Each `ModelDataset` entry can carry a list of `SemanticField` records that describe the columns
exposed by the semantic model:

- **`type`** — required `SemanticFieldType` enum identifying the kind of field: `DIMENSION` (grouping /
  filtering attribute), `MEASURE` (aggregatable numeric value), `FILTER` (named boolean predicate),
  or `OTHER` (forward-compat escape hatch for source constructs that do not map cleanly to the
  three named kinds).
- **`name`** — the field name as used in metric expressions.
- **`expression`** — the underlying SQL expression(s) in one or more dialects.
- **`dimension`** — optional `Dimension` record; populated only when `type == DIMENSION`. Currently
  exposes `isTime: boolean` to flag time dimensions used for date-range filtering.
- **`aiContext`** — AI hints specific to this field.

### Governance

The semantic model entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`.

## Relationships with Other Entities

| Relationship | Direction | Target entity      | Aspect / edge name  |
| ------------ | --------- | ------------------ | ------------------- |
| SourcedBy    | outbound  | `dataset`, `query` | `semanticModelInfo` |
| ModeledBy    | inbound   | `metric`           | `metricInfo`        |

The `SourcedBy` edges are derived from the `datasets[].source` URN fields in `semanticModelInfo`,
so every referenced dataset or query automatically appears as an upstream dependency in the
lineage graph.

## Notable Exceptions

### SemanticModelRelationship vs common Relationship

The join-path record is named `SemanticModelRelationship` (rather than `Relationship`) to avoid a
name collision with DataHub's `com.linkedin.common.Relationship` model.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `semanticModel` entity. Structured properties support typed values, governance controls,
search facets, and PATCH semantics — they are the recommended mechanism for any platform-specific
metadata that does not warrant a first-class PDL field.

Ingestion sources that need to store per-field vendor blobs should hoist the data into entity-level
`structuredProperties` keyed by field name.
