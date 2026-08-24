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
- **`datasets`** — **deprecated.** Retained so already-ingested aspects remain readable;
  `@Relationship` / `@Searchable` annotations are stripped so the field no longer writes
  graph or search edges. New writes must not populate it. Membership is authoritative on
  each logical dataset's `semanticModelProperties.semanticModel` (`IsPartOf`). Re-ingest
  to populate `metricUpstreams.datasetUpstreams` with per-metric routing by the connector.
- **`relationships`** — optional array of `SemanticModelRelationship` records describing join
  paths between the logical datasets in this model (from-alias, to-alias, join columns, optional
  name, optional cardinality reusing `ERModelRelationshipCardinality`, and optional per-relationship
  AI context). The from/to aliases refer to each logical dataset's `semanticModelProperties.alias`.

### AI Context

Optional AI/LLM hints for the semantic model itself are stored in the first-class `aiContext`
aspect (synonyms, natural-language instructions, few-shot examples, and custom instructions).
Per-relationship AI context on `SemanticModelRelationship` remains inlined.

### Logical Datasets as Dataset Entities

Each logical dataset exposed by a semantic model is its own `dataset` entity — with its own URN,
its own `schemaMetadata`, and its own governance surface. Independent identity is what
makes independent search, governance, and lineage possible without reimplementing them.

- **URN pattern**: use the **source platform** (already registered — e.g. `dbt`, `snowflake`,
  `databricks`, `cube`, `atscale`) and encode
  `<sm_path>.<sm_id>.<view_name>` in the dataset name so logical datasets stay unique across
  semantic models. Example:
  `urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.orders_ds,PROD)`.
- **`subTypes`** — carries `Semantic Model Dataset` (`DatasetSubTypes.SEMANTIC_MODEL_DATASET` in
  the Python ingestion SDK), a dedicated subtype distinct from `Semantic View`
  (`DatasetSubTypes.SEMANTIC_VIEW`). The latter remains reserved for a platform's native
  semantic-view object (e.g. a Snowflake `CREATE SEMANTIC VIEW`) ingested as its own top-level
  dataset.
- **`semanticModelProperties`** aspect — the semantic-model-specific facet on top of the standard
  Dataset aspects:
  - **`alias`** — the logical alias used to reference this dataset within its owning semantic
    model's `SemanticModelRelationship` join paths.
  - **`semanticModel`** — authoritative URN of the owning SemanticModel (`IsPartOf`, non-lineage).
    Search-indexed as `semanticModel` / `hasSemanticModel`. Listing a model's datasets is an ES
    filter on this field.
- **`viewProperties`** aspect (standard, already registered on `dataset`) — holds the native SQL
  (`viewLogic`) when the logical dataset is backed by a view or inline query, exactly as it does
  for any other view-backed dataset.
- **`schemaMetadata`** aspect (standard) — the field projection: which columns this logical
  dataset exposes, with their structural types. Field-level semantic metadata (expression,
  aggregation, dimension/measure/filter classification) is layered on top via
  `semanticFieldAnnotation` — see [Fields and Dimensions](#fields-and-dimensions) below.
- **`upstreamLineage`** aspect (standard) — physical lineage back to the table(s) or query this
  logical dataset derives from; see [Lineage](#lineage) below.

### Fields and Dimensions

Column identity, structural typing, and governance (tags, glossary terms, description) all live on
the standard `schemaField` entity — the same entity every other Dataset's columns use, which
already has column-level lineage, search indexing, and an audit trail. Semantic-specific metadata
is layered on top via the `semanticFieldAnnotation` aspect, attached to the `schemaField` entity
for that column:

- **`type`** — required `SemanticFieldType` enum identifying the kind of field: `DIMENSION`
  (grouping / filtering attribute), `MEASURE` (aggregatable numeric value), `FILTER` (named boolean
  predicate), or `OTHER` (forward-compat escape hatch for source constructs that do not map cleanly
  to the three named kinds).
- **`expression`** — the underlying SQL expression(s) in one or more dialects.
- **`aggregationFunction`** — optional aggregation function name (e.g. `SUM`, `COUNT_DISTINCT`,
  `AVG`) applied when `type == MEASURE`.
- **`dimension`** — optional `Dimension` record; populated only when `type == DIMENSION`. Currently
  exposes `isTime: boolean` to flag time dimensions used for date-range filtering.

Field-level AI/LLM hints live on the `schemaField` entity's first-class `aiContext` aspect.

### Inline Query Sources

Some semantic-layer platforms allow a logical dataset to source from an inline SQL query rather
than a persistent table (e.g. Databricks metric views with `source: <SELECT ...>`). Since the
logical dataset is a standard `dataset` entity, this uses the same machinery any other view-backed
dataset with unresolvable-to-a-single-table SQL uses: the raw SQL lives in `viewProperties.viewLogic`,
and `upstreamLineage.upstreams` carries one `Upstream` entry per physical table the SQL references
(extracted via SQL parsing), each optionally annotated with the causal `query` entity via
`Upstream.query`. No semantic-model-specific modeling is required for this case.

### Lineage

The SemanticModel is a container of its logical datasets and metrics (similar to Domains), not a
lineage hop. In the lineage explorer it renders as a bounding box around its members. The
canonical lineage chain for a semantic-model-backed metric is:

`Metric → Logical Dataset (Semantic Model Dataset) → Physical Dataset`

**Metric → logical dataset** — populate `metricUpstreams.datasetUpstreams` with the Semantic
Model Dataset URN(s) the metric reads from (`Consumes`, `isLineage: true`). Optionally populate
`metricUpstreams.fieldUpstreams` with the corresponding `schemaField` URNs for column-level
lineage.

**Metric ↔ semantic model** — membership is stored only on `metricInfo.semanticModel`
(`ModeledBy`, non-lineage). That field drives search/filter/facet and bounding-box membership
in the lineage explorer.

**Semantic model ↔ logical dataset** — membership is stored only on
`semanticModelProperties.semanticModel` (`IsPartOf`, non-lineage). Each logical dataset also keeps
`semanticModelProperties.alias` for join edges.

**Logical dataset → physical source** — standard `upstreamLineage` aspect on the logical dataset
(the same aspect every dataset already uses), populated with one `Upstream` entry per physical
source table or query.

**Column-level lineage** — standard `upstreamLineage.fineGrainedLineages` on the logical dataset,
using `schemaField` URNs on both sides — no semantic-model-specific column lineage infrastructure.

### Governance

The semantic model entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`, `aiContext`.

Its logical dataset entities (and their `schemaField`s) inherit the full standard Dataset/SchemaField
governance surface automatically — tags, glossary terms, ownership, descriptions, structured
properties — with no semantic-model-specific reimplementation.

## Relationships with Other Entities

| Relationship | Direction | Target entity | Aspect / edge name                      | Lineage? |
| ------------ | --------- | ------------- | --------------------------------------- | -------- |
| IsPartOf     | inbound   | `dataset`     | `semanticModelProperties.semanticModel` | no       |
| ModeledBy    | inbound   | `metric`      | `metricInfo.semanticModel`              | no       |

Metric → SMD lineage lives on `metricUpstreams.datasetUpstreams` (not on the semantic model) —
see [Lineage](#lineage) above. Traversal then continues via each logical dataset's
`upstreamLineage` (SemanticModelDataset → Physical Dataset).

## Notable Exceptions

### Deprecated `semanticModelInfo.datasets`

Earlier metrics-catalog ingestions stored logical-dataset membership on the container in
`semanticModelInfo.datasets` (with a lineage-flagged `Contains` edge). That field is
deprecated: it stays in the PDL for stored-data compatibility, but annotations are stripped
and producers no longer write it. Membership already lives on
`semanticModelProperties.semanticModel`; re-ingest to populate
`metricUpstreams.datasetUpstreams` with per-metric routing by the connector. GraphQL
`SemanticModelInfo.datasets` is likewise deprecated and returns empty — discover
member datasets via an Elasticsearch filter on `semanticModelProperties.semanticModel`
(and `metrics(...)` / `metricInfo.semanticModel` for metrics).

### SemanticModelRelationship vs common Relationship

The join-path record is named `SemanticModelRelationship` (rather than `Relationship`) to avoid a
name collision with DataHub's `com.linkedin.common.Relationship` model.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `semanticModel` entity (and the `dataset`/`schemaField` entities that back its logical
datasets and fields). Structured properties support typed values, governance controls, search
facets, and PATCH semantics — they are the recommended mechanism for any platform-specific metadata
that does not warrant a first-class PDL field.
