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
- **`datasets`** — array of `dataset` URNs: the logical datasets that this semantic model exposes.
  Each is a full `dataset` entity carrying the `Semantic Model Dataset` subtype. See
  [Logical Datasets as Dataset Entities](#logical-datasets-as-dataset-entities) below.
- **`relationships`** — optional array of `SemanticModelRelationship` records describing join
  paths between the logical datasets in this model (from-alias, to-alias, join columns, optional
  name, optional cardinality reusing `ERModelRelationshipCardinality`, and AI context). The
  from/to aliases refer to each logical dataset's `semanticModelProperties.alias`.
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.

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
  - **`semanticModel`** — back-reference to the owning `semanticModel` entity (`IsPartOf`
    relationship), enabling "which semantic model does this view belong to" search facets.
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
- **`aiContext`** — AI hints specific to this field.

### Inline Query Sources

Some semantic-layer platforms allow a logical dataset to source from an inline SQL query rather
than a persistent table (e.g. Databricks metric views with `source: <SELECT ...>`). Since the
logical dataset is a standard `dataset` entity, this uses the same machinery any other view-backed
dataset with unresolvable-to-a-single-table SQL uses: the raw SQL lives in `viewProperties.viewLogic`,
and `upstreamLineage.upstreams` carries one `Upstream` entry per physical table the SQL references
(extracted via SQL parsing), each optionally annotated with the causal `query` entity via
`Upstream.query`. No semantic-model-specific modeling is required for this case.

### Lineage

The canonical lineage chain for a semantic-model-backed metric is:

`Metric → SemanticModel → Logical Dataset (Semantic Model Dataset) → Physical Dataset`

**Metric → semantic model** — metrics that declare `metricInfo.semanticModel` pointing at this
entity appear as downstream nodes via the `ModeledBy` lineage edge. Do not also populate
`metricUpstreams` for these metrics; that aspect is reserved for metrics without a semantic model.

**Semantic model → logical dataset** — the `Contains` relationship, derived from
`semanticModelInfo.datasets`, is a lineage edge (`isLineage: true`). Each logical dataset is an
upstream of the semantic model.

**Logical dataset → physical source** — standard `upstreamLineage` aspect on the logical dataset
(the same aspect every dataset already uses), populated with one `Upstream` entry per physical
source table or query.

**Column-level lineage** — standard `upstreamLineage.fineGrainedLineages` on the logical dataset,
using `schemaField` URNs on both sides — no semantic-model-specific column lineage infrastructure.

### Governance

The semantic model entity reuses these standard governance aspects: `ownership`, `domains`,
`globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `status`,
`deprecation`, `dataPlatformInstance`, `subTypes`, `documentation`, `browsePathsV2`,
`applications`.

Its logical dataset entities (and their `schemaField`s) inherit the full standard Dataset/SchemaField
governance surface automatically — tags, glossary terms, ownership, descriptions, structured
properties — with no semantic-model-specific reimplementation.

## Relationships with Other Entities

| Relationship | Direction | Target entity | Aspect / edge name        | Lineage? |
| ------------ | --------- | ------------- | ------------------------- | -------- |
| Contains     | outbound  | `dataset`     | `semanticModelInfo`       | yes      |
| IsPartOf     | inbound   | `dataset`     | `semanticModelProperties` | no       |
| ModeledBy    | inbound   | `metric`      | `metricInfo`              | yes      |

The `Contains` edges are derived from the `datasets` URN array in `semanticModelInfo` and are
lineage edges; each target logical dataset also carries the reverse `IsPartOf` edge via its own
`semanticModelProperties` aspect. Each logical dataset's own `upstreamLineage` aspect provides the
physical lineage graph traversal path (table-level and column-level) from there onward.

## Notable Exceptions

### SemanticModelRelationship vs common Relationship

The join-path record is named `SemanticModelRelationship` (rather than `Relationship`) to avoid a
name collision with DataHub's `com.linkedin.common.Relationship` model.

### Extensibility via structuredProperties

Entity-level extensibility uses the `structuredProperties` aspect, which is already registered
for the `semanticModel` entity (and the `dataset`/`schemaField` entities that back its logical
datasets and fields). Structured properties support typed values, governance controls, search
facets, and PATCH semantics — they are the recommended mechanism for any platform-specific metadata
that does not warrant a first-class PDL field.
