# Semantic Model

The semantic model entity represents a logical data model that groups datasets and defines the
dimensional context (fields, dimensions, relationships) used by metrics. It serves as the bridge
between raw datasets and the business metrics calculated over them.

## Identity

Semantic models are identified by two fields:

- **`namespace`** — typically the platform or project name (e.g. `dbt`, `snowflake`, `my_project`).
- **`id`** — the model name within that namespace (e.g. `orders_model`, `customer_360`).

An example URN: `urn:li:semanticModel:(dbt,orders_model)`.

## Important Capabilities

### Semantic Model Info

Core metadata is stored in the `semanticModelInfo` aspect:

- **`name`** — human-readable display name; used for full-text search and autocomplete.
- **`description`** — free-text description of what the model represents.
- **`sourcePlatform`** — required data platform URN (e.g. `urn:li:dataPlatform:snowflake`)
  indicating which platform owns this model. Mirrors the `platform` field on `datasetKey` so
  filtering and grouping behave identically across datasets and semantic models. Stored as a
  searchable URN field with autocomplete and a "Platform" filter.
- **`datasets`** — array of `ModelDataset` records, each linking a logical dataset name to a
  source `dataset` URN. Each entry may include `primaryKey`, `uniqueKeys`, typed `fields`, and
  `customExtensions`.
- **`relationships`** — optional array of `SemanticModelRelationship` records describing join
  paths between the logical datasets in this model (from-table, to-table, join columns, optional
  name, AI context, and custom extensions).
- **`aiContext`** — optional hints for AI/LLM consumers: synonyms, natural-language instructions,
  few-shot examples, and custom instructions.
- **`customExtensions`** — array of vendor-namespaced JSON blobs for platform-specific fields.

### Fields and Dimensions

Each `ModelDataset` entry can carry a list of `Field` records that describe the columns exposed by
the semantic model:

- **`name`** — the field name as used in metric expressions.
- **`expression`** — the underlying SQL expression(s) in one or more dialects.
- **`dimension`** — optional `Dimension` record; currently exposes `isTime: boolean` to flag
  time dimensions used for date-range filtering.
- **`description`** — free-text documentation for the field.
- **`aiContext`** — AI hints specific to this field.
- **`customExtensions`** — vendor-specific extensions for this field.

### Governance

The semantic model entity reuses the full set of standard governance aspects:

`ownership`, `domains`, `globalTags`, `glossaryTerms`, `institutionalMemory`,
`structuredProperties`, `status`, `deprecation`, `dataPlatformInstance`,
`subTypes`, `forms`, `testResults`, `documentation`, `browsePaths`, `browsePathsV2`,
`applications`, `container`, `displayProperties`, `assetSettings`.

## Relationships with Other Entities

| Relationship | Direction | Target entity | Aspect / edge name  |
| ------------ | --------- | ------------- | ------------------- |
| SourcedBy    | outbound  | `dataset`     | `semanticModelInfo` |
| ModeledBy    | inbound   | `metric`      | `metricInfo`        |

The `SourcedBy` edges are derived from the `datasets[].source` URN fields in `semanticModelInfo`,
so every referenced dataset automatically appears as an upstream dependency in the lineage graph.

## Notable Exceptions

### SemanticModelRelationship vs common Relationship

The join-path record is named `SemanticModelRelationship` (rather than `Relationship`) to avoid a
name collision with DataHub's `com.linkedin.common.Relationship` model.

### Extensibility via customExtensions

Platform-specific fields (e.g. filter predicates, grain definitions, certified status) ride in
`customExtensions` as vendor-namespaced JSON strings, keeping the core schema lean and
OSI-aligned.
