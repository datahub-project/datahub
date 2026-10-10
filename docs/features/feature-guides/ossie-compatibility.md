---
title: Apache Ossie Compatibility
description: How DataHub's Metric, Semantic Model, and Schema Field models line up with Apache Ossie, including fields that do not round-trip.
---

# DataHub Entity Model Compatibility with Apache Ossie (OSI)

> **TL;DR:** DataHub's Metric, SemanticModel, and SchemaField models overlap the
> [Apache Ossie](https://github.com/apache/ossie) (Open Semantic Interchange) spec on names,
> descriptions, expressions, and the object form of `ai_context`. They do not round-trip an Ossie
> document. There is no Ossie importer and no emitter that writes Ossie YAML from these entities.

---

## Background

[Apache Ossie (incubating)](https://github.com/apache/ossie) — also written "OSI" (Open Semantic
Interchange) — is an emerging Apache standard for exchanging semantic layer metadata (metrics,
dimensions, semantic models) across tools and platforms. DataHub is a founding launch partner,
helping shape the spec from the inside.

DataHub's metric and semantic model entities were designed to line up with that shape. This
document maps each field in Ossie schema `0.2.0.dev0` to the DataHub field that stores it, and
lists the Ossie fields that have no home.

Ossie document import, and export of these entities as Ossie YAML, are not implemented.
How the entities are used is covered in [Metrics & Semantic Models](./metrics-and-semantic-models.md).

---

## OSI → DataHub Field-by-Field Mapping

### `SemanticModel` (document root)

The Ossie JSON schema puts `version` on the document, alongside the semantic-model fields below.
`$defs/SemanticModel` itself does not include `version`.

| OSI Field             | Required | DataHub Equivalent                                                                 | Notes                                                                                                                                              |
| --------------------- | -------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `version`             | ✅       | No DataHub field                                                                   | Const `"0.2.0.dev0"` on the document                                                                                                               |
| `name`                | ✅       | `SemanticModelInfo.name`                                                           | Same role                                                                                                                                          |
| `description`         | ❌       | `SemanticModelInfo.description`                                                    | Same role                                                                                                                                          |
| `ai_context`          | ❌       | `aiContext` aspect on the semanticModel entity                                     | Object form only — see [AiContext](#aicontext)                                                                                                     |
| `datasets[]`          | ✅       | Dataset entities with `semanticModelProperties` (subtype `Semantic Model Dataset`) | First-class entities. `SemanticModelInfo.datasets` is deprecated and new writes leave it empty                                                     |
| `relationships[]`     | ❌       | `SemanticModelInfo.relationships[]` (`SemanticModelRelationship`)                  | See [Relationship](#relationship). `name` is optional in DataHub                                                                                   |
| `metrics[]`           | ❌       | Metric entities with `MetricInfo.semanticModel` pointing at the parent             | First-class entities. The URN is optional in `MetricInfo`                                                                                          |
| `custom_extensions[]` | ❌       | No DataHub field                                                                   | Ossie `custom_extensions` entries are `{vendor_name, data}`. DataHub stores extra facts on other aspects and does not serialize them as this array |

> **PDL source:** `com.linkedin.semanticmodel.SemanticModelInfo`

---

### `Dataset` (within SemanticModel)

| OSI Field             | Required | DataHub Equivalent                                                                                      | Notes                                                                                                                                      |
| --------------------- | -------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`                | ✅       | `semanticModelProperties.alias`                                                                         | Logical name used by relationship `from` / `to`. `datasetProperties.name` is a separate, optional display name                             |
| `source`              | ✅       | `upstreamLineage` upstream dataset                                                                      | Physical table or view. The logical dataset's own URN is a different entity                                                                |
| `description`         | ❌       | `datasetProperties.description`                                                                         | Same role                                                                                                                                  |
| `primary_key`         | ❌       | `SchemaField.isPartOfKey` on `schemaMetadata.fields`                                                    | Semantic-model writers set this flag per column and do not set `schemaMetadata.primaryKeys`. `datasetProperties.primaryKey` does not exist |
| `unique_keys`         | ❌       | No stored field                                                                                         | Snowflake semantic-view ingestion reads unique keys when choosing relationship cardinality and does not persist them                       |
| `ai_context`          | ❌       | No aspect on the dataset entity                                                                         | `aiContext` is not in the dataset aspect list. Snowflake stores logical-table synonyms on `datasetProperties.customProperties`             |
| `fields[]`            | ❌       | `schemaMetadata.fields` (`SchemaField` records) and schemaField entities with `semanticFieldAnnotation` | Structural fields live on the schema record. Semantic role and expression live on the schemaField entity                                   |
| `custom_extensions[]` | ❌       | No DataHub field                                                                                        | Tags, terms, owners, and platform are separate aspects, not an Ossie `custom_extensions` payload                                           |

> **PDL source:** `com.linkedin.dataset.SemanticModelProperties`, `com.linkedin.dataset.DatasetProperties`, `com.linkedin.dataset.UpstreamLineage`, `com.linkedin.schema.SchemaMetadata`, `com.linkedin.schema.SchemaField`

---

### `Field` (within Dataset)

| OSI Field             | Required | DataHub Equivalent                                                          | Notes                                                                                                                                                                                                             |
| --------------------- | -------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                | ✅       | `SchemaField.fieldPath`                                                     | Semantic-model ingestion may normalize the path. Snowflake uppercases it unless `preserve_column_case` is set                                                                                                     |
| `expression`          | ✅       | `SemanticFieldAnnotation.expression` (`MetricExpression`)                   | Required on the annotation. Shape is `dialects[]` of `DialectExpression`                                                                                                                                          |
| `dimension`           | ❌       | `SemanticFieldAnnotation.dimension` (`Dimension.isTime`)                    | Ossie `dimension` only has `is_time`. Populated when `type == DIMENSION`                                                                                                                                          |
| `label`               | ❌       | `SchemaField.label`                                                         | The field exists and is `@Deprecated`. Its comment says it is not surfaced in the UI                                                                                                                              |
| `description`         | ❌       | `SchemaField.description`                                                   | On the schema record inside `schemaMetadata.fields`                                                                                                                                                               |
| `datatype`            | ❌       | `SchemaField.type` (`SchemaFieldDataType`) and `SchemaField.nativeDataType` | DataHub's type is a union (`StringType`, `NumberType`, `DateType`, …). Ossie's `DataType` is a flat enum (`String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque`) |
| `ai_context`          | ❌       | `aiContext` aspect on the schemaField entity                                | Object form only — see [AiContext](#aicontext)                                                                                                                                                                    |
| `custom_extensions[]` | ❌       | No DataHub field                                                            | `SemanticFieldAnnotation.type` and `aggregationFunction` are DataHub fields with no Ossie counterpart                                                                                                             |

**DataHub fields on `SemanticFieldAnnotation` with no Ossie counterpart:**

| DataHub Field                                 | Description                                                                    |
| --------------------------------------------- | ------------------------------------------------------------------------------ |
| `SemanticFieldAnnotation.type`                | `DIMENSION`, `MEASURE`, `FILTER`, `OTHER`                                      |
| `SemanticFieldAnnotation.aggregationFunction` | Free string such as `SUM`, `COUNT_DISTINCT`, `AVG`. Set when `type == MEASURE` |

> **PDL sources:** `com.linkedin.semanticmodel.SemanticFieldAnnotation`, `com.linkedin.semanticmodel.SemanticFieldType`, `com.linkedin.semanticmodel.Dimension`, `com.linkedin.schema.SchemaField`

---

### `Relationship`

| OSI Field             | Required | DataHub Equivalent                      | Notes                                                                                              |
| --------------------- | -------- | --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `name`                | ✅       | `SemanticModelRelationship.name`        | Optional in DataHub. Ossie requires it                                                             |
| `from`                | ✅       | `SemanticModelRelationship.from`        | Dataset alias (`semanticModelProperties.alias`). Ossie defines `from` as the many side of the join |
| `to`                  | ✅       | `SemanticModelRelationship.to`          | Dataset alias. Ossie defines `to` as the one side                                                  |
| `from_columns`        | ✅       | `SemanticModelRelationship.fromColumns` | Column-name strings on the `from` alias. Snowflake semantic views store the logical field path     |
| `to_columns`          | ✅       | `SemanticModelRelationship.toColumns`   | Column-name strings on the `to` alias                                                              |
| `ai_context`          | ❌       | `SemanticModelRelationship.aiContext`   | Inline record, not an entity aspect — see [AiContext](#aicontext)                                  |
| `custom_extensions[]` | ❌       | No DataHub field                        | Join cardinality is a separate DataHub field, below                                                |

`SemanticModelRelationship.cardinality` is optional `ERModelRelationshipCardinality`:
`ONE_ONE`, `ONE_N`, `N_ONE`, `N_N`. Ossie has no cardinality field.

> **PDL source:** `com.linkedin.semanticmodel.SemanticModelRelationship`, `com.linkedin.ermodelrelation.ERModelRelationshipCardinality`

---

### `Metric`

| OSI Field             | Required | DataHub Equivalent                           | Notes                                                                                              |
| --------------------- | -------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `name`                | ✅       | `MetricInfo.name`                            | Same role                                                                                          |
| `expression`          | ✅       | `MetricInfo.expression` (`MetricExpression`) | Optional in DataHub. Ossie requires it, so a metric with no expression is not a valid Ossie metric |
| `description`         | ❌       | `MetricInfo.description`                     | Same role                                                                                          |
| `datatype`            | ❌       | No field on `MetricInfo`                     | Ossie `DataType` enum is not modeled for metrics                                                   |
| `ai_context`          | ❌       | `aiContext` aspect on the metric entity      | Object form only — see [AiContext](#aicontext)                                                     |
| `custom_extensions[]` | ❌       | No DataHub field                             | Lineage and hierarchy live on `metricRelationships` and `metricUpstreams`                          |

> **PDL source:** `com.linkedin.metric.MetricInfo`

---

### `Expression` and `Dialect`

An Ossie `expression` is `{ dialects: [{ dialect, expression }] }`. `MetricExpression` and
`DialectExpression` use that same shape. `DialectExpression.dialect` and
`DialectExpression.expression` are both required.

`Dialect.pdl` says: _"Value set is aligned 1:1 with the OSI (Open Semantic Interchange) spec."_
The enum is the six named dialects below, plus `OTHER`. Ossie `0.2.0.dev0` also has
`BIGQUERY`, `SIGMA`, `THOUGHTSPOT`, `DAX`, and `OSSIE_SQL_2026`. Those five have no named
DataHub symbol.

| OSI Dialect      | DataHub `Dialect`    | Status          |
| ---------------- | -------------------- | --------------- |
| `ANSI_SQL`       | `Dialect.ANSI_SQL`   | Named symbol    |
| `SNOWFLAKE`      | `Dialect.SNOWFLAKE`  | Named symbol    |
| `MDX`            | `Dialect.MDX`        | Named symbol    |
| `TABLEAU`        | `Dialect.TABLEAU`    | Named symbol    |
| `DATABRICKS`     | `Dialect.DATABRICKS` | Named symbol    |
| `MAQL`           | `Dialect.MAQL`       | Named symbol    |
| `BIGQUERY`       | `Dialect.OTHER`      | No named symbol |
| `SIGMA`          | `Dialect.OTHER`      | No named symbol |
| `THOUGHTSPOT`    | `Dialect.OTHER`      | No named symbol |
| `DAX`            | `Dialect.OTHER`      | No named symbol |
| `OSSIE_SQL_2026` | `Dialect.OTHER`      | No named symbol |

> **PDL source:** `com.linkedin.metric.Dialect`, `com.linkedin.metric.MetricExpression`, `com.linkedin.metric.DialectExpression`

---

### `AiContext`

`AiContext.pdl` says the record is AI context _"(OSI `ai_context` shape)."_ It is an aspect on
**metric**, **semanticModel**, and **schemaField**. It is not an aspect on dataset.
`SemanticModelRelationship.aiContext` embeds the same record on the relationship.

Ossie `ai_context` is either a string or an object. The object has `instructions`, `synonyms`,
and `examples`, and allows extra properties. DataHub stores the object fields below. A bare
string has no field. Extra properties have no open bag; `customInstructions` is one optional
string.

| OSI `ai_context`      | DataHub Equivalent             | Notes                                    |
| --------------------- | ------------------------------ | ---------------------------------------- |
| string form           | No field                       | Ossie allows `ai_context` to be a string |
| `instructions`        | `AiContext.instructions`       | Optional string                          |
| `synonyms`            | `AiContext.synonyms`           | Optional string array                    |
| `examples`            | `AiContext.examples`           | Optional string array                    |
| additional properties | `AiContext.customInstructions` | One free-form string, not a property bag |

> **PDL source:** `com.linkedin.common.AiContext`

---

## DataHub Fields Ossie Does Not Define

These are stored on DataHub aspects. Nothing in the product writes them into Ossie
`custom_extensions`.

### Metric

| DataHub Field                        | PDL Record            | Description                                                                                                                                |
| ------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `metricInfo.semanticModel`           | `MetricInfo`          | Optional URN of the parent semantic model (`ModeledBy`)                                                                                    |
| `metricRelationships.parentMetric`   | `MetricRelationships` | `IsPartOf` parent in a metric hierarchy                                                                                                    |
| `metricRelationships.derivedFrom`    | `MetricRelationships` | `DerivedFrom` edges. Each entry is a `DerivedMetricInput`, which is an `Edge` with no expression. The SQL stays on `metricInfo.expression` |
| `metricRelationships.relatedMetrics` | `MetricRelationships` | Non-lineage `RelatedTo` edges                                                                                                              |
| `metricUpstreams.datasetUpstreams`   | `MetricUpstreams`     | Dataset-level upstreams                                                                                                                    |
| `metricUpstreams.fieldUpstreams`     | `MetricUpstreams`     | Schema-field upstreams. The SQL stays on `metricInfo.expression`                                                                           |

### Semantic model

| DataHub Field                           | PDL Record                  | Description                                                 |
| --------------------------------------- | --------------------------- | ----------------------------------------------------------- |
| `semanticModelInfo.nativeDefinition`    | `SemanticModelInfo`         | Verbatim source text (Snowflake DDL, dbt YAML, and similar) |
| `semanticModelRelationship.cardinality` | `SemanticModelRelationship` | `ONE_ONE`, `ONE_N`, `N_ONE`, or `N_N`                       |

### Semantic field

| DataHub Field                                 | PDL Record                | Description                                  |
| --------------------------------------------- | ------------------------- | -------------------------------------------- |
| `semanticFieldAnnotation.type`                | `SemanticFieldAnnotation` | `DIMENSION`, `MEASURE`, `FILTER`, or `OTHER` |
| `semanticFieldAnnotation.aggregationFunction` | `SemanticFieldAnnotation` | Aggregation string for `MEASURE` fields      |

### AiContext

| DataHub Field                  | PDL Record  | Description                  |
| ------------------------------ | ----------- | ---------------------------- |
| `aiContext.customInstructions` | `AiContext` | One extra instruction string |

---

## Gaps That Block a Round Trip

An Ossie document cannot be stored and emitted again without loss, because these fields have no
matching DataHub representation:

| OSI Field                                                   | What DataHub stores instead                                                                          |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| document `version`                                          | Nothing                                                                                              |
| `Dataset.source`                                            | An `upstreamLineage` dataset URN, not the source string                                              |
| `Dataset.unique_keys`                                       | Nothing. Snowflake semantic views consult them when choosing cardinality and do not store them       |
| `Dataset.ai_context`                                        | Logical-table synonyms may land in `datasetProperties.customProperties`                              |
| `Dataset.primary_key`                                       | Per-field `SchemaField.isPartOfKey`. Semantic-model writers leave `schemaMetadata.primaryKeys` unset |
| `Field.label`                                               | Deprecated `SchemaField.label`, not shown in the UI                                                  |
| `Field.datatype`                                            | `SchemaFieldDataType` union plus `nativeDataType`, not the Ossie `DataType` enum                     |
| `Metric.datatype`                                           | Nothing                                                                                              |
| `Metric.expression` when absent                             | Legal in DataHub, invalid in Ossie                                                                   |
| `Relationship.name` when absent                             | Legal in DataHub, invalid in Ossie                                                                   |
| `Relationship.from` / `to` many-side / one-side             | Aliases, with cardinality on a separate enum                                                         |
| `ai_context` string form, and extra object properties       | `customInstructions` is a single string                                                              |
| `BIGQUERY`, `SIGMA`, `THOUGHTSPOT`, `DAX`, `OSSIE_SQL_2026` | `Dialect.OTHER`                                                                                      |

The other direction has the same limit. DataHub-only fields in the previous section have no Ossie
field. Ossie `custom_extensions` (`vendor_name` plus a JSON `data` string) could carry them, and
no code does that today.

---

## Key PDL Source Files

All schema files live under `metadata-models/src/main/pegasus/`:

| Entity/Record                  | PDL Path                                                          |
| ------------------------------ | ----------------------------------------------------------------- |
| SemanticModelInfo              | `com/linkedin/semanticmodel/SemanticModelInfo.pdl`                |
| SemanticModelRelationship      | `com/linkedin/semanticmodel/SemanticModelRelationship.pdl`        |
| SemanticModelProperties        | `com/linkedin/dataset/SemanticModelProperties.pdl`                |
| SemanticFieldAnnotation        | `com/linkedin/semanticmodel/SemanticFieldAnnotation.pdl`          |
| SemanticFieldType              | `com/linkedin/semanticmodel/SemanticFieldType.pdl`                |
| Dimension                      | `com/linkedin/semanticmodel/Dimension.pdl`                        |
| ERModelRelationshipCardinality | `com/linkedin/ermodelrelation/ERModelRelationshipCardinality.pdl` |
| SchemaField                    | `com/linkedin/schema/SchemaField.pdl`                             |
| SchemaMetadata                 | `com/linkedin/schema/SchemaMetadata.pdl`                          |
| MetricInfo                     | `com/linkedin/metric/MetricInfo.pdl`                              |
| MetricExpression               | `com/linkedin/metric/MetricExpression.pdl`                        |
| DialectExpression              | `com/linkedin/metric/DialectExpression.pdl`                       |
| Dialect                        | `com/linkedin/metric/Dialect.pdl`                                 |
| DerivedMetricInput             | `com/linkedin/metric/DerivedMetricInput.pdl`                      |
| MetricRelationships            | `com/linkedin/metric/MetricRelationships.pdl`                     |
| MetricUpstreams                | `com/linkedin/metric/MetricUpstreams.pdl`                         |
| AiContext                      | `com/linkedin/common/AiContext.pdl`                               |

---

## References

- [Apache Ossie (incubating)](https://github.com/apache/ossie) — upstream OSI specification
- [OSI JSON Schema](https://raw.githubusercontent.com/apache/ossie/main/core-spec/ossie-schema.json) — machine-readable spec (current version: `0.2.0.dev0`)
- DataHub PDL schemas: `metadata-models/src/main/pegasus/com/linkedin/{metric,semanticmodel,dataset,schema,common}/`
- [Metrics & Semantic Models](./metrics-and-semantic-models.md)
