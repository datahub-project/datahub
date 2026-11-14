
# DataHub Entity Model Compatibility with Apache Ossie (OSI)

> **TL;DR:** If you already have an Apache Ossie (OSI) semantic model, almost all of it has a
> place in DataHub: names, descriptions, expressions, joins, keys, time dimensions, and AI
> context. DataHub does not read or write Ossie YAML yet. Today you load an Ossie model with the
> [Python SDK](#loading-an-ossie-model-into-datahub-today), and a handful of fields need a
> workaround or are dropped. Those are listed in
> [Limitations](#limitations).

**Who this is for:** data engineers who author or maintain Ossie semantic models and want to
catalog them in DataHub, or who are deciding whether DataHub metadata can be exported to an
Ossie-compatible tool.

**Spec version:** Ossie `0.2.0.dev0`. For how Metrics and Semantic Models work in DataHub
generally, see [Metrics & Semantic Models](./metrics-and-semantic-models.md).

---

## Background

[Apache Ossie (incubating)](https://github.com/apache/ossie), also written OSI (Open Semantic
Interchange), is an open standard for exchanging semantic-layer metadata (semantic models,
datasets, fields, relationships, and metrics) between tools. DataHub is a launch partner and
designed its Metric and Semantic Model entities to line up with the Ossie shape.

### Current support at a glance

| Capability                                         | Status                                                                                   |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Represent Ossie concepts as DataHub entities       | ✅ Most fields map directly (see the [reference](#field-by-field-reference))             |
| Load an Ossie model with the Python SDK            | ✅ You write the mapping (see the [example](#loading-an-ossie-model-into-datahub-today)) |
| Import an Ossie YAML file directly (recipe or CLI) | ❌ Not implemented                                                                       |
| Export DataHub metrics and models as Ossie YAML    | ❌ Not implemented                                                                       |
| Lossless round trip (Ossie → DataHub → Ossie)      | ❌ Some fields are dropped (see [gaps](#limitations))                                    |

---

## Concept Mapping

The biggest difference is structural, not field by field:

- **Ossie** is one **nested document**. A semantic model contains its datasets, each dataset
  contains its fields, and relationships and metrics sit alongside them.
- **DataHub** splits that document into **separate entities linked by references**. Each
  dataset, field, and metric gets its own URN, page, owners, tags, glossary terms, search
  results, and lineage.

| Ossie Concept             | DataHub Concept                                                                   | Notes                                                                                                             |
| ------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Semantic model (document) | [Semantic Model](/docs/generated/metamodel/entities/semanticModel.md)             |                                                                                                                   |
| `datasets[]`              | [Dataset](/docs/generated/metamodel/entities/dataset.md)                          | Subtype `Semantic Model Dataset` (a logical dataset). `semanticModelProperties.semanticModel` points at the model |
| `datasets[].fields[]`     | [Schema Field](/docs/generated/metamodel/entities/schemaField.md)                 | A column in the logical dataset's `schemaMetadata`. Its role and expression live on the schema field entity       |
| `relationships[]`         | Part of the [Semantic Model](/docs/generated/metamodel/entities/semanticModel.md) | Stored as records in `semanticModelInfo.relationships`, not as separate entities                                  |
| `metrics[]`               | [Metric](/docs/generated/metamodel/entities/metric.md)                            | `metricInfo.semanticModel` points at the model                                                                    |
| `datasets[].source`       | [Dataset](/docs/generated/metamodel/entities/dataset.md)                          | The physical table your warehouse connector already ingested, linked by upstream lineage from the logical dataset |

Example URNs for the model used later in this guide:

```text
urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)
urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD),amount)
urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,total_revenue)
```

Two consequences matter in practice:

1. **Lineage runs through the logical datasets.** The chain is
   `Metric → Logical dataset → Physical table`. The semantic model groups its members but is
   not a step in that chain.
2. **The URNs you choose matter.** An Ossie dataset is identified by its `name` within one
   document. In DataHub it needs a globally unique URN, so pick a stable naming scheme such as
   `<schema>.<model>.<dataset>`. Metric URNs carry no environment (`PROD`, `DEV`), so the same
   metric in two environments is one entity.

---

## Loading an Ossie model into DataHub today

There is no Ossie importer yet, but the Python SDK builders (`SemanticModel`,
`SemanticModelDataset`, `Metric`) follow the same structure, so a loader is a direct
translation. Here is a small Ossie model:

```yaml
version: "0.2.0.dev0"
name: orders_model
description: A semantic model over the raw orders and customers tables.
ai_context:
  instructions: Use for revenue and customer analytics.
  synonyms: [orders model]
datasets:
  - name: ORDERS
    source: analytics.raw.orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: { dialects: [{ dialect: ANSI_SQL, expression: order_id }] }
      - name: customer_id
        expression:
          { dialects: [{ dialect: ANSI_SQL, expression: customer_id }] }
      - name: order_ts
        expression: { dialects: [{ dialect: ANSI_SQL, expression: order_ts }] }
        dimension: { is_time: true }
      - name: amount
        expression: { dialects: [{ dialect: SNOWFLAKE, expression: amount }] }
        ai_context: { synonyms: [revenue] }
  - name: CUSTOMERS
    source: analytics.raw.customers
    primary_key: [customer_id]
    fields:
      - name: customer_id
        expression:
          { dialects: [{ dialect: ANSI_SQL, expression: customer_id }] }
      - name: customer_name
        expression:
          { dialects: [{ dialect: ANSI_SQL, expression: customer_name }] }
relationships:
  - name: orders_to_customers
    from: ORDERS # the "many" side
    to: CUSTOMERS # the "one" side
    from_columns: [customer_id]
    to_columns: [customer_id]
metrics:
  - name: total_revenue
    description: Sum of all order amounts.
    expression:
      { dialects: [{ dialect: SNOWFLAKE, expression: SUM(ORDERS.amount) }] }
    ai_context: { synonyms: [revenue] }
```

The same model built with the SDK. Comments mark the Ossie field each argument comes from:

```python
from pathlib import Path

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.metadata.schema_classes import (
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
)
from datahub.metadata.urns import SemanticModelUrn
from datahub.sdk import (
    AiContextInput,
    DataHubClient,
    DialectExpressionInput,
    Metric,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

platform = "snowflake"
model_urn = SemanticModelUrn(platform=platform, path="analytics", id="orders_model")

orders = SemanticModelDataset(
    platform=platform,
    name="analytics.orders_model.orders_ds",  # your URN naming scheme
    semantic_model=model_urn,
    alias="ORDERS",  # datasets[].name, used by relationships
    upstreams=[make_dataset_urn(platform, "analytics.raw.orders")],  # datasets[].source
    schema=[
        SemanticFieldInput(
            field_path="order_id",  # fields[].name
            type="int",  # DataHub needs a type; Ossie datatype is optional
            semantic_type=SemanticFieldTypeClass.DIMENSION,  # not in Ossie, see below
            is_part_of_key=True,  # datasets[].primary_key
        ),
        SemanticFieldInput(
            field_path="customer_id",
            type="int",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
        ),
        SemanticFieldInput(
            field_path="order_ts",
            type="timestamp",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            is_time_dimension=True,  # fields[].dimension.is_time
        ),
        SemanticFieldInput(
            field_path="amount",
            type="float",
            semantic_type=SemanticFieldTypeClass.MEASURE,
            expression=DialectExpressionInput(  # fields[].expression
                expression="amount", dialect=DialectClass.SNOWFLAKE
            ),
            aggregation_function="SUM",  # not in Ossie
            ai_context=AiContextInput(synonyms=["revenue"]),  # fields[].ai_context
        ),
    ],
)

customers = SemanticModelDataset(
    platform=platform,
    name="analytics.orders_model.customers_ds",
    semantic_model=model_urn,
    alias="CUSTOMERS",
    upstreams=[make_dataset_urn(platform, "analytics.raw.customers")],
    schema=[
        SemanticFieldInput(
            field_path="customer_id",
            type="int",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            is_part_of_key=True,
        ),
        SemanticFieldInput(
            field_path="customer_name",
            type="varchar",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
        ),
    ],
)

total_revenue = Metric(
    platform=platform,
    path="analytics",
    id="total_revenue",  # metrics[].name
    name="Total Revenue",
    description="Sum of all order amounts.",  # metrics[].description
    semantic_model=str(model_urn),
    expression=DialectExpressionInput(  # metrics[].expression
        expression="SUM(ORDERS.amount)", dialect=DialectClass.SNOWFLAKE
    ),
    upstream_datasets=[orders.urn],  # lineage, not in Ossie
    ai_context=AiContextInput(synonyms=["revenue"]),  # metrics[].ai_context
)

model = SemanticModel(
    platform=platform,
    path="analytics",
    id="orders_model",
    name="orders_model",  # name
    description="A semantic model over the raw orders and customers tables.",
    native_definition=Path("orders_model.ossie.yaml").read_text(),  # keep the original
    datasets=[orders, customers],
    relationships=[
        SemanticModelRelationshipInput(
            name="orders_to_customers",  # relationships[].name
            from_alias="ORDERS",  # relationships[].from
            from_columns=["customer_id"],  # relationships[].from_columns
            to_alias="CUSTOMERS",  # relationships[].to
            to_columns=["customer_id"],  # relationships[].to_columns
            cardinality=ERModelRelationshipCardinalityClass.N_ONE,  # Ossie's from=many, to=one
        )
    ],
    ai_context=AiContextInput(  # ai_context
        synonyms=["orders model"],
        instructions="Use for revenue and customer analytics.",
    ),
)

client = DataHubClient(server="<your-gms-url>", token="<your-token>")
for entity in [model, orders, customers, total_revenue]:
    client.entities.upsert(entity)
```

Things to decide when you write the loader:

- **Field type and role.** DataHub requires a column type and a semantic role (`DIMENSION`,
  `MEASURE`, `FILTER`, or `OTHER`) for every field. Ossie has neither as a required field.
  Map Ossie `datatype` to a native type string when it is present. For the role, a common rule
  is `dimension` present → `DIMENSION`, otherwise `MEASURE`, adjusted by naming conventions.
- **Cardinality.** Ossie defines `from` as the many side and `to` as the one side, so every
  Ossie relationship maps to `N_ONE`.
- **Keep the original YAML.** Store the source file in `native_definition`. It is shown on the
  semantic model page and keeps everything DataHub has no field for, such as the spec
  `version`, `unique_keys`, and `custom_extensions`.
- **Physical sources.** `upstreams` expects the URN of a physical table that your warehouse
  connector has already ingested. Build it from Ossie `source` with the same platform, platform
  instance, and environment, or lineage will point at a table that does not exist.

The SDK also writes things Ossie has no field for, including the dataset subtype, membership
pointers, and metric lineage. For the full aspect list, see the
[Semantic Models & Metrics SDK guide](../../api/tutorials/semantic-models.md).

---

## Limitations

These Ossie fields have no exact DataHub field. Use the workaround column if the information
matters to you.

| Ossie field                                          | What happens in DataHub                                                      | Workaround                                                                                          |
| ---------------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Document `version`                                   | Not stored                                                                   | Kept in `native_definition` if you store the source YAML                                            |
| `Dataset.source` when it is a **query**, not a table | Lineage needs a table URN, so a query has nowhere to go                      | Point `upstreams` at the tables the query reads. Keep the query in `native_definition`              |
| `Dataset.unique_keys`                                | Not stored                                                                   | Use a structured property, or rely on `native_definition`                                           |
| `Dataset.ai_context`                                 | Logical datasets do not have an AI context aspect                            | Put the synonyms and instructions on the model's or the fields' AI context                          |
| `Dataset.primary_key`                                | Stored per column (`is_part_of_key`), not as an ordered list                 | Column order of a composite key is not preserved. Keep it in `native_definition` if order matters   |
| `Field.label`                                        | Stored on a deprecated field that the UI does not show                       | Put the display label in the field description                                                      |
| `Field.datatype`                                     | Stored as DataHub's own type plus the native type string, not the Ossie enum | Map in both directions in your loader                                                               |
| `Metric.datatype`                                    | Not stored                                                                   | Use a structured property on the metric                                                             |
| `ai_context` as a plain string                       | DataHub only stores the object form                                          | Put the string in `instructions`                                                                    |
| Extra keys in an `ai_context` object                 | Only one free-form string, `customInstructions`                              | Serialize the extra keys into `customInstructions` (for example as JSON)                            |
| A dialect DataHub has no named value for             | Stored as `OTHER`, so the dialect name is lost                               | Keep the original in `native_definition`. See [Expressions and dialects](#expressions-and-dialects) |
| `custom_extensions[]` (everywhere)                   | Not stored                                                                   | Use structured properties, which are typed, searchable, and shown in the UI                         |

### Going the other way: DataHub → Ossie

DataHub allows a few things that Ossie rejects. If you plan to export later, enforce these in
your loader:

- **Every metric needs an `expression`.** It is optional in DataHub and required in Ossie.
- **Every relationship needs a `name`.** It is optional in DataHub and required in Ossie.
- **Ossie has no many-to-many join.** `N_ONE` maps directly, `ONE_N` maps by swapping `from`
  and `to`, and `ONE_ONE` fits either way. `N_N` has no Ossie representation.

DataHub also stores information that Ossie has no field for:

| DataHub field                                                | What it holds                                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `semanticFieldAnnotation.type`                               | Field role: `DIMENSION`, `MEASURE`, `FILTER`, or `OTHER`                       |
| `semanticFieldAnnotation.aggregationFunction`                | Aggregation for `MEASURE` fields, such as `SUM`, `COUNT_DISTINCT`, or `AVG`    |
| `semanticModelRelationship.cardinality`                      | Join cardinality: `ONE_ONE`, `ONE_N`, `N_ONE`, or `N_N`                        |
| `semanticModelInfo.nativeDefinition`                         | The original source definition, verbatim (Snowflake DDL, dbt YAML, Ossie YAML) |
| `metricRelationships.parentMetric`                           | Parent in a metric hierarchy                                                   |
| `metricRelationships.derivedFrom`                            | Metrics this metric is computed from                                           |
| `metricRelationships.relatedMetrics`                         | Related metrics, not lineage                                                   |
| `metricUpstreams.datasetUpstreams` / `fieldUpstreams`        | Dataset- and column-level lineage for the metric                               |
| Owners, domains, tags, glossary terms, structured properties | Governance metadata on every entity                                            |

An exporter could carry these in Ossie `custom_extensions` (a `vendor_name` plus a JSON
`data` string). No DataHub code does this today.

---

## Field-by-field reference

Status key: ✅ maps directly · ⚠️ maps with differences · ❌ not stored. "Required" is whether
**Ossie** requires the field.

### Semantic model (document root)

| Ossie field           | Required | Status | Where it lives in DataHub                             | Notes                                                                           |
| --------------------- | -------- | ------ | ----------------------------------------------------- | ------------------------------------------------------------------------------- |
| `version`             | Yes      | ❌     | —                                                     | Set on the document, not on `$defs/SemanticModel`                               |
| `name`                | Yes      | ✅     | `semanticModelInfo.name`                              |                                                                                 |
| `description`         |          | ✅     | `semanticModelInfo.description`                       |                                                                                 |
| `ai_context`          |          | ⚠️     | `aiContext` aspect on the semantic model              | Object form only. See [AI context](#ai-context)                                 |
| `datasets[]`          | Yes      | ✅     | Logical `dataset` entities pointing back at the model | `semanticModelInfo.datasets` is deprecated. New writes leave it empty           |
| `relationships[]`     |          | ✅     | `semanticModelInfo.relationships[]`                   | See [Relationship](#relationship)                                               |
| `metrics[]`           |          | ✅     | `metric` entities with `metricInfo.semanticModel` set | The model pointer is optional in DataHub, so a metric can exist without a model |
| `custom_extensions[]` |          | ❌     | —                                                     | Use structured properties                                                       |

### Dataset

| Ossie field           | Required | Status | Where it lives in DataHub                                                     | Notes                                                                                                      |
| --------------------- | -------- | ------ | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `name`                | Yes      | ✅     | `semanticModelProperties.alias`                                               | The name relationships use. `datasetProperties.name` is a separate, optional display name                  |
| `source`              | Yes      | ⚠️     | `upstreamLineage` to the physical table                                       | Stored as a lineage edge to a table URN, not as the source string. A query source has no direct home       |
| `description`         |          | ✅     | `datasetProperties.description`                                               |                                                                                                            |
| `primary_key`         |          | ⚠️     | `isPartOfKey` on each key column in `schemaMetadata.fields`                   | Per-column flag. Key column order is not kept, and `schemaMetadata.primaryKeys` is not set                 |
| `unique_keys`         |          | ❌     | —                                                                             | Snowflake semantic-view ingestion reads them to infer cardinality but does not store them                  |
| `ai_context`          |          | ❌     | —                                                                             | Datasets have no `aiContext` aspect. Snowflake ingestion puts logical-table synonyms in `customProperties` |
| `fields[]`            |          | ✅     | `schemaMetadata.fields`, plus `semanticFieldAnnotation` on each `schemaField` | Structure lives on the schema. Role and expression live on the `schemaField` entity                        |
| `custom_extensions[]` |          | ❌     | —                                                                             | Use structured properties                                                                                  |

### Field

| Ossie field           | Required | Status | Where it lives in DataHub                           | Notes                                                                                                                                                                                                  |
| --------------------- | -------- | ------ | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`                | Yes      | ✅     | `SchemaField.fieldPath`                             | Connectors may normalize it. Snowflake uppercases it unless `preserve_column_case` is set                                                                                                              |
| `expression`          | Yes      | ✅     | `semanticFieldAnnotation.expression`                | Same `dialects[]` shape as Ossie. See [Expressions and dialects](#expressions-and-dialects)                                                                                                            |
| `dimension`           |          | ✅     | `semanticFieldAnnotation.dimension.isTime`          | Ossie `dimension` only has `is_time`                                                                                                                                                                   |
| `label`               |          | ⚠️     | `SchemaField.label`                                 | Deprecated in DataHub and not shown in the UI                                                                                                                                                          |
| `description`         |          | ✅     | `SchemaField.description`                           |                                                                                                                                                                                                        |
| `datatype`            |          | ⚠️     | `SchemaField.type` and `SchemaField.nativeDataType` | DataHub uses its own type union (`StringType`, `NumberType`, `DateType`, …), not Ossie's enum (`String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque`) |
| `ai_context`          |          | ⚠️     | `aiContext` aspect on the `schemaField`             | Object form only. See [AI context](#ai-context)                                                                                                                                                        |
| `custom_extensions[]` |          | ❌     | —                                                   | Use structured properties                                                                                                                                                                              |

### Relationship

| Ossie field           | Required | Status | Where it lives in DataHub               | Notes                                                              |
| --------------------- | -------- | ------ | --------------------------------------- | ------------------------------------------------------------------ |
| `name`                | Yes      | ⚠️     | `SemanticModelRelationship.name`        | Optional in DataHub                                                |
| `from`                | Yes      | ✅     | `SemanticModelRelationship.from`        | A dataset alias. In Ossie this is the many side of the join        |
| `to`                  | Yes      | ✅     | `SemanticModelRelationship.to`          | A dataset alias. In Ossie this is the one side                     |
| `from_columns`        | Yes      | ✅     | `SemanticModelRelationship.fromColumns` | Column names on the `from` dataset                                 |
| `to_columns`          | Yes      | ✅     | `SemanticModelRelationship.toColumns`   | Column names on the `to` dataset                                   |
| `ai_context`          |          | ⚠️     | `SemanticModelRelationship.aiContext`   | Stored inside the relationship, not as an aspect. Object form only |
| `custom_extensions[]` |          | ❌     | —                                       |                                                                    |

DataHub adds an optional `cardinality` (`ONE_ONE`, `ONE_N`, `N_ONE`, `N_N`). Ossie implies
`N_ONE` through its from/to convention.

### Metric

| Ossie field           | Required | Status | Where it lives in DataHub        | Notes                                                                            |
| --------------------- | -------- | ------ | -------------------------------- | -------------------------------------------------------------------------------- |
| `name`                | Yes      | ✅     | `metricInfo.name`                |                                                                                  |
| `expression`          | Yes      | ⚠️     | `metricInfo.expression`          | Optional in DataHub. A metric without one is not valid Ossie                     |
| `description`         |          | ✅     | `metricInfo.description`         |                                                                                  |
| `datatype`            |          | ❌     | —                                | Use a structured property                                                        |
| `ai_context`          |          | ⚠️     | `aiContext` aspect on the metric | Object form only. See [AI context](#ai-context)                                  |
| `custom_extensions[]` |          | ❌     | —                                | Metric hierarchy and lineage live on `metricRelationships` and `metricUpstreams` |

### Expressions and dialects

An Ossie expression is `{ dialects: [{ dialect, expression }] }`. DataHub's
`MetricExpression` and `DialectExpression` use the same shape, and both `dialect` and
`expression` are required. One expression can carry several dialects, and the SDK accepts a
list of `DialectExpressionInput`.

DataHub supports a subset of the Ossie dialects as named values. The current list is the
`Dialect` enum in
[`Dialect.pdl`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metric/Dialect.pdl).
Any Ossie dialect without a named value there is stored as **`OTHER`**, the fallback, and the
original dialect name is lost. Keep the source YAML in `native_definition` if you need it.

### AI context

DataHub's `AiContext` record follows the object form of Ossie `ai_context`. It is an aspect on
**metrics**, **semantic models**, and **schema fields**, and is stored inside each
**relationship**. Logical datasets do not have it.

| Ossie `ai_context`      | DataHub                           | Notes                                     |
| ----------------------- | --------------------------------- | ----------------------------------------- |
| `instructions`          | ✅ `AiContext.instructions`       |                                           |
| `synonyms`              | ✅ `AiContext.synonyms`           |                                           |
| `examples`              | ✅ `AiContext.examples`           |                                           |
| Plain-string form       | ❌ —                              | Put the string in `instructions`          |
| Extra object properties | ⚠️ `AiContext.customInstructions` | One free-form string, not a key-value map |

---

## Schema source files

For exact field definitions, the PDL schemas are under
`metadata-models/src/main/pegasus/com/linkedin/`:

| Record                                         | Path                                                                                |
| ---------------------------------------------- | ----------------------------------------------------------------------------------- |
| SemanticModelInfo                              | `semanticmodel/SemanticModelInfo.pdl`                                               |
| SemanticModelRelationship                      | `semanticmodel/SemanticModelRelationship.pdl`                                       |
| SemanticFieldAnnotation                        | `semanticmodel/SemanticFieldAnnotation.pdl`                                         |
| SemanticFieldType                              | `semanticmodel/SemanticFieldType.pdl`                                               |
| Dimension                                      | `semanticmodel/Dimension.pdl`                                                       |
| SemanticModelProperties                        | `dataset/SemanticModelProperties.pdl`                                               |
| ERModelRelationshipCardinality                 | `ermodelrelation/ERModelRelationshipCardinality.pdl`                                |
| SchemaField / SchemaMetadata                   | `schema/SchemaField.pdl`, `schema/SchemaMetadata.pdl`                               |
| MetricInfo                                     | `metric/MetricInfo.pdl`                                                             |
| MetricExpression / DialectExpression / Dialect | `metric/MetricExpression.pdl`, `metric/DialectExpression.pdl`, `metric/Dialect.pdl` |
| MetricRelationships / DerivedMetricInput       | `metric/MetricRelationships.pdl`, `metric/DerivedMetricInput.pdl`                   |
| MetricUpstreams                                | `metric/MetricUpstreams.pdl`                                                        |
| AiContext                                      | `common/AiContext.pdl`                                                              |

---

## References

- [Apache Ossie (incubating)](https://github.com/apache/ossie): the upstream specification
- [Ossie JSON Schema](https://raw.githubusercontent.com/apache/ossie/main/core-spec/ossie-schema.json): machine-readable spec (`0.2.0.dev0`)
- [Metrics & Semantic Models](./metrics-and-semantic-models.md): feature overview
- [Semantic Models & Metrics SDK guide](../../api/tutorials/semantic-models.md): full SDK walkthrough
