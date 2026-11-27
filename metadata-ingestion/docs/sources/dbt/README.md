## Overview

dbt is a data platform used to store and query analytical or operational data. Learn more in the [official dbt documentation](https://www.getdbt.com/).

The DataHub integration for dbt covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage and stateful deletion detection.

:::info Run both dbt and data warehouse ingestion for lineage

1. You must **run ingestion for both dbt and your data warehouse** (target platform). They can be run in any order.
2. It generates column lineage between the `dbt` nodes (e.g. when a model/snapshot depends on a dbt source or ephemeral model) as well as lineage between the `dbt` nodes and the underlying target platform nodes (e.g. BigQuery Table -> dbt source, dbt model -> BigQuery table/view).
3. It automatically generates "sibling" relationships between the dbt nodes and the target / data warehouse nodes. These nodes will show up in the UI with both platform logos.
4. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.
5. dbt adds its lineage edge to the warehouse table as a patch (`incremental_lineage` defaults to `true` in the dbt source). Warehouse sources such as Snowflake and BigQuery default `incremental_lineage` to `false` and re-state the table's lineage on each run, which can remove the edge dbt added. If dbt models show no downstream lineage after a warehouse run, set `incremental_lineage: true` in the warehouse recipe. With it enabled, lineage edges are only added, so edges that no longer exist in the warehouse are not removed.

:::

## Concept Mapping

| Source Concept | DataHub Concept                                                        | Notes                                                                        |
| -------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Source         | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Source`                                                             |
| Seed           | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Seed`                                                               |
| Model          | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Model`                                                              |
| Snapshot       | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Snapshot`                                                           |
| Semantic View  | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Semantic View`                                                      |
| Semantic Model | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Semantic Model`                                                     |
| dbt project    | [Semantic Model](../../metamodel/entities/semanticModel.md)            | With `emit_semantic_model_entities`; one per project                         |
| Metric         | [Metric](../../metamodel/entities/metric.md)                           | With `emit_semantic_model_entities`; `create_metric` measures and `metrics:` |
| Test           | [Assertion](../../metamodel/entities/assertion.md)                     |                                                                              |
| Test Result    | [Assertion Run Result](../../metamodel/entities/assertion.md)          |                                                                              |
| Model Runs     | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) |                                                                              |
| Exposure       | [Dashboard](../../metamodel/entities/dashboard.md)                     | Platform `dbt`; subtype from the exposure type. See Exposures below          |
