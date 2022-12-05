Ingesting metadata from dbt requires either using the **dbt** module or the **dbt-cloud** module.

### Concept Mapping

| Source Concept           | DataHub Concept                                               | Notes                 |
| ------------------------ | ------------------------------------------------------------- | --------------------- |
| `"dbt"`                  | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                       |
| dbt Source               | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `source`      |
| dbt Seed                 | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `seed`        |
| dbt Model - materialized | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `table`       |
| dbt Model - view         | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `view`        |
| dbt Model - incremental  | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `incremental` |
| dbt Model - ephemeral    | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `ephemeral`   |
| dbt Test                 | [Assertion](../../metamodel/entities/assertion.md)            |                       |
| dbt Test Result          | [Assertion Run Result](../../metamodel/entities/assertion.md) |                       |

Note:

1. It also generates lineage between the `dbt` nodes (e.g. ephemeral nodes that depend on other dbt sources) as well as lineage between the `dbt` nodes and the underlying (target) platform nodes (e.g. BigQuery Table -> dbt Source, dbt View -> BigQuery View).
2. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.
