Ingesting metadata from dbt requires either using the **dbt** module or the **dbt-cloud** module.

### Concept Mapping

| Source Concept | DataHub Concept                                                        | Notes              |
| -------------- | ---------------------------------------------------------------------- | ------------------ |
| Source         | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Source`   |
| Seed           | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Seed`     |
| Model          | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Model`    |
| Snapshot       | [Dataset](../../metamodel/entities/dataset.md)                         | Subtype `Snapshot` |
| Test           | [Assertion](../../metamodel/entities/assertion.md)                     |                    |
| Test Result    | [Assertion Run Result](../../metamodel/entities/assertion.md)          |                    |
| Model Runs     | [DataProcessInstance](../../metamodel/entities/dataProcessInstance.md) |                    |

Note:

1. You must **run ingestion for both dbt and your data warehouse** (target platform). They can be run in any order.
2. It generates column lineage between the `dbt` nodes (e.g. when a model/snapshot depends on a dbt source or ephemeral model) as well as lineage between the `dbt` nodes and the underlying target platform nodes (e.g. BigQuery Table -> dbt source, dbt model -> BigQuery table/view).
3. It automatically generates "sibling" relationships between the dbt nodes and the target / data warehouse nodes. These nodes will show up in the UI with both platform logos.
4. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.
