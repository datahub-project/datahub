Ingesting metadata from Dataform requires either using the **dataform** module for Google Cloud Dataform or local Dataform Core projects.

### Concept Mapping

| Source Concept | DataHub Concept                                    | Notes              |
| -------------- | -------------------------------------------------- | ------------------ |
| Declaration    | [Dataset](../../metamodel/entities/dataset.md)     | Subtype `External` |
| Table          | [Dataset](../../metamodel/entities/dataset.md)     | Subtype `Table`    |
| View           | [Dataset](../../metamodel/entities/dataset.md)     | Subtype `View`     |
| Incremental    | [Dataset](../../metamodel/entities/dataset.md)     | Subtype `Table`    |
| Assertion      | [Assertion](../../metamodel/entities/assertion.md) |                    |
| Operation      | Custom SQL Operation                               |                    |

Note:

1. You must **run ingestion for both Dataform and your data warehouse** (target platform). They can be run in any order.
2. It generates column lineage between the `dataform` nodes (e.g. when a table depends on a dataform declaration or other tables) as well as lineage between the `dataform` nodes and the underlying target platform nodes (e.g. BigQuery Table -> dataform declaration, dataform table -> BigQuery table/view).
3. It automatically generates "sibling" relationships between the dataform nodes and the target / data warehouse nodes. These nodes will show up in the UI with both platform logos.
4. We support automated actions (like add a tag, term or owner) based on properties defined in Dataform configurations and tags.
