### Concept Mapping

- **Dremio Datasets**: Mapped to DataHub’s `Dataset` entity.
    - A dataset can be physical or virtual.
- **Lineage**: Mapped to DataHub’s `UpstreamLineage` aspect, representing the flow of data between datasets and columns.
- **Containers**: Spaces, folders, and sources in Dremio are mapped to DataHub’s `Container` aspect, organizing datasets logically.

Here's a table for **Concept Mapping** between Dremio and DataHub to provide a clear overview of how entities and concepts in Dremio are mapped to corresponding entities in DataHub:

| **Dremio Concept** | **DataHub Entity/Aspect** | **Description** |  |
| --- | --- | --- | --- |
| **Physical Dataset** | `Dataset` | A dataset directly queried from an external source without modifications. |  |
| **Virtual Dataset** | `Dataset` | A dataset built from SQL-based transformations on other datasets. |  |
| **Spaces** | `Container` | Top-level organizational unit in Dremio, used to group datasets. Mapped to DataHub’s `Container` aspect. |  |
| **Folders** | `Container` | Substructure inside spaces, used for organizing datasets. Mapped as a `Container` in DataHub. |  |
| **Sources** | `Container` | External data sources connected to Dremio (e.g., S3, databases). Represented as a `Container` in DataHub. |  |
| **Column Lineage** | `ColumnLineage` | Lineage between columns in datasets, showing how individual columns are transformed across datasets. |  |
| **Dataset Lineage** | `UpstreamLineage` | Lineage between datasets, tracking the flow and transformations between different datasets. |  |
| **Ownership (Dataset)** | `Ownership` | Ownership information for datasets, representing the technical owner in DataHub’s `Ownership` aspect. |  |
| **Glossary Terms** | `GlossaryTerms` | Business terms associated with datasets, providing context. Mapped as `GlossaryTerms` in DataHub. |  |
| **Schema Metadata** | `SchemaMetadata` | Schema details (columns, data types) for datasets. Mapped to DataHub’s `SchemaMetadata` aspect. |  |
| **SQL Transformations** | `Dataset` (with lineage) | SQL queries in Dremio that transform datasets. Represented as `Dataset` in DataHub, with lineage showing dependency. |  |
| **Queries** | `Query` (if mapped) | Historical SQL queries executed on Dremio datasets. These can be tracked for audit purposes in DataHub. |  |