## Overview

Databricks is a data platform used to store and query analytical or operational data. Learn more in the [official Databricks documentation](https://www.databricks.com/).

The DataHub integration for Databricks covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, ownership, and stateful deletion detection.

DataHub supports integration with Databricks ecosystem using a multitude of connectors, depending on your exact setup.

### Databricks Unity Catalog (new)

The recently introduced [Unity Catalog](https://www.databricks.com/product/unity-catalog) provides a new way to govern your assets within the Databricks lakehouse. If you have Unity Catalog Enabled Workspace, you can use the `databricks` source (aka `unity-catalog` source, see below for details) to integrate your metadata into DataHub as an alternate to the Hive pathway. This also ingests hive metastore catalog in Databricks and is recommended approach to ingest Databricks ecosystem in DataHub.

### Databricks Hive (old)

The alternative way to integrate is via the Hive connector. The [Hive starter recipe](http://datahubproject.io/docs/generated/ingestion/sources/hive#starter-recipe) has a section describing how to connect to your Databricks workspace.

### Databricks Spark

To complete the picture, we recommend adding push-based ingestion from your Spark jobs to see real-time activity and lineage between your Databricks tables and your Spark jobs. Use the Spark agent to push metadata to DataHub using the instructions [here](../../../../metadata-integration/java/acryl-spark-lineage/README.md#configuration-instructions-databricks).

### Watch the DataHub Talk at the Data and AI Summit 2022

For a deeper look at how to think about DataHub within and across your Databricks ecosystem, watch the recording of our talk at the Data and AI Summit 2022.

<p align="center">
<a href="https://www.youtube.com/watch?v=SCP0PR3t7dc">
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-ingestion/databricks/data_and_ai_summit_2022.png"/>
</a>
</p>

## Concept Mapping

| Databricks Concept                        | DataHub Entity (Subtype)          | Notes                                                                                              |
| ----------------------------------------- | --------------------------------- | -------------------------------------------------------------------------------------------------- |
| Workspace / Account                       | Platform Instance                 | Top-level scope; all URNs include the configured platform instance.                                |
| Metastore                                 | Container (METASTORE)             | Top-level Unity Catalog container.                                                                 |
| Catalog                                   | Container (CATALOG)               | Namespace within a metastore. Hive Metastore is ingested as a special catalog type.                |
| Schema                                    | Container (SCHEMA)                | Nested under its Catalog container.                                                                |
| Table (managed, external, Delta, Iceberg) | Dataset (TABLE)                   | All non-view table types including streaming tables. Schema, descriptions, and tags are extracted. |
| View / Materialized View                  | Dataset (VIEW)                    | View definition is captured.                                                                       |
| Notebook                                  | Dataset (NOTEBOOK)                | Ingested when `include_notebooks` is enabled. Lineage to and from tables is extracted.             |
| ML Model Group                            | MLModelGroup                      | Represents an MLflow Registered Model.                                                             |
| ML Model Version                          | MLModel                           | Each registered version with run metrics, parameters, tags, and aliases.                           |
| Column / field                            | SchemaField                       | Column type, nullability, and descriptions are extracted.                                          |
| User / Service Principal                  | CorpUser                          | Ownership; service principals mapped via display name to user URN.                                 |
| Group                                     | CorpGroup                         | Ownership mapped as `urn:li:corpGroup:{group_name}`.                                               |
| Unity Catalog Tag                         | Tag                               | Extracted at catalog, schema, table, and column levels.                                            |
| Table / column lineage                    | Lineage edges                     | From view definitions and SQL query history. Notebook-to-table lineage also extracted.             |
| Query operations and usage                | DatasetUsageStatistics, Operation | Per-dataset query counts and DML operation metrics.                                                |
