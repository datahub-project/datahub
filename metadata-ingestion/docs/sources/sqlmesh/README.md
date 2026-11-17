## Overview

[SQLMesh](https://sqlmesh.com) is an open-source DataOps framework for building and operating SQL transformation pipelines. It manages model versioning, virtual environments, incremental execution, and data quality audits across warehouses including Snowflake, BigQuery, Databricks, DuckDB, and others.

DataHub ingests SQLMesh model metadata—schema, lineage, column-level lineage, descriptions, and data quality audits—and links each model to its corresponding warehouse view as a sibling entity. This follows the same pattern as the dbt connector: the SQLMesh entity owns model definitions and lineage while the warehouse connector contributes runtime metadata such as query history, profiling, and usage. DataHub merges both views in the UI automatically.

## Concept Mapping

| SQLMesh concept     | DataHub entity / aspect                          |
| ------------------- | ------------------------------------------------ |
| Model               | Dataset (`urn:li:dataPlatform:sqlmesh,...`)      |
| Model depends_on    | UpstreamLineage (coarse-grained)                 |
| Column dependencies | FineGrainedLineage (column-level lineage)        |
| Model description   | DatasetProperties.description                    |
| Column descriptions | SchemaMetadata field descriptions                |
| Model tags          | GlobalTags                                       |
| Model owner         | Ownership                                        |
| Audit definition    | Assertion entity (AssertionInfo)                 |
| Audit run result    | AssertionRunEvent (pass / fail)                  |
| Warehouse view      | Sibling dataset on the target warehouse platform |
| Database / Schema   | Container hierarchy                              |
