## Overview

[dlt (data load tool)](https://dlthub.com) is an open-source Python ELT library for building data pipelines that load data from REST APIs, databases, and other sources into destinations like Postgres, BigQuery, Snowflake, and DuckDB.

The DataHub integration for dlt reads pipeline metadata from dlt's local state directory (`~/.dlt/pipelines/`) and emits DataFlow, DataJob, and lineage entities to DataHub. The connector also supports per-run history (DataProcessInstance) when the dlt package is installed and destination credentials are available, plus stateful deletion detection.

## Concept Mapping

| dlt                              | DataHub                                                                                                |
| -------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `Pipeline` (`pipeline_name`)     | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)                       |
| `Resource` / destination `Table` | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                         |
| Destination table                | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/) (DataJob output)        |
| User-configured upstream         | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/) (DataJob input)         |
| `_dlt_loads` row                 | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) |

Destination tables are mapped to Dataset URNs that match the destination platform's own DataHub connector (Postgres, BigQuery, etc.), enabling lineage stitching when both connectors run.
