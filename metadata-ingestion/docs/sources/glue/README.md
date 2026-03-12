## Overview

Glue is a data platform used to store and query analytical or operational data. Learn more in the [official Glue documentation](https://aws.amazon.com/glue/).

The DataHub integration for Glue covers core metadata entities such as datasets/tables/views, schema fields, and containers. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

:::tip
If you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See here for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.
:::

## Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |
