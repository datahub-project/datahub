## Overview

Vertica is a data platform used to store and query analytical or operational data. Learn more in the [official Vertica documentation](https://www.opentext.com/products/vertica).

The DataHub integration for Vertica covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, data profiling, and stateful deletion detection.

## Concept Mapping

| Source Concept | DataHub Concept                                           | Notes |
| -------------- | --------------------------------------------------------- | ----- |
| `Vertica`      | [Data Platform](../../metamodel/entities/dataPlatform.md) |       |
| Table          | [Dataset](../../metamodel/entities/dataset.md)            |       |
| View           | [Dataset](../../metamodel/entities/dataset.md)            |       |
| Projections    | [Dataset](../../metamodel/entities/dataset.md)            |       |
