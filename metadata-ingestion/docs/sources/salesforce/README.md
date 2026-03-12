## Overview

Salesforce is a DataHub utility or metadata-focused integration. Learn more in the [official Salesforce documentation](https://www.salesforce.com/).

The DataHub integration for Salesforce covers metadata entities and operational objects relevant to this connector. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept  | DataHub Concept                                           | Notes                     |
| --------------- | --------------------------------------------------------- | ------------------------- |
| `Salesforce`    | [Data Platform](../../metamodel/entities/dataPlatform.md) |                           |
| Standard Object | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Standard Object" |
| Custom Object   | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Custom Object"   |
