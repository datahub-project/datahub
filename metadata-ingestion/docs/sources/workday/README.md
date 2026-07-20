## Overview

[Workday](https://www.workday.com/) is a cloud platform for HR, finance, and analytics. [Workday Prism Analytics](https://www.workday.com/en-us/products/prism-analytics/overview.html) is its data-lake and self-service analytics layer, where external and Workday-sourced data land in tables, are shaped by datasets (pipelines), and draw from data sources (including RaaS custom reports and Workday business objects).

The DataHub integration for Workday ingests Prism Analytics tables as datasets with schemas, Prism datasets (pipeline definitions) and data sources as datasets, and Workday-sourced data sources as report entities. Lineage is emitted from tables to the datasets and data sources they derive from, and from external data sources to the upstream warehouse dataset they were loaded from when a `data_source_platform_mapping` entry is provided.

## Concept Mapping

| Workday Concept                     | DataHub Concept                   | Notes                                                                                |
| ----------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------ |
| Tenant                              | Container (`Tenant` subtype)      | Top-level grouping for all ingested Workday objects.                                 |
| Prism table                         | Dataset (`Prism Table` subtype)   | Schema fields come from the table's Prism schema; primary-key fields are tagged.     |
| Prism dataset (pipeline)            | Dataset (`Prism Dataset` subtype) | Optional via `extract_datasets`; lineage flows from its sources to its output table. |
| Prism data source (External)        | Dataset (`Data Source` subtype)   | External inputs; map to a warehouse via `data_source_platform_mapping` for lineage.  |
| Prism data source (Workday-sourced) | Dataset (`Report` subtype)        | RaaS custom reports and Workday business-object sources, via `extract_reports`.      |
| Field                               | Schema field                      | Prism semantic types map to DataHub types; unknown types fall back to string.        |
| Owner (createdBy)                   | CorpUser ownership                | Emitted when owner fields are exposed and `ingest_owner` is enabled.                 |
