## Overview

SAP HANA is an in-memory, column-oriented relational database that powers SAP Business Suite, SAP S/4HANA, and bespoke analytics built on the HANA Cloud / HANA on-premise platform. Learn more in the [official SAP HANA documentation](https://help.sap.com/docs/SAP_HANA_PLATFORM).

The DataHub integration for SAP HANA covers core metadata entities such as datasets (tables, regular views, and calculation views), schema fields, and containers. It also captures table- and column-level lineage from calculation views, table-level and procedure-to-procedure lineage from stored procedures, data profiling, and stateful deletion detection.

## Concept Mapping

| SAP HANA Concept                 | DataHub Entity (Subtype)                                         | Notes                                                                                                                                                               |
| -------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tenant database                  | Platform Instance                                                | Top-level scope; all URNs include the configured platform instance.                                                                                                 |
| Schema                           | Container (SCHEMA)                                               | Top-level namespace inside the tenant database.                                                                                                                     |
| Table (row / column / virtual)   | Dataset (TABLE)                                                  | Includes column-store, row-store, and virtual tables. Schema, descriptions, and tags (where available) are extracted.                                               |
| Regular View                     | Dataset (VIEW)                                                   | Standard SQL views; the view definition is captured for lineage.                                                                                                    |
| Calculation View                 | Dataset (`Calculation View`)                                     | Opt-in via `include_calculation_views: true`. Activated calc views are pulled from `_SYS_REPO.ACTIVE_OBJECT`; column-level lineage is parsed from the XML.          |
| `SqlScriptView` Calculation View | Dataset (`Calculation View`)                                     | Calc views whose body is HANA SQLScript. Table-level lineage is extracted from the procedure body; column-level lineage from SQLScript is out of scope.             |
| Stored Procedure (SQLScript)     | DataFlow (`Procedures Container`) + DataJob (`Stored Procedure`) | Opt-in via `include_stored_procedures` (on by default). One `DataFlow` per schema; each procedure becomes a `DataJob`. Table-level lineage is parsed from the body. |
| Column / field                   | SchemaField                                                      | Native type, nullability, and (for calc views) comments are extracted.                                                                                              |
| Table- and column-level lineage  | Lineage edges                                                    | Extracted from view definitions and calc-view XML; column-level lineage flows through the `SqlParsingAggregator` like Snowflake / Redshift.                         |
| Query history / usage            | DatasetUsageStatistics + Operations + query-derived lineage      | Opt-in via `include_query_usage`. Mines `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` (requires the statistics service and the `MONITORING` role).                          |
