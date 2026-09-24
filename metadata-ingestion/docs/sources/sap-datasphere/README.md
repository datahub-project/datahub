## Overview

The SAP Datasphere connector extracts metadata from [SAP Datasphere](https://www.sap.com/products/technology-platform/datasphere.html) — SAP's cloud-native data warehouse platform (the successor to SAP Data Warehouse Cloud). It reads the SAP-supported REST/OData v4 Catalog and Consumption APIs (authenticated via XSUAA OAuth), so no JDBC driver or SQL dialect is required.

DataHub ingests Spaces as containers and Views, Analytic Models, and Local Tables as datasets, with column schema read from the per-asset OData EDMX (`$metadata`) surface. Optional features include table- and column-level lineage (`include_lineage`), SAP CDS semantic annotations emitted as tags, federated Remote Tables routed to their native storage platform (so they merge with native warehouse connectors), and stateful stale-entity removal.

## Concept Mapping

| SAP Datasphere concept                       | DataHub entity                      | Notes                                                                         |
| -------------------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------- |
| Space                                        | Container                           | 2-tier Space → object model (no folder layer)                                 |
| View                                         | Dataset (subtype `View`)            | Emits `viewProperties`; SQL-editor views use `viewLanguage="SQL"`, else `CSN` |
| Analytic Model                               | Dataset (subtype `Analytic Model`)  | Assets with `supportsAnalyticalQueries: true`; star-schema lineage            |
| Local Table (base table)                     | Dataset (subtype `Local Table`)     | Discovered when `include_local_tables: true`; schema from per-table CSN       |
| Remote Table (federated)                     | Dataset on its **storage** platform | Routed via `connection_to_platform_map` / `platform_type_defaults`            |
| Column                                       | Schema field                        | Types from OData EDMX                                                         |
| CDS semantic annotation (Dimension, Measure) | Tag                                 | Emitted when `emit_sap_semantics_as_tags: true`                               |
