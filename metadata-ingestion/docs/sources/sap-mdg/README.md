## Overview

[SAP Master Data Governance (MDG)](https://www.sap.com/products/technology-platform/master-data-governance.html) is SAP's platform for centrally creating, governing, and distributing master data such as business partners, materials, and finance master data. MDG exposes its data models through OData services published on the SAP Gateway.

DataHub ingests each configured MDG OData service by reading its `$metadata` (EDMX/CSDL) document. Every entity set is emitted as a dataset with its columns, key fields, and SAP field labels, grouped under a container that represents the OData service. Relationships between entity sets are captured as foreign keys derived from OData navigation properties, and stale datasets are removed on subsequent runs via stateful ingestion.

## Concept Mapping

| Source Concept                   | DataHub Concept                                             | Notes                                        |
| -------------------------------- | ----------------------------------------------------------- | -------------------------------------------- |
| OData service                    | [Container](docs/generated/metamodel/entities/container.md) | Subtype `OData Service`                      |
| Entity set                       | [Dataset](docs/generated/metamodel/entities/dataset.md)     | Subtype `Entity Set`                         |
| Entity type property             | Schema field                                                | Key properties are marked as part of the key |
| `sap:label` / `sap:quickinfo`    | Field / dataset description                                 |                                              |
| Navigation property + constraint | Foreign key                                                 | Requires a referential constraint            |
