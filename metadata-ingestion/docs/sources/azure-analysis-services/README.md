## Overview

[Azure Analysis Services](https://learn.microsoft.com/azure/analysis-services/) is a fully managed
platform-as-a-service that hosts SQL Server Analysis Services tabular models in Azure. Analysts
connect to these models from tools such as Power BI, Excel, and any client that speaks the XMLA
protocol. The same connector also works against Power BI Premium semantic models exposed through
the Power BI XMLA endpoint.

DataHub ingests each tabular model as a semantic-model container holding one dataset per table,
with columns and DAX measures as schema fields, relationships as foreign-key constraints, and a
model-level cube dataset that carries the full TMSL definition. Optional features include upstream
lineage parsed from partition M/Power Query and native SQL, and intra-model column-level lineage
derived from DAX dependencies.

## Concept Mapping

| Source Concept                 | DataHub Concept                                            | Notes                                            |
| ------------------------------ | --------------------------------------------------------- | ------------------------------------------------ |
| Server                         | [Container](../../metamodel/entities/container.md)        | Subtype `Analysis Services Server`               |
| Model (database / catalog)     | [Container](../../metamodel/entities/container.md)         | Subtype `Semantic Model`                         |
| Table                          | [Dataset](../../metamodel/entities/dataset.md)             | Subtype `Table` (plus `View` when M-backed)      |
| Calculated table               | [Dataset](../../metamodel/entities/dataset.md)             | Subtypes `Table`, `Calculated Table`             |
| Model (cube view)              | [Dataset](../../metamodel/entities/dataset.md)             | Subtypes `Cube`, `Semantic Model`; carries TMSL  |
| Column / calculated column     | Schema Field                                               | DAX stored in the field description              |
| Measure                        | Schema Field                                               | `nativeDataType=measure`; DAX in the description |
| Relationship                   | Foreign Key Constraint                                     |                                                  |
