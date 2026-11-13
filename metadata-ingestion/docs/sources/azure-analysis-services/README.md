## Overview

[Azure Analysis Services](https://learn.microsoft.com/azure/analysis-services/) is a fully managed
platform-as-a-service that hosts SQL Server Analysis Services tabular models in Azure. Analysts
connect to these models from tools such as Power BI, Excel, and any client that speaks the XMLA
protocol. The same connector also works against Power BI Premium semantic models exposed through
the Power BI XMLA endpoint.

DataHub ingests each tabular model as a semantic model whose member datasets are the model's
tables. Columns that are not hidden are dimensions of the model, and each measure that is not
hidden is a metric. Set `ingest_hidden_metrics.enabled` to include hidden measures as metrics.
Relationships are foreign-key
constraints and semantic-model joins, and the TMSL definition is the semantic model's native
definition. Optional features include upstream lineage parsed from partition M/Power Query and
native SQL, and intra-model column-level lineage derived from DAX dependencies.

## Concept Mapping

| Source Concept             | DataHub Concept                                             | Notes                                                                        |
| -------------------------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Server                     | [Container](../../metamodel/entities/container.md)          | Subtype `Analysis Services Server`                                           |
| Model (database / catalog) | [Container](../../metamodel/entities/container.md)          | Subtype `Semantic Model`                                                     |
| Model (cube)               | [Semantic Model](../../metamodel/entities/semanticModel.md) | Name is the catalog; TMSL is the native definition                           |
| Table                      | [Dataset](../../metamodel/entities/dataset.md)              | Subtype `Semantic Model Dataset` (plus `View` when M-backed)                 |
| Calculated table           | [Dataset](../../metamodel/entities/dataset.md)              | Subtypes `Semantic Model Dataset`, `Calculated Table`                        |
| Column / calculated column | Schema Field                                                | Columns that are not hidden are also semantic dimensions                     |
| Measure                    | Schema Field, [Metric](../../metamodel/entities/metric.md)  | Hidden measures are metrics only when `ingest_hidden_metrics.enabled` is set |
| Relationship               | Foreign Key Constraint                                      | Also a join on the semantic model                                            |
