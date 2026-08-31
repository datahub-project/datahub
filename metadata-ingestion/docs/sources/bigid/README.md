## Overview

[BigID](https://bigid.com/) is a data intelligence platform for data discovery, classification, and privacy. It scans connected data sources, classifies columns and documents against a catalog of classifiers and business glossary terms, and correlates personal data to identities (IDSoR). Learn more in the [official BigID documentation](https://docs.bigid.com/).

The DataHub integration for BigID is an **enrichment** connector: it syncs BigID's classification findings, business glossary terms, and tags onto data assets that already exist in DataHub. It maps BigID business glossary items to GlossaryTerms, classification findings to column-level GlossaryTerms with attribution, BigID tags to DataHub Tags, and BigID risk scores to a structured property. It can optionally create Dataset and schema entities for sources not yet present in DataHub, and supports platform instance mapping, domains, ownership on terms, and stateful ingestion for stale entity removal.

## Concept Mapping

| Source Concept              | DataHub Concept                                                         | Notes                                                                                  |
| --------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Data source (connection)    | [Data Platform](../../metamodel/entities/dataPlatform.md)               | Mapped to a DataHub platform (e.g. `snowflake`, `mysql`) for URN build.                |
| Catalog object              | [Dataset](../../metamodel/entities/dataset.md)                          | Enriched in place; created only when `create_datasets` is enabled.                     |
| Business glossary item      | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID` root [GlossaryNode](../../metamodel/entities/glossaryNode.md). |
| Classification finding      | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md) on SchemaField | Emitted with `MetadataAttribution` recording confidence and counts.                    |
| Unlinked classifier         | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID > Classifier` GlossaryNode when not linked to a term.           |
| IDSoR correlation attribute | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID > IDSoR` GlossaryNode when not linked to a term.                |
| Tag (OBJECT-scoped)         | [Tag](../../metamodel/entities/tag.md)                                  | Applied to datasets; `hidden` and non-`OBJECT` tags are skipped.                       |
| Risk score                  | Structured Property (`bigid.riskScore`)                                 | Numeric 0–100 value patched onto the dataset.                                          |
| Domain / sub-domain         | [Domain](../../metamodel/entities/domain.md)                            | Optional; controlled by `domain_mode`.                                                 |
| Column profile              | Dataset Profile                                                         | Column-level statistics from BigID `columnProfile` data.                               |
