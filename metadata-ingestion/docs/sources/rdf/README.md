## Overview

This connector ingests RDF (Resource Description Framework) ontologies into DataHub, with a focus on **business glossaries**. It extracts glossary terms, term hierarchies, and relationships from RDF files using standard vocabularies like SKOS, OWL, and RDFS.

The RDF ingestion source processes RDF/OWL ontologies in various formats (Turtle, RDF/XML, JSON-LD, N3, N-Triples) and converts them to DataHub entities. It supports loading RDF from files, folders, URLs, and comma-separated file lists.

## Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                   | DataHub Concept                                                                           | Notes                                        |
| -------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------- |
| `"rdf"`                          | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                                              |
| `skos:Concept`                   | [GlossaryTerm](https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/)  | SKOS concepts become glossary terms          |
| `owl:Class`                      | [GlossaryTerm](https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/)  | OWL classes become glossary terms            |
| IRI path hierarchy               | [GlossaryNode](https://docs.datahub.com/docs/generated/metamodel/entities/glossarynode/)  | Path segments create glossary node hierarchy |
| `skos:broader` / `skos:narrower` | `isRelatedTerms` relationship                                                             | Term relationships                           |
