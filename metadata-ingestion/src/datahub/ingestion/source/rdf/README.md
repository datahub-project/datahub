# RDF Ingestion Source

A lightweight RDF ontology ingestion system for DataHub focused on **business glossaries**. This source enables ingestion of SKOS-based glossaries with term definitions, hierarchical organization, and relationships.

## Overview

The RDF ingestion source provides:

- **Glossary Terms**: Import SKOS concepts as DataHub glossary terms
- **Term Groups**: Automatic creation of glossary nodes from IRI path hierarchies
- **Relationships**: Support for `skos:broader` and `skos:narrower` term relationships
- **Standards-Based**: Native support for SKOS, OWL, and RDFS vocabularies
- **Modular Architecture**: Pluggable entity system with auto-discovery

## Quick Start

### Installation

```bash
pip install acryl-datahub[rdf]
```

### Basic Usage

Create a recipe file (`rdf_glossary.yml`):

```yaml
source:
  type: rdf
  config:
    source: path/to/glossary.ttl
    environment: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

Run ingestion:

```bash
# Ingest glossary
datahub ingest -c rdf_glossary.yml

# Dry run (preview without ingesting)
datahub ingest -c rdf_glossary.yml --dry-run
```

## RDF-to-DataHub Mapping

### Glossary Terms

RDF concepts are mapped to DataHub glossary terms:

- `skos:Concept` → `GlossaryTerm`
- `skos:prefLabel` OR `rdfs:label` → term name
- `skos:definition` OR `rdfs:comment` → term definition
- IRI path segments → glossary node hierarchy

### Term Groups (Domains)

IRI path hierarchies are automatically converted to glossary node hierarchies:

```
https://example.com/finance/credit-risk
→ Glossary Node: finance
  └─ Glossary Node: credit-risk
     └─ Glossary Term: (final segment)
```

**Note**: Domains are used internally as a data structure to organize glossary terms. They are **not** ingested as DataHub domain entities (which are for datasets/products).

### Relationships

- `skos:broader` → creates `isRelatedTerms` relationships in DataHub
- `skos:narrower` → creates `isRelatedTerms` relationships (inverse direction)

### IRI-to-URN Examples

```
http://example.com/finance/credit-risk
→ urn:li:glossaryTerm:finance/credit-risk

fibo:FinancialInstrument
→ urn:li:glossaryTerm:fibo:FinancialInstrument
```

## Configuration

### Source Configuration

| Parameter     | Description                          | Default                              |
| ------------- | ------------------------------------ | ------------------------------------ |
| `source`      | RDF source (file, folder, URL)       | **required**                         |
| `environment` | DataHub environment                  | `PROD`                               |
| `format`      | RDF format (turtle, xml, n3, etc.)   | auto-detect                          |
| `dialect`     | RDF dialect (default, fibo, generic) | auto-detect                          |
| `export_only` | Export only specified types          | all                                  |
| `skip_export` | Skip specified types                 | none                                 |
| `recursive`   | Recursive folder processing          | `true`                               |
| `extensions`  | File extensions to process           | `.ttl`, `.rdf`, `.owl`, `.n3`, `.nt` |

### Export Types (CLI Options)

- `glossary` or `glossary_terms` - Glossary terms only
- `relationship` or `relationships` - Term relationships only

**Note**: The `domain` option is not available in MVP. Domains are used internally as a data structure for organizing glossary terms into hierarchies.

## Example RDF File

```turtle
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<https://example.com/finance/credit-risk>
    a skos:Concept ;
    skos:prefLabel "Credit Risk" ;
    skos:definition "The risk of loss due to a borrower's failure to repay a loan" ;
    skos:broader <https://example.com/finance/risk> .

<https://example.com/finance/risk>
    a skos:Concept ;
    skos:prefLabel "Risk" ;
    skos:definition "General category of financial risk" .
```

This will create:

- Glossary Node: `finance`
- Glossary Term: `Risk` (under `finance` node)
- Glossary Term: `Credit Risk` (under `finance` node, with relationship to `Risk`)

## Architecture

RDF uses a modular, pluggable entity architecture:

1. **Entity Extractors**: Extract RDF entities from graphs
2. **Entity Converters**: Convert RDF AST to DataHub AST
3. **MCP Builders**: Generate Metadata Change Proposals (MCPs)
4. **Auto-Discovery**: Entity modules are automatically discovered and registered

### Processing Flow

1. Load RDF files into RDF graph
2. Extract entities (glossary terms, relationships)
3. Build domain hierarchy from IRI paths
4. Convert to DataHub AST
5. Generate MCPs for glossary nodes and terms
6. Emit to DataHub

## Documentation

- **[RDF Specification](docs/rdf-specification.md)** - Complete technical specification
- **[Entity Plugin Contract](docs/ENTITY_PLUGIN_CONTRACT.md)** - Guide for adding new entity types
- **[Documentation Index](docs/README.md)** - All documentation files

## Features

- ✅ **Glossary Terms**: Full SKOS concept support
- ✅ **Term Groups**: Automatic hierarchy from IRI paths
- ✅ **Relationships**: `skos:broader`/`narrower` support
- ✅ **Multiple Formats**: TTL, RDF/XML, JSON-LD, N3, N-Triples
- ✅ **Multiple Sources**: Files, folders, URLs
- ✅ **Standards-Based**: SKOS, OWL, RDFS support
- ✅ **Modular**: Pluggable entity architecture

## MVP Scope

**Current MVP includes:**

- Glossary terms
- Term groups (domains) - used as data structure for hierarchy
- Term relationships

**Not included in MVP:**

- Datasets
- Data products
- Structured properties
- Lineage processing
- Schema fields

These features are available in the `rdf-full-features` branch.

## Requirements

- Python 3.8+
- DataHub instance
- `rdflib`, `acryl-datahub`

## Getting Help

1. **Start with**: [RDF Specification](docs/rdf-specification.md) - Complete technical reference
2. **Adding entities**: [Entity Plugin Contract](docs/ENTITY_PLUGIN_CONTRACT.md) - Plugin development guide
3. **Examples**: Review example RDF files in test fixtures
4. **CLI help**: Run `datahub ingest --help` for command options
