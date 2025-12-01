# Field Solution Proposal: RDF Ontology Ingestion for DataHub

## 1. Motivation

Organizations often need to import existing glossaries and ontologies into DataHub. In many cases, those ontologies are managed through RDF using standards like SKOS, OWL, and PROV-O. Currently, there's no unified solution for RDF ontology ingestion into DataHub, requiring extensive manual configuration and custom development. An official RDF ingestion connector would be a valuable tool to integrate with these systems, particularly relevant in sectors that could benefit from DataHub offering pre-existing libraries for regulatory compliance and data governance.

## 2. Requirements

### Core Requirements (Phase 1: Glossary Management)

- [ ] **RDF Glossary Ingestion**: Support TTL, RDF/XML, JSON-LD, and N-Triples formats for glossary processing up to 100K triples
- [ ] **Glossary Term Detection**: Automatically detect and process `skos:Concept`, `owl:Class`, `owl:NamedIndividual`, and custom class instances
- [ ] **Relationship Mapping**: Map SKOS relationships (`skos:broader`, `skos:related`, `skos:exactMatch`) to DataHub glossary relationships
- [ ] **Domain Management**: Automatically create DataHub domains from IRI hierarchy and assign glossary terms
- [ ] **Basic CLI/API**: Provide CLI commands (`ingest`, `list`, `delete`) and Python API for glossary management
- [ ] **Strategy Pattern**: Clean separation between dry run and live execution modes
- [ ] **IRI-to-URN Conversion**: Transform RDF IRIs to DataHub URNs with hierarchical structure
- [ ] **Validation & Error Handling**: Comprehensive validation with graceful error recovery
- [ ] **Multi-Source Support**: Handle file-based, directory-based, and server-based sources
- [ ] **Structured Properties**: Auto-detect `rdf:Property` declarations and map to DataHub structured properties
- [ ] **Glossary Node Support**: Process `skos:ConceptScheme` and `skos:Collection` as DataHub glossary nodes
- [ ] **Custom Properties**: Handle additional RDF properties and custom metadata
- [ ] **Language Support**: Preserve language tags for multilingual glossaries
- [ ] **External References**: Map `owl:sameAs` and `skos:exactMatch` to DataHub external references

### Advanced Requirements (Phase 2: Datasets and Lineage)

- [ ] **Dataset Processing**: Detect and process `void:Dataset`, `dcterms:Dataset`, `schema:Dataset` with platform integration
- [ ] **Comprehensive Lineage**: Full PROV-O support with `prov:Activity` extraction, relationship mapping, and field-level lineage
- [ ] **Structured Properties**: Auto-detect `rdf:Property` declarations and map to appropriate DataHub entity types
- [ ] **Platform Integration**: Support `dcat:accessService`, SPARQL endpoints, and database connections
- [ ] **Export Target Management**: Unified export targets (`entities`, `links`, `lineage`, `all`) with legacy compatibility
- [ ] **Schema Field Processing**: Extract and map dataset schema fields with data types and constraints
- [ ] **Temporal Lineage**: Handle `prov:startedAtTime`, `prov:endedAtTime` and user attribution
- [ ] **Field-Level Lineage**: Column-to-column lineage mapping for detailed data flow analysis
- [ ] **Dialect Support**: FIBO, BCBS 239, and Generic RDF dialect handling
- [ ] **Dependency Injection**: Modular architecture with pluggable components
- [ ] **Enterprise Examples**: BCBS 239 regulatory compliance example with unauthorized data flow demonstration

### Experimental Features (Advanced)

- [ ] **Dynamic Routing**: Query-based processing that automatically detects entity types using SPARQL
- [ ] **Custom Query Support**: Advanced SPARQL query customization for specialized use cases

## 3. Proposed Solution

RDF uses a three-phase transpiler architecture that provides clean separation of concerns: RDF parsing → internal AST → DataHub entities. The system employs dynamic routing based on SPARQL queries to automatically detect entity types and route processing accordingly, eliminating the need for hardcoded logic. This approach leverages semantic web standards (SKOS, PROV-O, DCAT) for interoperability while providing enterprise-grade features like automatic domain management and comprehensive lineage processing.

### Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RDF Graph     │───▶│   RDF AST       │───▶│  DataHub AST    │───▶│  DataHub SDK    │
│   (Input)       │    │   (Internal)    │    │   (Internal)    │    │   (Output)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │                        │
        │                        │                        │                        │
        ▼                        ▼                        ▼                        ▼
   RDFToASTConverter    ASTToDataHubConverter    OutputStrategy         DataHub API
```

## 4. Success Criteria

- **Customer Adoption**: 3+ enterprise customers using glossary features in production (Phase 1), 5+ using full solution (Phase 2)
- **Time to Value**: Reduce RDF glossary ingestion setup from weeks to hours
- **Customer Satisfaction**: 4.0+/5 rating (Phase 1), 4.5+/5 rating (Phase 2)
- **Revenue Impact**: $200K+ in field solution revenue (Phase 1), $500K+ total (Phase 2)
- **Technical Performance**: Process 100K triples in under 2 minutes (Phase 1), 1M triples in under 5 minutes (Phase 2)

## 5. Implementation Plan

### Phase 1: Glossary Management (MVP)

- Core RDF glossary ingestion with SKOS support
- Automatic glossary term detection and processing
- Glossary node support (`skos:ConceptScheme`, `skos:Collection`)
- Domain management and assignment
- IRI-to-URN conversion with hierarchical structure
- Strategy pattern for dry run and live execution
- Basic CLI and Python API
- Multi-source support (files, directories, servers)
- Structured properties auto-detection and mapping
- Custom properties and metadata handling
- Language tag preservation for multilingual support
- External reference mapping (`owl:sameAs`, `skos:exactMatch`)
- Comprehensive validation and error handling

### Phase 2: Datasets and Lineage (Advanced)

- Comprehensive dataset processing with platform integration
- Full PROV-O lineage processing with field-level tracking
- Structured properties support with automatic entity type mapping
- Export target management with unified and legacy support
- Schema field processing with data types and constraints
- Temporal lineage with user attribution
- Dialect support (FIBO, BCBS 239, Generic)
- Dependency injection framework for modular architecture
- Advanced CLI and enterprise examples
- BCBS 239 regulatory compliance demonstration

### Experimental Phase: Advanced Query Features

- Dynamic routing based on SPARQL queries
- Custom query support for specialized use cases
- Advanced query optimization and performance tuning
