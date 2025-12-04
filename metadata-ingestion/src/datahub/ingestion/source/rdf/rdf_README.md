# RDF Package

RDF ontology ingestion system for DataHub.

## Components

- **Core**: Ontology processing and DataHub client
- **Standards**: Ontology dialect handlers
- **Scripts**: CLI tools for ingestion and management

## Usage

RDF is used as a DataHub ingestion source plugin. See the main [README.md](README.md) for usage examples.

## RDF Mapping

RDF concepts are mapped to DataHub entities:

- `skos:Concept` → `GlossaryTerm`
- `skos:broader` / `skos:narrower` → Glossary term relationships

📖 **See detailed mapping specifications:**

- [RDF Specification](./docs/rdf-specification.md) - Complete RDF ingestion specification
- [Entity Plugin Contract](./docs/ENTITY_PLUGIN_CONTRACT.md) - Plugin architecture

## CLI

```bash
python -m datahub ingest -c config.yaml
```

See [RDF Source Configuration](./docs/rdf-specification.md#configuration) for details.
