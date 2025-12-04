# RDF Package

RDF ontology ingestion system for DataHub.

## Components

- **Core**: Ontology processing and DataHub client
- **Standards**: Ontology dialect handlers
- **Scripts**: CLI tools for ingestion and management

## Usage

```python
from src.rdf.core import OntologyToDataHub
from src.rdf.core.datahub_client import DataHubClient

client = DataHubClient("http://localhost:8080", "token")
converter = OntologyToDataHub(client)
results = converter.process_ontology_graph(graph)
```

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
