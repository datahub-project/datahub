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
- `void:Dataset` → `Dataset`
- `prov:wasDerivedFrom` → lineage relationships

📖 **See detailed mapping specifications:**

- [RDF Glossary Mapping](../../docs/RDF_GLOSSARY_MAPPING.md) - Glossary terms and relationships
- [RDF Dataset Mapping](../../docs/RDF_DATASET_MAPPING.md) - Datasets, lineage, and platforms

## CLI

```bash
python -m src.rdf.scripts.datahub_rdf ingest \
  --server http://localhost:8080 --token "" \
  ontology.ttl
```
