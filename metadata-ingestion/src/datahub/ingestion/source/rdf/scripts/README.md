# CLI Tool

Unified command-line interface for RDF operations.

## Commands

| Command  | Description                          |
| -------- | ------------------------------------ |
| `ingest` | Load TTL files into DataHub glossary |
| `list`   | Display existing glossary items      |
| `delete` | Remove glossary terms/domains        |

## Usage

```bash
# Ingest ontology
python -m src.rdf.scripts.datahub_rdf ingest \
  --server http://localhost:8080 --token "" \
  ontology.ttl

# List items
python -m src.rdf.scripts.datahub_rdf list \
  --server http://localhost:8080 --token ""

# Delete domain
python -m src.rdf.scripts.datahub_rdf delete \
  --server http://localhost:8080 --token "" \
  --domain "urn:li:glossaryNode:test"
```

## Options

- `--server`: DataHub server URL
- `--token`: API token
- `--dry-run`: Simulate without changes
- `--verbose`: Detailed logging
