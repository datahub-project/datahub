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
# Ingest RDF glossary files
python -m datahub ingest -c config.yaml

# See rdf-specification.md for configuration details

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
