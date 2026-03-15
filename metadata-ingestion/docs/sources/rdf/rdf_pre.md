### Overview

The `rdf` module ingests RDF/OWL ontologies into DataHub as glossary terms, glossary nodes, and term relationships. It supports multiple RDF formats and dialects.

### Prerequisites

In order to ingest metadata from RDF files, you will need:

- Python 3.8 or higher
- Access to RDF files (local files, folders, or URLs)
- RDF files in supported formats: Turtle (.ttl), RDF/XML (.rdf, .xml), JSON-LD (.jsonld), N3 (.n3), or N-Triples (.nt)
- A DataHub instance to ingest into

#### RDF Format Support

The source supports multiple RDF serialization formats:

- **Turtle (.ttl)** - Recommended format, human-readable
- **RDF/XML (.rdf, .xml)** - XML-based RDF format
- **JSON-LD (.jsonld)** - JSON-based RDF format
- **N3 (.n3)** - Notation3 format
- **N-Triples (.nt)** - Line-based RDF format

The format is auto-detected from the file extension, or you can specify it explicitly using the `format` parameter.

#### Source Types

The `source` parameter accepts multiple input types:

- **Single file**: `source: path/to/glossary.ttl`
- **Folder**: `source: path/to/rdf_files/` (processes all RDF files, recursively if `recursive: true`)
- **URL**: `source: https://example.com/ontology.ttl`
- **Comma-separated files**: `source: file1.ttl, file2.ttl, file3.ttl`
- **Glob pattern**: `source: path/to/**/*.ttl`

#### RDF Dialects

The source supports different RDF dialects for specialized processing:

- **`default`** - Standard RDF processing (BCBS239-style)
- **`fibo`** - FIBO (Financial Industry Business Ontology) dialect
- **`generic`** - Generic RDF processing

The dialect is auto-detected based on the RDF content, or you can force a specific dialect using the `dialect` parameter.

#### SPARQL Filtering

You can use SPARQL CONSTRUCT queries to filter the RDF graph before ingestion. This is useful for filtering by namespace, applying complex filtering logic, or reducing the size of large RDF graphs.

```yaml
source:
  type: rdf
  config:
    source: large_ontology.ttl
    sparql_filter: |
      CONSTRUCT { ?s ?p ?o }
      WHERE {
        ?s ?p ?o .
        FILTER(STRSTARTS(STR(?s), "https://example.org/module1/"))
      }
```

Only `CONSTRUCT` queries are supported. The filter is applied before entity extraction.

#### Selective Entity Export

You can control which entity types are ingested using `export_only` or `skip_export`:

```yaml
source:
  type: rdf
  config:
    source: glossary.ttl
    export_only:
      - glossary # Only ingest glossary terms
```

Available entity types: `glossary` (or `glossary_terms`), `relationship` (or `relationships`).
