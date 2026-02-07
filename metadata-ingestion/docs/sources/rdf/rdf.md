![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

This connector ingests RDF (Resource Description Framework) ontologies into DataHub, with a focus on **business glossaries**. It extracts glossary terms, term hierarchies, and relationships from RDF files using standard vocabularies like SKOS, OWL, and RDFS.

## Integration Details

The RDF ingestion source processes RDF/OWL ontologies in various formats (Turtle, RDF/XML, JSON-LD, N3, N-Triples) and converts them to DataHub entities. It supports loading RDF from files, folders, URLs, and comma-separated file lists.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                   | DataHub Concept                                                                           | Notes                                        |
| -------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------- |
| `"rdf"`                          | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                                              |
| `skos:Concept`                   | [GlossaryTerm](https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/)  | SKOS concepts become glossary terms          |
| `owl:Class`                      | [GlossaryTerm](https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/)  | OWL classes become glossary terms            |
| IRI path hierarchy               | [GlossaryNode](https://docs.datahub.com/docs/generated/metamodel/entities/glossarynode/)  | Path segments create glossary node hierarchy |
| `skos:broader` / `skos:narrower` | `isRelatedTerms` relationship                                                             | Term relationships                           |

### Supported Capabilities

| Capability              | Status | Notes                                                         |
| ----------------------- | :----: | ------------------------------------------------------------- |
| Glossary Terms          |   ✅   | Enabled by default                                            |
| Glossary Nodes          |   ✅   | Auto-created from IRI path hierarchies                        |
| Term Relationships      |   ✅   | Supports `skos:broader` and `skos:narrower`                   |
| Detect Deleted Entities |   ✅   | Requires `stateful_ingestion.enabled: true`                   |
| Platform Instance       |   ✅   | Supported via `platform_instance` config                      |
| Data Domain             |   ❌   | Not applicable (domains used internally for hierarchy)        |
| Dataset Profiling       |   ❌   | Not applicable                                                |
| Extract Descriptions    |   ✅   | Enabled by default (from `skos:definition` or `rdfs:comment`) |
| Extract Lineage         |   ❌   | Not in MVP                                                    |
| Extract Ownership       |   ❌   | Not supported                                                 |
| Extract Tags            |   ❌   | Not supported                                                 |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from RDF files, you will need:

- Python 3.8 or higher
- Access to RDF files (local files, folders, or URLs)
- RDF files in supported formats: Turtle (.ttl), RDF/XML (.rdf, .xml), JSON-LD (.jsonld), N3 (.n3), or N-Triples (.nt)
- A DataHub instance to ingest into

### Install the Plugin

Run the following command to install the RDF ingestion plugin:

```bash
pip install 'acryl-datahub[rdf]'
```

### Configure the Ingestion Recipe

Use the following recipe to get started with RDF ingestion.

_For general pointers on writing and running a recipe, see our [main recipe guide](https://docs.datahub.com/docs/metadata-ingestion#recipes)._

#### Basic Recipe

```yaml
source:
  type: rdf
  config:
    source: path/to/glossary.ttl
    environment: PROD

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

#### Recipe with Stateful Ingestion (Recommended)

```yaml
source:
  type: rdf
  config:
    source: path/to/glossary.ttl
    environment: PROD
    stateful_ingestion:
      enabled: true

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

<details>
  <summary>View All Recipe Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `source` | ✅ | | RDF source: file path, folder path, URL, or comma-separated files |
  | `environment` | ❌ | `PROD` | DataHub environment (PROD, DEV, TEST, etc.) |
  | `format` | ❌ | auto-detect | RDF format: `turtle`, `xml`, `n3`, `nt`, `json-ld` |
  | `dialect` | ❌ | auto-detect | RDF dialect: `default`, `fibo`, `generic` |
  | `export_only` | ❌ | all | Export only specified entity types (e.g., `["glossary"]`) |
  | `skip_export` | ❌ | none | Skip specified entity types |
  | `recursive` | ❌ | `true` | Enable recursive folder processing |
  | `extensions` | ❌ | `[".ttl", ".rdf", ".owl", ".n3", ".nt"]` | File extensions to process when source is a folder |
  | `platform_instance` | ❌ | | Platform instance identifier |
  | `stateful_ingestion` | ❌ | `null` | Stateful ingestion configuration (see [Stateful Ingestion](#stateful-ingestion)) |
</details>

## RDF Format Support

The source supports multiple RDF serialization formats:

- **Turtle (.ttl)** - Recommended format, human-readable
- **RDF/XML (.rdf, .xml)** - XML-based RDF format
- **JSON-LD (.jsonld)** - JSON-based RDF format
- **N3 (.n3)** - Notation3 format
- **N-Triples (.nt)** - Line-based RDF format

The format is auto-detected from the file extension, or you can specify it explicitly using the `format` parameter.

## Source Types

The `source` parameter accepts multiple input types:

### Single File

```yaml
source: path/to/glossary.ttl
```

### Folder

```yaml
source: path/to/rdf_files/
```

Processes all RDF files in the folder (and subfolders if `recursive: true`).

### URL

```yaml
source: https://example.com/ontology.ttl
```

### Comma-Separated Files

```yaml
source: file1.ttl, file2.ttl, file3.ttl
```

### Glob Pattern

```yaml
source: path/to/**/*.ttl
```

## RDF Dialects

The source supports different RDF dialects for specialized processing:

- **`default`** - Standard RDF processing (BCBS239-style)
- **`fibo`** - FIBO (Financial Industry Business Ontology) dialect
- **`generic`** - Generic RDF processing

The dialect is auto-detected based on the RDF content, or you can force a specific dialect:

```yaml
source:
  type: rdf
  config:
    source: fibo_ontology.ttl
    dialect: fibo
```

## Selective Entity Export

You can control which entity types are ingested:

### Export Only Specific Types

```yaml
source:
  type: rdf
  config:
    source: glossary.ttl
    export_only:
      - glossary
```

### Skip Specific Types

```yaml
source:
  type: rdf
  config:
    source: glossary.ttl
    skip_export:
      - relationship
```

Available entity types:

- `glossary` or `glossary_terms` - Glossary terms
- `relationship` or `relationships` - Term relationships

## Stateful Ingestion

Stateful ingestion enables automatic removal of stale entities (entities that no longer exist in your RDF source). This is especially useful when RDF files change over time.

### Enable Stateful Ingestion

```yaml
source:
  type: rdf
  config:
    source: glossary.ttl
    environment: PROD
    stateful_ingestion:
      enabled: true
```

### Stateful Ingestion Options

| Field              | Required | Default | Description                         |
| ------------------ | :------: | :-----: | ----------------------------------- |
| `enabled`          |    ❌    | `false` | Enable stateful ingestion           |
| `ignore_old_state` |    ❌    | `false` | Ignore previous checkpoint state    |
| `ignore_new_state` |    ❌    | `false` | Don't save current checkpoint state |

For more details, see the [Stateful Ingestion documentation](../../../../metadata-ingestion/docs/dev_guides/stateful.md).

## Example RDF Files

### Basic Glossary Term

```turtle
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<https://example.com/finance/credit-risk>
    a skos:Concept ;
    skos:prefLabel "Credit Risk" ;
    skos:definition "The risk of loss due to a borrower's failure to repay a loan" .
```

This creates:

- Glossary Node: `finance` (from IRI path)
- Glossary Term: `Credit Risk` (under `finance` node)

### Glossary Term with Relationships

```turtle
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

<https://example.com/finance/credit-risk>
    a skos:Concept ;
    skos:prefLabel "Credit Risk" ;
    skos:broader <https://example.com/finance/risk> .

<https://example.com/finance/risk>
    a skos:Concept ;
    skos:prefLabel "Risk" ;
    skos:definition "General category of financial risk" .
```

This creates:

- Glossary Term: `Risk`
- Glossary Term: `Credit Risk` with `isRelatedTerms` relationship to `Risk`

### OWL Class as Glossary Term

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<https://example.com/FinancialInstrument>
    a owl:Class ;
    rdfs:label "Financial Instrument" ;
    rdfs:comment "A tradable asset of any kind" .
```

This creates a Glossary Term from the OWL class.

## IRI-to-URN Mapping

RDF IRIs are converted to DataHub URNs following this pattern:

```
http://example.com/finance/credit-risk
→ urn:li:glossaryTerm:example.com/finance/credit-risk
```

The IRI path segments create the glossary node hierarchy:

- `example.com` → Glossary Node
- `finance` → Glossary Node (child of `example.com`)
- `credit-risk` → Glossary Term (under `finance` node)

## Glossary Node Hierarchy

IRI path hierarchies are automatically converted to glossary node hierarchies:

```
https://bank.com/trading/loans/Customer_Name
→ Glossary Node: bank.com
  └─ Glossary Node: trading
     └─ Glossary Node: loans
        └─ Glossary Term: Customer_Name
```

**Note**: Domains are used internally as a data structure to organize glossary terms. They are **not** ingested as DataHub domain entities (which are for datasets/products).

## Supported RDF Vocabularies

The source recognizes entities from these standard vocabularies:

- **SKOS** (Simple Knowledge Organization System)

  - `skos:Concept` → Glossary Term
  - `skos:prefLabel` → Term name
  - `skos:definition` → Term definition
  - `skos:broader` → Term relationship
  - `skos:narrower` → Term relationship
  - `skos:notation` → Custom property
  - `skos:scopeNote` → Custom property
  - `skos:altLabel` → Custom property

- **OWL** (Web Ontology Language)

  - `owl:Class` → Glossary Term
  - `owl:NamedIndividual` → Glossary Term

- **RDFS** (RDF Schema)
  - `rdfs:label` → Term name (fallback if no `skos:prefLabel`)
  - `rdfs:comment` → Term definition (fallback if no `skos:definition`)

## Examples

### Basic Glossary Ingestion

```yaml
source:
  type: rdf
  config:
    source: my_glossary.ttl
    environment: PROD

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

### Folder with Multiple Files

```yaml
source:
  type: rdf
  config:
    source: /path/to/rdf_files/
    recursive: true
    extensions: [".ttl", ".rdf", ".owl"]
    environment: PROD

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

### With Stateful Ingestion

```yaml
source:
  type: rdf
  config:
    source: glossary.ttl
    environment: PROD
    stateful_ingestion:
      enabled: true

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

### Selective Export

```yaml
source:
  type: rdf
  config:
    source: ontology.ttl
    environment: PROD
    export_only:
      - glossary

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

### FIBO Dialect

```yaml
source:
  type: rdf
  config:
    source: fibo_ontology.ttl
    dialect: fibo
    environment: PROD

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

## Limitations

- **MVP Scope**: The current implementation focuses on glossary terms and relationships. Dataset, lineage, and structured property extraction are not included in the MVP.
- **Relationship Types**: Only `skos:broader` and `skos:narrower` relationships are extracted. `skos:related` and `skos:exactMatch` are not supported.
- **Term Requirements**: Terms must have a label (`skos:prefLabel` or `rdfs:label`) of at least 3 characters to be extracted.
- **Large Files**: Very large RDF files are loaded entirely into memory. Consider splitting large ontologies into multiple files.

## Troubleshooting

### No glossary terms extracted

If glossary terms aren't being extracted:

1. **Check term types** - Ensure entities are typed as `skos:Concept`, `owl:Class`, or `owl:NamedIndividual`
2. **Verify labels** - Terms must have `skos:prefLabel` or `rdfs:label` with at least 3 characters
3. **Check file format** - Verify the RDF file is valid and in a supported format
4. **Review logs** - Enable debug logging: `datahub ingest -c recipe.yml --debug`

### Terms not appearing in correct hierarchy

If terms aren't in the expected glossary node hierarchy:

1. **Check IRI structure** - Glossary nodes are created from IRI path segments
2. **Verify IRI format** - IRIs should be absolute (e.g., `https://example.com/path/term`)
3. **Review domain builder** - Check that IRI paths are being parsed correctly

### Relationships not extracted

If relationships aren't being extracted:

1. **Check relationship types** - Only `skos:broader` and `skos:narrower` are supported
2. **Verify term existence** - Both source and target terms must exist in the RDF
3. **Check export options** - Ensure `relationship` is not in `skip_export`

### File not found errors

If you see file not found errors:

1. **Check file path** - Verify the path is correct and accessible
2. **Use absolute paths** - Prefer absolute paths over relative paths
3. **Check permissions** - Ensure the ingestion process has read access to the files
4. **URL access** - For URLs, verify network connectivity and URL accessibility

### Stateful ingestion not working

If stale entities aren't being removed:

1. **Enable stateful ingestion** - Set `stateful_ingestion.enabled: true`
2. **Check server capability** - Verify your DataHub server is stateful ingestion capable (version >= 0.8.20)
3. **Review state provider** - Check that state provider is configured correctly
4. **Check logs** - Review ingestion logs for state-related errors

## Additional Resources

- **[RDF Specification](../../../../metadata-ingestion/src/datahub/ingestion/source/rdf/docs/rdf-specification.md)** - Complete technical specification
- **[Entity Plugin Contract](../../../../metadata-ingestion/src/datahub/ingestion/source/rdf/docs/ENTITY_PLUGIN_CONTRACT.md)** - Guide for adding new entity types
- **[Stateful Ingestion Guide](../../../../metadata-ingestion/docs/dev_guides/stateful.md)** - Stateful ingestion documentation
