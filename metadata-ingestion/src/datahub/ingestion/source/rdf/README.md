# RDF

A lightweight RDF ontology ingestion system for DataHub with **dynamic routing** based on SPARQL queries and **comprehensive lineage processing** via PROV-O.

## Architecture

RDF uses a **query-based approach** with **dynamic routing** that eliminates the need for separate processing methods for each entity type. Instead, it:

1. **Executes SPARQL queries** to extract entities with their types
2. **Routes dynamically** based on the `entity_type` field in results
3. **Processes generically** using appropriate handlers based on the data itself
4. **Extracts lineage** using PROV-O (Provenance Ontology) for complete data flow tracking

This makes the system more flexible, maintainable, and RDF-native with comprehensive lineage support.

## Quick Start

### Option 1: DataHub Ingestion Framework (Recommended)

```bash
# Install
pip install -e .

# Ingest using a recipe file
datahub ingest -c examples/recipe_basic.yml
```

### Option 2: CLI Tool

```bash
# Install
pip install -r requirements.txt

# Ingest ontology with dynamic routing
python -m src.rdf.scripts.datahub_rdf ingest \
  --source examples/bcbs239/ \
  --export entities \
  --server http://localhost:8080 \
  --token your-token

# List glossary items
python -m src.rdf.scripts.datahub_rdf list \
  --server http://localhost:8080 \
  --token your-token
```

## RDF-to-DataHub Mapping

RDF maps RDF concepts to DataHub entities through specific property mappings and IRI transformations.

### Quick Reference

**Glossary Mapping:**

- `skos:Concept` → `GlossaryTerm`
- `skos:ConceptScheme` → `GlossaryNode`
- `skos:prefLabel` → `name`
- `skos:definition` → `description`

**Dataset Mapping:**

- `void:Dataset` → `Dataset`
- `dcterms:title` → `name`
- `void:sparqlEndpoint` → `connection`

**Domain Mapping:**

- IRI hierarchy → Domain hierarchy (parent segments only)
- `https://example.com/finance/accounts` → `urn:li:domain:example_com`, `urn:li:domain:finance` (dataset `accounts` assigned to `finance` domain)
- Automatic domain creation and dataset assignment
- Follows same hierarchy logic as glossary terms

**Lineage Mapping:**

- `prov:wasDerivedFrom` → upstream lineage
- `prov:wasGeneratedBy` → downstream lineage

**IRI-to-URN Examples:**

```
http://example.com/finance/credit-risk
→ urn:li:glossaryTerm:(finance,credit-risk)

fibo:FinancialInstrument
→ fibo:FinancialInstrument (preserved)
```

📖 **For detailed mapping specifications, see:**

- [RDF Glossary Mapping](docs/RDF_GLOSSARY_MAPPING.md) - Glossary terms and relationships
- [RDF Dataset Mapping](docs/RDF_DATASET_MAPPING.md) - Datasets, lineage, and platforms

## Features

- **Dynamic Routing**: Routes processing based on SPARQL results, not hardcoded logic
- **Query-Based**: Uses SPARQL queries for flexible, RDF-native data extraction
- **Unified Processing**: Single pipeline for all entity types (datasets, glossary terms, properties)
- **Comprehensive Lineage**: Complete PROV-O lineage processing with activities and relationships
- **Field-Level Tracking**: Column-to-column lineage mapping for detailed data flow analysis
- **Strategy Pattern**: Clean separation between dry run and live execution
- **Universal**: Works with any TTL file or SPARQL endpoint
- **Smart**: Auto-detects ontology structure and entity types
- **Flexible**: Handles various IRI formats and RDF vocabularies
- **Clean**: Generates proper DataHub URNs
- **Fast**: Batch processing for large ontologies
- **Domain Management**: Automatic domain creation and dataset assignment based on IRI hierarchy

## Commands

| Command  | Description                                                  |
| -------- | ------------------------------------------------------------ |
| `ingest` | Load RDF files/directories into DataHub with dynamic routing |
| `list`   | Show existing glossary items                                 |
| `delete` | Remove glossary terms/domains                                |

### Export Targets

The `ingest` command supports these export targets:

- `entities` - Datasets, glossary terms, and structured properties (unified)
- `links` - Relationships, dataset-glossary links, dataset-property links (unified)
- `lineage` - Data lineage and provenance
- `all` - All export targets

### Legacy Targets (for backward compatibility)

- `glossary` - Glossary terms only
- `datasets` - Datasets only
- `properties` - Structured properties only
- `relationships` - SKOS relationships only
- `dataset_glossary_links` - Dataset-glossary links only
- `dataset_property_links` - Dataset-property links only

## Examples

```bash
# Dry run with dynamic routing
python -m src.rdf.scripts.datahub_rdf ingest \
  --source examples/bcbs239/ \
  --export entities \
  --server http://localhost:8080 --token "" --dry-run

# Live ingestion with unified export targets
python -m src.rdf.scripts.datahub_rdf ingest \
  --source examples/bcbs239/ \
  --export entities links lineage \
  --server http://localhost:8080 --token ""

# Process lineage with pretty print output
python -m rdf --folder examples/bcbs239 --dry-run

# Legacy single-target export (still supported)
python -m src.rdf.scripts.datahub_rdf ingest \
  --source examples/working_example_glossary.ttl \
  --export glossary \
  --server http://localhost:8080 --token ""

# Delete domain
python -m src.rdf.scripts.datahub_rdf delete \
  --server http://localhost:8080 --token "" \
  --domain "urn:li:glossaryNode:test"
```

## Lineage Processing

RDF provides comprehensive lineage processing through PROV-O (Provenance Ontology):

### Lineage Activities

Process data jobs and ETL activities:

```turtle
ex:LoanAggregationActivity a prov:Activity ;
    rdfs:label "Loan Data Aggregation" ;
    dcterms:description "ETL process that aggregates loan trading data" ;
    prov:startedAtTime "2024-01-01T06:00:00+00:00"^^xsd:dateTime ;
    prov:endedAtTime "2024-01-01T06:30:00+00:00"^^xsd:dateTime ;
    prov:wasAssociatedWith ex:DataEngineeringTeam .
```

### Lineage Relationships

Track data flow and dependencies:

```turtle
# Activity uses upstream data
ex:LoanAggregationActivity prov:used ex:LoanTradingDataset ;
                          prov:used ex:AccountDetailsDataset .

# Activity generates downstream data
ex:LoanAggregationActivity prov:generated ex:ConsolidatedLoansDataset .

# Direct derivation relationship
ex:ConsolidatedLoansDataset prov:wasDerivedFrom ex:LoanTradingDataset .
```

### Field-Level Lineage

Track column-to-column transformations:

```turtle
ex:AccountIdFieldMapping a prov:Activity ;
    rdfs:label "Account ID Field Mapping" ;
    prov:used ex:AccountDetailsDataset#account_id ;
    prov:generated ex:ConsolidatedLoansDataset#account_id ;
    prov:generated ex:FinanceLoanBalancesDataset#account_id .
```

**Features:**

- Complete PROV-O activity extraction
- All major PROV-O relationship types
- Field-level lineage tracking
- Temporal information and user attribution
- Unauthorized data flow detection
- DataHub native integration

## Programmatic Usage

```python
from src.rdf.core import OntologyToDataHub
from src.rdf.core.datahub_client import DataHubClient
from src.rdf.core.output_strategy import DryRunOutputStrategy, LiveDataHubOutputStrategy
from src.rdf.core.query_registry import ExportTarget

# Create client
client = DataHubClient("http://localhost:8080", "your-token")

# Create converter with dynamic routing
converter = OntologyToDataHub(client)

# Choose output strategy (dry run or live)
output_strategy = DryRunOutputStrategy()  # or LiveDataHubOutputStrategy(client)

# Process with unified export targets using dynamic routing
results = converter.process_graph(
    graph,
    [ExportTarget.ENTITIES, ExportTarget.LINKS],
    output_strategy
)

# Legacy single-target processing (still supported)
results = converter.process_graph(
    graph,
    [ExportTarget.GLOSSARY],
    output_strategy
)
```

## DataHub Ingestion Recipes

RDF is available as a native DataHub ingestion source plugin. This is the recommended approach for production use.

### Basic Recipe

```yaml
source:
  type: rdf
  config:
    source: examples/bcbs239/
    environment: PROD
    export_only:
      - glossary
      - datasets
      - lineage

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

### Running Recipes

```bash
# Run ingestion
datahub ingest -c examples/recipe_basic.yml

# Dry run (preview without ingesting)
datahub ingest -c examples/recipe_basic.yml --dry-run

# Debug mode
datahub ingest -c examples/recipe_basic.yml --debug
```

### Recipe Configuration

All CLI parameters are available in recipes:

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
| `sparql`      | SPARQL query to execute              | none                                 |
| `filter`      | Filter criteria                      | none                                 |

**Export Types:** `glossary`, `datasets`, `data_products`, `lineage`, `properties`, `ownership`

See [examples/RECIPES.md](examples/RECIPES.md) for more recipe examples and detailed documentation.

## Project Structure

```
src/rdf/
├── core/                    # Core processing logic
│   ├── query_based_processor.py    # Dynamic routing processor
│   ├── query_registry.py           # SPARQL query registry
│   ├── output_strategy.py          # Strategy pattern for dry run/live
│   ├── datahub_client.py           # DataHub API client
│   └── ...
├── scripts/                 # CLI tools
└── standards/               # Ontology handlers
```

### Key Components

- **QueryBasedProcessor**: Executes SPARQL queries and routes dynamically based on entity types
- **QueryRegistry**: Centralized SPARQL queries for each export target
- **OutputStrategy**: Strategy pattern for dry run vs live execution
- **DataHubClient**: Centralized DataHub API interactions

## Requirements

- Python 3.8+
- DataHub instance
- `rdflib`, `acryl-datahub`, `requests`
