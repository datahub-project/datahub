


# RDF

## Overview

This connector ingests RDF (Resource Description Framework) ontologies into DataHub, with a focus on **business glossaries**. It extracts glossary terms, term hierarchies, and relationships from RDF files using standard vocabularies like SKOS, OWL, and RDFS.

The RDF ingestion source processes RDF/OWL ontologies in various formats (Turtle, RDF/XML, JSON-LD, N3, N-Triples) and converts them to DataHub entities. It supports loading RDF from files, folders, URLs, and comma-separated file lists.

## Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                   | DataHub Concept                                                                           | Notes                                        |
| -------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------- |
| `"rdf"`                          | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                                              |
| `skos:Concept`                   | [GlossaryTerm](/docs/generated/metamodel/entities/glossaryterm/)  | SKOS concepts become glossary terms          |
| `owl:Class`                      | [GlossaryTerm](/docs/generated/metamodel/entities/glossaryterm/)  | OWL classes become glossary terms            |
| IRI path hierarchy               | [GlossaryNode](/docs/generated/metamodel/entities/glossarynode/)  | Path segments create glossary node hierarchy |
| `skos:broader` / `skos:narrower` | `isRelatedTerms` relationship                                                             | Term relationships                           |


## Module `rdf`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ❌ | Not applicable. |
| Descriptions | ✅ | Enabled by default (from skos:definition or rdfs:comment). |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful_ingestion.enabled: true. |
| [Domains](../../../domains.md) | ❌ | Not applicable (domains used internally for hierarchy). |
| Extract Ownership | ❌ | Not supported. |
| Extract Tags | ❌ | Not supported. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Supported via platform_instance config. |
| Table-Level Lineage | ❌ | Not in MVP. |

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


### Install the Plugin
```shell
pip install 'acryl-datahub[rdf]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: rdf
  config:
    # Required: RDF source (file, folder, URL, or comma-separated files)
    source: path/to/glossary.ttl
    
    # Optional: DataHub environment
    environment: PROD
    
    # Optional: RDF format (auto-detected if not specified)
    # format: turtle
    
    # Optional: RDF dialect (auto-detected if not specified)
    # dialect: default
    
    # Optional: Export only specific entity types
    # export_only:
    #   - glossary
    
    # Optional: Skip specific entity types
    # skip_export:
    #   - relationship
    
    # Optional: Enable stateful ingestion (recommended for production)
    # stateful_ingestion:
    #   enabled: true

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
    # token: "${DATAHUB_TOKEN}"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">source</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Primary: Web URL to .ttl or .zip file (e.g. https://example.org/glossary.ttl, https://example.org/data.zip). Also supports web folder URLs. Local file/folder paths work only when running via CLI. Examples: 'https://example.org/glossary.ttl', 'https://example.org/data.zip', 'https://example.org/folder/', '/path/to/file.ttl' (CLI-only)  |
| <div className="path-line"><span className="path-main">dialect</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Force a specific RDF dialect (default: auto-detect). Options: default, fibo, generic <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub environment (PROD, DEV, TEST, etc.) <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">format</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | RDF format (auto-detected if not specified). Examples: turtle, xml, n3, nt <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_provisional</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include terms with provisional/work-in-progress status (default: False). When False, only terms that have been fully approved/released are included. Many ontologies use workflow status properties (e.g., maturity level) to mark terms that are in the pipeline but not yet fully approved. Setting this to False helps reduce noise from unapproved or draft terms. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">parent_glossary_node</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional parent Term Group (glossary node) to place the loaded hierarchy under. Use a name (e.g. 'ExternalOntologies') or full URN (urn:li:glossaryNode:ExternalOntologies). If a name is provided, the parent node is created if it does not exist. When omitted, terms are placed at the top level. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">recursive</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable recursive folder processing (default: true) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sparql_filter</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional SPARQL CONSTRUCT query to filter the RDF graph before ingestion. Useful for filtering by namespace, module, or custom patterns. The query should use CONSTRUCT to build a filtered graph. Example: Filter to specific FIBO modules: 'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . FILTER(STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/")) }' <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">export_only</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Export only specified entity types. Options are dynamically determined from registered entity types. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">export_only.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">extensions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | File extensions to process when source is a folder <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.ttl&#x27;, &#x27;.rdf&#x27;, &#x27;.owl&#x27;, &#x27;.n3&#x27;, &#x27;.nt&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">extensions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">skip_export</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Skip exporting specified entity types. Options are dynamically determined from registered entity types. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">skip_export.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration. See https://datahubproject.io/docs/stateful-ingestion for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for RDF ingestion source.\n\nMirrors the CLI parameters to provide consistent behavior between\nCLI and ingestion framework usage.\n\nExample configuration (primary: web URL to .ttl or .zip):\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: https://example.org/glossary.ttl\n        format: turtle\n        environment: PROD\n        stateful_ingestion:\n          enabled: true\n          remove_stale_metadata: true\n    ```\n\nExample with parent Term Group:\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: https://example.org/glossary.ttl\n        environment: PROD\n        parent_glossary_node: ExternalOntologies\n    ```\n\nExample with zip URL:\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: https://example.org/rdf_data.zip\n        format: turtle\n        recursive: true\n        environment: PROD\n    ```\n\nExample with web folder URL:\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: https://example.org/rdf_data/\n        format: turtle\n        recursive: true\n        environment: PROD\n    ```\n\nExample with local path (CLI-only):\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: /path/to/glossary.ttl\n        format: turtle\n        recursive: true\n        environment: PROD\n    ```\n\nExample with filtering (CLI-only: local paths):\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: /path/to/ontology.owl\n        export_only: [\"glossary\"]\n        environment: PROD\n    ```\n\nExample with SPARQL filter (filtering by namespace/module):\n    ```yaml\n    source:\n      type: rdf\n      config:\n        source: https://spec.edmcouncil.org/fibo/ontology/master/latest/fibo-all.ttl\n        sparql_filter: |\n          CONSTRUCT { ?s ?p ?o }\n          WHERE {\n            ?s ?p ?o .\n            FILTER(\n              STRSTARTS(STR(?s), \"https://spec.edmcouncil.org/fibo/ontology/FBC/\") ||\n              STRSTARTS(STR(?s), \"https://spec.edmcouncil.org/fibo/ontology/FND/\")\n            )\n          }\n        environment: PROD\n    ```",
  "properties": {
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration. See https://datahubproject.io/docs/stateful-ingestion for more details."
    },
    "source": {
      "description": "Primary: Web URL to .ttl or .zip file (e.g. https://example.org/glossary.ttl, https://example.org/data.zip). Also supports web folder URLs. Local file/folder paths work only when running via CLI. Examples: 'https://example.org/glossary.ttl', 'https://example.org/data.zip', 'https://example.org/folder/', '/path/to/file.ttl' (CLI-only)",
      "title": "Source",
      "type": "string"
    },
    "format": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "RDF format (auto-detected if not specified). Examples: turtle, xml, n3, nt",
      "title": "Format"
    },
    "extensions": {
      "default": [
        ".ttl",
        ".rdf",
        ".owl",
        ".n3",
        ".nt"
      ],
      "description": "File extensions to process when source is a folder",
      "items": {
        "type": "string"
      },
      "title": "Extensions",
      "type": "array"
    },
    "recursive": {
      "default": true,
      "description": "Enable recursive folder processing (default: true)",
      "title": "Recursive",
      "type": "boolean"
    },
    "environment": {
      "default": "PROD",
      "description": "DataHub environment (PROD, DEV, TEST, etc.)",
      "title": "Environment",
      "type": "string"
    },
    "parent_glossary_node": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional parent Term Group (glossary node) to place the loaded hierarchy under. Use a name (e.g. 'ExternalOntologies') or full URN (urn:li:glossaryNode:ExternalOntologies). If a name is provided, the parent node is created if it does not exist. When omitted, terms are placed at the top level.",
      "title": "Parent Glossary Node"
    },
    "dialect": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Force a specific RDF dialect (default: auto-detect). Options: default, fibo, generic",
      "title": "Dialect"
    },
    "include_provisional": {
      "default": false,
      "description": "Include terms with provisional/work-in-progress status (default: False). When False, only terms that have been fully approved/released are included. Many ontologies use workflow status properties (e.g., maturity level) to mark terms that are in the pipeline but not yet fully approved. Setting this to False helps reduce noise from unapproved or draft terms.",
      "title": "Include Provisional",
      "type": "boolean"
    },
    "export_only": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Export only specified entity types. Options are dynamically determined from registered entity types.",
      "title": "Export Only"
    },
    "skip_export": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Skip exporting specified entity types. Options are dynamically determined from registered entity types.",
      "title": "Skip Export"
    },
    "sparql_filter": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional SPARQL CONSTRUCT query to filter the RDF graph before ingestion. Useful for filtering by namespace, module, or custom patterns. The query should use CONSTRUCT to build a filtered graph. Example: Filter to specific FIBO modules: 'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . FILTER(STRSTARTS(STR(?s), \"https://spec.edmcouncil.org/fibo/ontology/FBC/\")) }'",
      "title": "Sparql Filter"
    }
  },
  "required": [
    "source"
  ],
  "title": "RDFSourceConfig",
  "type": "object"
}
```





### Capabilities

#### IRI-to-URN Mapping

RDF IRIs are converted to DataHub URNs following this pattern:

```
http://example.com/finance/credit-risk
→ urn:li:glossaryTerm:example.com/finance/credit-risk
```

IRI path segments create the glossary node hierarchy:

- `example.com` → Glossary Node
- `finance` → Glossary Node (child of `example.com`)
- `credit-risk` → Glossary Term (under `finance` node)

#### Supported RDF Vocabularies

The source recognizes entities from these standard vocabularies:

- **SKOS**: `skos:Concept` → Glossary Term, `skos:prefLabel` → name, `skos:definition` → definition, `skos:broader`/`skos:narrower` → relationships
- **OWL**: `owl:Class` → Glossary Term, `owl:NamedIndividual` → Glossary Term
- **RDFS**: `rdfs:label` → name (fallback), `rdfs:comment` → definition (fallback)

### Limitations

- **MVP Scope**: The current implementation focuses on glossary terms and relationships. Dataset, lineage, and structured property extraction are not included.
- **Relationship Types**: Only `skos:broader` and `skos:narrower` relationships are extracted. `skos:related` and `skos:exactMatch` are not supported.
- **Term Requirements**: Terms must have a label (`skos:prefLabel` or `rdfs:label`) of at least 3 characters to be extracted.
- **Large Files**: Very large RDF files are loaded entirely into memory. Consider splitting large ontologies into multiple files.

### Troubleshooting

#### No glossary terms extracted

- **Check term types** - Ensure entities are typed as `skos:Concept`, `owl:Class`, or `owl:NamedIndividual`
- **Verify labels** - Terms must have `skos:prefLabel` or `rdfs:label` with at least 3 characters
- **Check file format** - Verify the RDF file is valid and in a supported format
- **Review logs** - Enable debug logging: `datahub ingest -c recipe.yml --debug`

#### Terms not appearing in correct hierarchy

- **Check IRI structure** - Glossary nodes are created from IRI path segments
- **Verify IRI format** - IRIs should be absolute (e.g., `https://example.com/path/term`)

#### Relationships not extracted

- **Check relationship types** - Only `skos:broader` and `skos:narrower` are supported
- **Verify term existence** - Both source and target terms must exist in the RDF
- **Check export options** - Ensure `relationship` is not in `skip_export`

#### Stateful ingestion not working

- **Enable it** - Set `stateful_ingestion.enabled: true`
- **Check server** - Verify your DataHub server supports stateful ingestion (version >= 0.8.20)


### Code Coordinates
- Class Name: `datahub.ingestion.source.rdf.ingestion.rdf_source.RDFSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/rdf/ingestion/rdf_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for RDF, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
