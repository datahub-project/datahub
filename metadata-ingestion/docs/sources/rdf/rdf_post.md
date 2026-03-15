### Capabilities

| Capability              | Status | Notes                                                         |
| ----------------------- | :----: | ------------------------------------------------------------- |
| Glossary Terms          |   ✅   | Enabled by default                                            |
| Glossary Nodes          |   ✅   | Auto-created from IRI path hierarchies                        |
| Term Relationships      |   ✅   | Supports `skos:broader` and `skos:narrower`                   |
| Detect Deleted Entities |   ✅   | Requires `stateful_ingestion.enabled: true`                   |
| Platform Instance       |   ✅   | Supported via `platform_instance` config                      |
| Extract Descriptions    |   ✅   | Enabled by default (from `skos:definition` or `rdfs:comment`) |

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
