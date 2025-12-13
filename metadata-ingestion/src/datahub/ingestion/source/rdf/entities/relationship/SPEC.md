# Relationship Specification

**Part of**: [RDF Specification](../../docs/rdf-specification.md)

This document specifies how RDF glossary term relationships are extracted, converted, and mapped to DataHub relationship entities.

## Overview

Glossary term relationships represent semantic connections between business terms. This entity type specifically handles **term-to-term** relationships extracted from SKOS properties.

**Important**: This entity only extracts `skos:broader` and `skos:narrower` relationships. Other SKOS properties (`skos:related`, `skos:exactMatch`, `skos:closeMatch`) are **not** extracted by this entity.

## RDF Source Patterns

### Supported Relationships

Only these SKOS properties are extracted:

1. **`skos:broader`** - Child term points to parent term (more general concept)
2. **`skos:narrower`** - Parent term points to child term (more specific concept)

**Example**:

```turtle
accounts:Customer_ID a skos:Concept ;
    skos:prefLabel "Customer Identifier" ;
    skos:broader accounts:Customer_Data .

accounts:Customer_Data a skos:Concept ;
    skos:prefLabel "Customer Data" ;
    skos:narrower accounts:Customer_ID ;
    skos:narrower accounts:Customer_Name .
```

### Unsupported Relationships

These SKOS properties are **not** extracted by the relationship entity:

- `skos:related` - Associative relationships (not supported)
- `skos:exactMatch` - Reserved for field-to-term mappings only
- `skos:closeMatch` - Similar concepts (not supported)
- `skos:broadMatch` - Broader match (not supported)
- `skos:narrowMatch` - Narrower match (not supported)

**Note**: `skos:exactMatch` is handled separately for field-to-term mappings in dataset field definitions, not as term-to-term relationships.

## Relationship Types

The relationship entity defines these relationship types:

```python
class RelationshipType(Enum):
    BROADER = "broader"      # skos:broader
    NARROWER = "narrower"    # skos:narrower
```

## DataHub Mapping

### Relationship Mapping

Term-to-term relationships are mapped to DataHub's `isRelatedTerms` relationship:

- **`skos:broader`** (child → parent):

  - Source term (child) → `isRelatedTerms` → Target term (parent)
  - Creates bidirectional relationship: child inherits from parent

- **`skos:narrower`** (parent → child):
  - Source term (parent) → `isRelatedTerms` → Target term (child)
  - Creates bidirectional relationship: parent contains child

**DataHub Relationship**:

- **Field**: `isRelatedTerms`
- **UI Display**: "Inherits" (for child) or "Contains" (for parent)
- **Semantic Meaning**: Hierarchical term relationship

### URN Generation

Both source and target terms use glossary term URN generation:

- Format: `urn:li:glossaryTerm:({path_segments})`
- Uses `GlossaryTermUrnGenerator` for consistent URN creation

## Extraction Process

### Bulk Extraction

Relationships are extracted in bulk from the entire RDF graph:

1. **Find all `skos:broader` triples**: `(subject, skos:broader, object)`
2. **Find all `skos:narrower` triples**: `(subject, skos:narrower, object)`
3. **Deduplicate**: Remove duplicate relationships
4. **Convert to RDFRelationship**: Create `RDFRelationship` objects with source/target URIs

### Per-Term Extraction

Relationships can also be extracted for a specific term:

```python
relationships = extractor.extract_for_term(graph, term_uri)
```

Returns all relationships where the specified term is the source.

## DataHub Integration

### MCP Creation

Relationships are converted to DataHub MCPs that create `isRelatedTerms` edges:

```python
# RDF Relationship
RDFRelationship(
    source_uri="http://example.com/terms/Customer_ID",
    target_uri="http://example.com/terms/Customer_Data",
    relationship_type=RelationshipType.BROADER
)

# DataHub Relationship
DataHubRelationship(
    source_urn="urn:li:glossaryTerm:(terms,Customer_ID)",
    target_urn="urn:li:glossaryTerm:(terms,Customer_Data)",
    relationship_type="broader"
)
```

### Bidirectional Relationships

When a `skos:broader` relationship is created:

- Child term gets `isRelatedTerms` pointing to parent (inherits)
- Parent term gets `hasRelatedTerms` pointing to child (contains)

This bidirectional mapping is handled automatically by DataHub's relationship model.

## Validation

### Relationship Validation

1. **Source/Target Validation**: Both source and target must be valid term URIs
2. **URN Generation**: Both URIs must successfully convert to DataHub URNs
3. **Deduplication**: Duplicate relationships (same source, target, type) are removed

## Limitations

1. **Only Hierarchical Relationships**: Only `skos:broader` and `skos:narrower` are supported
2. **No Associative Relationships**: `skos:related` and `skos:closeMatch` are not extracted
3. **No External References**: `skos:exactMatch` is reserved for field-to-term mappings only
4. **Term-to-Term Only**: This entity does not handle field-to-term relationships (handled by dataset entity)

## Relationship to Glossary Term Entity

The relationship entity is **separate** from the glossary term entity:

- **Glossary Term Entity**: Extracts term definitions, properties, constraints
- **Relationship Entity**: Extracts term-to-term relationships only

This separation allows relationships to be processed independently and enables selective export of relationships without full term processing.
