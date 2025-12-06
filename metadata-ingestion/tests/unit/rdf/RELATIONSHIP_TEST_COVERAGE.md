# Relationship Test Coverage

This document describes the comprehensive test coverage for glossary term relationship processing across all three stages of the transpiler pipeline.

## Test Files

### Stage 1: RDF Graph → RDF AST (`test_relationship_extraction_stage1.py`)

Tests relationship extraction from RDF graphs:

1. **`test_broader_relationship_extracted`**

   - Verifies `skos:broader` relationships are extracted
   - Relationships stored in `RDFGlossaryTerm.relationships`
   - Relationship type is `RelationshipType.BROADER`

2. **`test_narrower_relationship_extracted`**

   - Verifies `skos:narrower` relationships are extracted
   - Relationship type is `RelationshipType.NARROWER`

3. **`test_related_relationship_not_extracted`**

   - Verifies `skos:related` relationships are **NOT** extracted
   - Only `broader` and `narrower` are supported

4. **`test_close_match_relationship_not_extracted`**

   - Verifies `skos:closeMatch` relationships are **NOT** extracted
   - Only `broader` and `narrower` are supported

5. **`test_exact_match_not_extracted_for_terms`**

   - Verifies `skos:exactMatch` is **NOT** extracted for term-to-term relationships
   - `exactMatch` is only for field-to-term mappings

6. **`test_relationship_to_external_term_extracted`**

   - Verifies relationships to external terms (not in graph) are still extracted
   - Important for FIBO and other external ontology references

7. **`test_multiple_broader_relationships_extracted`**
   - Verifies multiple `broader` relationships from same term are all extracted

### Stage 2: RDF AST → DataHub AST (`test_relationship_conversion_stage2.py`)

Tests relationship conversion and collection:

1. **`test_term_relationships_collected_to_global_list`**

   - Verifies relationships from `RDFGlossaryTerm.relationships` are collected
   - Added to global `datahub_ast.relationships` list
   - Critical fix: relationships from terms are now processed

2. **`test_external_term_relationship_converted`**

   - Verifies relationships to external terms are converted correctly
   - Both source and target get URNs even if target doesn't exist in graph

3. **`test_multiple_term_relationships_collected`**

   - Verifies relationships from multiple terms are all collected
   - All relationships appear in global list

4. **`test_duplicate_relationships_avoided`**

   - Verifies duplicate relationships are not added twice
   - Prevents duplicate MCPs

5. **`test_broader_and_narrower_both_converted`**
   - Verifies both `BROADER` and `NARROWER` relationships are converted
   - Both relationship types are preserved

### Stage 3: DataHub AST → MCPs (`test_relationship_mcp_stage3.py`)

Tests MCP creation for relationships:

1. **`test_broader_creates_only_is_related_terms`**

   - Verifies `skos:broader` creates only `isRelatedTerms` (inherits)
   - Does **NOT** create `hasRelatedTerms` (contains)
   - Critical fix: removed bidirectional `hasRelatedTerms` creation

2. **`test_no_has_related_terms_created`**

   - Verifies `hasRelatedTerms` (contains) is **NOT** created
   - Only `isRelatedTerms` (inherits) is used

3. **`test_multiple_broader_relationships_aggregated`**

   - Verifies multiple `broader` relationships are aggregated correctly
   - All targets included in single MCP

4. **`test_duplicate_relationships_deduplicated`**
   - Verifies duplicate relationships are deduplicated
   - Single target in final MCP even if relationship appears multiple times

## Expected Behaviors Tested

### ✅ Supported Relationship Types

- `skos:broader` → `isRelatedTerms` (inherits)
- `skos:narrower` → (inferred from broader)

### ❌ Unsupported Relationship Types (Excluded)

- `skos:related` → **NOT** extracted
- `skos:closeMatch` → **NOT** extracted
- `skos:exactMatch` → **NOT** extracted for term-to-term (only field-to-term)

### ✅ Relationship Processing Rules

- Relationships stored in `RDFGlossaryTerm.relationships` are collected to global list
- External term relationships work (target doesn't need to exist in graph)
- Duplicate relationships are avoided
- Multiple relationships are aggregated correctly
- Only `isRelatedTerms` (inherits) is created, **NOT** `hasRelatedTerms` (contains)

## Running the Tests

```bash
# Run all relationship tests
pytest tests/test_relationship*.py -v

# Run tests for specific stage
pytest tests/test_relationship_extraction_stage1.py -v
pytest tests/test_relationship_conversion_stage2.py -v
pytest tests/test_relationship_mcp_stage3.py -v
```

## Test Results

All 16 relationship tests pass:

- 7 tests for Stage 1 (extraction)
- 5 tests for Stage 2 (conversion)
- 4 tests for Stage 3 (MCP creation)

These tests ensure that relationship processing logic stays aligned with the specification as the codebase evolves.
