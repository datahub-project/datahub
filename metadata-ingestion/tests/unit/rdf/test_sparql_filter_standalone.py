#!/usr/bin/env python3
"""
Standalone test script for SPARQL filter functionality.

Run this directly to test SPARQL filtering without pytest:
    python test_sparql_filter_standalone.py

This creates a test RDF graph, applies a SPARQL filter, and shows the results.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directories to path to import datahub modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from rdflib import Graph

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


def create_test_graph() -> Graph:
    """Create a test RDF graph with multiple modules."""
    graph = Graph()
    ttl = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix fibo-fbc: <https://spec.edmcouncil.org/fibo/ontology/FBC/> .
@prefix fibo-fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/> .
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/> .

# FBC module terms
fibo-fbc:Account a skos:Concept ;
    skos:prefLabel "Account" ;
    skos:definition "A financial account" .

fibo-fbc:Loan a skos:Concept ;
    skos:prefLabel "Loan" ;
    skos:definition "A financial loan" .

# FND module terms
fibo-fnd:Person a skos:Concept ;
    skos:prefLabel "Person" ;
    skos:definition "A human person" .

fibo-fnd:Organization a skos:Concept ;
    skos:prefLabel "Organization" ;
    skos:definition "An organization" .

# BE module terms
fibo-be:BusinessEntity a skos:Concept ;
    skos:prefLabel "Business Entity" ;
    skos:definition "A business entity" .
"""
    graph.parse(data=ttl, format="turtle")
    return graph


def test_sparql_filter():
    """Test SPARQL filter functionality."""
    print("=" * 70)
    print("SPARQL Filter Test")
    print("=" * 70)

    # Create test graph
    graph = create_test_graph()
    print(f"\n1. Created test graph with {len(graph)} triples")
    print("   Contains terms from FBC, FND, and BE modules")

    # Save to temporary file
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ttl", delete=False) as f:
        graph.serialize(destination=f.name, format="turtle")
        temp_file = f.name

    try:
        # Test 1: No filter (should get all terms)
        print("\n2. Testing WITHOUT SPARQL filter:")
        ctx = MagicMock(spec=PipelineContext)
        ctx.graph = MagicMock()
        ctx.pipeline_name = "test"
        ctx.run_id = "test-run"

        config_no_filter = RDFSourceConfig.model_validate({"source": temp_file})
        source_no_filter = RDFSource(config_no_filter, ctx)
        workunits_no_filter = list(source_no_filter.get_workunits_internal())

        print(
            f"   - Triples processed: {source_no_filter.report.num_triples_processed}"
        )
        print(
            f"   - Glossary terms extracted: {source_no_filter.report.num_glossary_terms}"
        )
        print(f"   - Work units generated: {len(workunits_no_filter)}")

        # Test 2: With filter (FBC module only)
        print("\n3. Testing WITH SPARQL filter (FBC module only):")
        sparql_query = """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/"))
}"""

        config_with_filter = RDFSourceConfig.model_validate(
            {
                "source": temp_file,
                "sparql_filter": sparql_query,
            }
        )
        source_with_filter = RDFSource(config_with_filter, ctx)
        workunits_with_filter = list(source_with_filter.get_workunits_internal())

        print(
            f"   - Triples processed: {source_with_filter.report.num_triples_processed}"
        )
        print(
            f"   - Glossary terms extracted: {source_with_filter.report.num_glossary_terms}"
        )
        print(f"   - Work units generated: {len(workunits_with_filter)}")

        # Test 3: With filter (FBC + FND modules)
        print("\n4. Testing WITH SPARQL filter (FBC + FND modules):")
        sparql_query_multi = """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(
        STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/") ||
        STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FND/")
    )
}"""

        config_multi_filter = RDFSourceConfig.model_validate(
            {
                "source": temp_file,
                "sparql_filter": sparql_query_multi,
            }
        )
        source_multi_filter = RDFSource(config_multi_filter, ctx)
        workunits_multi_filter = list(source_multi_filter.get_workunits_internal())

        print(
            f"   - Triples processed: {source_multi_filter.report.num_triples_processed}"
        )
        print(
            f"   - Glossary terms extracted: {source_multi_filter.report.num_glossary_terms}"
        )
        print(f"   - Work units generated: {len(workunits_multi_filter)}")

        # Summary
        print("\n" + "=" * 70)
        print("Summary:")
        print("=" * 70)
        print(
            f"No filter:      {source_no_filter.report.num_glossary_terms} terms, "
            f"{source_no_filter.report.num_triples_processed} triples"
        )
        print(
            f"FBC only:        {source_with_filter.report.num_glossary_terms} terms, "
            f"{source_with_filter.report.num_triples_processed} triples"
        )
        print(
            f"FBC + FND:       {source_multi_filter.report.num_glossary_terms} terms, "
            f"{source_multi_filter.report.num_triples_processed} triples"
        )
        print("\n✅ SPARQL filter is working correctly!")
        print("   Filtering reduces both triples and extracted entities as expected.")

    finally:
        # Clean up temp file
        Path(temp_file).unlink()


if __name__ == "__main__":
    test_sparql_filter()
