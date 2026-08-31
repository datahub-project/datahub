#!/usr/bin/env python3
"""
Unit tests for SPARQL filter functionality in RDF ingestion.

Tests verify:
- SPARQL CONSTRUCT queries filter RDF graphs correctly
- Filtering happens before AST conversion
- Filtered graphs produce expected entity counts
- Error handling for invalid queries
"""

from unittest.mock import MagicMock

import pytest
from rdflib import Graph

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


class TestSPARQLFilter:
    """Test SPARQL filter functionality."""

    @pytest.fixture
    def ctx(self):
        """Create a pipeline context for testing."""
        ctx = MagicMock(spec=PipelineContext)
        ctx.graph = MagicMock()
        ctx.pipeline_name = "test_pipeline"
        ctx.run_id = "test-run"
        return ctx

    @pytest.fixture
    def multi_module_graph(self, tmp_path):
        """Create a test RDF file with multiple modules/namespaces."""
        ttl_content = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
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
        test_file = tmp_path / "multi_module.ttl"
        test_file.write_text(ttl_content)
        return str(test_file)

    def test_sparql_filter_filters_by_namespace(self, ctx, multi_module_graph):
        """Test that SPARQL filter can filter by namespace prefix."""
        config_dict = {
            "source": multi_module_graph,
            "sparql_filter": """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/"))
}""",
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Get workunits - this will apply the filter
        workunits = list(source.get_workunits_internal())

        # Should have extracted only FBC module terms (2 terms)
        # The filter reduced from 5 terms (all modules) to 2 terms (FBC only)
        assert source.report.num_glossary_terms == 2

        # Verify workunits were generated (filtering worked)
        assert len(workunits) > 0, "Should have generated workunits"

    def test_sparql_filter_filters_multiple_namespaces(self, ctx, multi_module_graph):
        """Test that SPARQL filter can filter multiple namespaces."""
        config_dict = {
            "source": multi_module_graph,
            "sparql_filter": """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(
        STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/") ||
        STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FND/")
    )
}""",
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        list(source.get_workunits_internal())

        # Should have extracted FBC + FND module terms (4 terms total)
        assert source.report.num_glossary_terms == 4

    def test_sparql_filter_without_filter_produces_all_terms(
        self, ctx, multi_module_graph
    ):
        """Test that without SPARQL filter, all terms are extracted."""
        config_dict = {
            "source": multi_module_graph,
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        list(source.get_workunits_internal())

        # Should have extracted all terms (5 terms)
        assert source.report.num_glossary_terms == 5

    def test_sparql_filter_reduces_triple_count(self, ctx, multi_module_graph):
        """Test that SPARQL filter reduces the number of triples processed."""
        # First, get triple count without filter
        config_no_filter = RDFSourceConfig.model_validate(
            {"source": multi_module_graph}
        )
        source_no_filter = RDFSource(config_no_filter, ctx)
        list(source_no_filter.get_workunits_internal())
        triples_no_filter = source_no_filter.report.num_triples_processed

        # Then, get triple count with filter
        config_with_filter = RDFSourceConfig.model_validate(
            {
                "source": multi_module_graph,
                "sparql_filter": """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/"))
}""",
            }
        )
        source_with_filter = RDFSource(config_with_filter, ctx)
        list(source_with_filter.get_workunits_internal())
        triples_with_filter = source_with_filter.report.num_triples_processed

        # Filtered version should have fewer triples
        assert triples_with_filter < triples_no_filter

    def test_sparql_filter_direct_method(self, ctx):
        """Test the _apply_sparql_filter method directly."""
        # Create a test graph
        graph = Graph()
        graph.parse(
            data="""@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex1: <http://example.org/module1/> .
@prefix ex2: <http://example.org/module2/> .

ex1:Term1 a skos:Concept ; skos:prefLabel "Term 1" .
ex2:Term2 a skos:Concept ; skos:prefLabel "Term 2" .
""",
            format="turtle",
        )

        original_count = len(graph)

        # Create source with SPARQL filter
        config = RDFSourceConfig.model_validate(
            {
                "source": "dummy",
                "sparql_filter": """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(STRSTARTS(STR(?s), "http://example.org/module1/"))
}""",
            }
        )
        source = RDFSource(config, ctx)

        # Apply filter directly
        filtered_graph = source._apply_sparql_filter(graph)

        assert filtered_graph is not None
        assert len(filtered_graph) < original_count
        assert len(filtered_graph) > 0

        # Verify filtered graph only contains module1 terms
        subjects = {str(s) for s in filtered_graph.subjects()}
        assert any("module1" in s for s in subjects)
        assert not any("module2" in s for s in subjects)

    def test_sparql_filter_invalid_query_handled_gracefully(self, ctx, tmp_path):
        """Test that invalid SPARQL queries are handled gracefully."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
            'ex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        config_dict = {
            "source": str(test_file),
            "sparql_filter": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . FILTER(INVALID_FUNCTION(?s)) }",
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should handle error gracefully and report failure
        workunits = list(source.get_workunits_internal())
        assert len(workunits) == 0
        assert len(source.report.failures) > 0

    def test_sparql_filter_select_query_rejected(self, ctx):
        """Test that SELECT queries are rejected (only CONSTRUCT supported)."""
        from unittest.mock import patch

        from rdflib import URIRef
        from rdflib.namespace import RDF

        # Create a test graph
        graph = Graph()
        graph.add(
            (
                URIRef("http://example.org/Test"),
                RDF.type,
                URIRef("http://example.org/Type"),
            )
        )

        config = RDFSourceConfig.model_validate(
            {
                "source": "dummy",
                "sparql_filter": "SELECT ?s WHERE { ?s ?p ?o }",
            }
        )
        source = RDFSource(config, ctx)

        # Mock to ensure we hit the SELECT query rejection code path
        with patch("rdflib.plugins.sparql.prepareQuery") as mock_prepare:
            # Create a mock query object with SelectQuery type
            mock_query_obj = MagicMock()
            mock_query_obj.algebra.name = "SelectQuery"
            mock_prepare.return_value = mock_query_obj

            # Mock graph.query() to return a Result-like object
            with patch.object(graph, "query", return_value=MagicMock()):
                # Apply filter directly - should reject SELECT query
                filtered_graph = source._apply_sparql_filter(graph)

                # Should return None and report failure
                assert filtered_graph is None
                assert len(source.report.failures) > 0

                # Check that error message mentions SELECT queries
                failures_list = list(source.report.failures)
                error_message = str(failures_list[0]).lower()
                assert "select" in error_message

    def test_sparql_filter_empty_result_handled(self, ctx, tmp_path):
        """Test that SPARQL filter that returns no results is handled."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
            'ex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        # Filter that matches nothing
        config_dict = {
            "source": str(test_file),
            "sparql_filter": """CONSTRUCT { ?s ?p ?o }
WHERE {
    ?s ?p ?o .
    FILTER(STRSTARTS(STR(?s), "http://nonexistent.org/"))
}""",
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should handle empty result gracefully
        workunits = list(source.get_workunits_internal())
        # No workunits because filter removed all triples
        assert len(workunits) == 0
        assert source.report.num_glossary_terms == 0

    def test_sparql_filter_fallback_graph_building(self, ctx):
        """Test fallback Graph building when CONSTRUCT returns non-Graph iterable."""
        from unittest.mock import patch

        from rdflib import URIRef
        from rdflib.namespace import RDF

        # Create a test graph
        graph = Graph()
        graph.add(
            (
                URIRef("http://example.org/Test"),
                RDF.type,
                URIRef("http://example.org/Type"),
            )
        )

        config = RDFSourceConfig.model_validate(
            {
                "source": "dummy",
                "sparql_filter": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
            }
        )
        source = RDFSource(config, ctx)

        # Mock prepareQuery to return a query with ConstructQuery type
        # and mock graph.query() to return an iterable (not a Graph)
        mock_query_result = [
            (
                URIRef("http://example.org/Test"),
                RDF.type,
                URIRef("http://example.org/Type"),
            )
        ]

        # Patch prepareQuery where it's imported (inside the method)
        with patch("rdflib.plugins.sparql.prepareQuery") as mock_prepare:
            # Create a mock query object with ConstructQuery algebra
            mock_query_obj = MagicMock()
            mock_query_obj.algebra.name = "ConstructQuery"
            mock_prepare.return_value = mock_query_obj

            # Mock graph.query() to return an iterable (not a Graph instance)
            with patch.object(graph, "query", return_value=iter(mock_query_result)):
                # Apply filter - should use fallback to build Graph from iterable
                filtered_graph = source._apply_sparql_filter(graph)

                # Should have built a Graph from the iterable
                assert filtered_graph is not None
                assert isinstance(filtered_graph, Graph)
                assert len(filtered_graph) == 1

    def test_sparql_filter_unsupported_query_type(self, ctx):
        """Test that unsupported query types (other than CONSTRUCT/SELECT) are rejected."""
        from unittest.mock import patch

        graph = Graph()
        graph.parse(
            data="""@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/> .

ex:Term a skos:Concept ; skos:prefLabel "Term" .
""",
            format="turtle",
        )

        config = RDFSourceConfig.model_validate(
            {
                "source": "dummy",
                "sparql_filter": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
            }
        )
        source = RDFSource(config, ctx)

        # Mock prepareQuery to return a query with unsupported query type
        with patch("rdflib.plugins.sparql.prepareQuery") as mock_prepare:
            # Create a mock query object with unsupported query type
            mock_query_obj = MagicMock()
            mock_query_obj.algebra.name = "AskQuery"  # Unsupported query type
            mock_prepare.return_value = mock_query_obj

            # Mock graph.query() to return something
            with patch.object(graph, "query", return_value=MagicMock()):
                # Apply filter - should reject unsupported query type
                filtered_graph = source._apply_sparql_filter(graph)

                # Should return None and report failure
                assert filtered_graph is None
                assert len(source.report.failures) > 0

                # Check that error message mentions unsupported query type
                failures_list = list(source.report.failures)
                error_message = str(failures_list[0]).lower()
                assert (
                    "unsupported" in error_message
                    or "askquery" in error_message.lower()
                )
