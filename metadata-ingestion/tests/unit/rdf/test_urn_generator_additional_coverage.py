#!/usr/bin/env python3
"""
Additional comprehensive tests for URN generator to improve coverage.

Tests cover:
- Platform normalization edge cases
- IRI parsing edge cases
- Path derivation edge cases
- Error handling paths
- Label extraction edge cases
"""

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS, RDFS, SKOS

from datahub.ingestion.source.rdf.core.urn_generator import (
    UrnGeneratorBase,
    extract_name_from_label,
)


class TestPlatformNormalization:
    """Test platform normalization."""

    def test_normalize_platform_none(self):
        """Test platform normalization with None."""
        generator = UrnGeneratorBase()
        assert generator._normalize_platform(None) == "logical"

    def test_normalize_platform_string(self):
        """Test platform normalization with string."""
        generator = UrnGeneratorBase()
        assert generator._normalize_platform("mysql") == "mysql"
        assert generator._normalize_platform("postgres") == "postgres"

    def test_normalize_platform_urn(self):
        """Test platform normalization with URN."""
        generator = UrnGeneratorBase()
        assert generator._normalize_platform("urn:li:dataPlatform:mysql") == "mysql"
        assert (
            generator._normalize_platform("urn:li:dataPlatform:postgres") == "postgres"
        )

    def test_normalize_platform_empty_string(self):
        """Test platform normalization with empty string."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="cannot be an empty string"):
            generator._normalize_platform("")

    def test_normalize_platform_whitespace_only(self):
        """Test platform normalization with whitespace-only string."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="cannot be an empty string"):
            generator._normalize_platform("   ")

    def test_normalize_platform_invalid_type(self):
        """Test platform normalization with invalid type."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="must be a string"):
            generator._normalize_platform(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="must be a string"):
            generator._normalize_platform([])  # type: ignore[arg-type]

    def test_normalize_platform_urn_empty_name(self):
        """Test platform normalization with URN that has empty name."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="Invalid platform URN"):
            generator._normalize_platform("urn:li:dataPlatform:")


class TestIRIPathDerivation:
    """Test IRI path derivation."""

    def test_derive_path_from_iri_standard_url(self):
        """Test path derivation from standard HTTP URL."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("http://example.com/path/to/term")
        assert path == ["example.com", "path", "to", "term"]

    def test_derive_path_from_iri_https(self):
        """Test path derivation from HTTPS URL."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("https://example.com/path/to/term")
        assert path == ["example.com", "path", "to", "term"]

    def test_derive_path_from_iri_with_port(self):
        """Test path derivation from URL with port."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("http://example.com:8080/path/to/term")
        assert path == ["example.com", "path", "to", "term"]

    def test_derive_path_from_iri_with_www(self):
        """Test path derivation from URL with www prefix."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("http://www.example.com/path/to/term")
        assert path == ["example.com", "path", "to", "term"]

    def test_derive_path_from_iri_exclude_last(self):
        """Test path derivation excluding last segment."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri(
            "http://example.com/path/to/term", include_last=False
        )
        assert path == ["example.com", "path", "to"]

    def test_derive_path_from_iri_custom_scheme(self):
        """Test path derivation from custom scheme."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("trading:term/Customer_Name")
        assert len(path) > 0

    def test_derive_path_from_iri_no_path(self):
        """Test path derivation from URL with no path."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("http://example.com")
        assert path == ["example.com"]

    def test_derive_path_from_iri_empty(self):
        """Test path derivation from empty IRI."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.derive_path_from_iri("")

    def test_derive_path_from_iri_invalid_type(self):
        """Test path derivation from invalid type."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.derive_path_from_iri(None)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.derive_path_from_iri(123)  # type: ignore[arg-type]

    def test_derive_path_from_iri_no_scheme(self):
        """Test path derivation from IRI without scheme."""
        generator = UrnGeneratorBase()
        # This should work - urlparse treats it as a path
        path = generator.derive_path_from_iri("example.com/path/to/term")
        assert len(path) > 0

    def test_derive_path_from_iri_ftp(self):
        """Test path derivation from FTP URL."""
        generator = UrnGeneratorBase()
        path = generator.derive_path_from_iri("ftp://example.com/path/to/term")
        assert path == ["example.com", "path", "to", "term"]

    def test_parse_iri_path(self):
        """Test parse_iri_path method."""
        generator = UrnGeneratorBase()
        path = generator.parse_iri_path("http://example.com/path/to/term")
        assert path == ["example.com", "path", "to", "term"]


class TestPreserveIRIStructure:
    """Test IRI structure preservation."""

    def test_preserve_iri_structure_http(self):
        """Test preserving IRI structure for HTTP URLs."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("http://example.com/path/to/term")
        result = generator._preserve_iri_structure(parsed)
        assert result == "example.com/path/to/term"

    def test_preserve_iri_structure_https(self):
        """Test preserving IRI structure for HTTPS URLs."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("https://example.com/path/to/term")
        result = generator._preserve_iri_structure(parsed)
        assert result == "example.com/path/to/term"

    def test_preserve_iri_structure_ftp(self):
        """Test preserving IRI structure for FTP URLs."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("ftp://example.com/path/to/term")
        result = generator._preserve_iri_structure(parsed)
        assert result == "example.com/path/to/term"

    def test_preserve_iri_structure_custom_scheme(self):
        """Test preserving IRI structure for custom scheme."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("trading:term/Customer_Name")
        result = generator._preserve_iri_structure(parsed)
        assert "term" in result or "Customer_Name" in result

    def test_preserve_iri_structure_empty(self):
        """Test preserving IRI structure from empty IRI."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("")
        with pytest.raises(ValueError, match="Cannot extract path from empty IRI"):
            generator._preserve_iri_structure(parsed)

    def test_preserve_iri_structure_no_scheme(self):
        """Test preserving IRI structure without scheme."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("example.com/path/to/term")
        with pytest.raises(ValueError, match="must have a valid scheme"):
            generator._preserve_iri_structure(parsed)


class TestDerivePlatformFromIRI:
    """Test platform derivation from IRI."""

    def test_derive_platform_from_iri_http(self):
        """Test platform derivation from HTTP IRI."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("http://example.com/path/to/term")
        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "example.com"

    def test_derive_platform_from_iri_https(self):
        """Test platform derivation from HTTPS IRI."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("https://example.com/path/to/term")
        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "example.com"

    def test_derive_platform_from_iri_with_www(self):
        """Test platform derivation from IRI with www."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("http://www.example.com/path/to/term")
        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "example.com"

    def test_derive_platform_from_iri_no_netloc(self):
        """Test platform derivation from IRI without netloc."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("http:///path/to/term")
        platform = generator._derive_platform_from_iri(parsed)
        # Should fall back to scheme
        assert platform == "http"

    def test_derive_platform_from_iri_custom_scheme(self):
        """Test platform derivation from custom scheme."""
        generator = UrnGeneratorBase()
        from urllib.parse import urlparse

        parsed = urlparse("trading:term/Customer_Name")
        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "trading"


class TestGenerateGroupNameFromOwnerIRI:
    """Test group name generation from owner IRI."""

    def test_generate_group_name_from_owner_iri_with_slash(self):
        """Test group name generation from owner IRI with slash."""
        generator = UrnGeneratorBase()
        name = generator.generate_group_name_from_owner_iri(
            "http://example.com/FINANCE/Business_Owners"
        )
        assert name == "finance_business_owners"

    def test_generate_group_name_from_owner_iri_no_slash(self):
        """Test group name generation from owner IRI without slash."""
        generator = UrnGeneratorBase()
        name = generator.generate_group_name_from_owner_iri("Business_Owners")
        assert name == "business_owners"

    def test_generate_group_name_from_owner_iri_empty(self):
        """Test group name generation from empty owner IRI."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.generate_group_name_from_owner_iri("")

    def test_generate_group_name_from_owner_iri_invalid_type(self):
        """Test group name generation from invalid type."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.generate_group_name_from_owner_iri(None)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="must be a non-empty string"):
            generator.generate_group_name_from_owner_iri(123)  # type: ignore[arg-type]

    def test_generate_group_name_from_owner_iri_invalid_format(self):
        """Test group name generation from invalid format."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="Cannot extract domain and owner type"):
            generator.generate_group_name_from_owner_iri("http://example.com/")

    def test_generate_group_name_from_owner_iri_missing_parts(self):
        """Test group name generation from IRI with missing parts."""
        generator = UrnGeneratorBase()
        with pytest.raises(ValueError, match="Cannot extract domain and owner type"):
            generator.generate_group_name_from_owner_iri("http://example.com/")


class TestExtractNameFromLabel:
    """Test label extraction from RDF."""

    def test_extract_name_from_label_skos_preflabel(self):
        """Test extracting name from SKOS prefLabel."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, SKOS.prefLabel, Literal("Preferred Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "Preferred Label"

    def test_extract_name_from_label_rdfs_label(self):
        """Test extracting name from rdfs:label."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("RDFS Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "RDFS Label"

    def test_extract_name_from_label_dcterms_title(self):
        """Test extracting name from dcterms:title."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, DCTERMS.title, Literal("DCTERMS Title")))

        name = extract_name_from_label(graph, uri)
        assert name == "DCTERMS Title"

    def test_extract_name_from_label_priority_order(self):
        """Test that SKOS prefLabel takes priority over rdfs:label."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, SKOS.prefLabel, Literal("Preferred")))
        graph.add((uri, RDFS.label, Literal("RDFS Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "Preferred"

    def test_extract_name_from_label_too_short(self):
        """Test that labels that are too short are rejected."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("AB")))  # Too short

        name = extract_name_from_label(graph, uri)
        assert name is None

    def test_extract_name_from_label_whitespace(self):
        """Test that whitespace is stripped."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("  Label Value  ")))

        name = extract_name_from_label(graph, uri)
        assert name == "Label Value"

    def test_extract_name_from_label_no_label(self):
        """Test extraction when no label exists."""
        graph = Graph()
        uri = URIRef("http://example.org/term")

        name = extract_name_from_label(graph, uri)
        assert name is None

    def test_extract_name_from_label_invalid_graph(self):
        """Test extraction with invalid graph type."""
        with pytest.raises(ValueError, match="must be an RDFLib Graph"):
            extract_name_from_label("not a graph", URIRef("http://example.org/term"))  # type: ignore[arg-type]

    def test_extract_name_from_label_invalid_uri(self):
        """Test extraction with invalid URI type."""
        graph = Graph()
        with pytest.raises(ValueError, match="must be an RDFLib URIRef"):
            extract_name_from_label(graph, "http://example.org/term")  # type: ignore[arg-type]

    def test_extract_name_from_label_string_literal(self):
        """Test extraction with string literal."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        # RDFLib requires Literal objects, not plain strings
        graph.add((uri, RDFS.label, Literal("String Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "String Label"

    def test_extract_name_from_label_multiple_labels(self):
        """Test extraction when multiple labels exist."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, SKOS.prefLabel, Literal("First")))
        graph.add((uri, SKOS.prefLabel, Literal("Second")))

        # Should return the first one found
        name = extract_name_from_label(graph, uri)
        assert name in ["First", "Second"]
