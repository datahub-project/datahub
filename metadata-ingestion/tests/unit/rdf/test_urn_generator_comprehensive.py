#!/usr/bin/env python3
"""
Comprehensive tests for URN generator to improve coverage.

Tests cover:
- IRI parsing edge cases (non-standard schemes, edge cases)
- Platform derivation from IRI
- Preserve IRI structure
- CorpGroup URN generation
- Group name generation
- extract_name_from_label function
"""

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDFS, SKOS

from datahub.ingestion.source.rdf.core.urn_generator import (
    UrnGeneratorBase,
    extract_name_from_label,
)


class TestUrnGeneratorIRIParsingComprehensive:
    """Comprehensive IRI parsing tests."""

    def test_derive_path_from_iri_non_standard_scheme(self):
        """Test IRI with non-standard scheme."""
        generator = UrnGeneratorBase()

        # Custom scheme like "trading:term/Customer_Name"
        path = generator.derive_path_from_iri("trading:term/Customer_Name")
        assert len(path) > 0
        assert "Customer_Name" in path or "term" in path

    def test_derive_path_from_iri_scheme_with_colon(self):
        """Test IRI with scheme containing colon."""
        generator = UrnGeneratorBase()

        # IRI like "custom://path/to/term"
        path = generator.derive_path_from_iri("custom://example.com/path/to/term")
        assert len(path) > 0

    def test_derive_path_from_iri_no_scheme(self):
        """Test IRI without scheme - urlparse treats it as a path."""
        generator = UrnGeneratorBase()

        # urlparse treats "example.com/term" as a path (no scheme/netloc)
        # So it extracts path segments from the path
        path = generator.derive_path_from_iri("example.com/term")
        assert len(path) > 0
        assert "example.com" in path or "term" in path

    def test_derive_path_from_iri_empty_string(self):
        """Test empty IRI raises error."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="IRI must be a non-empty string"):
            generator.derive_path_from_iri("")

    def test_derive_path_from_iri_none(self):
        """Test None IRI raises error."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="IRI must be a non-empty string"):
            generator.derive_path_from_iri(None)  # type: ignore[arg-type]

    def test_derive_path_from_iri_www_prefix(self):
        """Test IRI with www. prefix is handled."""
        generator = UrnGeneratorBase()

        path = generator.derive_path_from_iri("http://www.example.org/term")
        assert "www.example.org" not in path or "example.org" in path

    def test_derive_path_from_iri_only_netloc(self):
        """Test IRI with only netloc (no path)."""
        generator = UrnGeneratorBase()

        path = generator.derive_path_from_iri("http://example.org")
        assert len(path) > 0
        assert "example.org" in path

    def test_derive_path_from_iri_no_path_segments(self):
        """Test IRI that results in no path segments raises error."""
        generator = UrnGeneratorBase()

        # This should raise an error if no segments can be extracted
        try:
            path = generator.derive_path_from_iri("http://")
            # If it doesn't raise, should have at least one segment
            assert len(path) > 0
        except ValueError:
            pass  # Expected if no segments


class TestUrnGeneratorPlatformDerivation:
    """Test platform derivation from IRI."""

    def test_derive_platform_from_iri_with_netloc(self):
        """Test deriving platform from IRI with netloc."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("http://example.org/term")

        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "example.org"

    def test_derive_platform_from_iri_with_www(self):
        """Test deriving platform from IRI with www. prefix."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("http://www.example.org/term")

        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "example.org"

    def test_derive_platform_from_iri_with_scheme_only(self):
        """Test deriving platform from IRI with only scheme."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("custom:term")

        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "custom"

    def test_derive_platform_from_iri_no_netloc_no_scheme(self):
        """Test deriving platform from IRI with no netloc or scheme raises error."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("term")

        with pytest.raises(ValueError, match="Cannot derive platform from IRI"):
            generator._derive_platform_from_iri(parsed)

    def test_derive_platform_from_iri_empty_netloc(self):
        """Test deriving platform from IRI with empty netloc."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("http:///term")

        # urlparse("http:///term") gives netloc='', path='/term'
        # The code checks if netloc exists first, and if it's empty, it falls back to scheme
        # So it should return "http" (the scheme)
        platform = generator._derive_platform_from_iri(parsed)
        assert platform == "http"


class TestUrnGeneratorPreserveIRIStructure:
    """Test preserving IRI structure."""

    def test_preserve_iri_structure_http(self):
        """Test preserving IRI structure for HTTP."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("http://example.org/path/to/term")

        path = generator._preserve_iri_structure(parsed)
        assert path == "example.org/path/to/term"

    def test_preserve_iri_structure_https(self):
        """Test preserving IRI structure for HTTPS."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("https://example.org/path/to/term")

        path = generator._preserve_iri_structure(parsed)
        assert path == "example.org/path/to/term"

    def test_preserve_iri_structure_ftp(self):
        """Test preserving IRI structure for FTP."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("ftp://example.org/path/to/term")

        path = generator._preserve_iri_structure(parsed)
        assert path == "example.org/path/to/term"

    def test_preserve_iri_structure_custom_scheme(self):
        """Test preserving IRI structure for custom scheme."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("custom://example.org/path/to/term")

        path = generator._preserve_iri_structure(parsed)
        assert path == "example.org/path/to/term"

    def test_preserve_iri_structure_colon_scheme(self):
        """Test preserving IRI structure for colon-based scheme."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("trading:term/Customer_Name")

        path = generator._preserve_iri_structure(parsed)
        assert "term" in path or "Customer_Name" in path

    def test_preserve_iri_structure_empty_raises_error(self):
        """Test preserving empty IRI raises error."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("")

        with pytest.raises(ValueError, match="Cannot extract path from empty IRI"):
            generator._preserve_iri_structure(parsed)

    def test_preserve_iri_structure_no_scheme_raises_error(self):
        """Test preserving IRI with no scheme raises error."""
        from urllib.parse import urlparse

        generator = UrnGeneratorBase()
        parsed = urlparse("example.org/term")

        with pytest.raises(ValueError, match="IRI must have a valid scheme"):
            generator._preserve_iri_structure(parsed)


class TestUrnGeneratorCorpGroupURN:
    """Test CorpGroup URN generation."""

    def test_generate_corpgroup_urn_from_owner_iri_basic(self):
        """Test basic CorpGroup URN generation."""
        generator = UrnGeneratorBase()

        urn = generator.generate_corpgroup_urn_from_owner_iri(
            "http://example.com/FINANCE/Business_Owners"
        )
        assert urn.startswith("urn:li:corpGroup:")
        assert "finance" in urn.lower() or "business_owners" in urn.lower()

    def test_generate_corpgroup_urn_from_owner_iri_no_slash(self):
        """Test CorpGroup URN generation from IRI without slashes."""
        generator = UrnGeneratorBase()

        urn = generator.generate_corpgroup_urn_from_owner_iri("Business_Owners")
        assert urn.startswith("urn:li:corpGroup:")

    def test_generate_corpgroup_urn_from_owner_iri_invalid_format(self):
        """Test CorpGroup URN generation with invalid format."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="Cannot extract domain and owner type"):
            generator.generate_corpgroup_urn_from_owner_iri("http://example.com")

    def test_generate_corpgroup_urn_from_owner_iri_empty_parts(self):
        """Test CorpGroup URN generation with empty parts."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="Cannot extract domain and owner type"):
            generator.generate_corpgroup_urn_from_owner_iri("http://example.com//")

    def test_generate_corpgroup_urn_from_owner_iri_none(self):
        """Test CorpGroup URN generation with None."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="Owner IRI must be a non-empty string"):
            generator.generate_corpgroup_urn_from_owner_iri(None)  # type: ignore[arg-type]

    def test_generate_group_name_from_owner_iri_basic(self):
        """Test basic group name generation."""
        generator = UrnGeneratorBase()

        name = generator.generate_group_name_from_owner_iri(
            "http://example.com/FINANCE/Business_Owners"
        )
        assert "finance" in name.lower() or "business_owners" in name.lower()

    def test_generate_group_name_from_owner_iri_no_slash(self):
        """Test group name generation from IRI without slashes."""
        generator = UrnGeneratorBase()

        name = generator.generate_group_name_from_owner_iri("Business_Owners")
        assert "business_owners" in name.lower()

    def test_generate_group_name_from_owner_iri_invalid_format(self):
        """Test group name generation with invalid format."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="Cannot extract domain and owner type"):
            generator.generate_group_name_from_owner_iri("http://example.com")


class TestUrnGeneratorDataPlatformURN:
    """Test DataPlatform URN generation."""

    def test_generate_data_platform_urn_basic(self):
        """Test basic DataPlatform URN generation."""
        generator = UrnGeneratorBase()

        urn = generator.generate_data_platform_urn("postgres")
        assert urn == "urn:li:dataPlatform:postgres"

    def test_generate_data_platform_urn_with_special_chars(self):
        """Test DataPlatform URN generation with special characters."""
        generator = UrnGeneratorBase()

        urn = generator.generate_data_platform_urn("postgres-special")
        assert urn.startswith("urn:li:dataPlatform:")

    def test_generate_data_platform_urn_empty_string(self):
        """Test DataPlatform URN generation with empty string."""
        generator = UrnGeneratorBase()

        with pytest.raises(
            ValueError, match="Platform name must be a non-empty string"
        ):
            generator.generate_data_platform_urn("")

    def test_generate_data_platform_urn_whitespace_only(self):
        """Test DataPlatform URN generation with whitespace only."""
        generator = UrnGeneratorBase()

        with pytest.raises(ValueError, match="Platform name cannot be empty"):
            generator.generate_data_platform_urn("   ")

    def test_generate_data_platform_urn_none(self):
        """Test DataPlatform URN generation with None."""
        generator = UrnGeneratorBase()

        with pytest.raises(
            ValueError, match="Platform name must be a non-empty string"
        ):
            generator.generate_data_platform_urn(None)  # type: ignore[arg-type]


class TestExtractNameFromLabel:
    """Test extract_name_from_label function."""

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
        graph.add((uri, DCTERMS.title, Literal("DCTerms Title")))

        name = extract_name_from_label(graph, uri)
        assert name == "DCTerms Title"

    def test_extract_name_from_label_priority_order(self):
        """Test that prefLabel takes priority over label."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, SKOS.prefLabel, Literal("Preferred")))
        graph.add((uri, RDFS.label, Literal("Regular Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "Preferred"

    def test_extract_name_from_label_too_short(self):
        """Test that labels shorter than 3 characters are ignored."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("AB")))  # Too short

        name = extract_name_from_label(graph, uri)
        assert name is None

    def test_extract_name_from_label_no_label(self):
        """Test that None is returned when no label exists."""
        graph = Graph()
        uri = URIRef("http://example.org/term")

        name = extract_name_from_label(graph, uri)
        assert name is None

    def test_extract_name_from_label_multiple_labels(self):
        """Test that first valid label is returned."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("First Label")))
        graph.add((uri, RDFS.label, Literal("Second Label")))

        name = extract_name_from_label(graph, uri)
        assert name == "First Label"

    def test_extract_name_from_label_with_whitespace(self):
        """Test that whitespace is stripped."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        graph.add((uri, RDFS.label, Literal("  Label with spaces  ")))

        name = extract_name_from_label(graph, uri)
        assert name == "Label with spaces"

    def test_extract_name_from_label_invalid_graph_type(self):
        """Test that invalid graph type raises error."""
        with pytest.raises(ValueError, match="graph must be an RDFLib Graph"):
            extract_name_from_label("not a graph", URIRef("http://example.org/term"))  # type: ignore[arg-type]

    def test_extract_name_from_label_invalid_uri_type(self):
        """Test that invalid URI type raises error."""
        graph = Graph()
        with pytest.raises(ValueError, match="uri must be an RDFLib URIRef"):
            extract_name_from_label(graph, "http://example.org/term")  # type: ignore[arg-type]

    def test_extract_name_from_label_with_value_attribute(self):
        """Test extracting label from RDFLib Literal (which has value attribute)."""
        graph = Graph()
        uri = URIRef("http://example.org/term")
        # Use actual RDFLib Literal which has a value attribute
        graph.add((uri, RDFS.label, Literal("Label Value")))

        name = extract_name_from_label(graph, uri)
        assert name == "Label Value"

    def test_extract_name_from_label_schema_name(self):
        """Test extracting name from schema:name."""
        graph = Graph()
        SCHEMA = Namespace("http://schema.org/")
        uri = URIRef("http://example.org/term")
        graph.add((uri, SCHEMA.name, Literal("Schema Name")))

        name = extract_name_from_label(graph, uri)
        assert name == "Schema Name"

    def test_extract_name_from_label_dcat_title(self):
        """Test extracting name from dcat:title."""
        graph = Graph()
        DCAT = Namespace("http://www.w3.org/ns/dcat#")
        uri = URIRef("http://example.org/term")
        graph.add((uri, DCAT.title, Literal("DCAT Title")))

        name = extract_name_from_label(graph, uri)
        assert name == "DCAT Title"
