"""
Tests for GlossaryTermMCPBuilder

Tests the creation of DataHub MCPs for glossary terms.
"""

import unittest

from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
    GlossaryTermMCPBuilder,
)


class TestGlossaryTermMCPBuilder(unittest.TestCase):
    """Test cases for GlossaryTermMCPBuilder."""

    def setUp(self):
        """Set up test fixtures."""
        self.mcp_builder = GlossaryTermMCPBuilder()

    def test_build_term_info_mcp(self):
        """Test building GlossaryTermInfo MCP."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/AccountIdentifier",
            name="Account Identifier",
            definition="A unique identifier for an account",
            source="http://example.org",
            custom_properties={
                "rdf:originalIRI": "http://example.org/AccountIdentifier"
            },
            path_segments=["example.org", "AccountIdentifier"],
        )

        mcps = self.mcp_builder.build_mcps(term)

        self.assertEqual(len(mcps), 1)
        mcp = mcps[0]
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryTermInfoClass

        assert isinstance(mcp.aspect, GlossaryTermInfoClass)
        self.assertEqual(mcp.entityUrn, term.urn)
        self.assertEqual(mcp.aspect.name, "Account Identifier")
        self.assertEqual(mcp.aspect.definition, "A unique identifier for an account")

    def test_build_term_info_mcp_with_default_definition(self):
        """Test MCP builder provides default definition when None."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/NoDefTerm",
            name="No Definition Term",
            definition=None,
            custom_properties={},
            path_segments=["example.org", "NoDefTerm"],
        )

        mcps = self.mcp_builder.build_mcps(term)

        self.assertEqual(len(mcps), 1)
        # Default definition should be generated
        assert mcps[0].aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryTermInfoClass

        assert isinstance(mcps[0].aspect, GlossaryTermInfoClass)
        self.assertIn("Glossary term:", mcps[0].aspect.definition)

    def test_build_term_info_mcp_with_custom_properties(self):
        """Test that custom properties are included in MCP."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/CustomPropTerm",
            name="Custom Properties Term",
            definition="Test term",
            custom_properties={
                "rdf:originalIRI": "http://example.org/CustomPropTerm",
                "skos:notation": "CPT-001",
            },
            path_segments=["example.org", "CustomPropTerm"],
        )

        mcps = self.mcp_builder.build_mcps(term)

        assert mcps[0].aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryTermInfoClass

        assert isinstance(mcps[0].aspect, GlossaryTermInfoClass)
        self.assertEqual(mcps[0].aspect.customProperties["skos:notation"], "CPT-001")

    def test_build_all_mcps(self):
        """Test building MCPs for multiple terms."""
        terms = [
            DataHubGlossaryTerm(
                urn=f"urn:li:glossaryTerm:example.org/Term{i}",
                name=f"Term {i}",
                definition=f"Definition {i}",
                custom_properties={},
                path_segments=["example.org", f"Term{i}"],
            )
            for i in range(3)
        ]

        mcps = self.mcp_builder.build_all_mcps(terms)

        self.assertEqual(len(mcps), 3)


class TestGlossaryTermMCPBuilderIntegration(unittest.TestCase):
    """Integration tests for GlossaryTermMCPBuilder."""

    def setUp(self):
        """Set up test fixtures."""
        self.mcp_builder = GlossaryTermMCPBuilder()

    def test_full_term_with_relationships(self):
        """Test building all MCPs for a term with relationships."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/AccountIdentifier",
            name="Account Identifier",
            definition="A unique identifier for an account",
            source="http://example.org",
            custom_properties={
                "rdf:originalIRI": "http://example.org/AccountIdentifier"
            },
            path_segments=["example.org", "AccountIdentifier"],
        )

        # Build term MCPs
        term_mcps = self.mcp_builder.build_mcps(term)
        self.assertEqual(len(term_mcps), 1)  # Just term info

        # Relationships are handled by the separate relationship entity, not glossary_term


if __name__ == "__main__":
    unittest.main()
