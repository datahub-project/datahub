#!/usr/bin/env python3
"""
Unit tests for MCPFactory.

Tests the shared MCP creation factory used by DataHubIngestionTarget.
"""

import unittest

from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
    GlossaryTermMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)
from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
    RelationshipMCPBuilder,
)


class TestMCPFactory(unittest.TestCase):
    """Test MCPFactory static methods."""

    def test_create_glossary_node_mcp(self):
        """Test creating glossary node MCP."""
        mcp = GlossaryTermMCPBuilder.create_glossary_node_mcp(
            node_urn="urn:li:glossaryNode:test",
            node_name="test",
            parent_urn="urn:li:glossaryNode:parent",
        )

        self.assertIsNotNone(mcp)
        assert mcp is not None  # Type narrowing for mypy
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryNode:test")
        self.assertIsNotNone(mcp.aspect)
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryNodeInfoClass

        assert isinstance(mcp.aspect, GlossaryNodeInfoClass)
        self.assertEqual(mcp.aspect.name, "test")
        self.assertEqual(mcp.aspect.parentNode, "urn:li:glossaryNode:parent")

    def test_create_glossary_node_mcp_no_parent(self):
        """Test creating glossary node MCP without parent."""
        mcp = GlossaryTermMCPBuilder.create_glossary_node_mcp(
            node_urn="urn:li:glossaryNode:root", node_name="root"
        )

        self.assertIsNotNone(mcp)
        assert mcp is not None  # Type narrowing for mypy
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryNodeInfoClass

        assert isinstance(mcp.aspect, GlossaryNodeInfoClass)
        self.assertIsNone(mcp.aspect.parentNode)

    def test_create_glossary_term_mcp(self):
        """Test creating glossary term MCP."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
            source="http://example.com/test",
            custom_properties={"key": "value"},
        )

        mcp_builder = GlossaryTermMCPBuilder()
        mcps = mcp_builder.build_mcps(
            term, {"parent_node_urn": "urn:li:glossaryNode:parent"}
        )
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        assert mcp is not None  # Type narrowing for mypy
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryTerm:test")
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryTermInfoClass

        assert isinstance(mcp.aspect, GlossaryTermInfoClass)
        self.assertEqual(mcp.aspect.name, "Test Term")
        self.assertEqual(mcp.aspect.definition, "Test definition")
        # parentNode should be set when provided in context
        self.assertEqual(mcp.aspect.parentNode, "urn:li:glossaryNode:parent")
        self.assertEqual(mcp.aspect.termSource, "EXTERNAL")
        self.assertEqual(mcp.aspect.customProperties, {"key": "value"})

    def test_create_glossary_term_mcp_no_parent(self):
        """Test creating glossary term MCP without parent."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test",
            name="Test Term",
            definition="Test definition",
        )

        mcp_builder = GlossaryTermMCPBuilder()
        mcps = mcp_builder.build_mcps(term)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        assert mcp is not None  # Type narrowing for mypy
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryTermInfoClass

        assert isinstance(mcp.aspect, GlossaryTermInfoClass)
        self.assertIsNone(mcp.aspect.parentNode)

    # Dataset, structured property, data product, and lineage tests removed - not supported in MVP
    # Domain MCP tests removed - domains are data structure only, not ingested as DataHub domain entities

    # test_create_relationship_mcp_related removed - RELATED enum value was removed
    # Only BROADER and NARROWER relationship types are supported

    def test_create_relationship_mcp_broader(self):
        """Test creating relationship MCP for BROADER."""
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:term1",
            target_urn="urn:li:glossaryTerm:term2",
            relationship_type=RelationshipType.BROADER,
        )

        mcp_builder = RelationshipMCPBuilder()
        # build_mcps returns empty for single relationships (needs aggregation)
        # Use build_all_mcps instead
        mcps = mcp_builder.build_all_mcps([relationship])
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        assert mcp is not None  # Type narrowing for mypy
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryTerm:term1")
        self.assertIsNotNone(mcp.aspect)
        assert mcp.aspect is not None  # Type narrowing for mypy
        from datahub.metadata.schema_classes import GlossaryRelatedTermsClass

        assert isinstance(mcp.aspect, GlossaryRelatedTermsClass)
        assert mcp.aspect.isRelatedTerms is not None  # Type narrowing for mypy
        self.assertIn("urn:li:glossaryTerm:term2", mcp.aspect.isRelatedTerms)

    # Dataset domain association and structured property value tests removed - not supported in MVP


if __name__ == "__main__":
    unittest.main()
