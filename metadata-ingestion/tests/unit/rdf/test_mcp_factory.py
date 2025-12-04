#!/usr/bin/env python3
"""
Unit tests for MCPFactory.

Tests the shared MCP creation factory that eliminates duplication
between DataHubTarget and DataHubIngestionTarget.
"""

import unittest

from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.mcp_builder import (
    DomainMCPBuilder,
)
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
from datahub.utilities.urns.domain_urn import DomainUrn


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
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryNode:test")
        self.assertIsNotNone(mcp.aspect)
        self.assertEqual(mcp.aspect.name, "test")
        self.assertEqual(mcp.aspect.parentNode, "urn:li:glossaryNode:parent")

    def test_create_glossary_node_mcp_no_parent(self):
        """Test creating glossary node MCP without parent."""
        mcp = GlossaryTermMCPBuilder.create_glossary_node_mcp(
            node_urn="urn:li:glossaryNode:root", node_name="root"
        )

        self.assertIsNotNone(mcp)
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
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryTerm:test")
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
        self.assertIsNone(mcp.aspect.parentNode)

    # Dataset, structured property, data product, and lineage tests removed - not supported in MVP

    def test_create_domain_mcp(self):
        """Test creating domain MCP with glossary terms."""
        from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
            DataHubGlossaryTerm,
        )

        domain = DataHubDomain(
            path_segments=["test", "domain"],
            urn=DomainUrn.from_string("urn:li:domain:test_domain"),
            name="test_domain",
            description="Test domain",
            parent_domain_urn=DomainUrn.from_string("urn:li:domain:parent"),
        )

        # Add a glossary term so domain is created
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test/domain/Term",
            name="Term",
            definition="Test term",
            path_segments=["test", "domain", "Term"],
        )
        domain.glossary_terms.append(term)

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), str(domain.urn))
        self.assertEqual(mcp.aspect.name, "test_domain")
        self.assertEqual(mcp.aspect.description, "Test domain")
        self.assertEqual(str(mcp.aspect.parentDomain), str(domain.parent_domain_urn))

    def test_create_domain_mcp_no_parent(self):
        """Test creating domain MCP without parent (with glossary terms)."""
        from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
            DataHubGlossaryTerm,
        )

        domain = DataHubDomain(
            path_segments=["root"],
            urn=DomainUrn.from_string("urn:li:domain:root"),
            name="root",
            description="Root domain",
        )

        # Add a glossary term so domain is created
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:root/Term",
            name="Term",
            definition="Test term",
            path_segments=["root", "Term"],
        )
        domain.glossary_terms.append(term)

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertIsNone(mcp.aspect.parentDomain)

    def test_create_domain_mcp_no_glossary_terms(self):
        """Test that domain MCP is not created when domain has no glossary terms."""
        domain = DataHubDomain(
            path_segments=["test", "domain"],
            urn=DomainUrn.from_string("urn:li:domain:test_domain"),
            name="test_domain",
            description="Test domain",
        )

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        # Should return None since domain has no glossary terms
        self.assertIsNone(mcp)

    def test_create_relationship_mcp_related(self):
        """Test creating relationship MCP for RELATED."""
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:term1",
            target_urn="urn:li:glossaryTerm:term2",
            relationship_type=RelationshipType.RELATED,
        )

        mcp_builder = RelationshipMCPBuilder()
        # build_mcps returns empty for single relationships (needs aggregation)
        # RELATED relationships are not processed (only BROADER)
        mcps = mcp_builder.build_all_mcps([relationship])
        # RELATED relationships don't create MCPs (only BROADER does)
        self.assertEqual(len(mcps), 0)

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
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryTerm:term1")
        self.assertIsNotNone(mcp.aspect)
        self.assertIn("urn:li:glossaryTerm:term2", mcp.aspect.isRelatedTerms)

    # Dataset domain association and structured property value tests removed - not supported in MVP


if __name__ == "__main__":
    unittest.main()
