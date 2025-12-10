#!/usr/bin/env python3
"""
Comprehensive tests for Stage 3: Relationship MCP Creation (DataHub AST → MCPs)

Tests that relationships are correctly converted to MCPs:
- skos:broader and skos:narrower create only inheritance relationships (isRelatedTerms)
- Both relationship types normalize to child → parent inheritance
- Relationships are aggregated correctly
- Multiple relationships to same parent are deduplicated
"""

import os
import sys
import unittest

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)


class TestRelationshipMCPStage3(unittest.TestCase):
    """Test relationship MCP creation in Stage 3 (DataHub AST → MCPs)."""

    def setUp(self):
        """Set up test fixtures."""
        # Note: We don't need to instantiate DataHubIngestionTarget for these tests
        # We're testing the relationship processing logic directly
        pass

    def test_broader_creates_inheritance_relationship(self):
        """Test that skos:broader creates only isRelatedTerms (inheritance)."""
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        # Create relationship: Account_ID broader AccountIdentifier
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )

        # Build MCPs using the actual builder
        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship])

        # Verify: Should have 1 MCP (child inherits from parent)
        self.assertEqual(len(mcps), 1)

        # Find child MCP (isRelatedTerms)
        child_mcp = next(
            (m for m in mcps if str(m.entityUrn) == "urn:li:glossaryTerm:Account_ID"),
            None,
        )
        self.assertIsNotNone(child_mcp, "Should have MCP for child")
        self.assertIsNotNone(child_mcp.aspect.isRelatedTerms)
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier", child_mcp.aspect.isRelatedTerms
        )

    def test_narrower_creates_inheritance_relationship(self):
        """Test that skos:narrower creates only isRelatedTerms (inheritance, normalized)."""
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        # Create relationship: AccountIdentifier narrower Account_ID
        # (parent → child, normalized to child → parent)
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:AccountIdentifier",
            target_urn="urn:li:glossaryTerm:Account_ID",
            relationship_type=RelationshipType.NARROWER,
        )

        # Build MCPs using the actual builder
        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship])

        # Verify: Should have 1 MCP (child inherits from parent, normalized)
        self.assertEqual(len(mcps), 1)

        # Find child MCP (isRelatedTerms, normalized from narrower)
        child_mcp = next(
            (m for m in mcps if str(m.entityUrn) == "urn:li:glossaryTerm:Account_ID"),
            None,
        )
        self.assertIsNotNone(child_mcp, "Should have MCP for child")
        self.assertIsNotNone(child_mcp.aspect.isRelatedTerms)
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier", child_mcp.aspect.isRelatedTerms
        )

    def test_multiple_broader_relationships_aggregated(self):
        """Test that multiple broader relationships are aggregated correctly."""
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        # Create multiple relationships from same child to different parents
        relationship1 = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )
        relationship2 = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:Entity",
            relationship_type=RelationshipType.BROADER,
        )

        # Build MCPs using the actual builder
        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship1, relationship2])

        # Find child MCP (should have both parents in isRelatedTerms)
        child_mcp = next(
            (m for m in mcps if str(m.entityUrn) == "urn:li:glossaryTerm:Account_ID"),
            None,
        )
        self.assertIsNotNone(child_mcp)
        self.assertIsNotNone(child_mcp.aspect.isRelatedTerms)
        self.assertEqual(len(child_mcp.aspect.isRelatedTerms), 2)
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier", child_mcp.aspect.isRelatedTerms
        )
        self.assertIn("urn:li:glossaryTerm:Entity", child_mcp.aspect.isRelatedTerms)

    def test_duplicate_relationships_deduplicated(self):
        """Test that duplicate relationships are deduplicated."""
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        # Create same relationship twice
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )

        # Build MCPs using the actual builder (should deduplicate)
        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship, relationship])  # Duplicate

        # Verify: Should have 1 MCP (child inherits from parent), deduplicated
        self.assertEqual(len(mcps), 1)

        # Find child MCP
        child_mcp = next(
            (m for m in mcps if str(m.entityUrn) == "urn:li:glossaryTerm:Account_ID"),
            None,
        )
        self.assertIsNotNone(child_mcp)
        self.assertEqual(
            len(child_mcp.aspect.isRelatedTerms),
            1,
            "Should deduplicate to single target",
        )


if __name__ == "__main__":
    unittest.main()
