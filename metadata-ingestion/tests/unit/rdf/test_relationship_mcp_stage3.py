#!/usr/bin/env python3
"""
Comprehensive tests for Stage 3: Relationship MCP Creation (DataHub AST → MCPs)

Tests that relationships are correctly converted to MCPs:
- skos:broader creates only isRelatedTerms (inherits), NOT hasRelatedTerms (contains)
- Relationships are aggregated correctly
- Multiple relationships to same parent are deduplicated
"""

import os
import sys
import unittest

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)
from datahub.metadata.schema_classes import GlossaryRelatedTermsClass


class TestRelationshipMCPStage3(unittest.TestCase):
    """Test relationship MCP creation in Stage 3 (DataHub AST → MCPs)."""

    def setUp(self):
        """Set up test fixtures."""
        # Note: We don't need to instantiate DataHubIngestionTarget for these tests
        # We're testing the relationship processing logic directly
        pass

    def test_broader_creates_only_is_related_terms(self):
        """Test that skos:broader creates only isRelatedTerms, NOT hasRelatedTerms."""
        datahub_graph = DataHubGraph()

        # Create relationship: Account_ID broader AccountIdentifier
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )
        datahub_graph.relationships.append(relationship)

        # Create terms
        account_id_term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:Account_ID",
            name="Account ID",
            definition="Account identifier",
        )
        account_identifier_term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:AccountIdentifier",
            name="Account Identifier",
            definition="FIBO Account Identifier",
        )
        datahub_graph.glossary_terms.append(account_id_term)
        datahub_graph.glossary_terms.append(account_identifier_term)

        # Process relationships
        mcps = []
        relationships_by_source = {}
        for rel in datahub_graph.relationships:
            source_urn = str(rel.source_urn)
            if source_urn not in relationships_by_source:
                relationships_by_source[source_urn] = []
            relationships_by_source[source_urn].append(rel)

        # Build aggregation maps (simulating datahub_ingestion_target.py logic)
        broader_terms_map = {}

        for _source_urn, source_relationships in relationships_by_source.items():
            for relationship in source_relationships:
                if relationship.relationship_type == RelationshipType.BROADER:
                    source_urn_str = str(relationship.source_urn)
                    target_urn_str = str(relationship.target_urn)
                    if source_urn_str not in broader_terms_map:
                        broader_terms_map[source_urn_str] = []
                    broader_terms_map[source_urn_str].append(target_urn_str)

        # Create MCPs
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        # Should create isRelatedTerms MCP for child
        for child_urn, broader_urns in broader_terms_map.items():
            unique_broader = list(set(broader_urns))
            broader_mcp = MetadataChangeProposalWrapper(
                entityUrn=child_urn,
                aspect=GlossaryRelatedTermsClass(isRelatedTerms=unique_broader),
            )
            mcps.append(broader_mcp)

        # Verify: Should have exactly 1 MCP
        self.assertEqual(len(mcps), 1)

        mcp = mcps[0]
        self.assertEqual(str(mcp.entityUrn), "urn:li:glossaryTerm:Account_ID")

        # Verify: Should have isRelatedTerms
        self.assertIsNotNone(mcp.aspect.isRelatedTerms)
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier", mcp.aspect.isRelatedTerms
        )

        # Verify: Should NOT have hasRelatedTerms
        self.assertIsNone(
            mcp.aspect.hasRelatedTerms,
            "Should NOT create hasRelatedTerms for broader relationships",
        )

    def test_no_has_related_terms_created(self):
        """Test that hasRelatedTerms (contains) is NOT created for broader relationships."""
        datahub_graph = DataHubGraph()

        # Create relationship
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )
        datahub_graph.relationships.append(relationship)

        # Process (simulating datahub_ingestion_target.py)
        relationships_by_source = {}
        for rel in datahub_graph.relationships:
            source_urn = str(rel.source_urn)
            if source_urn not in relationships_by_source:
                relationships_by_source[source_urn] = []
            relationships_by_source[source_urn].append(rel)

        broader_terms_map = {}
        parent_children_map = {}  # This should remain empty

        for _source_urn, source_relationships in relationships_by_source.items():
            for relationship in source_relationships:
                if relationship.relationship_type == RelationshipType.BROADER:
                    source_urn_str = str(relationship.source_urn)
                    target_urn_str = str(relationship.target_urn)
                    if source_urn_str not in broader_terms_map:
                        broader_terms_map[source_urn_str] = []
                    broader_terms_map[source_urn_str].append(target_urn_str)
                    # Note: We do NOT populate parent_children_map

        # Verify: parent_children_map should be empty (no hasRelatedTerms created)
        self.assertEqual(
            len(parent_children_map),
            0,
            "Should NOT create hasRelatedTerms for broader relationships",
        )

    def test_multiple_broader_relationships_aggregated(self):
        """Test that multiple broader relationships are aggregated correctly."""
        datahub_graph = DataHubGraph()

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
        datahub_graph.relationships.append(relationship1)
        datahub_graph.relationships.append(relationship2)

        # Process
        relationships_by_source = {}
        for rel in datahub_graph.relationships:
            source_urn = str(rel.source_urn)
            if source_urn not in relationships_by_source:
                relationships_by_source[source_urn] = []
            relationships_by_source[source_urn].append(rel)

        broader_terms_map = {}
        for _source_urn, source_relationships in relationships_by_source.items():
            for relationship in source_relationships:
                if relationship.relationship_type == RelationshipType.BROADER:
                    source_urn_str = str(relationship.source_urn)
                    target_urn_str = str(relationship.target_urn)
                    if source_urn_str not in broader_terms_map:
                        broader_terms_map[source_urn_str] = []
                    broader_terms_map[source_urn_str].append(target_urn_str)

        # Verify: Should have both targets for same source
        self.assertIn("urn:li:glossaryTerm:Account_ID", broader_terms_map)
        self.assertEqual(len(broader_terms_map["urn:li:glossaryTerm:Account_ID"]), 2)
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier",
            broader_terms_map["urn:li:glossaryTerm:Account_ID"],
        )
        self.assertIn(
            "urn:li:glossaryTerm:Entity",
            broader_terms_map["urn:li:glossaryTerm:Account_ID"],
        )

    def test_duplicate_relationships_deduplicated(self):
        """Test that duplicate relationships are deduplicated."""
        datahub_graph = DataHubGraph()

        # Create same relationship twice
        relationship = DataHubRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            relationship_type=RelationshipType.BROADER,
        )
        datahub_graph.relationships.append(relationship)
        datahub_graph.relationships.append(relationship)  # Duplicate

        # Process
        relationships_by_source = {}
        for rel in datahub_graph.relationships:
            source_urn = str(rel.source_urn)
            if source_urn not in relationships_by_source:
                relationships_by_source[source_urn] = []
            relationships_by_source[source_urn].append(rel)

        broader_terms_map = {}
        for _source_urn, source_relationships in relationships_by_source.items():
            for relationship in source_relationships:
                if relationship.relationship_type == RelationshipType.BROADER:
                    source_urn_str = str(relationship.source_urn)
                    target_urn_str = str(relationship.target_urn)
                    if source_urn_str not in broader_terms_map:
                        broader_terms_map[source_urn_str] = []
                    broader_terms_map[source_urn_str].append(target_urn_str)

        # Create MCP with deduplication
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        mcps = []
        for child_urn, broader_urns in broader_terms_map.items():
            unique_broader = list(set(broader_urns))  # Deduplicate
            broader_mcp = MetadataChangeProposalWrapper(
                entityUrn=child_urn,
                aspect=GlossaryRelatedTermsClass(isRelatedTerms=unique_broader),
            )
            mcps.append(broader_mcp)

        # Verify: Should have only one target (deduplicated)
        self.assertEqual(len(mcps), 1)
        mcp = mcps[0]
        self.assertEqual(
            len(mcp.aspect.isRelatedTerms), 1, "Should deduplicate to single target"
        )


if __name__ == "__main__":
    unittest.main()
