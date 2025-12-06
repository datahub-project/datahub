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
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
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
            relationships={"broader": [], "narrower": []},
            custom_properties={
                "rdf:originalIRI": "http://example.org/AccountIdentifier"
            },
            path_segments=("example.org", "AccountIdentifier"),
        )

        mcps = self.mcp_builder.build_mcps(term)

        self.assertEqual(len(mcps), 1)
        mcp = mcps[0]
        self.assertEqual(mcp.entityUrn, term.urn)
        self.assertEqual(mcp.aspect.name, "Account Identifier")
        self.assertEqual(mcp.aspect.definition, "A unique identifier for an account")

    def test_build_term_info_mcp_with_default_definition(self):
        """Test MCP builder provides default definition when None."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/NoDefTerm",
            name="No Definition Term",
            definition=None,
            relationships={"broader": [], "narrower": []},
            custom_properties={},
            path_segments=("example.org", "NoDefTerm"),
        )

        mcps = self.mcp_builder.build_mcps(term)

        self.assertEqual(len(mcps), 1)
        # Default definition should be generated
        self.assertIn("Glossary term:", mcps[0].aspect.definition)

    def test_build_term_info_mcp_with_custom_properties(self):
        """Test that custom properties are included in MCP."""
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:example.org/CustomPropTerm",
            name="Custom Properties Term",
            definition="Test term",
            relationships={"broader": [], "narrower": []},
            custom_properties={
                "rdf:originalIRI": "http://example.org/CustomPropTerm",
                "skos:notation": "CPT-001",
            },
            path_segments=("example.org", "CustomPropTerm"),
        )

        mcps = self.mcp_builder.build_mcps(term)

        self.assertEqual(mcps[0].aspect.customProperties["skos:notation"], "CPT-001")

    def test_build_all_mcps(self):
        """Test building MCPs for multiple terms."""
        terms = [
            DataHubGlossaryTerm(
                urn=f"urn:li:glossaryTerm:example.org/Term{i}",
                name=f"Term {i}",
                definition=f"Definition {i}",
                relationships={"broader": [], "narrower": []},
                custom_properties={},
                path_segments=("example.org", f"Term{i}"),
            )
            for i in range(3)
        ]

        mcps = self.mcp_builder.build_all_mcps(terms)

        self.assertEqual(len(mcps), 3)


class TestGlossaryTermMCPBuilderRelationships(unittest.TestCase):
    """Test cases for relationship MCP building."""

    def setUp(self):
        """Set up test fixtures."""
        self.mcp_builder = GlossaryTermMCPBuilder()

    def test_build_broader_relationship_mcp(self):
        """Test building isRelatedTerms MCP for broader relationships."""
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/ParentTerm",
                relationship_type=RelationshipType.BROADER,
                properties={},
            )
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Should create isRelatedTerms MCP for the child
        self.assertEqual(len(mcps), 1)
        self.assertEqual(mcps[0].entityUrn, "urn:li:glossaryTerm:example.org/ChildTerm")
        self.assertIsNotNone(mcps[0].aspect.isRelatedTerms)
        self.assertIn(
            "urn:li:glossaryTerm:example.org/ParentTerm", mcps[0].aspect.isRelatedTerms
        )

    def test_no_has_related_terms_for_broader(self):
        """Test that hasRelatedTerms is NOT created for broader relationships."""
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/ParentTerm",
                relationship_type=RelationshipType.BROADER,
                properties={},
            )
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Check that no MCP has hasRelatedTerms set
        for mcp in mcps:
            has_related = getattr(mcp.aspect, "hasRelatedTerms", None)
            self.assertTrue(
                has_related is None or len(has_related) == 0,
                f"hasRelatedTerms should not be set, but found: {has_related}",
            )

    def test_aggregate_multiple_broader_relationships(self):
        """Test aggregation of multiple broader relationships for same child."""
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/Parent1",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/Parent2",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Should create one MCP with both parents
        self.assertEqual(len(mcps), 1)
        self.assertEqual(len(mcps[0].aspect.isRelatedTerms), 2)

    def test_deduplicate_relationships_in_mcp(self):
        """Test that duplicate relationships are deduplicated in MCP."""
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/ParentTerm",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                target_urn="urn:li:glossaryTerm:example.org/ParentTerm",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Should deduplicate to 1 parent
        self.assertEqual(len(mcps), 1)
        self.assertEqual(len(mcps[0].aspect.isRelatedTerms), 1)

    def test_multiple_children_create_separate_mcps(self):
        """Test that multiple children create separate MCPs."""
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/Child1",
                target_urn="urn:li:glossaryTerm:example.org/Parent",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/Child2",
                target_urn="urn:li:glossaryTerm:example.org/Parent",
                relationship_type=RelationshipType.BROADER,
                properties={},
            ),
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Should create 2 MCPs, one for each child
        self.assertEqual(len(mcps), 2)

        entity_urns = [mcp.entityUrn for mcp in mcps]
        self.assertIn("urn:li:glossaryTerm:example.org/Child1", entity_urns)
        self.assertIn("urn:li:glossaryTerm:example.org/Child2", entity_urns)

    def test_narrower_not_creating_relationship_mcp(self):
        """Test that NARROWER relationships don't create separate isRelatedTerms MCPs."""
        # Per spec, narrower is the inverse of broader
        # If ChildTerm has broader:ParentTerm, ParentTerm implicitly has narrower:ChildTerm
        # We only send isRelatedTerms for the broader direction (child -> parent)
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/ParentTerm",
                target_urn="urn:li:glossaryTerm:example.org/ChildTerm",
                relationship_type=RelationshipType.NARROWER,
                properties={},
            )
        ]

        mcps = self.mcp_builder.build_relationship_mcps(relationships)

        # Should create no MCPs for narrower (only broader creates MCPs)
        self.assertEqual(len(mcps), 0)


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
            relationships={
                "broader": ["urn:li:glossaryTerm:fibo/AccountIdentifier"],
                "narrower": [],
            },
            custom_properties={
                "rdf:originalIRI": "http://example.org/AccountIdentifier"
            },
            path_segments=("example.org", "AccountIdentifier"),
        )

        # Build term MCPs
        term_mcps = self.mcp_builder.build_mcps(term)
        self.assertEqual(len(term_mcps), 1)  # Just term info

        # Build relationship MCPs separately
        relationships = [
            DataHubRelationship(
                source_urn="urn:li:glossaryTerm:example.org/AccountIdentifier",
                target_urn="urn:li:glossaryTerm:fibo/AccountIdentifier",
                relationship_type=RelationshipType.BROADER,
                properties={},
            )
        ]
        rel_mcps = self.mcp_builder.build_relationship_mcps(relationships)
        self.assertEqual(len(rel_mcps), 1)  # isRelatedTerms for broader

        # Total should be 2 MCPs
        all_mcps = term_mcps + rel_mcps
        self.assertEqual(len(all_mcps), 2)


if __name__ == "__main__":
    unittest.main()
