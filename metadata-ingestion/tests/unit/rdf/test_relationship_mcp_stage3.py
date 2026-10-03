#!/usr/bin/env python3
"""
Comprehensive tests for Stage 3: Relationship MCP Creation (DataHub AST → MCPs)

Tests that aligned native relationships are correctly converted to MCPs.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubNativeRelationship,
    MappingClass,
)
from datahub.metadata.schema_classes import GlossaryRelatedTermsClass


class TestRelationshipMCPStage3(unittest.TestCase):
    """Test relationship MCP creation in Stage 3 (DataHub AST → MCPs)."""

    def test_is_a_creates_inheritance_relationship(self):
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        relationship = DataHubNativeRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            field="isRelatedTerms",
            original_predicate_iri="http://www.w3.org/2004/02/skos/core#broader",
            mapping_class=MappingClass.ALIGNED,
            maps_to="IsA",
        )

        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship])

        self.assertEqual(len(mcps), 1)
        child_mcp = mcps[0]
        self.assertEqual(str(child_mcp.entityUrn), "urn:li:glossaryTerm:Account_ID")
        assert child_mcp.aspect is not None
        assert isinstance(child_mcp.aspect, GlossaryRelatedTermsClass)
        assert child_mcp.aspect.isRelatedTerms is not None
        self.assertIn(
            "urn:li:glossaryTerm:AccountIdentifier", child_mcp.aspect.isRelatedTerms
        )

    def test_is_related_to_creates_related_terms(self):
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        relationship = DataHubNativeRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:CustomerName",
            field="relatedTerms",
            original_predicate_iri="http://www.w3.org/2004/02/skos/core#related",
            mapping_class=MappingClass.ALIGNED,
            maps_to="IsRelatedTo",
        )

        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship])

        self.assertEqual(len(mcps), 1)
        assert mcps[0].aspect is not None
        assert isinstance(mcps[0].aspect, GlossaryRelatedTermsClass)
        assert mcps[0].aspect.relatedTerms is not None
        self.assertIn("urn:li:glossaryTerm:CustomerName", mcps[0].aspect.relatedTerms)

    def test_multiple_relationships_aggregated(self):
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        relationships = [
            DataHubNativeRelationship(
                source_urn="urn:li:glossaryTerm:Account_ID",
                target_urn="urn:li:glossaryTerm:AccountIdentifier",
                field="isRelatedTerms",
                maps_to="IsA",
            ),
            DataHubNativeRelationship(
                source_urn="urn:li:glossaryTerm:Account_ID",
                target_urn="urn:li:glossaryTerm:Entity",
                field="isRelatedTerms",
                maps_to="IsA",
            ),
        ]

        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps(relationships)

        self.assertEqual(len(mcps), 1)
        assert mcps[0].aspect is not None
        assert isinstance(mcps[0].aspect, GlossaryRelatedTermsClass)
        assert mcps[0].aspect.isRelatedTerms is not None
        self.assertEqual(len(mcps[0].aspect.isRelatedTerms), 2)

    def test_duplicate_relationships_deduplicated(self):
        from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
            RelationshipMCPBuilder,
        )

        relationship = DataHubNativeRelationship(
            source_urn="urn:li:glossaryTerm:Account_ID",
            target_urn="urn:li:glossaryTerm:AccountIdentifier",
            field="isRelatedTerms",
            maps_to="IsA",
        )

        builder = RelationshipMCPBuilder()
        mcps = builder.build_all_mcps([relationship, relationship])

        self.assertEqual(len(mcps), 1)
        assert mcps[0].aspect is not None
        assert isinstance(mcps[0].aspect, GlossaryRelatedTermsClass)
        assert mcps[0].aspect.isRelatedTerms is not None
        self.assertEqual(len(mcps[0].aspect.isRelatedTerms), 1)


if __name__ == "__main__":
    unittest.main()
