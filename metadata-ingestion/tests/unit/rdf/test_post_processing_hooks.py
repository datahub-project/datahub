"""
Tests for post-processing hooks in MCP builders.
"""

import unittest
from unittest.mock import MagicMock

from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.dataset.ast import DataHubDataset
from datahub.ingestion.source.rdf.entities.dataset.mcp_builder import (
    DatasetMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
    GlossaryTermMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
    DataHubStructuredPropertyValue,
)
from datahub.ingestion.source.rdf.entities.structured_property.mcp_builder import (
    StructuredPropertyMCPBuilder,
)


class TestPostProcessingHooks(unittest.TestCase):
    """Test cases for post-processing hooks."""

    def test_dataset_domain_association_hook(self):
        """Test that DatasetMCPBuilder creates domain association MCPs."""
        builder = DatasetMCPBuilder()

        # Create mock graph with domains and datasets
        domain = DataHubDomain(
            urn="urn:li:domain:test.domain",
            name="Test Domain",
            path_segments=("test", "domain"),
            parent_domain_urn=None,
            datasets=[],
            glossary_terms=[],
            subdomains=[],
        )

        dataset = DataHubDataset(
            urn="urn:li:dataset:test.platform/test_dataset",
            name="Test Dataset",
            platform="test.platform",
            environment="PROD",
        )

        domain.datasets = [dataset]

        mock_graph = MagicMock()
        mock_graph.domains = [domain]

        mcps = builder.build_post_processing_mcps(mock_graph)

        self.assertEqual(len(mcps), 1)
        self.assertEqual(mcps[0].entityUrn, str(dataset.urn))
        self.assertIn(str(domain.urn), str(mcps[0].aspect.domains))

    def test_dataset_domain_association_hook_no_domains(self):
        """Test that DatasetMCPBuilder returns empty list when no domains."""
        builder = DatasetMCPBuilder()

        mock_graph = MagicMock()
        mock_graph.domains = []

        mcps = builder.build_post_processing_mcps(mock_graph)

        self.assertEqual(len(mcps), 0)

    def test_glossary_term_post_processing_hook(self):
        """Test that GlossaryTermMCPBuilder creates nodes from domains."""
        builder = GlossaryTermMCPBuilder()

        # Create mock graph with domain containing glossary terms
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test.term",
            name="Test Term",
            definition="Test definition",
            source="http://test.org",
            relationships={"broader": [], "narrower": []},
            custom_properties={},
            path_segments=("test", "term"),
        )

        domain = DataHubDomain(
            urn="urn:li:domain:test.domain",
            name="Test Domain",
            path_segments=("test", "domain"),
            parent_domain_urn=None,
            datasets=[],
            glossary_terms=[term],
            subdomains=[],
        )

        mock_graph = MagicMock()
        mock_graph.domains = [domain]
        mock_graph.glossary_terms = []

        context = {"report": MagicMock()}

        mcps = builder.build_post_processing_mcps(mock_graph, context)

        # Should create at least one MCP (the glossary node)
        self.assertGreater(len(mcps), 0)

        # Check that a glossary node MCP was created
        node_mcps = [mcp for mcp in mcps if "glossaryNode" in str(mcp.entityUrn)]
        self.assertGreater(len(node_mcps), 0)

    def test_structured_property_post_processing_hook(self):
        """Test that StructuredPropertyMCPBuilder creates value assignment MCPs."""
        builder = StructuredPropertyMCPBuilder()

        # Create a structured property definition
        prop = DataHubStructuredProperty(
            urn="urn:li:structuredProperty:test.property",
            name="Test Property",
            description="Test description",
            value_type="string",
            allowed_values=None,
            entity_types=["dataset"],
            cardinality=None,
            properties={},
        )

        # Create a value assignment
        value = DataHubStructuredPropertyValue(
            property_urn="urn:li:structuredProperty:test.property",
            entity_urn="urn:li:dataset:test.platform/test_dataset",
            property_name="test.property",
            entity_type="dataset",
            value="test value",
        )

        mock_graph = MagicMock()
        mock_graph.structured_properties = [prop]
        mock_graph.structured_property_values = [value]

        context = {"report": MagicMock()}

        mcps = builder.build_post_processing_mcps(mock_graph, context)

        # Should create one MCP for the value assignment
        self.assertEqual(len(mcps), 1)
        self.assertEqual(mcps[0].entityUrn, value.entity_urn)

    def test_structured_property_post_processing_hook_skips_undefined(self):
        """Test that StructuredPropertyMCPBuilder skips values for undefined properties."""
        builder = StructuredPropertyMCPBuilder()

        # Create a value assignment for a property that doesn't exist
        value = DataHubStructuredPropertyValue(
            property_urn="urn:li:structuredProperty:undefined.property",
            entity_urn="urn:li:dataset:test.platform/test_dataset",
            property_name="undefined.property",
            entity_type="dataset",
            value="test value",
        )

        mock_graph = MagicMock()
        mock_graph.structured_properties = []  # No properties defined
        mock_graph.structured_property_values = [value]

        context = {"report": MagicMock()}

        mcps = builder.build_post_processing_mcps(mock_graph, context)

        # Should return empty list (value skipped)
        self.assertEqual(len(mcps), 0)

    def test_post_processing_hook_default_implementation(self):
        """Test that default post-processing hook returns empty list."""

        class TestMCPBuilder(EntityMCPBuilder[MagicMock]):
            @property
            def entity_type(self) -> str:
                return "test"

            def build_mcps(self, entity: MagicMock, context: dict = None) -> list:
                return []

            def build_all_mcps(self, entities: list, context: dict = None) -> list:
                return []

        builder = TestMCPBuilder()
        mock_graph = MagicMock()

        mcps = builder.build_post_processing_mcps(mock_graph)

        self.assertEqual(len(mcps), 0)


if __name__ == "__main__":
    unittest.main()
