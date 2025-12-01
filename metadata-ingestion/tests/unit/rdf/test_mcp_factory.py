#!/usr/bin/env python3
"""
Unit tests for MCPFactory.

Tests the shared MCP creation factory that eliminates duplication
between DataHubTarget and DataHubIngestionTarget.
"""

import unittest
from unittest.mock import Mock

from datahub.ingestion.source.rdf.entities.data_product.ast import (
    DataHubDataProduct,
)
from datahub.ingestion.source.rdf.entities.data_product.mcp_builder import (
    DataProductMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.dataset.ast import DataHubDataset
from datahub.ingestion.source.rdf.entities.dataset.mcp_builder import (
    DatasetMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.mcp_builder import (
    DomainMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)

# MCPFactory has been distributed to entity modules
# Import entity MCP builders instead
from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
    GlossaryTermMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.lineage.ast import (
    DataHubLineageRelationship,
)
from datahub.ingestion.source.rdf.entities.lineage.mcp_builder import (
    LineageMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)
from datahub.ingestion.source.rdf.entities.relationship.mcp_builder import (
    RelationshipMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
)
from datahub.ingestion.source.rdf.entities.structured_property.mcp_builder import (
    StructuredPropertyMCPBuilder,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.domain_urn import DomainUrn
from datahub.utilities.urns.structured_properties_urn import StructuredPropertyUrn


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

    def test_create_dataset_mcp(self):
        """Test creating dataset MCP."""
        dataset = DataHubDataset(
            urn=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
            ),
            name="test_table",
            description="Test dataset",
            platform="urn:li:dataPlatform:postgres",
            environment="PROD",
            schema_fields=[],
            custom_properties={"key": "value"},
        )

        mcp_builder = DatasetMCPBuilder()
        mcps = mcp_builder.build_mcps(dataset)

        self.assertIsInstance(mcps, list)
        self.assertGreater(len(mcps), 0)
        self.assertEqual(str(mcps[0].entityUrn), str(dataset.urn))
        self.assertEqual(mcps[0].aspect.name, "test_table")
        self.assertEqual(mcps[0].aspect.description, "Test dataset")

    def test_create_dataset_mcp_with_schema(self):
        """Test creating dataset MCP with schema fields."""
        from datahub.metadata.schema_classes import SchemaFieldClass, StringTypeClass

        schema_field = SchemaFieldClass(
            fieldPath="column1", type=StringTypeClass(), nativeDataType="VARCHAR"
        )

        dataset = DataHubDataset(
            urn=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
            ),
            name="test_table",
            description="Test dataset",
            platform="urn:li:dataPlatform:postgres",
            environment="PROD",
            schema_fields=[schema_field],
            custom_properties={},
        )

        mcp_builder = DatasetMCPBuilder()
        mcps = mcp_builder.build_mcps(dataset)

        # Should have 2 MCPs: properties and schema
        self.assertEqual(len(mcps), 2)
        # Second MCP should be schema
        self.assertIsNotNone(mcps[1].aspect.fields)
        self.assertEqual(len(mcps[1].aspect.fields), 1)

    def test_create_structured_property_mcp(self):
        """Test creating structured property MCP."""
        prop = DataHubStructuredProperty(
            urn=StructuredPropertyUrn.from_string("urn:li:structuredProperty:prop1"),
            name="prop1",
            description="Test property",
            value_type="urn:li:dataType:datahub.string",
            cardinality="SINGLE",
            entity_types=["DATASET"],
            allowed_values=["value1", "value2"],
        )

        mcp_builder = StructuredPropertyMCPBuilder()
        mcps = mcp_builder.build_mcps(prop)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), str(prop.urn))
        self.assertIsNotNone(mcp.aspect)
        self.assertEqual(mcp.aspect.displayName, "prop1")
        self.assertEqual(mcp.aspect.valueType, "urn:li:dataType:datahub.string")
        self.assertEqual(len(mcp.aspect.allowedValues), 2)

    def test_create_structured_property_mcp_multiple(self):
        """Test creating structured property MCP with MULTIPLE cardinality."""
        prop = DataHubStructuredProperty(
            urn=StructuredPropertyUrn.from_string("urn:li:structuredProperty:prop2"),
            name="prop2",
            description="Test property",
            value_type="urn:li:dataType:datahub.string",
            cardinality="MULTIPLE",
            entity_types=["DATASET"],
        )

        mcp_builder = StructuredPropertyMCPBuilder()
        mcps = mcp_builder.build_mcps(prop)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        from datahub.metadata.schema_classes import PropertyCardinalityClass

        self.assertEqual(mcp.aspect.cardinality, PropertyCardinalityClass.MULTIPLE)

    def test_create_data_product_mcp(self):
        """Test creating data product MCP."""
        # Use proper dataset URN format
        proper_dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
        )
        product = DataHubDataProduct(
            urn="urn:li:dataProduct:product1",
            name="Product 1",
            description="Test product",
            domain="urn:li:domain:test",
            owner="urn:li:corpGroup:test_team",
            owner_type="BUSINESS_OWNER",  # Owner type required (supports custom types)
            assets=[proper_dataset_urn],
            properties={"key": "value"},
        )

        mcp_builder = DataProductMCPBuilder()
        mcps = mcp_builder.build_mcps(product)

        self.assertIsInstance(mcps, list)
        self.assertGreater(len(mcps), 0)

    def test_create_domain_mcp(self):
        """Test creating domain MCP with datasets."""
        from datahub.ingestion.source.rdf.entities.dataset.ast import (
            DataHubDataset,
        )
        from datahub.utilities.urns.dataset_urn import DatasetUrn

        domain = DataHubDomain(
            path_segments=["test", "domain"],
            urn=DomainUrn.from_string("urn:li:domain:test_domain"),
            name="test_domain",
            description="Test domain",
            parent_domain_urn=DomainUrn.from_string("urn:li:domain:parent"),
        )

        # Add a dataset so domain is created
        dataset = DataHubDataset(
            urn=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:test_platform,test_dataset,PROD)"
            ),
            name="test_dataset",
            environment="PROD",
            path_segments=["test", "domain", "test_dataset"],
        )
        domain.datasets.append(dataset)

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), str(domain.urn))
        self.assertEqual(mcp.aspect.name, "test_domain")
        self.assertEqual(mcp.aspect.description, "Test domain")
        self.assertEqual(str(mcp.aspect.parentDomain), str(domain.parent_domain_urn))

    def test_create_domain_mcp_no_parent(self):
        """Test creating domain MCP without parent (with datasets)."""
        from datahub.ingestion.source.rdf.entities.dataset.ast import (
            DataHubDataset,
        )
        from datahub.utilities.urns.dataset_urn import DatasetUrn

        domain = DataHubDomain(
            path_segments=["root"],
            urn=DomainUrn.from_string("urn:li:domain:root"),
            name="root",
            description="Root domain",
        )

        # Add a dataset so domain is created
        dataset = DataHubDataset(
            urn=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:test_platform,test_dataset,PROD)"
            ),
            name="test_dataset",
            environment="PROD",
            path_segments=["root", "test_dataset"],
        )
        domain.datasets.append(dataset)

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertIsNone(mcp.aspect.parentDomain)

    def test_create_domain_mcp_no_datasets(self):
        """Test that domain MCP is not created when domain has no datasets (only terms)."""
        from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
            DataHubGlossaryTerm,
        )

        domain = DataHubDomain(
            path_segments=["test", "domain"],
            urn=DomainUrn.from_string("urn:li:domain:test_domain"),
            name="test_domain",
            description="Test domain",
        )

        # Add only a glossary term (no datasets)
        term = DataHubGlossaryTerm(
            urn="urn:li:glossaryTerm:test/domain/Term",
            name="Term",
            path_segments=["test", "domain", "Term"],
        )
        domain.glossary_terms.append(term)

        mcp_builder = DomainMCPBuilder()
        mcps = mcp_builder.build_mcps(domain)
        mcp = mcps[0] if mcps else None

        # Should return None since domain has no datasets
        self.assertIsNone(mcp)

    def test_create_lineage_mcp(self):
        """Test creating lineage MCP."""
        lineage = DataHubLineageRelationship(
            source_urn="urn:li:dataset:source",
            target_urn="urn:li:dataset:target",
            lineage_type=Mock(),
        )
        lineage.lineage_type.value = "used"

        mcp_builder = LineageMCPBuilder()
        # build_mcps returns empty for single relationships (needs aggregation)
        # Use build_all_mcps instead
        mcps = mcp_builder.build_all_mcps([lineage])
        mcp = mcps[0] if mcps else None

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), "urn:li:dataset:target")
        self.assertIsNotNone(mcp.aspect)
        self.assertGreater(len(mcp.aspect.upstreams), 0)
        self.assertEqual(str(mcp.aspect.upstreams[0].dataset), "urn:li:dataset:source")

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

    def test_create_dataset_domain_association_mcp(self):
        """Test creating dataset-domain association MCP."""
        mcp = DatasetMCPBuilder.create_dataset_domain_association_mcp(
            dataset_urn="urn:li:dataset:test", domain_urn="urn:li:domain:test"
        )

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), "urn:li:dataset:test")
        self.assertIsNotNone(mcp.aspect)
        self.assertIn("urn:li:domain:test", mcp.aspect.domains)

    def test_create_structured_property_values_mcp(self):
        """Test creating structured property values MCP."""
        from datahub.ingestion.source.rdf.entities.structured_property.ast import (
            DataHubStructuredPropertyValue,
        )

        prop_values = [
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop1",
                property_name="prop1",
                value="value1",
            ),
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop2",
                property_name="prop2",
                value="value2",
            ),
        ]

        mcp = StructuredPropertyMCPBuilder.create_structured_property_values_mcp(
            entity_urn="urn:li:dataset:test", prop_values=prop_values
        )

        self.assertIsNotNone(mcp)
        self.assertEqual(str(mcp.entityUrn), "urn:li:dataset:test")
        self.assertIsNotNone(mcp.aspect)
        self.assertEqual(len(mcp.aspect.properties), 2)

    def test_create_structured_property_values_mcp_skips_empty(self):
        """Test that empty/null property values are skipped."""
        from datahub.ingestion.source.rdf.entities.structured_property.ast import (
            DataHubStructuredPropertyValue,
        )

        prop_values = [
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop1",
                property_name="prop1",
                value="value1",
            ),
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop2",
                property_name="prop2",
                value=None,  # Empty value
            ),
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop3",
                property_name="prop3",
                value="",  # Empty string
            ),
        ]

        mcp = StructuredPropertyMCPBuilder.create_structured_property_values_mcp(
            entity_urn="urn:li:dataset:test", prop_values=prop_values
        )

        # Should only have one property (the non-empty one)
        self.assertEqual(len(mcp.aspect.properties), 1)

    def test_create_structured_property_values_mcp_all_empty_raises(self):
        """Test that all empty property values raises ValueError."""
        from datahub.ingestion.source.rdf.entities.structured_property.ast import (
            DataHubStructuredPropertyValue,
        )

        prop_values = [
            DataHubStructuredPropertyValue(
                entity_urn="urn:li:dataset:test",
                entity_type="DATASET",
                property_urn="urn:li:structuredProperty:prop1",
                property_name="prop1",
                value=None,
            )
        ]

        with self.assertRaises(ValueError) as context:
            StructuredPropertyMCPBuilder.create_structured_property_values_mcp(
                entity_urn="urn:li:dataset:test", prop_values=prop_values
            )

        self.assertIn("No valid structured property values", str(context.exception))


if __name__ == "__main__":
    unittest.main()
