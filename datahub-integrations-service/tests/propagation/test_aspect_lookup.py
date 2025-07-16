from typing import Any, Optional, Union
from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL
from datahub.metadata.schema_classes import SchemaMetadataClass, SiblingsClass
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.stats_util import ActionStageReport

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    AspectLookup,
)
from datahub_integrations.propagation.propagation.propagation_strategy.aspect_lookup_strategy import (
    AspectBasedStrategy,
    AspectBasedStrategyConfig,
    get_urns_from_aspect,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    DirectionType,
    PropagationDirective,
    RelationshipType,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import EntityPropagator


@pytest.fixture
def graph() -> MagicMock:
    mock = MagicMock(spec=AcrylDataHubGraph)
    mock.graph = MagicMock()
    return mock


@pytest.fixture
def stats() -> MagicMock:
    return MagicMock(spec=ActionStageReport)


@pytest.fixture
def aspect_lookup() -> AspectLookup:
    return AspectLookup(aspect_name="schemaMetadata", field="platform")


@pytest.fixture
def strategy(
    graph: MagicMock, stats: MagicMock, aspect_lookup: AspectLookup
) -> AspectBasedStrategy:
    config = AspectBasedStrategyConfig()
    return AspectBasedStrategy(config, graph, stats)


@pytest.fixture
def propagator() -> MagicMock:
    return MagicMock(spec=EntityPropagator)


class TestAspectBasedStrategy:
    @patch(
        "datahub_integrations.propagation.propagation.propagation_strategy.aspect_lookup_strategy.get_urns_from_aspect"
    )
    def test_propagate(
        self,
        mock_get_urns: MagicMock,
        strategy: AspectBasedStrategy,
        propagator: MagicMock,
    ) -> None:
        """Test that propagate correctly handles the propagation flow."""
        # Setup
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        directive = PropagationDirective(
            entity=entity_urn,
            propagate=True,
            operation="ADD",
            relationships={RelationshipType.SIBLING: [DirectionType.ALL]},
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)",
        )
        context = SourceDetails(
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)",
        )

        lookup = AspectLookup(aspect_name="Siblings", field="sibling")
        # Mock sibling URNs returned from aspect lookup
        sibling_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,test.test,PROD),fieldA)"
        mock_get_urns.return_value = [sibling_urn]

        # Mock the propagator to return a MCP
        mock_mcp = MagicMock(spec=MetadataChangeProposalWrapper)
        propagator.create_property_change_proposal.return_value = [mock_mcp]

        # Execute
        result = list(strategy.propagate(lookup, propagator, directive, context))

        # Verify
        mock_get_urns.assert_called_once_with(strategy.graph, entity_urn, lookup)

        # Verify the propagator was called with the correct URN
        propagator.create_property_change_proposal.assert_called_once()
        args, _ = propagator.create_property_change_proposal.call_args
        assert args[0] == directive
        assert str(args[1]) == sibling_urn
        assert args[2] == context

        # Verify the result contains the MCP
        assert len(result) == 1
        assert result[0] == mock_mcp

    @patch(
        "datahub_integrations.propagation.propagation.propagation_strategy.aspect_lookup_strategy.get_urns_from_aspect"
    )
    def test_propagate_different_entity_type(
        self,
        mock_get_urns: MagicMock,
        strategy: AspectBasedStrategy,
        propagator: MagicMock,
    ) -> None:
        """Test that propagation is skipped for entities of different types."""
        # Setup
        entity_urn = "urn:li:schemaField:(urn:li:chart:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        directive = PropagationDirective(
            entity=entity_urn,
            propagate=True,
            operation="ADD",
            relationships={RelationshipType.SIBLING: [DirectionType.ALL]},
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)",
        )
        context = SourceDetails(
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)",
        )

        lookup = AspectLookup(aspect_name="Siblings", field="sibling")

        # Mock URNs returned from aspect lookup - different entity type
        different_urn_type = "urn:li:chart:(tableau,my-dashboard,PROD)"
        mock_get_urns.return_value = [different_urn_type]

        # Execute
        result = list(strategy.propagate(lookup, propagator, directive, context))

        # Verify
        assert len(result) == 0
        # The propagator should not be called for different entity types
        propagator.create_property_change_proposal.assert_not_called()


class TestGetUrnsFromAspect:
    def test_aspect_not_in_schema_types(self, graph: MagicMock) -> None:
        """Test that an empty list is returned when aspect is not in schema types."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        aspect_lookup = AspectLookup(aspect_name="nonExistentAspect", field="platform")

        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        assert result == []

    def test_aspect_not_found(self, graph: MagicMock) -> None:
        """Test that an empty list is returned when the aspect is not found."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        aspect_lookup = AspectLookup(aspect_name="Siblings", field="siblings")

        # Mock the graph to return None for the aspect
        graph.graph.get_aspect.return_value = None

        # Execute
        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        # Verify
        assert result == []
        graph.graph.get_aspect.assert_called_once()

    def test_field_not_found(self, graph: MagicMock) -> None:
        """Test that an empty list is returned when the field is not found in the aspect."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        aspect_lookup = AspectLookup(aspect_name="Siblings", field="nonExistentField")

        # Mock the aspect
        mock_aspect = MagicMock()
        mock_aspect.to_obj.return_value = {"platform": "snowflake"}
        mock_aspect.get.return_value = None  # Field not found
        graph.graph.get_aspect.return_value = mock_aspect

        # Execute
        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        # Verify
        assert result == []

    def test_string_field_value(self, graph: MagicMock) -> None:
        """Test that a string field value is correctly processed."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        parent_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)"
        target_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test.test,PROD)"
        aspect_lookup = AspectLookup(aspect_name="Siblings", field="sibling")

        # Mock the aspect
        mock_aspect = MagicMock()
        mock_aspect.to_obj.return_value = {"platform": target_urn}
        mock_aspect.get.return_value = target_urn

        # Mock the schema fields for the target dataset
        mock_schema_metadata = MagicMock(spec=models.SchemaMetadataClass)
        mock_field_metadata = MagicMock()
        mock_field_metadata.fieldPath = "fieldA"
        mock_schema_metadata.fields = [mock_field_metadata]

        # Setup the get_aspect to return different values based on the URN
        def get_aspect_side_effect(urn: str, aspect_type: Any) -> Optional[MagicMock]:
            if urn == parent_urn:
                return mock_aspect
            elif urn == target_urn:
                return mock_schema_metadata
            return None

        graph.graph.get_aspect.side_effect = get_aspect_side_effect

        # Execute
        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        # Verify
        assert len(result) == 1
        # The exact URN format should be checked - assuming make_schema_field_urn works properly
        assert "schemaField" in result[0]
        assert "bigquery" in result[0]
        assert "fieldA" in result[0]

    def test_list_field_value(self, graph: MagicMock) -> None:
        """Test that a list field value is correctly processed."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        parent_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)"
        target_urn1 = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test.test_sibling1,PROD)"
        )
        target_urn2 = (
            "urn:li:dataset:(urn:li:dataPlatform:hive,test.test_sibling2,PROD)"
        )
        aspect_lookup = AspectLookup(aspect_name="Siblings", field="siblings")

        mock_schema_metadata1 = SchemaMetadataClass(
            platformSchema=MySqlDDL(tableSchema=""),
            version=0,
            fields=[
                models.SchemaFieldClass(
                    fieldPath="fieldA",
                    nativeDataType="STRING",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                )
            ],
            platform="hive",
            schemaName="test",
            hash="hash",
        )

        mock_schema_metadata2 = SchemaMetadataClass(
            platformSchema=MySqlDDL(tableSchema=""),
            version=0,
            fields=[
                models.SchemaFieldClass(
                    fieldPath="fieldB",
                    nativeDataType="STRING",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                )
            ],
            platform="hive",
            schemaName="test",
            hash="hash",
        )
        mock_field_metadata2 = MagicMock()
        mock_field_metadata2.fieldPath = "fieldA"
        mock_schema_metadata2.fields = [mock_field_metadata2]

        # Setup the get_aspect to return different values based on the URN
        def get_aspect_side_effect(
            urn: str, aspect_type: Any
        ) -> Optional[Union[SiblingsClass, SchemaMetadataClass]]:
            if urn == parent_urn:
                return SiblingsClass(siblings=[target_urn1, target_urn2], primary=True)
            elif urn == target_urn1:
                return mock_schema_metadata1
            elif urn == target_urn2:
                return mock_schema_metadata2
            return None

        graph.graph.get_aspect.side_effect = get_aspect_side_effect

        # Execute
        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        # Verify
        assert len(result) == 2
        # Check that we have one URN with bigquery and one with hive
        assert any("bigquery" in urn for urn in result)
        assert any("hive" in urn for urn in result)
        assert all("fieldA" in urn for urn in result)

    def test_nested_field_lookup(self, graph: MagicMock) -> None:
        """Test that a nested field lookup works correctly."""
        entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD),fieldA)"
        parent_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.test,PROD)"
        target_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test.test,PROD)"
        aspect_lookup = AspectLookup(
            aspect_name="Siblings", field="properties.upstream"
        )

        # Mock the aspect with a nested field
        mock_aspect = MagicMock()
        mock_aspect.to_obj.return_value = {"properties": {"upstream": target_urn}}

        # Mock the schema fields for the target dataset
        mock_schema_metadata1 = SchemaMetadataClass(
            platformSchema=MySqlDDL(tableSchema=""),
            version=0,
            fields=[
                models.SchemaFieldClass(
                    fieldPath="fieldA",
                    nativeDataType="STRING",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                )
            ],
            platform="hive",
            schemaName="test",
            hash="hash",
        )

        # Setup the get_aspect to return different values based on the URN
        def get_aspect_side_effect(
            urn: str, aspect_type: Any
        ) -> Optional[Union[SiblingsClass, SchemaMetadataClass]]:
            if urn == parent_urn:
                return mock_aspect
            elif urn == target_urn:
                return mock_schema_metadata1
            return None

        graph.graph.get_aspect.side_effect = get_aspect_side_effect

        # Execute
        result = get_urns_from_aspect(graph, entity_urn, aspect_lookup)

        # Verify
        assert len(result) == 1
        assert "schemaField" in result[0]
        assert "bigquery" in result[0]
        assert "fieldA" in result[0]
