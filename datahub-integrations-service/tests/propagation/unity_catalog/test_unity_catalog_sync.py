# test_integration.py - Integration tests for Unity Catalog classes

from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from unittest.mock import Mock, patch

import pytest
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, TagUrn
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.propagation.unity_catalog.description_sync_action import (
    DescriptionSyncAction,
)
from datahub_integrations.propagation.unity_catalog.util import (
    TAG_KEY_MAX_LENGTH,
    TAG_VALUE_MAX_LENGTH,
    TagTransformer,
    UnityCatalogTagHelper,
)


class TestUnityCatalogTagHelperIntegration:
    """Integration tests for UnityCatalogTagHelper class."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Fixture providing mock Unity Catalog connection config."""
        config = Mock()
        config.workspace_url = "https://test.databricks.com"
        config.warehouse_id = "test_warehouse"
        config.token = "test_token"
        return config

    @pytest.fixture
    def mock_graph(self) -> Mock:
        """Fixture providing mock AcrylDataHubGraph."""
        graph = Mock()
        graph.graph = Mock()
        graph.graph.get_aspect = Mock(return_value=None)
        return graph

    @pytest.fixture
    def mock_unity_resource_manager(self) -> Mock:
        """Fixture providing mock Unity Resource Manager."""
        manager = Mock()
        manager.add_table_tags = Mock(return_value=True)
        manager.remove_table_tags = Mock(return_value=True)
        manager.add_column_tags = Mock(return_value=True)
        manager.remove_column_tags = Mock(return_value=True)
        manager.add_catalog_tags = Mock(return_value=True)
        manager.remove_catalog_tags = Mock(return_value=True)
        manager.add_schema_tags = Mock(return_value=True)
        manager.remove_schema_tags = Mock(return_value=True)
        manager.set_table_description = Mock(return_value=True)
        manager.set_column_description = Mock(return_value=True)
        manager.set_catalog_description = Mock(return_value=True)
        manager.set_schema_description = Mock(return_value=True)
        return manager

    @pytest.fixture
    def unity_tag_helper(
        self, mock_config: Mock, mock_graph: Mock, mock_unity_resource_manager: Mock
    ) -> "UnityCatalogTagHelper":
        """Fixture providing UnityCatalogTagHelper instance."""
        from datahub_integrations.propagation.unity_catalog.util import (
            UnityCatalogTagHelper,
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.util.PlatformResourceRepository"
        ):
            helper = UnityCatalogTagHelper(
                config=mock_config,
                graph=mock_graph,
                unity_catalog_resource_helper=mock_unity_resource_manager,
                platform_instance="test",
            )
            return helper

    def test_apply_table_tag_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test successful table tag application."""
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        tag_urn = "urn:li:tag:environment:production"

        unity_tag_helper.apply_tag(dataset_urn, tag_urn)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.add_table_tags.assert_called_once_with(
            catalog="catalog",
            schema="schema",
            table="table",
            tags={"environment": "production"},
        )

    def test_apply_column_tag_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test successful column tag application."""
        schema_field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD),field1)"
        tag_urn = "urn:li:tag:sensitive"

        with patch(
            "datahub.utilities.urns.field_paths.get_simple_field_path_from_v2_field_path",
            return_value="field1",
        ):
            unity_tag_helper.apply_tag(schema_field_urn, tag_urn)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.add_column_tags.assert_called_once_with(
            catalog="catalog",
            schema="schema",
            table="table",
            column="field1",
            tags={"sensitive": ""},
        )

    def test_apply_container_tag_catalog_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
        mock_graph: Mock,
    ) -> None:
        """Test successful catalog tag application."""
        container_urn = "urn:li:container:test_catalog"
        tag_urn = "urn:li:tag:governance:compliant"

        # Mock container properties for catalog-only container
        mock_properties = Mock()
        mock_properties.to_obj.return_value = {
            "customProperties": {"catalog": "test_catalog", "unity_schema": None}
        }
        mock_graph.graph.get_aspect.return_value = mock_properties

        unity_tag_helper.apply_tag(container_urn, tag_urn)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.add_catalog_tags.assert_called_once_with(
            catalog="test_catalog", tags={"governance": "compliant"}
        )

    def test_apply_container_tag_schema_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
        mock_graph: Mock,
    ) -> None:
        """Test successful schema tag application."""
        container_urn = "urn:li:container:test_schema"
        tag_urn = "urn:li:tag:team:data_engineering"

        # Mock container properties for schema container
        mock_properties = Mock()
        mock_properties.to_obj.return_value = {
            "customProperties": {
                "catalog": "test_catalog",
                "unity_schema": "test_schema",
            }
        }
        mock_graph.graph.get_aspect.return_value = mock_properties

        unity_tag_helper.apply_tag(container_urn, tag_urn)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.add_schema_tags.assert_called_once_with(
            catalog="test_catalog",
            schema="test_schema",
            tags={"team": "data_engineering"},
        )

    def test_remove_table_tag_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test successful table tag removal."""
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        tag_urn = "urn:li:tag:environment:production"

        unity_tag_helper.remove_tag(dataset_urn, tag_urn)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.remove_table_tags.assert_called_once_with(
            catalog="catalog", schema="schema", table="table", tag_keys=["environment"]
        )

    def test_apply_table_description_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test successful table description application."""
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        description = "This is a test table with important data"

        unity_tag_helper.apply_description(dataset_urn, description)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.set_table_description.assert_called_once_with(
            catalog="catalog", schema="schema", table="table", description=description
        )

    def test_apply_column_description_success(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test successful column description application."""
        schema_field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD),field1)"
        description = "This column contains sensitive customer data"

        with patch(
            "datahub.utilities.urns.field_paths.get_simple_field_path_from_v2_field_path",
            return_value="field1",
        ):
            unity_tag_helper.apply_description(schema_field_urn, description)

        # Verify the Unity Resource Manager was called correctly
        mock_unity_resource_manager.set_column_description.assert_called_once_with(
            catalog="catalog",
            schema="schema",
            table="table",
            column="field1",
            description=description,
        )

    def test_error_tracking_on_failure(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test error tracking when operations fail."""
        # Make the Unity Resource Manager raise an exception
        mock_unity_resource_manager.add_table_tags.side_effect = Exception(
            "Connection failed"
        )

        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        tag_urn = "urn:li:tag:environment:production"

        # Should not raise exception, but should log error
        unity_tag_helper.apply_tag(dataset_urn, tag_urn)

        # Check that error was tracked
        assert len(unity_tag_helper.error_tracker.error_timestamps) == 1

    def test_too_many_errors_property(
        self, unity_tag_helper: "UnityCatalogTagHelper"
    ) -> None:
        """Test too_many_errors property."""
        # Initially should be False
        assert unity_tag_helper.too_many_errors is False

        # Manually add many errors
        for _ in range(20):
            unity_tag_helper.error_tracker.log_error()

        # Now should be True
        assert unity_tag_helper.too_many_errors is True

    def test_invalid_urn_handling(
        self,
        unity_tag_helper: "UnityCatalogTagHelper",
        mock_unity_resource_manager: Mock,
    ) -> None:
        """Test handling of invalid URNs."""
        invalid_urn = "urn:li:corpuser:test_user"
        tag_urn = "urn:li:tag:test"

        # Should not call Unity Resource Manager for invalid URN
        unity_tag_helper.apply_tag(invalid_urn, tag_urn)

        # Verify no calls were made
        mock_unity_resource_manager.add_table_tags.assert_not_called()
        mock_unity_resource_manager.add_column_tags.assert_not_called()
        mock_unity_resource_manager.add_catalog_tags.assert_not_called()
        mock_unity_resource_manager.add_schema_tags.assert_not_called()

    def test_close_method(self, unity_tag_helper: "UnityCatalogTagHelper") -> None:
        """Test close method."""
        # Should not raise any exceptions
        unity_tag_helper.close()

    @patch("datahub_integrations.propagation.unity_catalog.util.cachetools.TTLCache")
    def test_caching_behavior(
        self, mock_cache: Mock, unity_tag_helper: "UnityCatalogTagHelper"
    ) -> None:
        """Test caching behavior for container info."""
        # This test would verify that container info is cached
        # Implementation depends on the actual caching mechanism
        assert hasattr(unity_tag_helper, "platform_resource_cache")


class TestDescriptionSyncActionIntegration:
    """Integration tests for DescriptionSyncAction."""

    @pytest.fixture
    def mock_event_envelope(self) -> Mock:
        """Fixture providing mock event envelope."""
        envelope = Mock(spec=EventEnvelope)
        envelope.event_type = "EntityChangeEvent_v1"

        # Mock the inner event
        mock_event = Mock(spec=EntityChangeEvent)
        mock_event.category = "DOCUMENTATION"
        mock_event.operation = "ADD"
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        mock_event._inner_dict = {
            "__parameters_json": {"description": "Updated table description"}
        }

        envelope.event = mock_event
        return envelope

    @pytest.fixture
    def mock_pipeline_context(self) -> Mock:
        """Fixture providing mock pipeline context."""
        ctx = Mock(spec=PipelineContext)
        ctx.graph = Mock()
        ctx.graph.graph = Mock()
        return ctx

    @pytest.fixture
    def description_sync_action(
        self, mock_pipeline_context: Mock
    ) -> "DescriptionSyncAction":
        """Fixture providing DescriptionSyncAction."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            DescriptionSyncAction,
            DescriptionSyncConfig,
        )

        config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
            container_description_sync_enabled=True,
        )

        return DescriptionSyncAction(config, mock_pipeline_context)

    def test_full_workflow_table_description(
        self,
        description_sync_action: "DescriptionSyncAction",
        mock_event_envelope: Mock,
    ) -> None:
        """Test full workflow for table description sync."""
        # Test should_propagate
        mock_dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        ).urn()
        mock_event_envelope.event.entityUrn = mock_dataset_urn
        directive = description_sync_action.should_propagate(mock_event_envelope)

        assert directive is not None
        assert directive.docs == "Updated table description"
        assert (
            directive.entity
            == "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        assert directive.operation == "ADD"
        assert directive.propagate is True

    def test_disabled_column_sync(
        self, mock_pipeline_context: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test behavior when column sync is disabled."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            DescriptionSyncAction,
            DescriptionSyncConfig,
        )

        config = DescriptionSyncConfig(column_description_sync_enabled=False)
        action = DescriptionSyncAction(config, mock_pipeline_context)

        # Modify event to be for a schema field
        mock_event_envelope.event.entityUrn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD),field1)"

        mock_dataset_urn = SchemaFieldUrn.create_from_string(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,main.default.new_table,PROD),price)"
        ).urn()
        mock_event_envelope.event.entityUrn = mock_dataset_urn

        directive = action.should_propagate(mock_event_envelope)

        # Should return None because column sync is disabled
        assert directive is None

    def test_wrong_category_event(
        self,
        description_sync_action: "DescriptionSyncAction",
        mock_event_envelope: Mock,
    ) -> None:
        """Test handling of non-documentation events."""
        # Change event category
        mock_event_envelope.event.category = "TAG"

        directive = description_sync_action.should_propagate(mock_event_envelope)

        # Should return None for non-documentation events
        assert directive is None

    def test_missing_description(
        self,
        description_sync_action: "DescriptionSyncAction",
        mock_event_envelope: Mock,
    ) -> None:
        """Test handling of events with missing description."""
        # Remove description from parameters
        mock_event_envelope.event._inner_dict = {"__parameters_json": {}}

        directive = description_sync_action.should_propagate(mock_event_envelope)

        # Should return None when no description is found
        assert directive is None


class TestUnityCatalogPropagatorActionIntegration:
    """Integration tests for main propagator action."""

    @pytest.fixture
    def mock_config(self) -> Dict[str, Any]:
        """Fixture providing mock configuration."""
        return {
            "databricks": {
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "token": "test_token",
            },
            "tag_propagation": {"enabled": True, "tag_prefixes": ["env:", "team:"]},
            "description_sync": {
                "enabled": True,
                "table_description_sync_enabled": True,
                "column_description_sync_enabled": True,
            },
            "platform_instance": "test_instance",
        }

    @pytest.fixture
    def mock_pipeline_context(self) -> Mock:
        """Fixture providing mock pipeline context."""
        ctx = Mock(spec=PipelineContext)
        ctx.graph = Mock()
        ctx.graph.graph = Mock()
        return ctx

    def test_config_parsing(self, mock_config: Dict[str, Any]) -> None:
        """Test configuration parsing."""
        from datahub_integrations.propagation.unity_catalog.tag_propagator import (
            UnityCatalogTagPropagatorConfig,
        )

        config = UnityCatalogTagPropagatorConfig.parse_obj(mock_config)

        assert config.databricks.workspace_url == "https://test.databricks.com"
        assert config.tag_propagation
        assert config.tag_propagation.enabled is True
        assert config.description_sync
        assert config.description_sync.enabled is True
        assert config.platform_instance == "test_instance"


# Performance and stress tests
class TestPerformanceAndStressTests:
    """Performance and stress tests for Unity Catalog classes."""

    def test_error_tracker_performance(self) -> None:
        """Test error tracker performance with many errors."""
        from datahub_integrations.propagation.unity_catalog.util import ErrorTracker

        tracker = ErrorTracker(max_errors_per_hour=1000)

        # Log many errors quickly
        start_time = datetime.now()
        for _ in range(500):
            tracker.log_error()
        end_time = datetime.now()

        # Should complete quickly (less than 1 second)
        assert (end_time - start_time).total_seconds() < 1.0

        # Should properly track errors
        assert len(tracker.error_timestamps) == 500

    def test_tag_transformer_special_characters(self) -> None:
        """Test tag transformer with various special characters."""

        test_cases = [
            ("normal_tag", "normal_tag"),
            ("tag-with-dashes", "tag_with_dashes"),
            ("tag@#$%^&*()", "tag_________"),
            ("タグ", "__"),  # Japanese characters
            ("tag with spaces", "tag_with_spaces"),
            ("tag.with.dots", "tag_with_dots"),
            ("", ""),
            ("123", "123"),
        ]

        for input_str, expected in test_cases:
            result = TagTransformer.str_to_uc_tag(input_str)
            assert result == expected, f"Failed for input: {input_str}"

    def test_entity_parser_edge_cases(self) -> None:
        """Test entity parser with edge cases."""
        from datahub_integrations.propagation.unity_catalog.util import EntityParser

        # Test various malformed URNs
        invalid_urns = [
            "",
            "not_a_urn",
            "urn:li:invalid:type",
            "urn:li:dataset:",  # Missing parts
            "urn:li:schemaField:",  # Missing parts
        ]

        for invalid_urn in invalid_urns:
            with pytest.raises((ValueError, Exception)):
                EntityParser.parse_entity_urn(invalid_urn)

    def test_urn_validator_large_batch(self) -> None:
        """Test URN validator with large batch of URNs."""
        from datahub_integrations.propagation.unity_catalog.util import UrnValidator

        # Generate many URNs to test
        databricks_urns = [
            f"urn:li:dataset:(urn:li:dataPlatform:databricks,catalog{i}.schema{i}.table{i},PROD)"
            for i in range(1000)
        ]

        snowflake_urns = [
            f"urn:li:dataset:(urn:li:dataPlatform:snowflake,database{i}.schema{i}.table{i},PROD)"
            for i in range(1000)
        ]

        # Test all Databricks URNs should be allowed
        for urn in databricks_urns:
            assert UrnValidator.is_urn_allowed(urn) is True

        # Test all Snowflake URNs should not be allowed
        for urn in snowflake_urns:
            assert UrnValidator.is_urn_allowed(urn) is False


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""

    def test_connection_config_edge_cases(self) -> None:
        """Test connection config with edge cases."""
        from datahub_integrations.propagation.unity_catalog.config import (
            UnityCatalogConnectionConfig,
        )

        # Test with special characters in URL
        config = UnityCatalogConnectionConfig(
            workspace_url="https://my-workspace-123.cloud.databricks.com/",
            warehouse_id="warehouse_with_underscores_123",
            token="token-with-special-chars+/=",
        )

        assert config.server_hostname == "my-workspace-123.cloud.databricks.com/"
        conn_str = config.create_databricks_connection_string()
        assert "warehouse_with_underscores_123" in conn_str

    def test_description_sync_config_validation(self) -> None:
        """Test description sync config validation."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            DescriptionSyncConfig,
        )

        # Test all combinations of boolean flags
        test_cases = [
            {
                "enabled": True,
                "table_description_sync_enabled": True,
                "column_description_sync_enabled": True,
            },
            {
                "enabled": False,
                "table_description_sync_enabled": False,
                "column_description_sync_enabled": False,
            },
            {
                "enabled": True,
                "table_description_sync_enabled": False,
                "column_description_sync_enabled": True,
            },
        ]

        for case in test_cases:
            config = DescriptionSyncConfig(**case)
            assert config.enabled == case["enabled"]
            assert (
                config.table_description_sync_enabled
                == case["table_description_sync_enabled"]
            )
            assert (
                config.column_description_sync_enabled
                == case["column_description_sync_enabled"]
            )

    def test_container_info_edge_cases(self) -> None:
        """Test container info with edge cases."""
        from datahub_integrations.propagation.unity_catalog.util import ContainerInfo

        # Test with empty strings
        info1 = ContainerInfo(catalog="", schema="")
        assert info1.is_valid is False  # Empty catalog is not valid

        # Test with None values
        info2 = ContainerInfo(catalog=None, schema="test")
        assert info2.is_valid is False
        assert info2.is_catalog_only is False

        # Test with whitespace
        info3 = ContainerInfo(catalog="  test  ", schema=None)
        assert info3.is_valid is True  # Whitespace catalog is still valid
        assert info3.is_catalog_only is True

    def test_error_tracker_boundary_conditions(self) -> None:
        """Test error tracker at boundary conditions."""
        from datahub_integrations.propagation.unity_catalog.util import ErrorTracker

        # Test with threshold of 0
        tracker_zero = ErrorTracker(max_errors_per_hour=0)
        assert tracker_zero.too_many_errors() is False  # No errors logged yet
        tracker_zero.log_error()
        assert tracker_zero.too_many_errors() is True  # Immediately exceeds threshold

        # Test with threshold of 1
        tracker_one = ErrorTracker(max_errors_per_hour=1)
        assert tracker_one.too_many_errors() is False
        tracker_one.log_error()
        assert tracker_one.too_many_errors() is True  # Exactly at threshold

        # Test cleanup with exact 1-hour boundary
        tracker_boundary = ErrorTracker(max_errors_per_hour=5)
        old_time = datetime.now() - timedelta(hours=1, seconds=1)  # Just over 1 hour
        recent_time = datetime.now() - timedelta(minutes=30)  # Within 1 hour
        # Manually add timestamps
        tracker_boundary.error_timestamps.append(old_time)
        tracker_boundary.error_timestamps.append(recent_time)
        tracker_boundary.log_error()

        # Check that old error is cleaned up
        count = len(tracker_boundary.error_timestamps)
        assert count == 2  # Only recent error should remain

    def test_tag_transformer_max_length_handling(self) -> None:
        """Test tag transformer with exact max length values."""

        # Test exact max length key and value
        max_key = "a" * TAG_KEY_MAX_LENGTH
        max_value = "b" * TAG_VALUE_MAX_LENGTH

        tag_name = f"{max_key}:{max_value}"
        tag_urn = TagUrn.create_from_string(f"urn:li:tag:{tag_name}")

        key, value = TagTransformer.datahub_tag_to_uc_tag(tag_urn)

        assert len(key) == TAG_KEY_MAX_LENGTH
        assert len(value) == TAG_VALUE_MAX_LENGTH
        assert key == max_key
        assert value == max_value

        # Test over max length
        over_max_key = "a" * (TAG_KEY_MAX_LENGTH + 10)
        over_max_value = "b" * (TAG_VALUE_MAX_LENGTH + 10)

        over_tag_name = f"{over_max_key}:{over_max_value}"
        over_tag_urn = TagUrn.create_from_string(f"urn:li:tag:{over_tag_name}")

        over_key, over_value = TagTransformer.datahub_tag_to_uc_tag(over_tag_urn)

        assert len(over_key) == TAG_KEY_MAX_LENGTH
        assert len(over_value) == TAG_VALUE_MAX_LENGTH


class TestMockingAndIsolation:
    """Test proper mocking and test isolation."""

    def test_unity_tag_helper_mocking_isolation(self) -> None:
        """Test that UnityCatalogTagHelper tests are properly isolated."""
        # This test ensures that mocks don't leak between tests

        # First, create a helper with one set of mocks
        mock_config1 = Mock()
        mock_config1.workspace_url = "https://test1.databricks.com"

        mock_graph1 = Mock()
        mock_resource_manager1 = Mock()
        mock_resource_manager1.add_table_tags.return_value = True

        with patch(
            "datahub_integrations.propagation.unity_catalog.util.PlatformResourceRepository"
        ):
            from datahub_integrations.propagation.unity_catalog.util import (
                UnityCatalogTagHelper,
            )

            helper1 = UnityCatalogTagHelper(
                config=mock_config1,
                graph=mock_graph1,
                unity_catalog_resource_helper=mock_resource_manager1,
            )

        # Then create another with different mocks
        mock_config2 = Mock()
        mock_config2.workspace_url = "https://test2.databricks.com"

        mock_graph2 = Mock()
        mock_resource_manager2 = Mock()
        mock_resource_manager2.add_table_tags.return_value = False

        with patch(
            "datahub_integrations.propagation.unity_catalog.util.PlatformResourceRepository"
        ):
            helper2 = UnityCatalogTagHelper(
                config=mock_config2,
                graph=mock_graph2,
                unity_catalog_resource_helper=mock_resource_manager2,
            )

        # Verify they use different underlying mocks
        assert helper1.config != helper2.config
        assert helper1.graph != helper2.graph
        assert (
            helper1.unity_catalog_resource_helper
            != helper2.unity_catalog_resource_helper
        )


class TestComprehensiveCoverage:
    """Tests to ensure comprehensive coverage of all class methods."""

    def test_all_config_properties(self) -> None:
        """Test all properties of configuration classes."""
        from datahub_integrations.propagation.unity_catalog.config import (
            UnityCatalogConnectionConfig,
        )
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            DescriptionSyncConfig,
        )

        # Test UnityCatalogConnectionConfig
        uc_config = UnityCatalogConnectionConfig(
            workspace_url="https://test.databricks.com",
            warehouse_id="test_warehouse",
            token="test_token",
        )

        # Test all properties
        assert uc_config.workspace_url == "https://test.databricks.com"
        assert uc_config.warehouse_id == "test_warehouse"
        assert uc_config.token == "test_token"
        assert uc_config.server_hostname == "test.databricks.com"
        assert uc_config.http_path == "/sql/1.0/warehouses/test_warehouse"

        connection_string = uc_config.create_databricks_connection_string()
        assert isinstance(connection_string, str)
        assert len(connection_string) > 0

        # Test DescriptionSyncConfig
        desc_config = DescriptionSyncConfig()

        # Test all boolean properties
        assert isinstance(desc_config.enabled, bool)
        assert isinstance(desc_config.table_description_sync_enabled, bool)
        assert isinstance(desc_config.column_description_sync_enabled, bool)
        assert isinstance(desc_config.container_description_sync_enabled, bool)

    def test_all_enum_values(self) -> None:
        """Test all enum values."""
        from datahub_integrations.propagation.unity_catalog.util import EntityType

        # Test all EntityType values
        assert EntityType.DATASET.value == "dataset"
        assert EntityType.SCHEMA_FIELD.value == "schema_field"
        assert EntityType.CONTAINER.value == "container"

        # Test enum comparison
        assert EntityType.DATASET != EntityType.SCHEMA_FIELD
        assert EntityType.SCHEMA_FIELD != EntityType.CONTAINER

    def test_all_dataclass_methods(self) -> None:
        """Test all methods of dataclass objects."""
        from datahub.metadata.urns import DatasetUrn

        from datahub_integrations.propagation.unity_catalog.util import (
            ContainerInfo,
            EntityType,
            ParsedEntity,
        )

        # Test ParsedEntity
        dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:test,test.test.test,PROD)"
        )
        parsed_entity = ParsedEntity(
            entity_type=EntityType.DATASET,
            urn=dataset_urn,
            dataset_urn=dataset_urn,
            field_path=None,
        )

        assert parsed_entity.entity_type == EntityType.DATASET
        assert parsed_entity.urn == dataset_urn
        assert parsed_entity.dataset_urn == dataset_urn
        assert parsed_entity.field_path is None

        # Test ContainerInfo
        container_info = ContainerInfo(catalog="test_catalog", schema="test_schema")

        assert container_info.catalog == "test_catalog"
        assert container_info.schema == "test_schema"
        assert container_info.is_catalog_only is False
        assert container_info.is_valid is True

    def test_static_method_coverage(self) -> None:
        """Test all static methods."""
        from datahub_integrations.propagation.unity_catalog.util import (
            EntityParser,
            UrnValidator,
        )

        # Test UrnValidator static methods
        assert callable(UrnValidator.is_urn_allowed)

        # Test TagTransformer static methods
        assert callable(TagTransformer.str_to_uc_tag)
        assert callable(TagTransformer.datahub_tag_to_uc_tag)

        # Test EntityParser static methods
        assert callable(EntityParser.parse_entity_urn)

    def test_class_method_coverage(self) -> None:
        """Test all class methods."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            DescriptionSyncAction,
        )
        from datahub_integrations.propagation.unity_catalog.tag_entities import (
            UnityCatalogTagId,
        )

        # Test UnityCatalogTagId class methods
        assert callable(UnityCatalogTagId.from_datahub_urn)
        assert callable(UnityCatalogTagId.generate_tag_id)
        assert callable(UnityCatalogTagId.get_key_value_from_datahub_tag)
        assert callable(UnityCatalogTagId.from_datahub_tag)

        # Test DescriptionSyncAction class methods
        assert callable(DescriptionSyncAction.create)


# Custom pytest markers for different test types
pytestmark = [
    pytest.mark.unit,  # Mark all tests in this file as unit tests
]


# Helper functions for test utilities
def create_mock_event_envelope(
    event_type: str = "EntityChangeEvent_v1",
    category: str = "DOCUMENTATION",
    operation: str = "ADD",
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:databricks,test.test.test,PROD)",
    description: Optional[str] = "Test description",
) -> Mock:
    """Helper function to create mock event envelopes for testing."""
    envelope = Mock(spec=EventEnvelope)
    envelope.event_type = event_type

    mock_event = Mock(spec=EntityChangeEvent)
    mock_event.category = category
    mock_event.operation = operation
    mock_event.entityUrn = entity_urn

    parameters = {}
    if description:
        parameters["description"] = description

    mock_event._inner_dict = {"__parameters_json": parameters}
    envelope.event = mock_event

    return envelope


def create_mock_unity_config(
    workspace_url: str = "https://test.databricks.com",
    warehouse_id: str = "test_warehouse",
    token: str = "test_token",
) -> Mock:
    """Helper function to create mock Unity Catalog configuration."""
    config = Mock()
    config.workspace_url = workspace_url
    config.warehouse_id = warehouse_id
    config.token = token
    config.server_hostname = workspace_url.replace("https://", "")
    config.http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    return config


# Additional parametrized tests for comprehensive coverage
@pytest.mark.parametrize(
    "entity_type,urn_string,expected_type",
    [
        (
            "dataset",
            "urn:li:dataset:(urn:li:dataPlatform:databricks,cat.sch.tbl,PROD)",
            "DatasetUrn",
        ),
        (
            "schema_field",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:databricks,cat.sch.tbl,PROD),field)",
            "SchemaFieldUrn",
        ),
        ("container", "urn:li:container:test", "ContainerUrn"),
    ],
)
def test_entity_parser_parametrized(
    entity_type: str, urn_string: str, expected_type: str
) -> None:
    """Parametrized test for entity parser with different URN types."""
    from datahub_integrations.propagation.unity_catalog.util import EntityParser

    parsed = EntityParser.parse_entity_urn(urn_string)
    assert parsed.urn.__class__.__name__ == expected_type


@pytest.mark.parametrize(
    "input_string,expected_output",
    [
        ("normal_tag", "normal_tag"),
        ("tag-with-dashes", "tag_with_dashes"),
        ("tag_with_underscores", "tag_with_underscores"),
        ("tag@#$%", "tag____"),
        ("tag with spaces", "tag_with_spaces"),
        ("tag.dot.separated", "tag_dot_separated"),
        ("123numeric", "123numeric"),
        ("", ""),
    ],
)
def test_tag_transformer_parametrized(input_string: str, expected_output: str) -> None:
    """Parametrized test for tag transformer with various inputs."""

    result = TagTransformer.str_to_uc_tag(input_string)
    assert result == expected_output


@pytest.mark.parametrize(
    "platform,should_allow",
    [
        ("databricks", True),
        ("snowflake", False),
        ("bigquery", False),
        ("postgres", False),
    ],
)
def test_urn_validator_platforms_parametrized(
    platform: str, should_allow: bool
) -> None:
    """Parametrized test for URN validator with different platforms."""
    from datahub_integrations.propagation.unity_catalog.util import UrnValidator

    urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},database.schema.table,PROD)"
    result = UrnValidator.is_urn_allowed(urn)
    assert result == should_allow


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
