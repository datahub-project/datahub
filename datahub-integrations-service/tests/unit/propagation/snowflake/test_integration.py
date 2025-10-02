from typing import Any
from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.description_propagation_action import (
    DescriptionPropagationConfig,
)
from datahub_integrations.propagation.snowflake.metadata_sync_action import (
    SnowflakeMetadataSyncAction,
    SnowflakeMetadataSyncConfig,
)


class TestSnowflakeIntegration:
    def test_config_with_description_propagation(self) -> None:
        """Test that the main config properly includes description propagation configuration."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_propagation_config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_propagation=description_propagation_config,
        )

        assert config.description_propagation is not None
        assert config.description_propagation.enabled is True
        assert config.description_propagation.table_description_sync_enabled is True
        assert config.description_propagation.column_description_sync_enabled is True

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_action_initialization_with_description_propagation(
        self, mock_tag_helper: Any
    ) -> None:
        """Test that the action properly initializes description propagation when enabled."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_propagation_config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=False,
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_propagation=description_propagation_config,
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Verify description propagation action was initialized
        assert action.description_propagation_action is not None
        assert action.description_propagation_action.config.enabled is True
        assert (
            action.description_propagation_action.config.table_description_sync_enabled
            is True
        )
        assert (
            action.description_propagation_action.config.column_description_sync_enabled
            is False
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_action_initialization_without_description_propagation(
        self, mock_tag_helper: Any
    ) -> None:
        """Test that the action works without description propagation configuration."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config
            # No description_propagation config
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Verify description propagation action was not initialized
        assert action.description_propagation_action is None

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.is_snowflake_urn"
    )
    def test_description_propagation_event_processing(
        self, mock_is_snowflake_urn: Any, mock_tag_helper: Any
    ) -> None:
        """Test that description propagation events are properly processed."""
        mock_is_snowflake_urn.return_value = True

        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_propagation_config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_propagation=description_propagation_config,
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Mock the description propagation action's should_propagate method
        mock_directive = Mock()
        mock_directive.propagate = True
        action.description_propagation_action.should_propagate = Mock(
            return_value=mock_directive
        )
        action.description_propagation_action.process_directive = Mock()

        # Create a mock event
        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        # Process the event
        action.act(envelope)

        # Verify that description propagation was called
        action.description_propagation_action.should_propagate.assert_called_once_with(
            envelope
        )
        action.description_propagation_action.process_directive.assert_called_once_with(
            mock_directive
        )

    def test_mcl_event_processing(self) -> None:
        """Test that MetadataChangeLogEvent events are properly handled."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_propagation_config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_propagation=description_propagation_config,
        )

        ctx = Mock()
        ctx.graph = Mock()

        with patch(
            "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
        ):
            action = SnowflakeMetadataSyncAction(config, ctx)

        # Create a mock MCL event
        event = Mock()
        event.event_type = "MetadataChangeLogEvent_v1"

        mcl_event = Mock()
        mcl_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        mcl_event.aspectName = "editableDatasetProperties"

        event.event = mcl_event

        envelope = EventEnvelope(event=event, meta={})

        # Mock the description propagation action
        action.description_propagation_action.should_propagate = Mock(return_value=None)

        # Process the event - should not raise an exception
        action.act(envelope)

        # Verify that description propagation was called
        action.description_propagation_action.should_propagate.assert_called_once_with(
            envelope
        )
