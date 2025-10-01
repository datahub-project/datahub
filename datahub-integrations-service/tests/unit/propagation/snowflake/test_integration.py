from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.description_sync_action import (
    DescriptionSyncConfig,
)
from datahub_integrations.propagation.snowflake.tag_propagator import (
    SnowflakeTagPropagatorAction,
    SnowflakeTagPropagatorConfig,
)


class TestSnowflakeIntegration:
    def test_config_with_description_sync(self):
        """Test that the main config properly includes description sync configuration."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_sync_config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        config = SnowflakeTagPropagatorConfig(
            snowflake=snowflake_config, description_sync=description_sync_config
        )

        assert config.description_sync is not None
        assert config.description_sync.enabled is True
        assert config.description_sync.table_description_sync_enabled is True
        assert config.description_sync.column_description_sync_enabled is True

    @patch(
        "datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagHelper"
    )
    def test_action_initialization_with_description_sync(self, mock_tag_helper):
        """Test that the action properly initializes description sync when enabled."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_sync_config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=False,
        )

        config = SnowflakeTagPropagatorConfig(
            snowflake=snowflake_config, description_sync=description_sync_config
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeTagPropagatorAction(config, ctx)

        # Verify description sync action was initialized
        assert action.description_sync_action is not None
        assert action.description_sync_action.config.enabled is True
        assert (
            action.description_sync_action.config.table_description_sync_enabled is True
        )
        assert (
            action.description_sync_action.config.column_description_sync_enabled
            is False
        )

    @patch(
        "datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagHelper"
    )
    def test_action_initialization_without_description_sync(self, mock_tag_helper):
        """Test that the action works without description sync configuration."""
        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        config = SnowflakeTagPropagatorConfig(
            snowflake=snowflake_config
            # No description_sync config
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeTagPropagatorAction(config, ctx)

        # Verify description sync action was not initialized
        assert action.description_sync_action is None

    @patch(
        "datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagHelper"
    )
    @patch("datahub_integrations.propagation.snowflake.tag_propagator.is_snowflake_urn")
    def test_description_sync_event_processing(
        self, mock_is_snowflake_urn, mock_tag_helper
    ):
        """Test that description sync events are properly processed."""
        mock_is_snowflake_urn.return_value = True

        snowflake_config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
        )

        description_sync_config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        config = SnowflakeTagPropagatorConfig(
            snowflake=snowflake_config, description_sync=description_sync_config
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeTagPropagatorAction(config, ctx)

        # Mock the description sync action's should_propagate method
        mock_directive = Mock()
        mock_directive.propagate = True
        action.description_sync_action.should_propagate = Mock(
            return_value=mock_directive
        )
        action.description_sync_action.process_directive = Mock()

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

        # Verify that description sync was called
        action.description_sync_action.should_propagate.assert_called_once_with(
            envelope
        )
        action.description_sync_action.process_directive.assert_called_once_with(
            mock_directive
        )
