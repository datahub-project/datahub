from typing import Any
from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
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
            description_sync=description_propagation_config,
        )

        assert config.description_sync is not None
        assert config.description_sync.enabled is True
        assert config.description_sync.table_description_sync_enabled is True
        assert config.description_sync.column_description_sync_enabled is True

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
            description_sync=description_propagation_config,
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Verify description propagation strategy was added to dispatcher
        assert action.propagation_dispatcher is not None
        assert len(action.propagation_dispatcher.strategies) > 0

        # Find the description propagation strategy
        description_strategy = None
        for strategy in action.propagation_dispatcher.strategies:
            if hasattr(strategy, "config") and hasattr(
                strategy.config, "table_description_sync_enabled"
            ):
                description_strategy = strategy
                break

        assert description_strategy is not None
        assert description_strategy.config.enabled is True
        assert description_strategy.config.table_description_sync_enabled is True
        assert description_strategy.config.column_description_sync_enabled is False

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
            # No description_sync config
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Verify no description propagation strategy was added to dispatcher
        assert action.propagation_dispatcher is not None
        assert len(action.propagation_dispatcher.strategies) == 0

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
            description_sync=description_propagation_config,
        )

        ctx = Mock()
        ctx.graph = Mock()

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Mock the propagation dispatcher's process_event method
        mock_directive = DescriptionPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)",
            description="Test description",
            operation="ADD",
            subtype="table",
            propagate=True,
        )
        # Create a mock event
        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        with patch.object(
            action.propagation_dispatcher,
            "process_event",
            return_value=[mock_directive],
        ) as mock_process_event:
            with patch.object(action, "process_directive") as mock_process_directive:
                # Process the event
                action.act(envelope)

                # Verify that description propagation was called through the dispatcher
                mock_process_event.assert_called_once_with(envelope)
                mock_process_directive.assert_called_once_with(mock_directive)

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
            description_sync=description_propagation_config,
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

        # Mock the propagation dispatcher
        with patch.object(
            action.propagation_dispatcher, "process_event", return_value=[]
        ) as mock_process_event:
            # Process the event - should not raise an exception
            action.act(envelope)

            # Verify that description propagation was called through the dispatcher
            mock_process_event.assert_called_once_with(envelope)
