from typing import Any
from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)

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
            password="test_password",  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
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
        ctx.pipeline_name = "test_pipeline"

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
            password="test_password",  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
            role="test_role",
        )

        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config
            # No description_sync config
        )

        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

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
            password="test_password",  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
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
        ctx.pipeline_name = "test_pipeline"

        action = SnowflakeMetadataSyncAction(config, ctx)

        # Mock the propagation dispatcher's process_event method
        mock_directive = DescriptionPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)",
            description="Test description",
            operation="ADD",
            subtype="table",
            propagate=True,
        )
        # Create a mock EntityChangeEvent for integration testing
        # Note: Using Mock(spec=EntityChangeEvent) ensures isinstance() checks pass
        # in the SnowflakeMetadataSyncAction.act() method
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.entityType = "dataset"
        entity_change_event.category = (
            "DOCUMENTATION"  # Required for description extraction
        )
        entity_change_event.operation = "ADD"
        entity_change_event.version = 1
        # Create a proper audit stamp with numeric time for stats collection
        # The stats utility divides by 1000.0 to convert from milliseconds to seconds
        audit_stamp = Mock()
        audit_stamp.time = 1609459200000  # 2021-01-01 00:00:00 UTC in milliseconds
        entity_change_event.auditStamp = audit_stamp
        # Set both safe_parameters and _inner_dict to support different access patterns
        entity_change_event.safe_parameters = {"description": "Test description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

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
            password="test_password",  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
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
        ctx.pipeline_name = "test_pipeline"

        with patch(
            "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
        ):
            action = SnowflakeMetadataSyncAction(config, ctx)

        # Create a mock MCL event
        mcl_event = Mock(spec=MetadataChangeLogEvent)
        mcl_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        mcl_event.aspectName = "editableDatasetProperties"
        mcl_event.entityType = "dataset"
        mcl_event.changeType = "UPSERT"
        mcl_event.aspect = Mock()
        mcl_event.aspect.value = b'{"description": "Test description"}'
        # Create a proper audit header with time
        audit_header = Mock()
        audit_header.time = 1609459200000  # 2021-01-01 00:00:00 UTC in milliseconds
        mcl_event.auditHeader = audit_header

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1", event=mcl_event, meta={}
        )

        # Mock the propagation dispatcher
        with patch.object(
            action.propagation_dispatcher, "process_event", return_value=[]
        ) as mock_process_event:
            # Process the event - should not raise an exception
            action.act(envelope)

            # Verify that description propagation was called through the dispatcher
            mock_process_event.assert_called_once_with(envelope)
