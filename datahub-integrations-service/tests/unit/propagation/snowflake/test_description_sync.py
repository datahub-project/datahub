from unittest.mock import Mock, patch

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub_actions.event.event_envelope import EventEnvelope

from datahub_integrations.propagation.snowflake.description_sync_action import (
    DescriptionSyncAction,
    DescriptionSyncConfig,
)


class TestDescriptionSyncAction:
    def test_config_creation(self):
        """Test creation of description sync configuration."""
        config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        assert config.enabled is True
        assert config.table_description_sync_enabled is True
        assert config.column_description_sync_enabled is True

    def test_should_propagate_table_description(self):
        """Test that table description changes are detected for propagation."""
        config = DescriptionSyncConfig(
            enabled=True, table_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        # Create a mock event for table description change
        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test table description"}
        }

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        with patch(
            "datahub_integrations.propagation.snowflake.description_sync_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.docs == "Test table description"
        assert directive.operation == "ADD"

    def test_should_propagate_column_description(self):
        """Test that column description changes are detected for propagation."""
        config = DescriptionSyncConfig(
            enabled=True, column_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        # Create a mock event for column description change
        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "MODIFY"
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test column description"}
        }

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        with patch(
            "datahub_integrations.propagation.snowflake.description_sync_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.docs == "Test column description"
        assert directive.operation == "MODIFY"

    def test_should_not_propagate_non_snowflake_urn(self):
        """Test that non-Snowflake URNs are ignored."""
        config = DescriptionSyncConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
        )
        entity_change_event.category = "DOCUMENTATION"

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        with patch(
            "datahub_integrations.propagation.snowflake.description_sync_action.is_snowflake_urn",
            return_value=False,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_should_not_propagate_when_disabled(self):
        """Test that description sync is ignored when disabled."""
        config = DescriptionSyncConfig(enabled=False)
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        envelope = EventEnvelope(event=event, meta={})

        directive = action.should_propagate(envelope)

        assert directive is None

    def test_should_not_propagate_table_when_table_sync_disabled(self):
        """Test that table description sync is ignored when table sync is disabled."""
        config = DescriptionSyncConfig(
            enabled=True,
            table_description_sync_enabled=False,
            column_description_sync_enabled=True,
        )
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        entity_change_event = Mock()
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test table description"}
        }

        event.event = entity_change_event

        envelope = EventEnvelope(event=event, meta={})

        with patch(
            "datahub_integrations.propagation.snowflake.description_sync_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_update_table_comment(self):
        """Test updating table comment SQL generation."""
        config = DescriptionSyncConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        )

        action._update_table_comment(dataset_urn, "Test description with 'quotes'")

        # Verify the SQL was executed with proper escaping
        snowflake_helper._run_query.assert_called_once()
        args = snowflake_helper._run_query.call_args[0]

        assert args[0] == "test_db"  # database
        assert args[1] == "test_schema"  # schema
        assert (
            "ALTER TABLE test_db.test_schema.test_table SET COMMENT = 'Test description with ''quotes'''"
            in args[2]
        )  # SQL with escaped quotes

    def test_update_column_comment(self):
        """Test updating column comment SQL generation."""
        config = DescriptionSyncConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        snowflake_helper = Mock()

        action = DescriptionSyncAction(config, ctx, snowflake_helper)

        field_urn = SchemaFieldUrn.create_from_string(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        )

        action._update_column_comment(field_urn, "Test column description")

        # Verify the SQL was executed
        snowflake_helper._run_query.assert_called_once()
        args = snowflake_helper._run_query.call_args[0]

        assert args[0] == "test_db"  # database
        assert args[1] == "test_schema"  # schema
        assert (
            "ALTER TABLE test_db.test_schema.test_table ALTER COLUMN test_column COMMENT 'Test column description'"
            in args[2]
        )
