from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope

from datahub_integrations.propagation.snowflake.description_propagation_action import (
    DescriptionPropagationAction,
    DescriptionPropagationConfig,
)


class TestDescriptionPropagationAction:
    def test_config_creation(self) -> None:
        """Test creation of description propagation configuration."""
        config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

        assert config.enabled is True
        assert config.table_description_sync_enabled is True
        assert config.column_description_sync_enabled is True

    def test_should_propagate_table_description(self) -> None:
        """Test that table description changes are detected for propagation."""
        config = DescriptionPropagationConfig(
            enabled=True, table_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

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
            "datahub_integrations.propagation.snowflake.description_propagation_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.docs == "Test table description"
        assert directive.operation == "ADD"

    def test_should_propagate_column_description(self) -> None:
        """Test that column description changes are detected for propagation."""
        config = DescriptionPropagationConfig(
            enabled=True, column_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

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
            "datahub_integrations.propagation.snowflake.description_propagation_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.docs == "Test column description"
        assert directive.operation == "MODIFY"

    def test_should_not_propagate_non_snowflake_urn(self) -> None:
        """Test that non-Snowflake URNs are ignored."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

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
            "datahub_integrations.propagation.snowflake.description_propagation_action.is_snowflake_urn",
            return_value=False,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_should_not_propagate_when_disabled(self) -> None:
        """Test that description propagation is ignored when disabled."""
        config = DescriptionPropagationConfig(enabled=False)
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

        event = Mock()
        event.event_type = "EntityChangeEvent_v1"

        envelope = EventEnvelope(event=event, meta={})

        directive = action.should_propagate(envelope)

        assert directive is None

    def test_should_not_propagate_table_when_table_sync_disabled(self) -> None:
        """Test that table description propagation is ignored when table sync is disabled."""
        config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=False,
            column_description_sync_enabled=True,
        )
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

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
            "datahub_integrations.propagation.snowflake.description_propagation_action.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_extract_description_from_mcl_event(self) -> None:
        """Test extracting description from MetadataChangeLogEvent."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

        # Mock MCL event with table description
        mcl_event = Mock()
        mcl_event.aspectName = "editableDatasetProperties"
        mcl_event.aspect = Mock()
        mcl_event.aspect.value = b'{"description": "Test table description"}'

        description = action._extract_description_from_aspect(mcl_event)

        assert description == "Test table description"

    def test_extract_description_from_mcl_event_column(self) -> None:
        """Test extracting column description from MetadataChangeLogEvent."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()

        action = DescriptionPropagationAction(config, ctx)

        # Mock MCL event with column description
        mcl_event = Mock()
        mcl_event.aspectName = "editableSchemaMetadata"
        mcl_event.aspect = Mock()
        mcl_event.aspect.value = b'{"editableSchemaFieldInfo": [{"fieldPath": "test_column", "description": "Test column description"}]}'

        description = action._extract_description_from_aspect(mcl_event)

        assert description == "Test column description"
