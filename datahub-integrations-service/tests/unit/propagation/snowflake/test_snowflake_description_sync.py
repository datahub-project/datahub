from unittest.mock import Mock, patch

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent

from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
)
from datahub_integrations.propagation.snowflake.description_propagation_action import (
    DescriptionPropagationAction,
)


class TestDescriptionPropagationAction:
    def test_should_propagate_table_description(self) -> None:
        """Test that table description changes are detected for propagation."""
        config = DescriptionPropagationConfig(
            enabled=True, table_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EntityChangeEvent for table description change
        # Note: Using Mock(spec=EntityChangeEvent) to ensure type compatibility
        # while allowing us to set custom attributes for testing
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.entityType = "dataset"
        entity_change_event.category = (
            "DOCUMENTATION"  # Required for description extraction
        )
        entity_change_event.operation = "ADD"
        entity_change_event.version = 1
        entity_change_event.auditStamp = Mock()
        # Set both safe_parameters and _inner_dict to support different access patterns
        entity_change_event.safe_parameters = {"description": "Test table description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test table description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

        with patch(
            "datahub_integrations.propagation.snowflake.event_processor.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.description == "Test table description"
        assert directive.operation == "ADD"

    def test_should_propagate_column_description(self) -> None:
        """Test that column description changes are detected for propagation."""
        config = DescriptionPropagationConfig(
            enabled=True, column_description_sync_enabled=True
        )
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EntityChangeEvent for column description change
        # Using schemaField URN to test column-level description propagation
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        entity_change_event.entityType = "schemaField"
        entity_change_event.category = (
            "DOCUMENTATION"  # Required for description extraction
        )
        entity_change_event.operation = "MODIFY"
        entity_change_event.version = 1
        entity_change_event.auditStamp = Mock()
        # Set both safe_parameters and _inner_dict to support different access patterns
        entity_change_event.safe_parameters = {"description": "Test column description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test column description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

        with patch(
            "datahub_integrations.propagation.snowflake.event_processor.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is not None
        assert directive.propagate is True
        assert directive.description == "Test column description"
        assert directive.operation == "MODIFY"

    def test_should_not_propagate_non_snowflake_urn(self) -> None:
        """Test that non-Snowflake URNs are ignored."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EntityChangeEvent for non-Snowflake URN
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
        )
        entity_change_event.entityType = "dataset"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"
        entity_change_event.version = 1
        entity_change_event.auditStamp = Mock()
        entity_change_event.safe_parameters = {"description": "Test description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

        with patch(
            "datahub_integrations.propagation.snowflake.event_processor.is_snowflake_urn",
            return_value=False,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_should_not_propagate_when_disabled(self) -> None:
        """Test that description propagation is ignored when disabled."""
        config = DescriptionPropagationConfig(enabled=False)
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a minimal mock event for disabled test
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.entityType = "dataset"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"
        entity_change_event.version = 1
        entity_change_event.auditStamp = Mock()
        entity_change_event.safe_parameters = {"description": "Test description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

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
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EntityChangeEvent for table description when table sync is disabled
        entity_change_event = Mock(spec=EntityChangeEvent)
        entity_change_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        entity_change_event.entityType = "dataset"
        entity_change_event.category = "DOCUMENTATION"
        entity_change_event.operation = "ADD"
        entity_change_event.version = 1
        entity_change_event.auditStamp = Mock()
        entity_change_event.safe_parameters = {"description": "Test table description"}
        entity_change_event._inner_dict = {
            "__parameters_json": {"description": "Test table description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=entity_change_event, meta={}
        )

        with patch(
            "datahub_integrations.propagation.snowflake.event_processor.is_snowflake_urn",
            return_value=True,
        ):
            directive = action.should_propagate(envelope)

        assert directive is None

    def test_extract_description_from_mcl_event(self) -> None:
        """Test extracting description from MetadataChangeLogEvent."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EventData for MCL event with table description
        event_data = Mock()
        event_data.event_type = "MetadataChangeLogEvent_v1"
        event_data.entity_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        event_data.aspect_name = "editableDatasetProperties"
        event_data.aspect_value = '{"description": "Test table description"}'
        event_data.change_type = "UPSERT"
        event_data.aspect_data = {"description": "Test table description"}

        description = action.description_extractor.extract_description(event_data)

        assert description == "Test table description"

    def test_extract_description_from_mcl_event_column(self) -> None:
        """Test extracting column description from MetadataChangeLogEvent."""
        config = DescriptionPropagationConfig(enabled=True)
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"

        action = DescriptionPropagationAction(config, ctx)

        # Create a mock EventData for MCL event with column description
        event_data = Mock()
        event_data.event_type = "MetadataChangeLogEvent_v1"
        event_data.entity_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        event_data.aspect_name = "editableSchemaMetadata"
        event_data.aspect_value = '{"editableSchemaFieldInfo": [{"fieldPath": "test_column", "description": "Test column description"}]}'
        event_data.change_type = "UPSERT"
        event_data.aspect_data = {
            "editableSchemaFieldInfo": [
                {"fieldPath": "test_column", "description": "Test column description"}
            ]
        }

        description = action.description_extractor.extract_description(event_data)

        assert description == "Test column description"
