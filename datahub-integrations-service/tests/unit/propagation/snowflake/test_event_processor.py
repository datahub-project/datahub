import json
from unittest.mock import MagicMock

import pytest
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)

from datahub_integrations.propagation.snowflake.event_processor import (
    EventData,
    EventProcessor,
)


class TestEventData:
    """Test EventData container class."""

    def test_event_data_initialization(self):
        """Test EventData initialization with all parameters."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
            parameters={"description": "test"},
        )

        assert (
            event_data.entity_urn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        assert event_data.event_type == "EntityChangeEvent_v1"
        assert event_data.operation == "ADD"
        assert event_data.category == "DOCUMENTATION"
        assert event_data.parameters == {"description": "test"}

    def test_event_data_default_parameters(self):
        """Test EventData with default empty parameters."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
        )

        assert event_data.parameters == {}
        assert event_data.aspect_data == {}
        assert event_data.category is None
        assert event_data.aspect_name is None
        assert event_data.change_type is None


class TestEventProcessor:
    """Test EventProcessor class."""

    @pytest.fixture
    def processor(self):
        """Create an EventProcessor instance."""
        return EventProcessor()

    def test_process_entity_change_event_snowflake(self, processor):
        """Test processing EntityChangeEvent for Snowflake URN."""
        # Create mock event
        mock_event = MagicMock(spec=EntityChangeEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        mock_event.operation = "ADD"
        mock_event.category = "DOCUMENTATION"
        mock_event._inner_dict = {
            "__parameters_json": {"description": "Test description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert (
            result.entity_urn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        assert result.event_type == "EntityChangeEvent_v1"
        assert result.operation == "ADD"
        assert result.category == "DOCUMENTATION"
        assert result.parameters == {"description": "Test description"}

    def test_process_entity_change_event_non_snowflake(self, processor):
        """Test that non-Snowflake URNs are filtered out."""
        # Create mock event for BigQuery (non-Snowflake)
        mock_event = MagicMock(spec=EntityChangeEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
        )
        mock_event.operation = "ADD"
        mock_event.category = "DOCUMENTATION"

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is None

    def test_process_metadata_change_log_event_snowflake(self, processor):
        """Test processing MetadataChangeLogEvent for Snowflake URN."""
        # Create mock MCL event
        mock_event = MagicMock(spec=MetadataChangeLogEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        mock_event.changeType = "UPSERT"
        mock_event.aspectName = "editableDatasetProperties"

        # Mock aspect data
        aspect_data = {"description": "Test table description"}
        mock_aspect = MagicMock()
        mock_aspect.value = json.dumps(aspect_data).encode("utf-8")
        mock_event.aspect = mock_aspect

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert (
            result.entity_urn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        assert result.event_type == "MetadataChangeLogEvent_v1"
        assert result.operation == "UPSERT"
        assert result.aspect_name == "editableDatasetProperties"
        assert result.change_type == "UPSERT"
        assert result.aspect_data == aspect_data

    def test_process_metadata_change_log_event_non_snowflake(self, processor):
        """Test that non-Snowflake MCL events are filtered out."""
        mock_event = MagicMock(spec=MetadataChangeLogEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
        )
        mock_event.changeType = "UPSERT"
        mock_event.aspectName = "editableDatasetProperties"

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is None

    def test_process_metadata_change_log_event_with_schema_field_urn(self, processor):
        """Test processing MCL event for schema field (column)."""
        mock_event = MagicMock(spec=MetadataChangeLogEvent)
        mock_event.entityUrn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)"
        mock_event.changeType = "UPSERT"
        mock_event.aspectName = "editableSchemaMetadata"

        aspect_data = {
            "editableSchemaFieldInfo": [
                {"fieldPath": "column_name", "description": "Column description"}
            ]
        }
        mock_aspect = MagicMock()
        mock_aspect.value = json.dumps(aspect_data).encode("utf-8")
        mock_event.aspect = mock_aspect

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert "column_name" in result.entity_urn
        assert result.aspect_data == aspect_data

    def test_process_metadata_change_log_event_no_aspect(self, processor):
        """Test processing MCL event without aspect data."""
        mock_event = MagicMock(spec=MetadataChangeLogEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        mock_event.changeType = "UPSERT"
        mock_event.aspectName = "editableDatasetProperties"
        mock_event.aspect = None

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert result.aspect_data == {}

    def test_process_metadata_change_log_event_invalid_json(self, processor):
        """Test processing MCL event with invalid JSON aspect data."""
        mock_event = MagicMock(spec=MetadataChangeLogEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        mock_event.changeType = "UPSERT"
        mock_event.aspectName = "editableDatasetProperties"

        mock_aspect = MagicMock()
        mock_aspect.value = b"invalid json"
        mock_event.aspect = mock_aspect

        envelope = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert result.aspect_data == {}

    def test_process_unsupported_event_type(self, processor):
        """Test that unsupported event types return None."""
        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="UnsupportedEventType_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is None

    def test_process_entity_change_event_with_platform_instance(self, processor):
        """Test processing EntityChangeEvent with platform instance in URN."""
        mock_event = MagicMock(spec=EntityChangeEvent)
        # URN with platform instance (prod)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        )
        mock_event.operation = "MODIFY"
        mock_event.category = "DOCUMENTATION"
        mock_event._inner_dict = {
            "__parameters_json": {"description": "Updated description"}
        }

        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1",
            event=mock_event,
            meta={},
        )

        result = processor.process_event(envelope)

        assert result is not None
        assert "prod.db.schema.table" in result.entity_urn
        assert result.operation == "MODIFY"
