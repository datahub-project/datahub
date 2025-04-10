# test_offsets_store.py

from typing import Optional, cast
from unittest.mock import MagicMock

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    PlatformResourceInfoClass,
    SerializedValueClass,
    SerializedValueContentTypeClass,
    SerializedValueSchemaTypeClass,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer_offsets_store import (
    DataHubEventsConsumerPlatformResourceOffsetsStore,
    EventConsumerState,
)


def test_event_consumer_state_serialization() -> None:
    """
    Tests that EventConsumerState can be serialized and deserialized properly.
    """
    state = EventConsumerState(offset_id="test-offset", timestamp=1234567890)
    serialized_value = state.to_serialized_value()

    assert serialized_value.contentType == SerializedValueContentTypeClass.JSON
    assert serialized_value.schemaType == SerializedValueSchemaTypeClass.JSON
    assert serialized_value.schemaRef is not None
    assert "EventConsumerState@" in serialized_value.schemaRef

    # Round-trip: from_serialized_value
    deserialized_state = EventConsumerState.from_serialized_value(serialized_value)
    assert deserialized_state.offset_id == "test-offset"
    assert deserialized_state.timestamp == 1234567890


def test_event_consumer_state_missing_schema_handling() -> None:
    """
    Tests that from_serialized_value gracefully handles missing or incorrect schemaRef.
    """
    # Force an empty schemaRef
    invalid_serialized_value = SerializedValueClass(
        blob=b'{"offset_id":"test-offset","timestamp":987654321}',
        contentType=SerializedValueContentTypeClass.JSON,
        schemaType=SerializedValueSchemaTypeClass.JSON,
        schemaRef=None,
    )
    deserialized_state = EventConsumerState.from_serialized_value(
        invalid_serialized_value
    )
    assert deserialized_state.offset_id == "test-offset"
    assert deserialized_state.timestamp == 987654321


def test_store_offset_id() -> None:
    """
    Tests that store_offset_id emits the correct MCP to DataHubGraph.
    """
    mock_graph: DataHubGraph = MagicMock()
    store = DataHubEventsConsumerPlatformResourceOffsetsStore(
        graph=mock_graph,
        consumer_id="test_consumer",
    )

    # Store an offset
    offset_value: str = "my-test-offset"
    returned_offset = store.store_offset_id(offset_value)

    # Check the returned value
    assert returned_offset == offset_value

    # Cast the emit method to MagicMock to access call_count and call_args
    emit_mock = cast(MagicMock, mock_graph.emit)
    # Ensure we called `mock_graph.emit` once with an MCP
    assert emit_mock.call_count == 1

    # You can dig deeper into call args if you want to check the actual MCP content:
    call_args = emit_mock.call_args
    emitted_mcp = call_args[0][0]  # The first argument to emit
    assert emitted_mcp.entityUrn == store.state_resource_urn
    # The aspect should be a PlatformResourceInfoClass
    resource_info = cast(PlatformResourceInfoClass, emitted_mcp.aspect)
    assert resource_info.primaryKey == "test_consumer"
    assert resource_info.resourceType == "eventConsumerState"


def test_load_offset_id_no_existing_state() -> None:
    """
    Tests that load_offset_id returns None if the aspect is not present.
    """
    mock_graph: DataHubGraph = MagicMock()
    # Cast get_aspect to MagicMock before setting return_value and asserting calls
    get_aspect_mock = cast(MagicMock, mock_graph.get_aspect)
    get_aspect_mock.return_value = None  # Simulate no stored aspect

    store = DataHubEventsConsumerPlatformResourceOffsetsStore(
        graph=mock_graph,
        consumer_id="test_consumer",
    )

    offset: Optional[str] = store.load_offset_id()
    assert offset is None

    # Ensure we called `get_aspect` once
    get_aspect_mock.assert_called_once()


def test_load_offset_id_existing_state() -> None:
    """
    Tests that load_offset_id returns the correct offset_id if the aspect is present.
    """
    # Prepare a mocked, serialized EventConsumerState
    state = EventConsumerState(offset_id="existing-offset", timestamp=1111111)
    serialized_value = state.to_serialized_value()

    # Create a mock PlatformResourceInfoClass that includes our serialized state
    mock_resource_info = PlatformResourceInfoClass(
        resourceType="eventConsumerState",
        primaryKey="test_consumer",
        value=serialized_value,
    )

    mock_graph: DataHubGraph = MagicMock()
    # Cast get_aspect to MagicMock before setting return_value and asserting calls
    get_aspect_mock = cast(MagicMock, mock_graph.get_aspect)
    get_aspect_mock.return_value = mock_resource_info

    store = DataHubEventsConsumerPlatformResourceOffsetsStore(
        graph=mock_graph,
        consumer_id="test_consumer",
    )

    offset: Optional[str] = store.load_offset_id()
    assert offset == "existing-offset"

    # Ensure we called `get_aspect` once
    get_aspect_mock.assert_called_once()
