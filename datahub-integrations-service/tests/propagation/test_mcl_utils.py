from unittest.mock import MagicMock

import pytest
from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
from datahub_actions.plugin.action.mcl_utils import MCLProcessor


@pytest.fixture
def mock_mcl_event() -> MagicMock:
    """Create a mock MetadataChangeLogClass event for testing"""
    mock_event = MagicMock(spec=MetadataChangeLogClass)
    mock_event.entityType = "dataset"
    mock_event.aspectName = "ownership"
    mock_event.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_table,PROD)"
    mock_event.aspect = {"owners": [{"owner": "user1", "type": "DATAOWNER"}]}
    mock_event.previousAspectValue = {"owners": []}
    return mock_event


@pytest.fixture
def event_envelope(mock_mcl_event: MagicMock) -> EventEnvelope:
    """Create a mock event envelope containing the MCL event"""
    return EventEnvelope(
        meta={},  # Not used in tests
        event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE,
        event=mock_mcl_event,
    )


@pytest.fixture
def non_mcl_event() -> EventEnvelope:
    """Create a non-MCL event for testing"""
    return EventEnvelope(
        meta={},  # Not used in tests
        event_type="some_other_event_type",
        event={"some": "data"},
    )


@pytest.fixture
def processor() -> MCLProcessor:
    """Create a basic MCLProcessor instance for testing"""
    return MCLProcessor()


def test_init() -> None:
    """Test the initialization of the MCLProcessor class"""
    # Test default initialization
    processor = MCLProcessor()
    assert processor.entity_aspect_processors == {}


def test_is_mcl(
    processor: MCLProcessor, event_envelope: EventEnvelope, non_mcl_event: EventEnvelope
) -> None:
    """Test the is_mcl method correctly identifies MCL events"""
    assert processor.is_mcl(event_envelope) is True
    assert processor.is_mcl(non_mcl_event) is False


def test_register_processor(processor: MCLProcessor) -> None:
    """Test registering a processor for a specific entity type and aspect"""
    mock_processor = MagicMock(return_value="processed")

    # Register a processor
    processor.register_processor("dataset", "ownership", mock_processor)

    # Check if the processor was registered correctly
    assert "dataset" in processor.entity_aspect_processors
    assert "ownership" in processor.entity_aspect_processors["dataset"]
    assert processor.entity_aspect_processors["dataset"]["ownership"] == mock_processor

    # Register another processor for the same entity type but different aspect
    mock_processor2 = MagicMock(return_value="processed2")
    processor.register_processor("dataset", "properties", mock_processor2)

    assert "properties" in processor.entity_aspect_processors["dataset"]
    assert (
        processor.entity_aspect_processors["dataset"]["properties"] == mock_processor2
    )

    # Register a processor for a different entity type
    mock_processor3 = MagicMock(return_value="processed3")
    processor.register_processor("chart", "ownership", mock_processor3)

    assert "chart" in processor.entity_aspect_processors
    assert "ownership" in processor.entity_aspect_processors["chart"]
    assert processor.entity_aspect_processors["chart"]["ownership"] == mock_processor3


def test_process_with_registered_processor(
    processor: MCLProcessor, event_envelope: EventEnvelope, mock_mcl_event: MagicMock
) -> None:
    """Test processing an event with a registered processor"""
    # Register a mock processor
    mock_processor = MagicMock(return_value="processed result")
    processor.register_processor("dataset", "ownership", mock_processor)

    # Process the event
    result = processor.process(event_envelope)

    # Verify the processor was called with the right arguments
    mock_processor.assert_called_once_with(
        entity_urn=mock_mcl_event.entityUrn,
        aspect_name=mock_mcl_event.aspectName,
        aspect_value=mock_mcl_event.aspect,
        previous_aspect_value=mock_mcl_event.previousAspectValue,
    )

    # Verify the result
    assert result == "processed result"


def test_process_without_registered_processor(
    processor: MCLProcessor, event_envelope: EventEnvelope
) -> None:
    """Test processing an event with no registered processor"""
    # No processors registered
    result = processor.process(event_envelope)

    # Should return None as no processor is registered
    assert result is None

    # Register a processor for a different entity/aspect
    mock_processor = MagicMock(return_value="processed result")
    processor.register_processor("user", "properties", mock_processor)

    # Process the event again
    result = processor.process(event_envelope)

    # Should still return None as no matching processor
    assert result is None

    # Processor should not have been called
    mock_processor.assert_not_called()


def test_process_non_mcl_event(
    processor: MCLProcessor, non_mcl_event: EventEnvelope
) -> None:
    """Test processing a non-MCL event"""
    # Register a processor that would match if it were an MCL event
    mock_processor = MagicMock(return_value="processed result")
    processor.register_processor("dataset", "ownership", mock_processor)

    # Process a non-MCL event
    result = processor.process(non_mcl_event)

    # Should return None
    assert result is None

    # Processor should not have been called
    mock_processor.assert_not_called()


def test_multiple_processors(
    processor: MCLProcessor, event_envelope: EventEnvelope, mock_mcl_event: MagicMock
) -> None:
    """Test registering and using multiple processors"""
    mock_processor1 = MagicMock(return_value="result1")
    mock_processor2 = MagicMock(return_value="result2")

    # Register multiple processors
    processor.register_processor("dataset", "ownership", mock_processor1)
    processor.register_processor("dataset", "schema", mock_processor2)

    # Process with the first processor
    result1 = processor.process(event_envelope)
    assert result1 == "result1"
    mock_processor1.assert_called_once()
    mock_processor2.assert_not_called()

    # Change the aspect name and process again
    mock_mcl_event.aspectName = "schema"
    result2 = processor.process(event_envelope)
    assert result2 == "result2"
    mock_processor2.assert_called_once()
