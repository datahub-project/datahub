# test_datahub_event_source.py

from typing import List, cast
from unittest.mock import MagicMock, patch

import pytest

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    EntityChangeEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext

# Import your source + config classes from the correct module path.
from datahub_actions.plugin.source.acryl.datahub_cloud_event_source import (
    DataHubEventSource,
    DataHubEventsSourceConfig,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_ack_manager import (
    AckManager,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer import (
    DataHubEventsConsumer,
    ExternalEvent,
    ExternalEventsResponse,
)


@pytest.fixture
def mock_pipeline_context() -> PipelineContext:
    """
    Create a mock PipelineContext with the attributes needed for DataHubEventSource.
    """
    mock_ctx = MagicMock(spec=PipelineContext)
    # Add pipeline_name so we don't get AttributeError:
    mock_ctx.pipeline_name = "test-pipeline"

    # We also assume mock_ctx.graph has a .graph attribute with a mock DataHubGraph.
    mock_ctx.graph = MagicMock()
    mock_ctx.graph.graph = MagicMock()  # The underlying DataHubGraph

    return cast(PipelineContext, mock_ctx)


@pytest.fixture
def base_config_dict() -> dict:
    """
    Base config dict that can be updated for specific tests.
    We will parse this into DataHubEventsSourceConfig in each test.
    """
    return {
        "topic": "PlatformEvent_v1",
        "lookback_days": None,
        "reset_offsets": False,
        "kill_after_idle_timeout": True,
        "idle_timeout_duration_seconds": 5,
        "event_processing_time_max_duration_seconds": 5,
    }


def test_create_source(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    """
    Validate that DataHubEventSource.create() properly instantiates the source.
    """
    source = DataHubEventSource.create(base_config_dict, mock_pipeline_context)
    assert isinstance(source, DataHubEventSource)
    # The consumer_id on the instance includes the action prefix from pipeline_name
    assert source.consumer_id == "urn:li:dataHubAction:test-pipeline"


def test_get_pipeline_urn() -> None:
    """
    Validate that _get_pipeline_urn() handles pipeline_name with or without the prefix.
    """
    urn = DataHubEventSource._get_pipeline_urn("some-pipeline")
    assert urn == "urn:li:dataHubAction:some-pipeline"

    urn2 = DataHubEventSource._get_pipeline_urn("urn:li:dataHubAction:already-a-urn")
    assert urn2 == "urn:li:dataHubAction:already-a-urn"


def test_source_initialization(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    """
    Validate that DataHubEventSource constructor sets up DataHubEventsConsumer and AckManager.
    """
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)
    assert source.consumer_id == "urn:li:dataHubAction:test-pipeline"
    assert isinstance(source.datahub_events_consumer, DataHubEventsConsumer)
    assert isinstance(source.ack_manager, AckManager)
    assert source.safe_to_ack_offset is None


def test_events_with_no_events(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    base_config_dict["idle_timeout_duration_seconds"] = 1
    base_config_dict["kill_after_idle_timeout"] = True
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    mock_consumer = MagicMock(spec=DataHubEventsConsumer)
    mock_consumer.offset_id = "offset-100"  # Set the mocked offset_id
    source.datahub_events_consumer = mock_consumer

    # We'll simulate that poll_events returns a response with 0 events repeatedly.
    empty_response = ExternalEventsResponse(offsetId="offset-100", count=0, events=[])
    mock_consumer.poll_events.return_value = empty_response

    with patch.object(
        source, "_get_current_timestamp_seconds", side_effect=[100, 101, 102, 103]
    ):
        events_iter = source.events()
        emitted_events = list(events_iter)  # Convert generator to list

        assert len(emitted_events) == 0

    assert mock_consumer.poll_events.call_count >= 1
    assert source.running is False


def test_events_with_some_events(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    """
    If poll_events returns events, verify that the source yields them and resets idle timer.
    """
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    mock_consumer = MagicMock(spec=DataHubEventsConsumer)
    mock_consumer.offset_id = "offset-100"
    source.datahub_events_consumer = mock_consumer
    mock_ack_manager = MagicMock(spec=AckManager)
    mock_ack_manager.outstanding_acks.side_effect = [0]

    source.ack_manager = mock_ack_manager

    # Simulate the consumer returning a batch of events
    event_value = '{"header":{"timestampMillis":1737170481713},"name":"entityChangeEvent","payload":{"value":"{\\"auditStamp\\":{\\"actor\\":\\"urn:li:corpuser:john.joyce@acryl.io\\",\\"time\\":1737170481713},\\"entityUrn\\":\\"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub_community.datahub_slack.message_file,PROD)\\",\\"entityType\\":\\"dataset\\",\\"modifier\\":\\"urn:li:tag:COLUMNFIELD\\",\\"category\\":\\"TAG\\",\\"operation\\":\\"ADD\\",\\"version\\":0,\\"parameters\\":{\\"tagUrn\\":\\"urn:li:tag:COLUMNFIELD\\"}}","contentType":"application/json"}}'
    fake_event = ExternalEvent(contentType="application/json", value=event_value)
    poll_response = ExternalEventsResponse(
        offsetId="offset-101", count=1, events=[fake_event]
    )
    mock_consumer.poll_events.return_value = poll_response

    # Add a side effect to exit the loop
    def _side_effect(*args, **kwargs):
        source.running = False
        return poll_response

    mock_consumer.poll_events.side_effect = _side_effect

    # Patch _get_current_timestamp_seconds
    with patch.object(source, "_get_current_timestamp_seconds", return_value=100):
        emitted = list(source.events())

    # Assertions
    assert len(emitted) == 1
    assert emitted[0].event_type == ENTITY_CHANGE_EVENT_V1_TYPE
    assert isinstance(emitted[0].event, EntityChangeEvent)
    mock_ack_manager.get_meta.assert_called_once()
    assert source.safe_to_ack_offset == "offset-100"  # Previous offset.
    assert mock_consumer.poll_events.call_count == 1


def test_outstanding_acks_timeout(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    """
    If ack_manager.outstanding_acks() never returns 0, we eventually raise an exception
    due to event_processing_time_max_duration_seconds.
    """
    base_config_dict["event_processing_time_max_duration_seconds"] = 2
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    mock_ack_manager = MagicMock(spec=AckManager)

    mock_ack_manager.outstanding_acks.return_value = 1  # always 1
    mock_ack_manager.acks = {"values": []}
    source.ack_manager = mock_ack_manager

    mock_consumer = MagicMock(spec=DataHubEventsConsumer)
    mock_consumer.offset_id = "offset-100"
    source.datahub_events_consumer = mock_consumer

    source.running = True

    # Ensure the call times out.
    list(source.events())

    assert source.running is False


def test_handle_pe() -> None:
    """
    Verify that handle_pe yields an EntityChangeEvent if the 'name' is 'entityChangeEvent',
    otherwise yields nothing.
    """
    # Valid "entityChangeEvent" object
    event_value = '{"header":{"timestampMillis":1737170481713},"name":"entityChangeEvent","payload":{"value":"{\\"auditStamp\\":{\\"actor\\":\\"urn:li:corpuser:john.joyce@acryl.io\\",\\"time\\":1737170481713},\\"entityUrn\\":\\"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub_community.datahub_slack.message_file,PROD)\\",\\"entityType\\":\\"dataset\\",\\"modifier\\":\\"urn:li:tag:COLUMNFIELD\\",\\"category\\":\\"TAG\\",\\"operation\\":\\"ADD\\",\\"version\\":0,\\"parameters\\":{\\"tagUrn\\":\\"urn:li:tag:COLUMNFIELD\\"}}","contentType":"application/json"}}'
    msg = ExternalEvent(contentType="application/json", value=event_value)

    envelopes: List[EventEnvelope] = list(DataHubEventSource.handle_pe(msg))
    assert len(envelopes) == 1
    assert envelopes[0].event_type == ENTITY_CHANGE_EVENT_V1_TYPE
    assert isinstance(envelopes[0].event, EntityChangeEvent)

    # Different event name => no yield
    event_value_2 = '{"header":{"timestampMillis":1737170481713},"name":"anotherEvent","payload":{"value":"{\\"auditStamp\\":{\\"actor\\":\\"urn:li:corpuser:john.joyce@acryl.io\\",\\"time\\":1737170481713},\\"entityUrn\\":\\"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub_community.datahub_slack.message_file,PROD)\\",\\"entityType\\":\\"dataset\\",\\"modifier\\":\\"urn:li:tag:COLUMNFIELD\\",\\"category\\":\\"TAG\\",\\"operation\\":\\"ADD\\",\\"version\\":0,\\"parameters\\":{\\"tagUrn\\":\\"urn:li:tag:COLUMNFIELD\\"}}","contentType":"application/json"}}'
    msg2 = ExternalEvent(contentType="application/json", value=event_value_2)
    envelopes2 = list(DataHubEventSource.handle_pe(msg2))
    assert len(envelopes2) == 0


def test_ack(mock_pipeline_context: PipelineContext, base_config_dict: dict) -> None:
    """
    Verify that ack() calls ack_manager.ack with the event's metadata.
    """
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    mock_ack_manager = MagicMock(spec=AckManager)
    source.ack_manager = mock_ack_manager

    envelope = EventEnvelope(
        event_type=ENTITY_CHANGE_EVENT_V1_TYPE,
        event=MagicMock(spec=EntityChangeEvent),
        meta={"batch_id": 1, "msg_id": 2},
    )
    source.ack(envelope, processed=True)
    mock_ack_manager.ack.assert_called_once_with(envelope.meta, processed=True)


def test_close(mock_pipeline_context: PipelineContext, base_config_dict: dict) -> None:
    """
    Verify that close() stops the source, commits offsets, and calls consumer.close().
    """
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    mock_consumer = MagicMock(spec=DataHubEventsConsumer)
    source.datahub_events_consumer = mock_consumer

    source.safe_to_ack_offset = "some-offset-id"

    source.close()
    assert source.running is False
    mock_consumer.commit_offsets.assert_called_once_with(offset_id="some-offset-id")
    mock_consumer.close.assert_called_once()


def test_should_idle_timeout(
    mock_pipeline_context: PipelineContext, base_config_dict: dict
) -> None:
    """
    Verify the idle timeout logic in _should_idle_timeout().
    """
    base_config_dict["idle_timeout_duration_seconds"] = 5
    config_model = DataHubEventsSourceConfig.parse_obj(base_config_dict)
    source = DataHubEventSource(config_model, mock_pipeline_context)

    # If events > 0 => always False
    assert (
        source._should_idle_timeout(num_events=2, last_idle_response_timestamp=100)
        is False
    )

    # If time difference < idle_timeout_duration_seconds => False
    with patch.object(source, "_get_current_timestamp_seconds", return_value=104):
        # 4 seconds from 100 => not timed out
        assert (
            source._should_idle_timeout(num_events=0, last_idle_response_timestamp=100)
            is False
        )

    # If time difference > idle_timeout_duration_seconds => returns True + sets running=False
    with patch.object(source, "_get_current_timestamp_seconds", return_value=106):
        # 6 seconds from 100 => timed out
        assert (
            source._should_idle_timeout(num_events=0, last_idle_response_timestamp=100)
            is True
        )
        assert source.running is False
