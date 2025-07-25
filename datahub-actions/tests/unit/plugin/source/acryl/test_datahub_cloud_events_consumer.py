# test_acryl_datahub_events_consumer.py

from typing import List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectionError, HTTPError
from requests.models import Response

from datahub.ingestion.graph.client import DataHubGraph
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer import (
    DataHubEventsConsumer,
    ExternalEvent,
    ExternalEventsResponse,
)
from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer_offsets_store import (
    DataHubEventsConsumerPlatformResourceOffsetsStore,
)


@pytest.fixture
def mock_graph() -> DataHubGraph:
    """
    Provide a mock DataHubGraph instance, including a mock config and _session.
    This prevents 'AttributeError: Mock object has no attribute "config"'.
    """
    mock_graph = MagicMock(spec=DataHubGraph)

    # Mock config object with a server attribute
    mock_config = MagicMock()
    mock_config.server = "http://fake-datahub"
    mock_graph.config = mock_config

    # Mock _session with headers
    mock_session = MagicMock()
    mock_session.headers = {"Authorization": "Bearer test-token"}
    mock_graph._session = mock_session

    return cast(DataHubGraph, mock_graph)


@pytest.fixture
def external_events_response() -> ExternalEventsResponse:
    """
    Provide a sample ExternalEventsResponse for use in tests.
    """
    return ExternalEventsResponse(
        offsetId="next-offset-123",
        count=2,
        events=[
            ExternalEvent(contentType="application/json", value='{"key": "value"}'),
            ExternalEvent(contentType="application/json", value='{"another": "event"}'),
        ],
    )


def test_consumer_init_with_consumer_id_loads_offsets(mock_graph: DataHubGraph) -> None:
    """
    Verify that providing a consumer_id triggers loading existing offsets
    from the store, unless reset_offsets is True.
    """
    mock_store = MagicMock(spec=DataHubEventsConsumerPlatformResourceOffsetsStore)
    mock_store.load_offset_id.return_value = "loaded-offset"

    # Patch the store's constructor so it returns our mock_store
    with (
        patch.object(
            target=DataHubEventsConsumerPlatformResourceOffsetsStore,
            attribute="__init__",
            return_value=None,
        ),
        patch.object(
            DataHubEventsConsumerPlatformResourceOffsetsStore,
            "load_offset_id",
            new=mock_store.load_offset_id,
        ),
    ):
        # Construct the consumer
        consumer = DataHubEventsConsumer(
            graph=mock_graph, consumer_id="test-consumer", reset_offsets=False
        )
        # Because we've mocked __init__, manually set consumer.offsets_store
        consumer.offsets_store = mock_store

        assert consumer.consumer_id == "test-consumer"
        # load_offset_id was called
        mock_store.load_offset_id.assert_called_once()
        # consumer.offset_id should come from the store
        assert consumer.offset_id == "loaded-offset"


def test_consumer_init_with_consumer_id_and_reset_offsets(
    mock_graph: DataHubGraph,
) -> None:
    """
    Verify that when reset_offsets=True, we do NOT load the offset
    from the store.
    """
    mock_store = MagicMock(spec=DataHubEventsConsumerPlatformResourceOffsetsStore)

    with patch.object(
        target=DataHubEventsConsumerPlatformResourceOffsetsStore,
        attribute="__init__",
        return_value=None,
    ):
        consumer = DataHubEventsConsumer(
            graph=mock_graph, consumer_id="test-consumer", reset_offsets=True
        )
        consumer.offsets_store = mock_store

        # load_offset_id should NOT be called if reset_offsets=True
        mock_store.load_offset_id.assert_not_called()
        assert consumer.offset_id is None


def test_consumer_init_without_consumer_id(mock_graph: DataHubGraph) -> None:
    """
    Verify that if no consumer_id is provided, no offsets store is created.
    """
    consumer = DataHubEventsConsumer(graph=mock_graph, consumer_id=None)
    assert consumer.offsets_store is None
    assert consumer.consumer_id is None


@pytest.mark.parametrize("given_offset_id", [None, "custom-offset-789"])
def test_poll_events_success(
    mock_graph: DataHubGraph,
    external_events_response: ExternalEventsResponse,
    given_offset_id: Optional[str],
) -> None:
    """
    Test that poll_events calls the correct endpoint, updates offset_id,
    and returns an ExternalEventsResponse.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset-456",
    )

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        # Simulate JSON decoding
        mock_response.json.return_value = {
            "offsetId": external_events_response.offsetId,
            "count": external_events_response.count,
            "events": [
                {"contentType": evt.contentType, "value": evt.value}
                for evt in external_events_response.events
            ],
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        polled_response = consumer.poll_events(
            topic="TestTopic",
            offset_id=given_offset_id,
            limit=10,
            poll_timeout_seconds=5,
        )

        # Verify the request params
        expected_params = {
            "topic": "TestTopic",
            "offsetId": given_offset_id or "initial-offset-456",
            "limit": 10,
            "pollTimeoutSeconds": 5,
        }

        # Check that requests.get was called once
        mock_get.assert_called_once()
        call_args, call_kwargs = mock_get.call_args
        assert call_args[0] == f"{consumer.base_url}/v1/events/poll"
        assert call_kwargs["params"] == expected_params

        # Verify returned response
        assert polled_response.offsetId == external_events_response.offsetId
        assert polled_response.count == external_events_response.count
        assert len(polled_response.events) == len(external_events_response.events)

        # The consumer's offset_id should be updated
        assert consumer.offset_id == external_events_response.offsetId


def test_poll_events_http_error(mock_graph: DataHubGraph) -> None:
    """
    Test that poll_events retries and raises an HTTPError if requests.get fails.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
    )
    dummy_response = Response()  # Create a dummy Response object
    with patch(
        "requests.get", side_effect=HTTPError(response=dummy_response)
    ) as mock_get:
        # The default Tenacity stop_after_attempt=3
        with pytest.raises(HTTPError):
            consumer.poll_events(topic="TestTopic")

        # requests.get should be called multiple times due to retry
        assert mock_get.call_count == 3


def test_poll_events_connection_error(mock_graph: DataHubGraph) -> None:
    """
    Test that poll_events retries and raises a ConnectionError if requests.get fails.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
    )

    with patch(
        "requests.get", side_effect=ConnectionError("Connection Error")
    ) as mock_get:
        with pytest.raises(ConnectionError):
            consumer.poll_events(topic="TestTopic")

        # requests.get should be called multiple times due to retry
        assert mock_get.call_count == 3


def test_get_events(
    mock_graph: DataHubGraph, external_events_response: ExternalEventsResponse
) -> None:
    """
    Test that get_events simply returns the events list from the response.
    """
    consumer = DataHubEventsConsumer(graph=mock_graph)
    events: List[ExternalEvent] = consumer.get_events(external_events_response)
    assert len(events) == 2
    assert all(isinstance(e, ExternalEvent) for e in events)


def test_commit_offsets_with_explicit_offset(mock_graph: DataHubGraph) -> None:
    """
    Test commit_offsets when an explicit offset_id is passed.
    """
    mock_store = MagicMock(spec=DataHubEventsConsumerPlatformResourceOffsetsStore)
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id=None,
    )
    consumer.offsets_store = mock_store

    consumer.commit_offsets(offset_id="commit-test-offset")
    mock_store.store_offset_id.assert_called_once_with("commit-test-offset")


def test_commit_offsets_with_consumer_offset(mock_graph: DataHubGraph) -> None:
    """
    Test commit_offsets uses the consumer's current offset if none is provided.
    """
    mock_store = MagicMock(spec=DataHubEventsConsumerPlatformResourceOffsetsStore)
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="current-offset",
    )
    consumer.offsets_store = mock_store

    consumer.commit_offsets()
    mock_store.store_offset_id.assert_called_once_with("current-offset")


def test_commit_offsets_no_store(mock_graph: DataHubGraph) -> None:
    """
    If offsets_store is None, commit_offsets should do nothing and not error.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id=None,
        offset_id="some-offset",
    )
    # offsets_store remains None
    consumer.commit_offsets("ignored-offset")
    # No error should occur, and no calls to the store.
    # We just assert that the code doesn't raise an exception.
    # (No additional assert needed here.)
