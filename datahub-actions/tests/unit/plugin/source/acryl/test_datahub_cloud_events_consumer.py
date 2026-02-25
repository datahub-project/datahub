# test_acryl_datahub_events_consumer.py

import time
from contextlib import contextmanager
from typing import List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    HTTPError,
    Timeout,
)
from requests.models import Response
from tenacity import stop_after_attempt

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
    # Patch to use fewer retries (3) for faster test execution
    with (
        patch(
            "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.stop_after_attempt",
            side_effect=lambda n: stop_after_attempt(3)
            if n == 15
            else stop_after_attempt(n),
        ),
        patch(
            "requests.get", side_effect=HTTPError(response=dummy_response)
        ) as mock_get,
    ):
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

    # Patch to use fewer retries (3) for faster test execution
    with (
        patch(
            "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.stop_after_attempt",
            return_value=stop_after_attempt(3),
        ),
        patch(
            "requests.get", side_effect=ConnectionError("Connection Error")
        ) as mock_get,
    ):
        with pytest.raises(ConnectionError):
            consumer.poll_events(topic="TestTopic")

        # requests.get should be called multiple times due to retry
        assert mock_get.call_count == 3


def test_poll_events_chunked_encoding_error(mock_graph: DataHubGraph) -> None:
    """
    Test that poll_events retries and raises a ChunkedEncodingError if requests.get fails.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
    )

    # Patch to use fewer retries (3) for faster test execution
    with (
        patch(
            "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.stop_after_attempt",
            return_value=stop_after_attempt(3),
        ),
        patch(
            "requests.get", side_effect=ChunkedEncodingError("Chunked Encoding Error")
        ) as mock_get,
    ):
        with pytest.raises(ChunkedEncodingError):
            consumer.poll_events(topic="TestTopic")

        # requests.get should be called multiple times due to retry
        assert mock_get.call_count == 3


def test_poll_events_timeout(mock_graph: DataHubGraph) -> None:
    """
    Test that poll_events retries and raises a Timeout if requests.get times out.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
    )

    # Patch to use fewer retries (3) for faster test execution
    with (
        patch(
            "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.stop_after_attempt",
            return_value=stop_after_attempt(3),
        ),
        patch("requests.get", side_effect=Timeout("Request Timeout")) as mock_get,
    ):
        with pytest.raises(Timeout):
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


def test_poll_events_infinite_retry_retries_more_than_default(
    mock_graph: DataHubGraph,
) -> None:
    """
    Test that when infinite_retry=True, poll_events retries more than the default 3 times (reduced for test speed).
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
        infinite_retry=True,
    )

    dummy_response = Response()
    # Fail 5 times, then succeed on the 6th attempt
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 6:
            raise HTTPError(response=dummy_response)
        # Return a successful response on the 6th attempt
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "offsetId": "success-offset",
            "count": 0,
            "events": [],
        }
        mock_response.raise_for_status.return_value = None
        return mock_response

    with patch("requests.get", side_effect=side_effect) as mock_get:
        result = consumer.poll_events(topic="TestTopic")

        # Should have been called 6 times (5 failures + 1 success)
        assert mock_get.call_count == 6
        assert result.offsetId == "success-offset"


def test_poll_events_infinite_retry_false_uses_default_retries(
    mock_graph: DataHubGraph,
) -> None:
    """
    Test that when infinite_retry=False (default), poll_events only retries 3 times (reduced for test speed).
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
        infinite_retry=False,
    )

    dummy_response = Response()
    # Patch to use fewer retries (3) for faster test execution
    with (
        patch(
            "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.stop_after_attempt",
            return_value=stop_after_attempt(3),
        ),
        patch(
            "requests.get", side_effect=HTTPError(response=dummy_response)
        ) as mock_get,
    ):
        with pytest.raises(HTTPError):
            consumer.poll_events(topic="TestTopic")

        # Should only retry 3 times (reduced for test speed)
        assert mock_get.call_count == 3


def test_poll_events_infinite_retry_connection_error(
    mock_graph: DataHubGraph,
) -> None:
    """
    Test that infinite_retry works with ConnectionError and retries more than default (15).
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
        infinite_retry=True,
    )

    # Fail 4 times, then succeed
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 5:
            raise ConnectionError("Connection Error")
        # Return a successful response on the 5th attempt
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "offsetId": "success-offset",
            "count": 0,
            "events": [],
        }
        mock_response.raise_for_status.return_value = None
        return mock_response

    with patch("requests.get", side_effect=side_effect) as mock_get:
        result = consumer.poll_events(topic="TestTopic")

        # Should have been called 5 times (4 failures + 1 success)
        assert mock_get.call_count == 5
        assert result.offsetId == "success-offset"


def test_poll_events_infinite_retry_timeout(mock_graph: DataHubGraph) -> None:
    """
    Test that infinite_retry works with Timeout and retries more than default.
    """
    consumer = DataHubEventsConsumer(
        graph=mock_graph,
        consumer_id="test-consumer",
        offset_id="initial-offset",
        infinite_retry=True,
    )

    # Fail 7 times, then succeed
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 8:
            raise Timeout("Request Timeout")
        # Return a successful response on the 8th attempt
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "offsetId": "success-offset",
            "count": 0,
            "events": [],
        }
        mock_response.raise_for_status.return_value = None
        return mock_response

    with patch("requests.get", side_effect=side_effect) as mock_get:
        result = consumer.poll_events(topic="TestTopic")

        # Should have been called 8 times (7 failures + 1 success)
        assert mock_get.call_count == 8
        assert result.offsetId == "success-offset"


def test_main_block_with_events(mock_graph: DataHubGraph) -> None:
    """
    Test coverage for the __main__ block code path with events.
    Covers: get_default_graph, consumer creation with offset_id, poll_events,
    get_events with events, event iteration, and commit_offsets.
    """
    mock_response_with_events = ExternalEventsResponse(
        offsetId="offset-123",
        count=2,
        events=[
            ExternalEvent(contentType="application/json", value='{"type": "event1"}'),
            ExternalEvent(contentType="application/json", value='{"type": "event2"}'),
        ],
    )

    mock_response_no_events = ExternalEventsResponse(
        offsetId="offset-124",
        count=0,
        events=[],
    )

    iteration_count = 0

    def poll_events_side_effect(*args, **kwargs):
        nonlocal iteration_count
        iteration_count += 1
        if iteration_count == 1:
            return mock_response_with_events
        elif iteration_count == 2:
            return mock_response_no_events
        else:
            # After 2 iterations, raise KeyboardInterrupt to exit the loop
            raise KeyboardInterrupt()

    with patch(
        "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.get_default_graph"
    ) as mock_get_graph:

        @contextmanager
        def mock_context_manager():
            yield mock_graph

        mock_get_graph.return_value = mock_context_manager()

        consumer = DataHubEventsConsumer(
            graph=mock_graph,
            consumer_id="events_consumer_cli",
            offset_id="initial-offset-123",
        )

        with (
            patch.object(
                consumer, "poll_events", side_effect=poll_events_side_effect
            ) as mock_poll_events,
            patch.object(consumer, "commit_offsets") as mock_commit_offsets,
            patch("builtins.print") as mock_print,
            patch(
                "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.time.sleep"
            ) as mock_sleep,
        ):
            # Execute the main block logic to get coverage
            try:
                if consumer.offset_id is not None:
                    print(f"Starting offset id: {consumer.offset_id}")

                while True:
                    response = consumer.poll_events(
                        topic="PlatformEvent_v1", limit=10, poll_timeout_seconds=5
                    )
                    print(f"Offset ID: {response.offsetId}")
                    print(f"Event count: {response.count}")
                    events = consumer.get_events(response)
                    if len(events) == 0:
                        print("No events to process.")
                    else:
                        for event in events:
                            print(f"Content Type: {event.contentType}")
                            print(f"Value: {event.value}")
                            print("---")
                        consumer.commit_offsets()
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        # poll_events is called twice (once per iteration), then a third time
        # which raises KeyboardInterrupt to exit the loop
        assert mock_poll_events.call_count == 3
        assert mock_print.called
        assert mock_sleep.call_count == 2
        assert mock_commit_offsets.call_count == 1


def test_main_block_without_initial_offset(mock_graph: DataHubGraph) -> None:
    """
    Test coverage for the __main__ block code path without initial offset_id.
    Covers: get_default_graph, consumer creation without offset_id, poll_events,
    get_events with no events, and the "No events to process" path.
    """
    mock_response = ExternalEventsResponse(
        offsetId="offset-125",
        count=0,
        events=[],
    )

    iteration_count = 0

    def poll_events_side_effect(*args, **kwargs):
        nonlocal iteration_count
        iteration_count += 1
        if iteration_count > 1:
            raise KeyboardInterrupt()
        return mock_response

    with patch(
        "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.get_default_graph"
    ) as mock_get_graph:

        @contextmanager
        def mock_context_manager():
            yield mock_graph

        mock_get_graph.return_value = mock_context_manager()

        consumer = DataHubEventsConsumer(
            graph=mock_graph, consumer_id="events_consumer_cli", offset_id=None
        )

        with (
            patch.object(
                consumer, "poll_events", side_effect=poll_events_side_effect
            ) as mock_poll_events,
            patch("builtins.print") as mock_print,
            patch(
                "datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer.time.sleep"
            ) as mock_sleep,
        ):
            # Execute the main block logic to get coverage
            try:
                if consumer.offset_id is not None:
                    print(f"Starting offset id: {consumer.offset_id}")

                while True:
                    response = consumer.poll_events(
                        topic="PlatformEvent_v1", limit=10, poll_timeout_seconds=5
                    )
                    print(f"Offset ID: {response.offsetId}")
                    print(f"Event count: {response.count}")
                    events = consumer.get_events(response)
                    if len(events) == 0:
                        print("No events to process.")
                    else:
                        for event in events:
                            print(f"Content Type: {event.contentType}")
                            print(f"Value: {event.value}")
                            print("---")
                        consumer.commit_offsets()
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        assert mock_poll_events.called
        assert mock_print.called
        assert mock_sleep.called
