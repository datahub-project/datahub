from datetime import datetime, timezone
from unittest.mock import Mock, patch

from datahub_integrations.telemetry.chat_events import ChatbotInteractionEvent
from datahub_integrations.telemetry.telemetry import (
    BaseEvent,
    track_saas_event,
)


# Test event class
class TestEvent(BaseEvent):
    type: str = "test_event"
    test_field: str = "test_value"


def test_track_saas_event() -> None:
    # Create a test event
    event = TestEvent(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        user_urn="urn:li:corpuser:testuser",
    )

    # Mock the telemetry client
    with (
        patch("datahub_integrations.telemetry.telemetry.telemetry_client"),
        patch(
            "datahub_integrations.telemetry.telemetry._get_origin",
            return_value="http://test-frontend",
        ),
        patch(
            "datahub_integrations.telemetry.telemetry.graph_as_user"
        ) as mock_graph_as_user,
        patch("datahub_integrations.telemetry.telemetry.SEND_TELEMETRY_EVENTS", True),
    ):
        mock_user_graph = Mock()
        mock_user_graph.config.server = "http://test-server"

        mock_user_graph.server_id = "test-server-id"
        mock_user_graph._session = Mock()
        mock_user_graph._session.post.return_value = Mock(status_code=200)
        mock_graph_as_user.return_value = mock_user_graph

        # Call the method
        track_saas_event(event)

        # Verify API tracking was called
        mock_graph_as_user.assert_called_once_with("urn:li:corpuser:testuser")
        mock_user_graph._session.post.assert_called_once()

        # Verify the request payload
        call_args = mock_user_graph._session.post.call_args
        assert call_args[0][0] == "http://test-server/openapi/v1/tracking/track"

        payload = call_args[1]["json"]
        assert payload["type"] == "test_event"
        assert payload["timestamp"] == "2024-01-01T00:00:00+00:00"
        assert payload["actorUrn"] == "urn:li:corpuser:testuser"
        assert payload["origin"] == "http://test-frontend"
        assert payload["test_field"] == "test_value"
        assert payload["datahub_integrations_version"] is not None


def test_mixpanel_failures_do_not_break_logic() -> None:
    """Test that Mixpanel failures don't break the main logic flow."""
    # Create a test event with heavy data
    event = ChatbotInteractionEvent(
        chat_id="test_chat",
        message_id="test_message",
        slack_thread_id="test_thread",
        slack_message_id="test_msg",
        slack_user_id="test_user",
        slack_user_name="Test User",
        message_contents="Test message",
        response_contents="Test response",
        response_generation_duration_sec=1.5,
        full_history='{"messages": ["very long conversation history..."] * 1000}',
        reduction_sequence='{"reductions": ["large json data..."] * 500}',
    )

    # Mock the telemetry client to raise an exception
    with (
        patch(
            "datahub_integrations.telemetry.telemetry.telemetry_client"
        ) as mock_client,
        patch(
            "datahub_integrations.telemetry.telemetry._get_origin",
            return_value="http://test-frontend",
        ),
        patch(
            "datahub_integrations.telemetry.telemetry.graph_as_user"
        ) as mock_graph_as_user,
        patch("datahub_integrations.telemetry.telemetry.SEND_MIXPANEL_EVENTS", True),
        patch("datahub_integrations.telemetry.telemetry.SEND_TELEMETRY_EVENTS", True),
    ):
        # Make Mixpanel client raise an exception
        mock_client.track.side_effect = Exception(
            "Mixpanel error: item payload exceeds max size of 1MB"
        )

        mock_user_graph = Mock()
        mock_user_graph.config.server = "http://test-server"
        mock_user_graph._session = Mock()
        mock_user_graph._session.post.return_value = Mock(status_code=200)
        mock_graph_as_user.return_value = mock_user_graph

        # This should not raise an exception even though Mixpanel fails
        track_saas_event(event)

        # Verify Mixpanel was called (and failed)
        mock_client.track.assert_called_once()

        # Verify DataHub API was still called successfully
        mock_graph_as_user.assert_called_once_with("urn:li:corpuser:admin")
        mock_user_graph._session.post.assert_called_once()


def test_full_history_not_sent_to_mixpanel() -> None:
    """Test that full_history and reduction_sequence are excluded from Mixpanel payloads."""
    # Create a test event with heavy fields
    event = ChatbotInteractionEvent(
        chat_id="test_chat",
        message_id="test_message",
        slack_thread_id="test_thread",
        slack_message_id="test_msg",
        slack_user_id="test_user",
        slack_user_name="Test User",
        message_contents="Test message",
        response_contents="Test response",
        response_generation_duration_sec=1.5,
        full_history='{"messages": ["very long conversation history..."] * 1000}',
        reduction_sequence='{"reductions": ["large json data..."] * 500}',
    )

    with (
        patch(
            "datahub_integrations.telemetry.telemetry.telemetry_client"
        ) as mock_client,
        patch(
            "datahub_integrations.telemetry.telemetry._get_origin",
            return_value="http://test-frontend",
        ),
        patch(
            "datahub_integrations.telemetry.telemetry.graph_as_user"
        ) as mock_graph_as_user,
        patch("datahub_integrations.telemetry.telemetry.SEND_MIXPANEL_EVENTS", True),
        patch("datahub_integrations.telemetry.telemetry.SEND_TELEMETRY_EVENTS", True),
    ):
        mock_user_graph = Mock()
        mock_user_graph.config.server = "http://test-server"
        mock_user_graph._session = Mock()
        mock_user_graph._session.post.return_value = Mock(status_code=200)
        mock_graph_as_user.return_value = mock_user_graph

        track_saas_event(event)

        # Verify Mixpanel was called
        mock_client.track.assert_called_once()

        # Get the properties sent to Mixpanel
        call_args = mock_client.track.call_args
        mixpanel_properties = call_args[0][2]  # Third argument is properties

        # Verify heavy fields are NOT in Mixpanel payload
        assert "full_history" not in mixpanel_properties
        assert "reduction_sequence" not in mixpanel_properties

        # Verify other fields are still present
        assert mixpanel_properties["chat_id"] == "test_chat"
        assert mixpanel_properties["message_id"] == "test_message"
        assert mixpanel_properties["message_contents"] == "Test message"
        assert mixpanel_properties["response_contents"] == "Test response"
        assert mixpanel_properties["response_generation_duration_sec"] == 1.5

        # Verify DataHub API was called with full data
        mock_graph_as_user.assert_called_once_with("urn:li:corpuser:admin")
        mock_user_graph._session.post.assert_called_once()

        # Get the payload sent to DataHub API
        api_call_args = mock_user_graph._session.post.call_args
        api_payload = api_call_args[1]["json"]

        # Verify DataHub API gets the full data including heavy fields
        assert api_payload["full_history"] == event.full_history
        assert api_payload["reduction_sequence"] == event.reduction_sequence
