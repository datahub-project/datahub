from datetime import datetime, timezone
from unittest.mock import Mock, patch

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
        patch("datahub_integrations.telemetry.telemetry.graph") as mock_graph,
        patch(
            "datahub_integrations.telemetry.telemetry.graph_as_user"
        ) as mock_graph_as_user,
    ):
        mock_graph.frontend_base_url = "http://test-frontend"

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
