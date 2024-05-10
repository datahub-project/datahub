from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)
from slack_sdk import WebClient

from datahub_integrations.notifications.sinks.slack.slack_sink import (
    SlackNotificationSink,
)


@pytest.fixture
def sink() -> SlackNotificationSink:
    sink = SlackNotificationSink()

    # Mock the IdentityProvider and its batch_get_actors method
    identity_provider_mock = Mock()
    identity_provider_mock.batch_get_actors.return_value = {  # type: ignore[attr-defined]
        "actor_urn1": "actor1",
        "actor_urn2": "actor2",
    }
    sink.identity_provider = identity_provider_mock

    sink.graph = Mock(spec=DataHubGraph)
    sink.base_url = "testbaseurl"
    sink.last_credentials_refresh = None
    sink.slack_client = Mock(spec=WebClient)
    return sink


# Test function to ensure the correct template builder method is called
def test_send_new_incident_notification(sink: SlackNotificationSink) -> None:

    with (
        patch.object(sink, "_maybe_reload_web_client") as mock_reload_web_client,
        patch.object(
            sink, "_send_change_notification"
        ) as mock_send_change_notification,
    ):

        # Call send method with a specific template
        sink.send(
            NotificationRequestClass(
                message=NotificationMessageClass(
                    template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
                    parameters={
                        "incidentTitle": "title",
                        "incidentMessage": "message",
                        "entityName": "name",
                        "entityPath": "/path",
                    },
                ),
                recipients=[NotificationRecipientClass(type="SLACK_DM")],
            ),
            Mock(),
        )

        # Assert mock_reload_web_client method was called once
        mock_reload_web_client.assert_called_once()

        # Assert _send_change_notification method was called once
        mock_send_change_notification.assert_called_once()


# Test function to ensure the correct template builder method is called
def test_send_updated_incident_notification(sink: SlackNotificationSink) -> None:

    with (
        patch.object(sink, "_maybe_reload_web_client") as mock_reload_web_client,
        patch.object(
            sink, "_send_change_notification"
        ) as mock_send_change_notification,
    ):

        # Call send method with a specific template
        sink.send(
            NotificationRequestClass(
                message=NotificationMessageClass(
                    template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
                    parameters={
                        "incidentTitle": "title",
                        "incidentMessage": "message",
                        "entityName": "name",
                        "entityPath": "/path",
                    },
                ),
                recipients=[NotificationRecipientClass(type="SLACK_DM")],
            ),
            Mock(),
        )

        # Assert mock_reload_web_client method was called with correct arguments
        mock_reload_web_client.assert_called_once()

        # Assert _send_change_notification method was called with correct arguments
        mock_send_change_notification.assert_called_once()


def test_send_unsupported_template_type_raises_exception(
    sink: SlackNotificationSink,
) -> None:
    # Test for unsupported template type
    mock_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INGESTION_RUN_CHANGE,
            parameters={
                "incidentTitle": "title",
                "incidentMessage": "message",
                "entityName": "name",
                "entityPath": "/path",
            },
        ),
        recipients=[NotificationRecipientClass(type="SLACK_DM")],
    )
    mock_context = Mock()

    with pytest.raises(Exception):
        sink.send(mock_request, mock_context)


def test_send_when_slack_config_not_available(sink: SlackNotificationSink) -> None:
    # Test behavior when Slack config is not available
    with patch.object(
        sink,
        "_maybe_reload_web_client",
        side_effect=Exception("Slack config not available"),
    ):
        mock_request = NotificationRequestClass(
            message=NotificationMessageClass(
                template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
                parameters={
                    "incidentTitle": "title",
                    "incidentMessage": "message",
                    "entityName": "name",
                    "entityPath": "/path",
                },
            ),
            recipients=[NotificationRecipientClass(type="SLACK_DM")],
        )
        mock_context = Mock()

        with pytest.raises(Exception):
            sink.send(mock_request, mock_context)
