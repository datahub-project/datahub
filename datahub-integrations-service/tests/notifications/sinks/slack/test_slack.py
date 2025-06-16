import unittest
from unittest.mock import ANY, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    IncidentNotificationDetailsClass,
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)
from slack_sdk import WebClient

from datahub_integrations.notifications.sinks.slack.slack_sink import (
    SlackNotificationSink,
)
from datahub_integrations.notifications.sinks.slack.types import SlackMessageDetails
from datahub_integrations.slack.config import SlackConnection


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
    sink.last_credentials_refresh_attempt = None
    sink.slack_client = Mock(spec=WebClient)
    sink.slack_connection_config = SlackConnection(bot_token="token")
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
def test_update_new_incident_notification(sink: SlackNotificationSink) -> None:
    # Prepare the list of SlackMessageDetails
    message_details = [
        SlackMessageDetails(
            channel_id="C123", message_id="msg1", channel_name="general"
        ),
        SlackMessageDetails(
            channel_id="C456", message_id="msg2", channel_name="random"
        ),
    ]

    with (
        patch.object(sink, "_maybe_reload_web_client") as mock_reload_web_client,
        patch.object(sink, "_update_messages") as mock_update_messages,
        patch.object(
            sink, "_get_saved_message_details", return_value=message_details
        ) as mock_get_saved_message_details,
    ):
        # Call send method with a specific template
        sink.send(
            NotificationRequestClass(
                message=NotificationMessageClass(
                    template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE,
                    parameters={
                        "incidentTitle": "title",
                        "incidentMessage": "message",
                        "entityName": "name",
                        "entityPath": "/path",
                        "newStatus": "RESOLVED",
                    },
                ),
                recipients=[NotificationRecipientClass(type="SLACK_DM")],
            ),
            Mock(),
        )

        # Assert mock_reload_web_client method was called once
        mock_reload_web_client.assert_called_once()

        # Assert that _get_saved_message_details was called and returned the expected list
        mock_get_saved_message_details.assert_called_once()

        # Assert that we called to update messages with the expected details
        # Unfortunately we don't validate the exact structure of the message.
        mock_update_messages.assert_called_once_with(
            message_details, ANY, ANY, ANY, ANY
        )


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


def test_send_unsupported_template_type(
    sink: SlackNotificationSink,
) -> None:
    with (
        patch.object(sink, "_maybe_reload_web_client") as mock_reload_web_client,
        patch.object(
            sink, "_send_change_notification"
        ) as mock_send_change_notification,
    ):
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

        sink.send(mock_request, mock_context)

        # Assert mock_reload_web_client method was called with correct arguments
        mock_reload_web_client.assert_called_once()

        # Nothing is sent
        mock_send_change_notification.assert_not_called()


def test_send_when_slack_config_not_available(sink: SlackNotificationSink) -> None:
    # Test behavior when Slack config is not available
    with patch.object(
        sink,
        "_maybe_reload_web_client",
        side_effect=ValueError("Slack config not available"),
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

        with pytest.raises(ValueError):
            sink.send(mock_request, mock_context)


def test_send_when_slack_config_not_available_no_raise(
    sink: SlackNotificationSink,
) -> None:
    # Test behavior when Slack config is not available and reload doesn't throw exception
    with (
        patch.object(sink, "_maybe_reload_web_client") as mock_reload_web_client,
        patch.object(
            sink, "_send_change_notification"
        ) as mock_send_change_notification,
    ):
        sink.slack_connection_config = None
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

        sink.send(mock_request, mock_context)

        mock_reload_web_client.assert_called_once()

        mock_send_change_notification.assert_not_called()


# Test get saved message details - channel id match
def test_get_saved_message_details_matching_ids(sink: SlackNotificationSink) -> None:
    # Setup
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="BROADCAST_NEW_INCIDENT_UPDATE",
        ),
        recipients=[
            NotificationRecipientClass(type="SLACK_DM", id="1"),
            NotificationRecipientClass(type="SLACK_DM", id="2"),
        ],
    )
    notification_details_mock = Mock()
    notification_details_mock.slack.messages = [
        Mock(channelId="1", channelName="general", messageId="msg1"),
        Mock(channelId="2", channelName="random", messageId="msg2"),
    ]

    # Mock graph.get_aspect to return our mocked notification details
    with patch.object(
        sink.graph, "get_aspect", return_value=notification_details_mock
    ) as mock_get_aspect:
        result = sink._get_saved_message_details(request)

        # Verify get_aspect was called correctly
        mock_get_aspect.assert_called_once_with(
            entity_urn="urn123", aspect_type=IncidentNotificationDetailsClass
        )

        # Assert that the result contains all the correct message details
        assert len(result) == 2
        assert result[0].channel_id == "1"
        assert result[0].channel_name == "general"
        assert result[0].message_id == "msg1"

        assert result[1].channel_id == "2"
        assert result[1].channel_name == "random"
        assert result[1].message_id == "msg2"


# Test get saved message details - channel name match
def test_get_saved_message_details_matching_channel_names(
    sink: SlackNotificationSink,
) -> None:
    # Setup
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="BROADCAST_NEW_INCIDENT_UPDATE",
        ),
        recipients=[
            NotificationRecipientClass(type="SLACK_DM", id="general"),
            NotificationRecipientClass(type="SLACK_DM", id="random"),
        ],
    )
    notification_details_mock = Mock()
    notification_details_mock.slack.messages = [
        Mock(channelId="1", channelName="general", messageId="msg1"),
        Mock(channelId="2", channelName="random", messageId="msg2"),
    ]

    # Mock graph.get_aspect to return our mocked notification details
    with patch.object(
        sink.graph, "get_aspect", return_value=notification_details_mock
    ) as mock_get_aspect:
        result = sink._get_saved_message_details(request)

        # Verify get_aspect was called correctly
        mock_get_aspect.assert_called_once_with(
            entity_urn="urn123", aspect_type=IncidentNotificationDetailsClass
        )

        # Assert that the result contains all the correct message details
        assert len(result) == 2
        assert result[0].channel_id == "1"
        assert result[0].channel_name == "general"
        assert result[0].message_id == "msg1"

        assert result[1].channel_id == "2"
        assert result[1].channel_name == "random"
        assert result[1].message_id == "msg2"


# Test get saved message details without matching messages
def test_get_saved_message_details_no_matching_recipients(
    sink: SlackNotificationSink,
) -> None:
    # Setup
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="BROADCAST_NEW_INCIDENT_UPDATE",
        ),
        recipients=[
            NotificationRecipientClass(type="SLACK_DM", id="general"),
            NotificationRecipientClass(type="SLACK_DM", id="random"),
        ],
    )
    notification_details_mock = Mock()
    notification_details_mock.slack.messages = []

    # Mock graph.get_aspect to return our mocked notification details
    with patch.object(
        sink.graph, "get_aspect", return_value=notification_details_mock
    ) as mock_get_aspect:
        result = sink._get_saved_message_details(request)

        # Verify get_aspect was called correctly
        mock_get_aspect.assert_called_once_with(
            entity_urn="urn123", aspect_type=IncidentNotificationDetailsClass
        )

        # No matching results
        assert len(result) == 0


# Test get saved message details when exception thrown by GMS.
def test_get_saved_message_details_graph_exception(sink: SlackNotificationSink) -> None:
    # Setup
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="BROADCAST_NEW_INCIDENT_UPDATE",
        ),
        recipients=[
            NotificationRecipientClass(type="SLACK_DM", id="general"),
            NotificationRecipientClass(type="SLACK_DM", id="random"),
        ],
    )
    notification_details_mock = Mock()
    notification_details_mock.slack.messages = []

    # Mock graph.get_aspect to return our mocked notification details
    with patch.object(
        sink.graph, "get_aspect", side_effect=Exception("Database failure")
    ) as mock_get_aspect:
        with unittest.TestCase().assertRaises(Exception) as context:
            sink._get_saved_message_details(request)

        # Verify get_aspect was called correctly
        mock_get_aspect.assert_called_once_with(
            entity_urn="urn123", aspect_type=IncidentNotificationDetailsClass
        )

        # Check if the raised exception is as expected
        assert "Database failure" in str(context.exception)


# Test get saved message details
def test_save_message_details(sink: SlackNotificationSink) -> None:
    message_details = [
        SlackMessageDetails(
            channel_id="C123", message_id="msg1", channel_name="general"
        ),
        SlackMessageDetails(
            channel_id="C456", message_id="msg2", channel_name="random"
        ),
    ]
    # Setup request with incidentUrn
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="BROADCAST_NEW_INCIDENT",
        ),
        recipients=[NotificationRecipientClass(type="SLACK_DM")],
    )

    # Patch the method to check it's called correctly
    with patch.object(
        sink, "_save_new_incident_message_details", return_value=True
    ) as mock_save_new_incident:
        sink._save_message_details(
            request, str(request.message.template), message_details
        )
        # Verify method calls
        mock_save_new_incident.assert_called_once_with("urn123", message_details)


def test_save_message_details_without_incident_urn(sink: SlackNotificationSink) -> None:
    message_details = [
        SlackMessageDetails(
            channel_id="C123", message_id="msg1", channel_name="general"
        ),
        SlackMessageDetails(
            channel_id="C456", message_id="msg2", channel_name="random"
        ),
    ]
    # Setup request without incidentUrn
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={},  # No incidentUrn provided
            template="BROADCAST_NEW_INCIDENT",
        ),
        recipients=[NotificationRecipientClass(type="SLACK_DM")],
    )

    # Patch the method to check it is not called
    with patch.object(
        sink, "_save_new_incident_message_details"
    ) as mock_save_new_incident:
        sink._save_message_details(
            request, str(request.message.template), message_details
        )

        # Verify method was not called
        mock_save_new_incident.assert_not_called()


def test_save_message_details_wrong_template_type(sink: SlackNotificationSink) -> None:
    message_details = [
        SlackMessageDetails(
            channel_id="C123", message_id="msg1", channel_name="general"
        ),
        SlackMessageDetails(
            channel_id="C456", message_id="msg2", channel_name="random"
        ),
    ]
    # Setup request with incidentUrn but wrong template_type
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            parameters={"incidentUrn": "urn123"},
            template="SOME_OTHER_TEMPLATE",
        ),
        recipients=[NotificationRecipientClass(type="SLACK_DM")],
    )

    # Ensure _save_new_incident_message_details is not called with wrong template_type
    with patch.object(
        sink, "_save_new_incident_message_details"
    ) as mock_save_new_incident:
        sink._save_message_details(
            request, str(request.message.template), message_details
        )

        mock_save_new_incident.assert_not_called()


def test_get_slack_recipients_filters_non_slack(
    sink: SlackNotificationSink,
) -> None:
    # Create a request with mixed recipient types
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_CHANNEL, id="slack-channel"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email@example.com"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.CUSTOM, id="custom-channel"
            ),
        ],
    )

    filtered_recipients = sink._get_slack_recipients(request)

    # Should only contain the Slack channel recipient
    assert len(filtered_recipients) == 1
    assert filtered_recipients[0].type == NotificationRecipientTypeClass.SLACK_CHANNEL
    assert filtered_recipients[0].id == "slack-channel"


def test_get_slack_recipients_keeps_all_slack(
    sink: SlackNotificationSink,
) -> None:
    # Create a request with only Slack recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_CHANNEL, id="slack-channel"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_DM, id="slack-dm"
            ),
        ],
    )

    filtered_recipients = sink._get_slack_recipients(request)

    # Should keep all Slack recipients
    assert len(filtered_recipients) == 2
    assert all(
        r.type
        in [
            NotificationRecipientTypeClass.SLACK_CHANNEL,
            NotificationRecipientTypeClass.SLACK_DM,
        ]
        for r in filtered_recipients
    )
    assert {r.id for r in filtered_recipients} == {"slack-channel", "slack-dm"}


def test_get_slack_recipients_empty_list(
    sink: SlackNotificationSink,
) -> None:
    # Create a request with no recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[],
    )

    filtered_recipients = sink._get_slack_recipients(request)

    # Should return empty list
    assert len(filtered_recipients) == 0


def test_get_slack_recipients_no_slack_recipients(
    sink: SlackNotificationSink,
) -> None:
    # Create a request with only non-Slack recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email@example.com"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.CUSTOM, id="custom-channel"
            ),
        ],
    )

    filtered_recipients = sink._get_slack_recipients(request)

    # Should return empty list since no Slack recipients
    assert len(filtered_recipients) == 0
