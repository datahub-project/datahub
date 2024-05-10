from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import NotificationRecipientClass
from slack_sdk import WebClient
from slack_sdk.web.slack_response import SlackResponse

from datahub_integrations.notifications.sinks.slack.send_slack_message import (
    send_notification_slack_message,
    send_notification_to_recipients,
    send_slack_message,
)


@pytest.fixture
def mock_client() -> WebClient:
    # Create a mock WebClient
    mock = MagicMock(spec=WebClient)
    return cast(WebClient, mock)


def test_send_slack_message_success(mock_client: WebClient) -> None:
    # Type-cast the MagicMock to simulate the Slack API correctly
    chat_postMessage = cast(Any, mock_client.chat_postMessage)
    chat_postMessage.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={"message": {"ts": "12345"}},
        status_code=200,
        headers={},
    )

    channel = "C123456"
    text = "Hello, world!"

    # Execute the function
    result = send_slack_message(mock_client, channel, text)

    # Verify the result
    assert result == "12345"
    chat_postMessage.assert_called_once_with(channel=channel, text=text)


def test_send_slack_message_failure(mock_client: WebClient) -> None:
    # Type-cast the MagicMock to simulate the Slack API correctly
    chat_postMessage = cast(Any, mock_client.chat_postMessage)
    chat_postMessage.side_effect = Exception("Slack API error")

    channel = "C123456"
    text = "Hello, world!"

    # Execute the function
    result = send_slack_message(mock_client, channel, text)

    # Verify the result
    assert result is None
    chat_postMessage.assert_called_once_with(channel=channel, text=text)


@patch(
    "datahub_integrations.notifications.sinks.slack.send_slack_message.send_slack_message"
)
def test_send_notification_slack_message(
    mock_send_slack: MagicMock, mock_client: WebClient
) -> None:
    # Setup the mock
    send_slack_message = cast(Any, mock_send_slack)
    send_slack_message.return_value = "12345"
    recipient = NotificationRecipientClass(id="user123", type="user")
    text = "Notification test"

    # Execute the function
    result = send_notification_slack_message(mock_client, recipient, text)

    # Verify the result
    assert result == "12345"
    send_slack_message.assert_called_once_with(mock_client, "user123", text)


@patch(
    "datahub_integrations.notifications.sinks.slack.send_slack_message.send_notification_slack_message"
)
def test_send_notification_to_recipients(
    mock_send_message: MagicMock, mock_client: WebClient
) -> None:
    # Setup the mock
    send_notification = cast(Any, mock_send_message)
    send_notification.side_effect = ["message_id1", "message_id2"]

    recipients = [
        NotificationRecipientClass(id="user1", type="user"),
        NotificationRecipientClass(id="user2", type="user"),
    ]
    text = "Test notification message"

    # Execute function
    result = send_notification_to_recipients(mock_client, recipients, text)

    # Assert correct message IDs are returned
    assert result == ["message_id1", "message_id2"]
