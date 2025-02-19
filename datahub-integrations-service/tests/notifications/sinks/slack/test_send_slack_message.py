import os
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
    update_messages,
    update_slack_message,
)
from datahub_integrations.notifications.sinks.slack.types import SlackMessageDetails


@pytest.fixture
def mock_client() -> WebClient:
    # Create a mock WebClient
    mock = MagicMock(spec=WebClient)
    return cast(WebClient, mock)


os.environ["STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED"] = "true"


def test_send_slack_message_success(mock_client: WebClient) -> None:
    # Type-cast the MagicMock to simulate the Slack API correctly
    chat_postMessage = cast(Any, mock_client.chat_postMessage)
    chat_postMessage.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={"message": {"ts": "12345"}, "channel": "C123456"},
        status_code=200,
        headers={},
    )
    conversations_info = cast(Any, mock_client.conversations_info)
    conversations_info.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={"channel": {"name": "channel display name"}},
        status_code=200,
        headers={},
    )

    channel = "C123456"
    text = "Hello, world!"

    # Execute the function
    result = send_slack_message(mock_client, channel, text, [], [])

    # Verify the result
    assert result is not None
    assert result.message_id == "12345"
    assert result.channel_id == "C123456"
    assert result.channel_name == "channel display name"
    chat_postMessage.assert_called_once_with(
        channel=channel, text=text, blocks=[], attachments=[]
    )


def test_send_slack_message_channel_resolution_failure(mock_client: WebClient) -> None:
    # Type-cast the MagicMock to simulate the Slack API correctly
    chat_postMessage = cast(Any, mock_client.chat_postMessage)
    chat_postMessage.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={"message": {"ts": "12345"}, "channel": "C123456"},
        status_code=200,
        headers={},
    )
    conversations_info = cast(Any, mock_client.conversations_info)
    conversations_info.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={
            "ok": False,
            "error": "channel_not_found",
        },
        status_code=404,  # Non-200 status code to indicate an error, 404 for "Not Found"
        headers={},
    )

    channel = "C123456"
    text = "Hello, world!"

    # Execute the function
    result = send_slack_message(mock_client, channel, text, [], [])

    # Verify the result
    assert result is not None
    assert result.message_id == "12345"
    assert result.channel_id == "C123456"
    assert (
        result.channel_name is None
    )  # No channel name could be be. Channel resolution failed.
    chat_postMessage.assert_called_once_with(
        channel=channel, text=text, blocks=[], attachments=[]
    )


def test_send_slack_message_failure_exception(mock_client: WebClient) -> None:
    # Type-cast the MagicMock to simulate the Slack API correctly
    chat_postMessage = cast(Any, mock_client.chat_postMessage)
    chat_postMessage.side_effect = Exception("Slack API error")

    channel = "C123456"
    text = "Hello, world!"

    # Execute the function
    result = send_slack_message(mock_client, channel, text, [], [])

    # Verify the result
    assert result is None
    chat_postMessage.assert_called_once_with(
        channel=channel, text=text, blocks=[], attachments=[]
    )


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
    result = send_notification_slack_message(mock_client, recipient, text, [], [])

    # Verify the result
    assert result == "12345"
    send_slack_message.assert_called_once_with(
        mock_client, "user123", "Notification test", [], []
    )


@patch(
    "datahub_integrations.notifications.sinks.slack.send_slack_message.send_notification_slack_message"
)
def test_send_notification_to_recipients(
    mock_send_message: MagicMock, mock_client: WebClient
) -> None:
    # message details
    message_details = [
        {
            "message_id": "message_id1",
            "channel_id": "channel_id1",
            "channel_name": "channel_name1",
        },
        {
            "message_id": "message_id2",
            "channel_id": "channel_id2",
            "channel_name": "channel_name2",
        },
    ]

    # Setup the mock
    send_notification = cast(Any, mock_send_message)
    send_notification.side_effect = message_details

    recipients = [
        NotificationRecipientClass(id="user1", type="user"),
        NotificationRecipientClass(id="user2", type="user"),
    ]
    text = "Test notification message"

    # Execute function
    result = send_notification_to_recipients(mock_client, recipients, text, [], [])

    # Assert correct message info are returned
    assert result == message_details


def test_update_slack_message_success(mock_client: WebClient) -> None:
    chat_update = cast(Any, mock_client.chat_update)
    chat_update.return_value = SlackResponse(
        client=mock_client,
        http_verb="POST",
        api_url="url",
        req_args={},
        data={"message": {"ts": "12345"}, "channel": "C123456"},
        status_code=200,
        headers={},
    )

    channel = "C123"
    message_id = "1234567890"
    text = "Updated message text"

    # Call the function with the mocked client
    with patch(
        "datahub_integrations.notifications.sinks.slack.send_slack_message.logger"
    ) as mocked_logger:
        update_slack_message(mock_client, channel, message_id, text, None, None)

        # Verify chat_update was called correctly
        chat_update.assert_called_once_with(
            channel=channel, ts=message_id, text=text, blocks=None, attachments=None
        )

        # Verify logging
        mocked_logger.info.assert_called_once_with("Message updated successfully!")


def test_update_slack_message_error(mock_client: WebClient) -> None:
    chat_update = cast(Any, mock_client.chat_update)
    chat_update.return_value = Exception("Slack API error")

    channel = "C123"
    message_id = "1234567890"
    text = "Updated message text"

    # Call the function with the mocked client
    with patch(
        "datahub_integrations.notifications.sinks.slack.send_slack_message.logger"
    ) as mocked_logger:
        update_slack_message(mock_client, channel, message_id, text, None, None)

        # Assert that an error was logged
        mocked_logger.exception.assert_called_once_with(
            f"Failed to update message {message_id} in channel with id {channel}"
        )


@patch(
    "datahub_integrations.notifications.sinks.slack.send_slack_message.update_slack_message"
)
def test_update_messages(
    mock_update_slack_message: MagicMock, mock_client: WebClient
) -> None:
    # Creating a list of SlackMessageDetails
    message_details = [
        SlackMessageDetails(
            channel_id="C123", message_id="msg1", channel_name="general"
        ),
        SlackMessageDetails(
            channel_id="C456", message_id="msg2", channel_name="random"
        ),
    ]
    text = "Hello, world!"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "New block"}}]
    attachments = [{"text": "New attachment"}]

    update_messages(mock_client, message_details, text, blocks, attachments)

    # Ensure update_slack_message is called correctly for each message
    calls: Any = [
        ((mock_client, "C123", "msg1", text, blocks, attachments),),
        ((mock_client, "C456", "msg2", text, blocks, attachments),),
    ]

    mock_update_slack_message.assert_has_calls(calls, any_order=True)
