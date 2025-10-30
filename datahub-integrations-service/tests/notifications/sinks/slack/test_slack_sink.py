from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationRequestClass,
)
from slack_sdk import WebClient

from datahub_integrations.identity.identity_provider import IdentityProvider
from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.slack.slack_sink import (
    SlackNotificationSink,
)
from datahub_integrations.notifications.sinks.slack.types import SlackMessageDetails


@pytest.fixture
def mock_slack_client() -> WebClient:
    client = MagicMock(spec=WebClient)
    return client


@pytest.fixture
def mock_identity_provider() -> IdentityProvider:
    provider = MagicMock(spec=IdentityProvider)
    return provider


@pytest.fixture
def slack_notification_sink() -> SlackNotificationSink:
    sink = SlackNotificationSink()
    sink.base_url = "https://example.acryl.io"
    sink.slack_client = MagicMock(spec=WebClient)
    sink.identity_provider = MagicMock(spec=IdentityProvider)
    sink.slack_connection_config = MagicMock()
    sink.slack_connection_config.bot_token = "test-token"
    return sink


@pytest.fixture
def slack_recipients() -> List[NotificationRecipientClass]:
    return [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.SLACK_DM, id="U12345"
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.SLACK_CHANNEL, id="C67890"
        ),
    ]


@pytest.fixture
def notification_context() -> NotificationContext:
    return NotificationContext()


#
# Tests for Workflow Request Assignment Notifications
#


@patch(
    "datahub_integrations.notifications.sinks.slack.slack_sink.build_workflow_request_assignment_message"
)
def test_send_workflow_request_assignment_notification(
    mock_build_message: Mock,
    slack_notification_sink: SlackNotificationSink,
    slack_recipients: List[NotificationRecipientClass],
    notification_context: NotificationContext,
) -> None:
    """
    Test that workflow request assignment notifications are properly handled in Slack sink.
    """
    # Mock the message builder return
    mock_build_message.return_value = (
        "John Joyce has created a new Data Access request for Table *FOO_BAR* on Snowflake",
        [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":clipboard: *Data Access Request Created*\n\nJohn Joyce has created a new Data Access request for Table *FOO_BAR* on Snowflake.",
                },
            }
        ],
        [],  # No attachments for assignment messages
    )

    # Mock the _send_change_notification method
    slack_notification_sink._send_change_notification = MagicMock()  # type: ignore
    slack_notification_sink._send_change_notification.return_value = [
        SlackMessageDetails(
            channel_id="C67890", channel_name="general", message_id="1234567890.123456"
        )
    ]

    fake_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
            parameters={
                "workflowName": "Data Access",
                "actorName": "John Joyce",
                "entityName": "FOO_BAR",
                "entityType": "Table",
                "entityPlatform": "Snowflake",
            },
        ),
        recipients=slack_recipients,
    )

    # Execute the test
    slack_notification_sink.send(fake_request, notification_context)

    # Verify build_workflow_request_assignment_message was called
    mock_build_message.assert_called_once_with(
        fake_request,
        slack_notification_sink.identity_provider,
        slack_notification_sink.slack_client,
        "https://example.acryl.io",
    )

    # Verify _send_change_notification was called with correct parameters
    slack_notification_sink._send_change_notification.assert_called_once()
    args, kwargs = slack_notification_sink._send_change_notification.call_args

    # Check arguments
    assert args[0] == slack_recipients  # recipients
    assert (
        args[1]
        == "John Joyce has created a new Data Access request for Table *FOO_BAR* on Snowflake"
    )  # text
    assert len(args[2]) == 1  # blocks
    assert args[3] == []  # attachments (empty for assignment)


#
# Tests for Workflow Request Status Change Notifications
#


@patch(
    "datahub_integrations.notifications.sinks.slack.slack_sink.build_workflow_request_status_change_message"
)
def test_send_workflow_request_status_change_notification(
    mock_build_message: Mock,
    slack_notification_sink: SlackNotificationSink,
    slack_recipients: List[NotificationRecipientClass],
    notification_context: NotificationContext,
) -> None:
    """
    Test that workflow request status change notifications are properly handled in Slack sink.
    """
    # Mock the message builder return
    mock_build_message.return_value = (
        "Your Data Access request for Table *FOO_BAR* on Snowflake has been approved",
        [],  # No main blocks for status change
        [
            {
                "color": "good",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":white_check_mark: *Request Approved*\n\nYour Data Access request for Table *FOO_BAR* on Snowflake has been *approved* by Jane Smith.",
                        },
                    }
                ],
            }
        ],  # Attachments for status change
    )

    # Mock the _send_change_notification method
    slack_notification_sink._send_change_notification = MagicMock()  # type: ignore
    slack_notification_sink._send_change_notification.return_value = [
        SlackMessageDetails(
            channel_id="U12345", channel_name=None, message_id="1234567890.123456"
        )
    ]

    fake_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "workflowName": "Data Access",
                "actorName": "Jane Smith",
                "result": "approved",
                "entityName": "FOO_BAR",
                "entityType": "Table",
                "entityPlatform": "Snowflake",
            },
        ),
        recipients=slack_recipients,
    )

    # Execute the test
    slack_notification_sink.send(fake_request, notification_context)

    # Verify build_workflow_request_status_change_message was called
    mock_build_message.assert_called_once_with(
        fake_request,
        slack_notification_sink.identity_provider,
        slack_notification_sink.slack_client,
        "https://example.acryl.io",
    )

    # Verify _send_change_notification was called with correct parameters
    slack_notification_sink._send_change_notification.assert_called_once()
    args, kwargs = slack_notification_sink._send_change_notification.call_args

    # Check arguments
    assert args[0] == slack_recipients  # recipients
    assert (
        args[1]
        == "Your Data Access request for Table *FOO_BAR* on Snowflake has been approved"
    )  # text
    assert args[2] == []  # blocks (empty for status change)
    assert len(args[3]) == 1  # attachments
    assert args[3][0]["color"] == "good"  # Green for approved


def test_workflow_request_notifications_supported_template_types(
    slack_notification_sink: SlackNotificationSink,
    notification_context: NotificationContext,
) -> None:
    """
    Test that the new workflow request template types are included in the Slack sink action map.
    """
    # Test assignment notification
    assignment_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
            parameters={"workflowName": "Test Workflow", "actorName": "Test Actor"},
        ),
        recipients=[],
    )

    # Test status change notification
    status_change_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "workflowName": "Test Workflow",
                "actorName": "Test Actor",
                "result": "approved",
            },
        ),
        recipients=[],
    )

    # Mock the workflow notification methods to verify they get called
    slack_notification_sink._send_workflow_request_assignment_notification = MagicMock()  # type: ignore
    slack_notification_sink._send_workflow_request_status_change_notification = (  # type: ignore
        MagicMock()
    )

    # Send both types of requests
    slack_notification_sink.send(assignment_request, notification_context)
    slack_notification_sink.send(status_change_request, notification_context)

    # Verify the appropriate methods were called
    slack_notification_sink._send_workflow_request_assignment_notification.assert_called_once()  # type: ignore
    slack_notification_sink._send_workflow_request_status_change_notification.assert_called_once()  # type: ignore


def test_get_slack_recipients_filters_non_slack(
    slack_notification_sink: SlackNotificationSink,
) -> None:
    """
    Test that _get_slack_recipients filters out non-Slack recipients.
    """
    # Create a request with mixed recipient types
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_DM, id="U12345"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_CHANNEL, id="C67890"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email@example.com"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.CUSTOM, id="teams-channel"
            ),
        ],
    )

    filtered_recipients = slack_notification_sink._get_slack_recipients(request)

    # Should only contain the Slack recipients
    assert len(filtered_recipients) == 2
    slack_recipient_types = {
        NotificationRecipientTypeClass.SLACK_DM,
        NotificationRecipientTypeClass.SLACK_CHANNEL,
    }
    assert all(r.type in slack_recipient_types for r in filtered_recipients)
    assert {r.id for r in filtered_recipients} == {"U12345", "C67890"}


def test_unsupported_template_type_logs_warning(
    slack_notification_sink: SlackNotificationSink,
    notification_context: NotificationContext,
) -> None:
    """
    Test that unsupported template types log a warning and don't crash.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="UNSUPPORTED_TEMPLATE",
            parameters={},
        ),
        recipients=[],
    )

    # Should not raise an exception
    slack_notification_sink.send(request, notification_context)


#
# Tests for Release Notifications
#


@patch(
    "datahub_integrations.notifications.sinks.slack.slack_sink.build_release_notification_message"
)
def test_send_release_notification(
    mock_build_message: Mock,
    slack_notification_sink: SlackNotificationSink,
    slack_recipients: List[NotificationRecipientClass],
    notification_context: NotificationContext,
) -> None:
    """
    Test that release notifications are properly handled in Slack sink.
    """
    # Mock the message builder return
    mock_build_message.return_value = (
        "DataHub v0.13.0 Released\nCheck out the new features including improved search.",
        [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataHub v0.13.0 Released*\nCheck out the new features including improved search.",
                },
            }
        ],
        [],  # No attachments for release notifications
    )

    # Mock the _send_change_notification method
    slack_notification_sink._send_change_notification = MagicMock()  # type: ignore
    slack_notification_sink._send_change_notification.return_value = [
        SlackMessageDetails(
            channel_id="C67890",
            channel_name="announcements",
            message_id="1234567890.123456",
        )
    ]

    fake_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="RELEASE_NOTIFICATION",
            parameters={
                "title": "DataHub v0.13.0 Released",
                "body": "Check out the new features including improved search.",
            },
        ),
        recipients=slack_recipients,
    )

    # Execute the test
    slack_notification_sink.send(fake_request, notification_context)

    # Verify build_release_notification_message was called
    mock_build_message.assert_called_once_with(fake_request)

    # Verify _send_change_notification was called with correct parameters
    slack_notification_sink._send_change_notification.assert_called_once()
    args, kwargs = slack_notification_sink._send_change_notification.call_args

    # Check arguments
    assert args[0] == slack_recipients  # recipients
    assert (
        args[1]
        == "DataHub v0.13.0 Released\nCheck out the new features including improved search."
    )  # text
    assert len(args[2]) == 1  # blocks
    assert args[3] == []  # attachments (empty for release notifications)


def test_release_notification_template_type_supported(
    slack_notification_sink: SlackNotificationSink,
    notification_context: NotificationContext,
) -> None:
    """
    Test that the RELEASE_NOTIFICATION template type is included in the Slack sink action map.
    """
    # Test release notification
    release_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="RELEASE_NOTIFICATION",
            parameters={
                "title": "New Release",
                "body": "Version 1.0 is now available.",
            },
        ),
        recipients=[],
    )

    # Mock the release notification method to verify it gets called
    slack_notification_sink._send_release_notification = MagicMock()  # type: ignore

    # Send the request
    slack_notification_sink.send(release_request, notification_context)

    # Verify the appropriate method was called
    slack_notification_sink._send_release_notification.assert_called_once()  # type: ignore
