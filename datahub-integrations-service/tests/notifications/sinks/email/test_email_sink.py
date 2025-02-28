from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationRequestClass,
)

from datahub_integrations.notifications.sinks.context import NotificationContext

# Replace 'your_module.email_sink' with the actual module path where EmailNotificationSink is located
from datahub_integrations.notifications.sinks.email.email_sink import (
    EmailNotificationSink,
    send_change_notification_to_recipients,
    send_ingestion_run_notification_to_recipients,
)


@pytest.fixture
def notification_sink() -> EmailNotificationSink:
    return EmailNotificationSink()


@pytest.fixture
def base_url() -> str:
    return "https://example.acryl.io"


@pytest.fixture
def recipients() -> List[NotificationRecipientClass]:
    return [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL, id="recipient1@example.com"
        )
    ]


@pytest.fixture
def notification_request_custom() -> NotificationRequestClass:
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template="CUSTOM",
            parameters={"title": "Custom Title", "message": "Custom Message"},
        ),
        recipients=[],
    )


@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.send_custom_email_to_recipients"
)
def test_send_custom_notification(
    mock_send_custom_email: MagicMock,
    notification_sink: EmailNotificationSink,
    notification_request_custom: NotificationRequestClass,
) -> None:
    notification_sink._send_custom_notification(notification_request_custom)
    mock_send_custom_email.assert_called_once_with(
        notification_request_custom.recipients, "Custom Title", "Custom Message"
    )


@patch("datahub_integrations.notifications.sinks.email.email_sink.retry_with_backoff")
@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_new_incident_parameters"
)
def test_send_change_notification(
    mock_build_parameters: Mock,
    mock_retry: MagicMock,
    notification_sink: EmailNotificationSink,
    recipients: List[NotificationRecipientClass],
    base_url: str,
) -> None:
    mock_build_parameters.return_value = {
        "subject": "Test Subject",
        "message": "Test Message",
        "entityName": "TestEntity",
        "detailsUrl": "https://example.com",
        "entityUrl": "https://example.com",
    }
    fake_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_INCIDENT",
            parameters={},
        ),
        recipients=recipients,
    )
    notification_sink.base_url = base_url
    context = NotificationContext()
    notification_sink.send(fake_request, context)

    mock_retry.assert_called_once()
    args, kwargs = mock_retry.call_args
    assert args[0] == send_change_notification_to_recipients
    assert kwargs["recipients"] == recipients
    assert kwargs["parameters"]["subject"] == "Test Subject"
    assert "max_attempts" in kwargs


@patch("datahub_integrations.notifications.sinks.email.email_sink.retry_with_backoff")
@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_ingestion_run_change_parameters"
)
def test_send_ingestion_notification(
    mock_build_parameters: Mock,
    mock_retry: MagicMock,
    notification_sink: EmailNotificationSink,
    recipients: List[NotificationRecipientClass],
    base_url: str,
) -> None:
    mock_build_parameters.return_value = {
        "subject": "Test Subject",
        "message": "Test Message",
        "entityName": "TestEntity",
        "detailsUrl": "https://example.com",
        "entityUrl": "https://example.com",
    }
    fake_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_INGESTION_RUN_CHANGE",
            parameters={},
        ),
        recipients=recipients,
    )
    notification_sink.base_url = base_url
    context = NotificationContext()
    notification_sink.send(fake_request, context)

    mock_retry.assert_called_once()
    args, kwargs = mock_retry.call_args
    assert args[0] == send_ingestion_run_notification_to_recipients
    assert kwargs["recipients"] == recipients
    assert kwargs["parameters"]["subject"] == "Test Subject"
    assert "max_attempts" in kwargs


@pytest.fixture
def notification_context() -> NotificationContext:
    return NotificationContext()


def test_send_function_unsupported_template(
    notification_sink: EmailNotificationSink,
    notification_request_custom: NotificationRequestClass,
    notification_context: NotificationContext,
) -> None:
    # Change the template to an unsupported one
    notification_request_custom.message.template = "UNSUPPORTED_TEMPLATE"
    # Just a warning is logged.
    notification_sink.send(notification_request_custom, notification_context)
