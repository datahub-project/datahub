from typing import Any, List
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
    RetryMode,
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


@pytest.fixture
def sink_with_base_url() -> EmailNotificationSink:
    sink = EmailNotificationSink()
    sink.base_url = "https://example.acryl.io"
    return sink


@pytest.fixture
def recipients_with_creator() -> list[NotificationRecipientClass]:
    return [
        # The creator recipient
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="creator@example.com",
            actor="urn:creator",
        ),
        # Another recipient
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="other@example.com",
            actor="urn:other",
        ),
    ]


@pytest.fixture
def recipients_without_creator() -> list[NotificationRecipientClass]:
    return [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="other1@example.com",
            actor="urn:other1",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="other2@example.com",
            actor="urn:other2",
        ),
    ]


@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_proposer_proposal_status_change_parameters"
)
@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_proposal_status_change_parameters"
)
def test_send_broadcast_proposal_status_change_with_creator(
    mock_build_broadcast_params: MagicMock,
    mock_build_proposer_params: MagicMock,
    sink_with_base_url: EmailNotificationSink,
    recipients_with_creator: List[Any],
) -> None:
    # Prepare dummy parameters
    dummy_proposer_params = {"subject": "Personal Notification"}
    dummy_broadcast_params = {"subject": "Broadcast Notification"}
    mock_build_proposer_params.return_value = dummy_proposer_params
    mock_build_broadcast_params.return_value = dummy_broadcast_params

    # Build a request that includes a creatorUrn matching one recipient.
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_PROPOSAL_STATUS_CHANGE",
            parameters={"creatorUrn": "urn:creator"},
        ),
        recipients=recipients_with_creator,
    )

    # Spy on _send_change_notification calls.
    sink_with_base_url._send_change_notification = MagicMock()  # type: ignore

    # Call the new method directly.
    sink_with_base_url._send_broadcast_proposal_status_change_notification(request)

    # Two calls should be made:
    # 1. To send the personal (proposer) notification to the creator.
    # 2. To send the broadcast notification to the remaining recipient.
    calls = sink_with_base_url._send_change_notification.call_args_list
    assert len(calls) == 2

    # First call: creator notification.
    args_creator, kwargs_creator = calls[0]
    # It should be called with a list containing only the creator.
    creator_recipients = args_creator[0]
    assert len(creator_recipients) == 1
    assert creator_recipients[0].actor == "urn:creator"
    # And parameters should equal dummy_proposer_params.
    assert args_creator[1] == dummy_proposer_params
    # Verify that retry_mode was passed as DISABLED.
    assert kwargs_creator["retry_mode"] == RetryMode.DISABLED

    # Second call: broadcast notification.
    args_broadcast, kwargs_broadcast = calls[1]
    broadcast_recipients = args_broadcast[0]
    # The broadcast list should not include the creator.
    for r in broadcast_recipients:
        assert r.actor != "urn:creator"
    # Parameters should equal dummy_broadcast_params.
    assert args_broadcast[1] == dummy_broadcast_params
    assert kwargs_broadcast["retry_mode"] == RetryMode.DISABLED


@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_proposal_status_change_parameters"
)
@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_proposer_proposal_status_change_parameters"
)
def test_send_broadcast_proposal_status_change_with_creator_no_match(
    mock_build_proposer_params: MagicMock,
    mock_build_broadcast_params: MagicMock,
    sink_with_base_url: EmailNotificationSink,
    recipients_without_creator: List[Any],
) -> None:
    # Even though a creatorUrn is provided, no recipient has that actor.
    dummy_broadcast_params = {"subject": "Broadcast Notification"}
    mock_build_broadcast_params.return_value = dummy_broadcast_params

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_PROPOSAL_STATUS_CHANGE",
            parameters={
                "creatorUrn": "urn:creator"
            },  # No recipient has actor 'urn:creator'
        ),
        recipients=recipients_without_creator,
    )

    # Spy on _send_change_notification.
    sink_with_base_url._send_change_notification = MagicMock()  # type: ignore

    sink_with_base_url._send_broadcast_proposal_status_change_notification(request)

    # Only one call should be made: broadcast notification to all recipients.
    sink_with_base_url._send_change_notification.assert_called_once()
    args, kwargs = sink_with_base_url._send_change_notification.call_args
    # All recipients are included.
    assert args[0] == recipients_without_creator
    assert args[1] == dummy_broadcast_params
    assert kwargs["retry_mode"] == RetryMode.DISABLED


@patch(
    "datahub_integrations.notifications.sinks.email.email_sink.build_proposal_status_change_parameters"
)
def test_send_broadcast_proposal_status_change_without_creator(
    mock_build_broadcast_params: MagicMock,
    sink_with_base_url: EmailNotificationSink,
    recipients_without_creator: List[Any],
) -> None:
    # Test the case when there is no creatorUrn at all.
    dummy_broadcast_params = {"subject": "Broadcast Notification"}
    mock_build_broadcast_params.return_value = dummy_broadcast_params

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_PROPOSAL_STATUS_CHANGE",
            parameters={},  # No creatorUrn provided.
        ),
        recipients=recipients_without_creator,
    )

    # Spy on _send_change_notification.
    sink_with_base_url._send_change_notification = MagicMock()  # type: ignore

    sink_with_base_url._send_broadcast_proposal_status_change_notification(request)

    # Only one call should be made, to send broadcast notifications to all recipients.
    sink_with_base_url._send_change_notification.assert_called_once()
    args, kwargs = sink_with_base_url._send_change_notification.call_args
    assert args[0] == recipients_without_creator
    assert args[1] == dummy_broadcast_params
    assert kwargs["retry_mode"] == RetryMode.DISABLED


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


def test_get_email_recipients_filters_non_email(
    notification_sink: EmailNotificationSink,
) -> None:
    # Create a request with mixed recipient types
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email@example.com"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_DM, id="slack-channel"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.CUSTOM, id="teams-channel"
            ),
        ],
    )

    filtered_recipients = notification_sink._get_email_recipients(request)

    # Should only contain the email recipient
    assert len(filtered_recipients) == 1
    assert filtered_recipients[0].type == NotificationRecipientTypeClass.EMAIL
    assert filtered_recipients[0].id == "email@example.com"


def test_get_email_recipients_keeps_all_email(
    notification_sink: EmailNotificationSink,
) -> None:
    # Create a request with only email recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email1@example.com"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.EMAIL, id="email2@example.com"
            ),
        ],
    )

    filtered_recipients = notification_sink._get_email_recipients(request)

    # Should keep all email recipients
    assert len(filtered_recipients) == 2
    assert all(
        r.type == NotificationRecipientTypeClass.EMAIL for r in filtered_recipients
    )
    assert {r.id for r in filtered_recipients} == {
        "email1@example.com",
        "email2@example.com",
    }


def test_get_email_recipients_empty_list(
    notification_sink: EmailNotificationSink,
) -> None:
    # Create a request with no recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[],
    )

    filtered_recipients = notification_sink._get_email_recipients(request)

    # Should return empty list
    assert len(filtered_recipients) == 0


def test_get_email_recipients_no_email_recipients(
    notification_sink: EmailNotificationSink,
) -> None:
    # Create a request with only non-email recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(template="CUSTOM"),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.SLACK_DM, id="slack-channel"
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.CUSTOM, id="teams-channel"
            ),
        ],
    )

    filtered_recipients = notification_sink._get_email_recipients(request)

    # Should return empty list since no email recipients
    assert len(filtered_recipients) == 0
