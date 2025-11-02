from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
)

from datahub_integrations.notifications.constants import (
    CUSTOM_TEMPLATE,
    DEFAULT_RECIPIENT_NAME,
    ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE,
)

# Import your module here
from datahub_integrations.notifications.sinks.email.send_email import (
    send_change_notification_to_recipients,
    send_custom_email_to_recipients,
    send_support_login_email,
)

# Add test for recipient name here.


@pytest.fixture
def mock_send_email() -> Any:
    with patch(
        "datahub_integrations.notifications.sinks.email.send_email.send_email"
    ) as mock:
        yield mock


@pytest.fixture
def mock_sg_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def recipients() -> List[NotificationRecipientClass]:
    # Assuming NotificationRecipientClass has an id attribute
    return [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="test1@example.com",
            displayName="Test Name",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL, id="test2@example.com"
        ),
    ]


def test_send_custom_email_to_recipients_success(
    recipients: List[NotificationRecipientClass],
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    subject = "Test Subject"
    message = "Test message body"

    send_custom_email_to_recipients(recipients, subject, message, mock_sg_client)

    # Ensure that each recipient received an email.
    assert mock_send_email.call_count == len(recipients)
    for call in mock_send_email.call_args_list:
        args, kwargs = call
        mail_obj = args[0]
        assert mail_obj.template_id.get() == CUSTOM_TEMPLATE
        # Verify dynamic parameters
        assert mail_obj.personalizations[0].dynamic_template_data["subject"] == subject
        assert mail_obj.personalizations[0].dynamic_template_data["message"] == message
        assert (
            mail_obj.personalizations[0].dynamic_template_data["recipientName"]
            == "Test Name"
            or mail_obj.personalizations[0].dynamic_template_data["recipientName"]
            == DEFAULT_RECIPIENT_NAME
        )


def test_send_change_notification_to_recipients_success(
    recipients: List[NotificationRecipientClass],
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    parameters = {
        "subject": "Test Subject",
        "preheader": "Test Preheader",
        "message": "Test message body",
        "entityName": "Test Entity Name",
        "detailsUrl": "https://example.com/entity/1234",
        "baseUrl": "https://example.com/",
        "recipientName": "John",
    }

    send_change_notification_to_recipients(recipients, parameters, mock_sg_client)

    expected_parameters_1 = parameters.copy()
    expected_parameters_1["recipientName"] = "Test Name"

    expected_parameters_2 = parameters.copy()
    expected_parameters_2["recipientName"] = DEFAULT_RECIPIENT_NAME

    # Ensure that each recipient received an email.
    assert mock_send_email.call_count == len(recipients)
    for call in mock_send_email.call_args_list:
        args, kwargs = call
        mail_obj = args[0]
        assert mail_obj.template_id.get() == ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE
        # Verify dynamic parameters
        assert (
            mail_obj.personalizations[0].dynamic_template_data == expected_parameters_1
            or mail_obj.personalizations[0].dynamic_template_data
            == expected_parameters_2
        )


@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_RECIPIENTS",
    ["recipient1@example.com", "recipient2@example.com"],
)
@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_TEMPLATE",
    "d-test-template-id",
)
def test_send_support_login_email_success(
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    """Test successful sending of support login email."""
    parameters = {
        "actorUrn": "urn:li:corpuser:test.user",
        "actorName": "Test User",
        "timestamp": "2023-11-04T12:30:56Z",  # ISO 8601 format in UTC
        "supportTicketId": "TICKET-123",
        "sourceIP": "192.168.1.1",
    }

    send_support_login_email(parameters, mock_sg_client)

    # Verify emails were sent to both recipients
    assert mock_send_email.call_count == 2

    # Verify first recipient
    call1_args, _ = mock_send_email.call_args_list[0]
    mail_obj1 = call1_args[0]
    assert mail_obj1.template_id.get() == "d-test-template-id"
    assert mail_obj1.personalizations[0].dynamic_template_data == parameters
    assert call1_args[1] == "recipient1@example.com"

    # Verify second recipient
    call2_args, _ = mock_send_email.call_args_list[1]
    mail_obj2 = call2_args[0]
    assert mail_obj2.template_id.get() == "d-test-template-id"
    assert mail_obj2.personalizations[0].dynamic_template_data == parameters
    assert call2_args[1] == "recipient2@example.com"


@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_RECIPIENTS",
    ["recipient1@example.com"],
)
@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_TEMPLATE",
    "d-test-template-id",
)
def test_send_support_login_email_minimal_parameters(
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    """Test sending support login email with minimal required parameters."""
    parameters = {
        "actorUrn": "urn:li:corpuser:test.user",
        "actorName": "Test User",
        "timestamp": "2023-11-04T12:30:56Z",  # ISO 8601 format in UTC
    }

    send_support_login_email(parameters, mock_sg_client)

    # Verify email was sent
    assert mock_send_email.call_count == 1
    call_args, _ = mock_send_email.call_args
    mail_obj = call_args[0]
    assert mail_obj.personalizations[0].dynamic_template_data == parameters


@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_RECIPIENTS",
    [],
)
@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_TEMPLATE",
    "d-test-template-id",
)
def test_send_support_login_email_no_recipients(
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    """Test that no emails are sent when no recipients are configured."""
    parameters = {
        "actorUrn": "urn:li:corpuser:test.user",
        "actorName": "Test User",
        "timestamp": "2023-11-04T12:30:56Z",  # ISO 8601 format in UTC
    }

    send_support_login_email(parameters, mock_sg_client)

    # Verify no emails were sent
    assert mock_send_email.call_count == 0


@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_RECIPIENTS",
    ["recipient1@example.com"],
)
@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_TEMPLATE",
    "",
)
def test_send_support_login_email_no_template(
    mock_send_email: Any,
    mock_sg_client: MagicMock,
) -> None:
    """Test that no emails are sent when template is not configured."""
    parameters = {
        "actorUrn": "urn:li:corpuser:test.user",
        "actorName": "Test User",
        "timestamp": "2023-11-04T12:30:56Z",  # ISO 8601 format in UTC
    }

    send_support_login_email(parameters, mock_sg_client)

    # Verify no emails were sent
    assert mock_send_email.call_count == 0


@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_RECIPIENTS",
    ["recipient1@example.com", "recipient2@example.com"],
)
@patch(
    "datahub_integrations.notifications.sinks.email.send_email.SUPPORT_LOGIN_EMAIL_TEMPLATE",
    "d-test-template-id",
)
@patch("datahub_integrations.notifications.sinks.email.send_email.send_email")
def test_send_support_login_email_partial_failure(
    mock_send_email_func: MagicMock,
    mock_sg_client: MagicMock,
) -> None:
    """Test that if sending to one recipient fails, it still tries the other."""
    # Make first call fail, second succeed
    mock_send_email_func.side_effect = [Exception("Failed to send"), None]

    parameters = {
        "actorUrn": "urn:li:corpuser:test.user",
        "actorName": "Test User",
        "timestamp": "2023-11-04T12:30:56Z",  # ISO 8601 format in UTC
        "supportTicketId": "TICKET-123",
    }

    # Should not raise exception, just log error
    send_support_login_email(parameters, mock_sg_client)

    # Verify both recipients were attempted
    assert mock_send_email_func.call_count == 2
