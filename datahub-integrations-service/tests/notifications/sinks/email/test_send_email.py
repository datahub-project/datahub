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
