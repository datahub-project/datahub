from typing import Dict, List

from datahub.metadata.schema_classes import (
    NotificationRecipientClass,
    NotificationRecipientOriginTypeClass,
)
from loguru import logger
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Asm, Email, Mail, To

from datahub_integrations.notifications.constants import (
    CUSTOM_TEMPLATE,
    DEFAULT_RECIPIENT_NAME,
    ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE,
    FROM_EMAIL_ADDRESS,
    FROM_EMAIL_TITLE,
    GLOBAL_CHANGE_NOTIFICATION_TEMPLATE,
    GLOBAL_NOTIFICATIONS_UNSUBSCRIBE_GROUP_ID,
    INGESTION_TEMPLATE,
    SEND_GRID_API_KEY,
)


def send_change_notification_to_recipients(
    recipients: List[NotificationRecipientClass], parameters: Dict[str, str]
) -> None:
    for recipient in recipients:
        send_change_notification_email(recipient, parameters)


def send_custom_email_to_recipients(
    recipients: List[NotificationRecipientClass], subject: str, message: str
) -> None:
    for recipient in recipients:
        send_custom_email(recipient, subject, message)


def send_ingestion_run_notification_to_recipients(
    recipients: List[NotificationRecipientClass], parameters: Dict[str, str]
) -> None:
    for recipient in recipients:
        send_ingestion_notification_email(recipient, parameters)


# TODO: handle non-subscription and subscription case in followup.
def send_change_notification_email(
    recipient: NotificationRecipientClass, parameters: Dict[str, str]
) -> None:
    # Extract recipient address
    recipient_address = recipient.id
    recipient_name = recipient.displayName or DEFAULT_RECIPIENT_NAME

    parameters["recipientName"] = recipient_name

    if recipient_address is None:
        logger.error(
            f"Recipient address is None, skipping sending email. Parameters: {parameters}"
        )
        return

    # Create new message
    message = Mail(
        from_email=Email(FROM_EMAIL_ADDRESS, FROM_EMAIL_TITLE),
        to_emails=To(recipient_address),
    )

    # Specify the sendgrid template
    message.template_id = (
        GLOBAL_CHANGE_NOTIFICATION_TEMPLATE
        if recipient.origin == NotificationRecipientOriginTypeClass.GLOBAL_NOTIFICATION
        else ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE
    )

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address)


def send_ingestion_notification_email(
    recipient: NotificationRecipientClass, parameters: Dict[str, str]
) -> None:
    # Extract recipient address
    recipient_address = recipient.id
    recipient_name = recipient.displayName or DEFAULT_RECIPIENT_NAME

    parameters["recipientName"] = recipient_name

    if recipient_address is None:
        logger.error(
            f"Recipient address is None, skipping sending email. Parameters: {parameters}"
        )
        return

    # Create new message
    message = Mail(
        from_email=Email(FROM_EMAIL_ADDRESS, FROM_EMAIL_TITLE),
        to_emails=To(recipient_address),
    )

    # Specify the sendgrid template
    message.template_id = INGESTION_TEMPLATE

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address)


def send_custom_email(
    recipient: NotificationRecipientClass, subject: str, message: str
) -> None:
    # Extract recipient address
    recipient_address = recipient.id
    recipient_name = recipient.displayName or DEFAULT_RECIPIENT_NAME

    if recipient_address is None:
        logger.error(
            f"Recipient address is None, skipping sending email. Message: {message}"
        )
        return

    # Create new message
    email = Mail(
        from_email=Email(FROM_EMAIL_ADDRESS, FROM_EMAIL_TITLE),
        to_emails=To(recipient_address),
    )

    # Specify the sendgrid template
    email.template_id = CUSTOM_TEMPLATE

    # Add dynamic data
    email.dynamic_template_data = {
        "subject": subject,
        "message": message,
        "recipientName": recipient_name,
    }

    send_email(email, recipient_address)


def send_email(message: Mail, context: str) -> None:
    try:
        # Add global unsubscribe group to the email
        asm = Asm(group_id=GLOBAL_NOTIFICATIONS_UNSUBSCRIBE_GROUP_ID)
        message.asm = asm
        sg = SendGridAPIClient(SEND_GRID_API_KEY)
        response = sg.send(message)
        if response.status_code != 202:
            logger.error(f"Failed to send email, context {context}: {response.body}")
    except Exception as e:
        logger.error(f"Failed to send email, context: {context}: {e}")
