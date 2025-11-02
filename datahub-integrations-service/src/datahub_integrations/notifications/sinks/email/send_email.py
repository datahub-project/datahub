from typing import Dict, List

from datahub.metadata.schema_classes import (
    NotificationRecipientClass,
    NotificationRecipientOriginTypeClass,
)
from loguru import logger
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Mail, To

from datahub_integrations.notifications.constants import (
    ACTOR_CHANGE_NOTIFICATION_TEMPLATE,
    COMPLIANCE_FORM_PUBLISH_TEMPLATE,
    CUSTOM_TEMPLATE,
    DEFAULT_RECIPIENT_NAME,
    ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE,
    FROM_EMAIL_ADDRESS,
    FROM_EMAIL_TITLE,
    GLOBAL_CHANGE_NOTIFICATION_TEMPLATE,
    INGESTION_TEMPLATE,
    SUPPORT_LOGIN_EMAIL_RECIPIENTS,
    SUPPORT_LOGIN_EMAIL_TEMPLATE,
    USER_INVITATION_TEMPLATE,
)


def send_change_notification_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_change_notification_email(recipient, parameters, sg_client)


def send_custom_email_to_recipients(
    recipients: List[NotificationRecipientClass],
    subject: str,
    message: str,
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_custom_email(recipient, subject, message, sg_client)


def send_ingestion_run_notification_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_ingestion_notification_email(recipient, parameters, sg_client)


def send_compliance_form_notification_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_compliance_form_email(recipient, parameters, sg_client)


def send_workflow_request_assignment_notification_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_workflow_request_assignment_email(recipient, parameters, sg_client)


def send_workflow_request_status_change_notification_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_workflow_request_status_change_email(recipient, parameters, sg_client)


# TODO: handle non-subscription and subscription case in followup.
def send_change_notification_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
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

    message.template_id = ENTITY_CHANGE_SUBSCRIPTION_TEMPLATE

    if recipient.origin == NotificationRecipientOriginTypeClass.GLOBAL_NOTIFICATION:
        message.template_id = GLOBAL_CHANGE_NOTIFICATION_TEMPLATE
    elif recipient.origin == NotificationRecipientOriginTypeClass.ACTOR_NOTIFICATION:
        message.template_id = ACTOR_CHANGE_NOTIFICATION_TEMPLATE

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address, sg_client)


def send_ingestion_notification_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
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

    send_email(message, recipient_address, sg_client)


def send_custom_email(
    recipient: NotificationRecipientClass,
    subject: str,
    message: str,
    sg_client: SendGridAPIClient,
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

    send_email(email, recipient_address, sg_client)


def send_compliance_form_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
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
    message.template_id = COMPLIANCE_FORM_PUBLISH_TEMPLATE

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address, sg_client)


def send_workflow_request_assignment_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
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
    message.template_id = ACTOR_CHANGE_NOTIFICATION_TEMPLATE

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address, sg_client)


def send_workflow_request_status_change_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
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
    message.template_id = ACTOR_CHANGE_NOTIFICATION_TEMPLATE

    # Add dynamic data
    message.dynamic_template_data = parameters

    send_email(message, recipient_address, sg_client)


def send_user_invitation_to_recipients(
    recipients: List[NotificationRecipientClass],
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    for recipient in recipients:
        send_user_invitation_email(recipient, parameters, sg_client)


def send_user_invitation_email(
    recipient: NotificationRecipientClass,
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    # Extract recipient address
    recipient_address = recipient.id
    recipient_name = recipient.displayName or DEFAULT_RECIPIENT_NAME

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
    message.template_id = USER_INVITATION_TEMPLATE

    # Format parameters for USER_INVITATION_TEMPLATE
    invite_link = parameters.get("inviteLink", "")
    title = parameters.get("title", "You have been invited to DataHub!")
    inviter_name = parameters.get("inviterName", "Someone")
    role_name = parameters.get("roleName", "")

    # Validate required parameters
    if not invite_link or invite_link == "null" or invite_link.startswith("null/"):
        logger.error(
            f"Invalid invite link '{invite_link}' for recipient {recipient_address}. Skipping email."
        )
        return

    template_data = {
        "inviteLink": invite_link,
        "title": title,
        "inviterName": inviter_name,
        "roleName": role_name,
        "recipientName": recipient_name,
    }

    # Add dynamic data
    message.dynamic_template_data = template_data

    send_email(message, recipient_address, sg_client)


def send_support_login_email(
    parameters: Dict[str, str],
    sg_client: SendGridAPIClient,
) -> None:
    """
    Send support login notification email to configured recipients.

    Expected parameters:
    - supportTicketId: Support ticket ID (optional)
    - actorUrn: User URN who logged in
    - actorName: Display name of the user
    - timestamp: Login timestamp (ISO 8601 format in UTC)
    - sourceIP: Source IP address (optional)
    """
    if not SUPPORT_LOGIN_EMAIL_TEMPLATE:
        logger.warning(
            "SUPPORT_LOGIN_EMAIL_TEMPLATE not configured, skipping support login email notification"
        )
        return

    if not SUPPORT_LOGIN_EMAIL_RECIPIENTS:
        logger.warning(
            "SUPPORT_LOGIN_EMAIL_RECIPIENTS not configured, skipping support login email notification"
        )
        return

    for recipient_email in SUPPORT_LOGIN_EMAIL_RECIPIENTS:
        try:
            # Create new message
            message = Mail(
                from_email=Email(FROM_EMAIL_ADDRESS, FROM_EMAIL_TITLE),
                to_emails=To(recipient_email),
            )

            # Specify the sendgrid template
            message.template_id = SUPPORT_LOGIN_EMAIL_TEMPLATE

            # Add dynamic data
            message.dynamic_template_data = parameters

            send_email(message, recipient_email, sg_client)
        except Exception as e:
            logger.error(
                f"Failed to send support login email to {recipient_email}: {e}"
            )


def send_email(message: Mail, context: str, sg_client: SendGridAPIClient) -> None:
    try:
        # Add global unsubscribe group to the email
        # asm = Asm(group_id=GLOBAL_NOTIFICATIONS_UNSUBSCRIBE_GROUP_ID)
        # message.asm = asm
        response = sg_client.send(message)
        if response.status_code != 202:
            logger.error(f"Failed to send email, context {context}: {response.body}")
        else:
            logger.info(f"Successfully sent email to {context}...")
    except Exception as e:
        logger.error(f"Failed to send email, context: {context}: {e}")
