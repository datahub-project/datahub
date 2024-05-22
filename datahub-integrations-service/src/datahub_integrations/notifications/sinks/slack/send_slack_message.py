from typing import Any, List, Optional

from datahub.metadata.schema_classes import NotificationRecipientClass
from loguru import logger
from slack_sdk import WebClient


def send_notification_to_recipients(
    client: WebClient,
    recipients: List[NotificationRecipientClass],
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> List[str]:
    message_ids = []

    for recipient in recipients:
        maybe_message_id = send_notification_slack_message(
            client, recipient, text, blocks, attachments
        )
        if maybe_message_id is not None:
            message_ids.append(maybe_message_id)
    return message_ids


def send_notification_slack_message(
    client: WebClient,
    recipient: NotificationRecipientClass,
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> Optional[str]:

    # Extract Recipient ID
    recipient_handle = recipient.id

    if recipient_handle is None:
        logger.error(
            f"Recipient handle is None, skipping sending Slack. Text: {text}, Recipient: {recipient}, Blocks: {blocks}"
        )
        return None

    return send_slack_message(client, recipient_handle, text, blocks, attachments)


def send_slack_message(
    client: WebClient,
    channel: str,
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> Optional[str]:
    try:
        response = client.chat_postMessage(
            channel=channel, blocks=blocks, text=text, attachments=attachments
        )
        # Access the 'ts' field from the message object, which is the message ID
        message_id = response["message"]["ts"]
        logger.debug(
            f"Slack message sent successfully. Message ID: {message_id}, Recipient: {channel}"
        )
        return message_id
    except Exception as e:
        logger.error(
            f"Failed to send slack message to channel: {channel} text: {text}: {e}"
        )
        return None
