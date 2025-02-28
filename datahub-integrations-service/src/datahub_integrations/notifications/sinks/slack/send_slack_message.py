import re
from typing import Any, Dict, List, Optional

from datahub.metadata.schema_classes import NotificationRecipientClass
from loguru import logger
from slack_sdk import WebClient

from datahub_integrations.notifications.constants import (
    STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED,
)
from datahub_integrations.notifications.sinks.slack.types import SlackMessageDetails

# Cache storing channel identifier provided in notification settings to well-formed channel name (or None)
channel_name_cache: Dict[str, Optional[str]] = {}

# Regex to check if a string is a Slack Channel ID (starts with C, G followed by numbers)
channel_id_regex_pattern = re.compile(r"^[CG][A-Z0-9]+$")


def send_notification_to_recipients(
    client: WebClient,
    recipients: List[NotificationRecipientClass],
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> List[SlackMessageDetails]:
    message_details = []

    for recipient in recipients:
        maybe_message_details = send_notification_slack_message(
            client, recipient, text, blocks, attachments
        )
        if maybe_message_details is not None:
            message_details.append(maybe_message_details)
    return message_details


def update_messages(
    client: WebClient,
    message_details: List[SlackMessageDetails],
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> None:
    for message_detail in message_details:
        update_slack_message(
            client,
            message_detail.channel_id,
            message_detail.message_id,
            text,
            blocks,
            attachments,
        )


def send_notification_slack_message(
    client: WebClient,
    recipient: NotificationRecipientClass,
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> Optional[SlackMessageDetails]:
    # Extract Recipient ID
    recipient_handle = recipient.id

    if recipient_handle is None:
        logger.error(
            f"Recipient handle is None, skipping sending Slack. Text: {text}, Recipient: {recipient}, Blocks: {blocks}"
        )
        return None

    return send_slack_message(client, recipient_handle, text, blocks, attachments)


def update_slack_message(
    client: WebClient,
    channel: str,
    message_id: str,
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> None:
    logger.debug(
        f"Attempting to update slack message {message_id} in channel with id {channel}"
    )

    """
    Updates Slack message with new text, blocks, or attachments.

    :param client: WebClient instance for making Slack API calls.
    :param channel: The channel ID where the message is posted.
    :param message_id: The message to update
    :param text: New text for the message.
    :param blocks: New blocks structure for the message (optional).
    :param attachments: New attachments for the message (optional).
    """
    try:
        response = client.chat_update(
            channel=channel,
            ts=message_id,
            text=text,
            blocks=blocks,
            attachments=attachments,
        )
        if response.status_code == 200:
            logger.info("Message updated successfully!")
        else:
            logger.error(
                f"Slack API returned non-200 response while attempting to update message: {response['error']}"
            )
    except Exception:
        logger.exception(
            f"Failed to update message {message_id} in channel with id {channel}"
        )


def send_slack_message(
    client: WebClient,
    channel: str,
    text: str,
    blocks: Optional[Any],
    attachments: Optional[Any],
) -> Optional[SlackMessageDetails]:
    try:
        response = client.chat_postMessage(
            channel=channel, text=text, blocks=blocks, attachments=attachments
        )

        if response.status_code == 200:
            message_id = response["message"]["ts"]
            channel_name = try_resolve_channel_name(client, channel)

            logger.debug(
                f"Slack message sent successfully. Message ID: {message_id}, Channel: {channel}, Channel name: {channel_name}"
            )

            return SlackMessageDetails(
                channel_id=response["channel"],
                channel_name=channel_name,
                message_id=message_id,
            )

        else:
            logger.error(
                f"Slack API returned non-200 response while attempting to send message: {response['error']}"
            )
    except Exception:
        logger.exception(
            f"Failed to send slack message to channel: {channel} text: {text}"
        )

    return None


def resolve_channel_name(client: WebClient, channel: str) -> Optional[str]:
    if STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED:
        # If channel is cached, return cached value.
        if channel in channel_name_cache:
            return channel_name_cache[channel]

        # Otherwise try to return the resolved channel name fetched from Slack.
        return try_resolve_channel_name(client, channel)

    # Stateful slack incident messages are disabled, we don't need to save channel names.
    return None


def try_resolve_channel_name(client: WebClient, channel: str) -> Optional[str]:
    """
    Try to retrieve the channel name from the Slack API.
    Works with both channel ID and name formats.
    Returns None if it's a DM channel or if the channel cannot be resolved.
    """

    # Short-Circuit: If it's a user channel / DM (U or D), then there is no channel name to resolve,
    # the only thing we can do is return the original channel identifier provided for matching later.
    if channel.startswith("U") or channel.startswith("D"):
        channel_name_cache[channel] = channel
        return channel

    if channel_id_regex_pattern.match(channel):
        # It's a channel ID, use conversations_info to look up the channel directly.
        try:
            response = client.conversations_info(channel=channel)
            if response.status_code == 200:
                channel_name = response["channel"]["name"]
                channel_name_cache[channel] = channel_name
                return channel_name
            else:
                logger.error(
                    f"Slack API returned non-200 response while attempting to hydrate channel name: {response['error']}"
                )
                return None
        except Exception:
            logger.exception(
                f"Failed to hydrate channel name for channel ID, returning none: {channel}"
            )
            return None
    else:
        # If it's a raw channel name, use conversations_list to find the ID
        try:
            response = client.conversations_list()
            if response.status_code == 200:
                for ch in response["channels"]:
                    if ch["name"] == channel.strip("#"):
                        channel_name_cache[ch["id"]] = ch["name"]
                        return ch["name"]
            else:
                logger.error(
                    f"Slack API returned non-200 response while attempting to hydrate channel name: {response['error']}"
                )
                return None
        except Exception:
            logger.exception(
                f"Failed to hydrate channel name for channel ID, returning none: {channel}"
            )
            return None

    logger.warning(
        f"Failed to resolve any channel name for channel provided {channel}, even with successful responses from Slack API. Returning None"
    )

    # Calls were successful, but we didn't find a matching channel. No channel name could be resolved from provided identifier.
    channel_name_cache[channel] = None
    return None
