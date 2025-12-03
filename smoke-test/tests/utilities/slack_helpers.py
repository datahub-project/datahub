import logging

from slack_sdk import WebClient
from slack_sdk.web import SlackResponse

from tests.utilities.env_vars import get_slack_clock_skew_buffer_seconds
from tests.utils import with_test_retry

logger = logging.getLogger(__name__)


def get_slack_client(token: str) -> WebClient:
    return WebClient(token=token)


def get_channel_id_by_name(token: str, channel_name: str) -> str | None:
    """
    Resolve a channel name to its ID.

    Args:
        token: Slack bot token
        channel_name: Channel name (with or without # prefix)

    Returns:
        Channel ID if found, None otherwise
    """
    client = get_slack_client(token)
    channel_name = channel_name.lstrip("#")
    try:
        cursor = None
        while True:
            response = client.conversations_list(
                types="public_channel,private_channel",
                limit=1000,
                cursor=cursor,
            )
            if not response["ok"]:
                return None

            for channel in response["channels"]:
                if channel["name"] == channel_name:
                    return channel["id"]

            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        return None
    except Exception as e:
        logger.error(f"Failed to get channel ID for channel {channel_name}: {e}")
        return None


def convert_permalink_timestamp(permalink_ts: str) -> str:
    """
    Convert permalink timestamp format to Slack timestamp format.

    Args:
        permalink_ts: Timestamp in permalink format. Can be:
            - "p1760517914961629" (permalink timestamp)
            - "1760517914961629" (timestamp without prefix)
            - "1760517914.961629" (already formatted)
            - "https://datahub.slack.com/archives/C03SGMN2R16/p1760517914961629" (full URL)

    Returns:
        Timestamp in Slack format (e.g., "1760517914.961629")
    """
    if permalink_ts.startswith(("http://", "https://")):
        parts = permalink_ts.split("/")
        for part in parts:
            if part.startswith("p") and len(part) > 10:
                permalink_ts = part
                break
        else:
            return permalink_ts

    if "?" in permalink_ts:
        permalink_ts = permalink_ts.split("?")[0]

    if "." in permalink_ts and permalink_ts.count(".") == 1:
        return permalink_ts

    permalink_ts = permalink_ts.removeprefix("p")
    return f"{permalink_ts[:10]}.{permalink_ts[10:]}"


def send_message(
    token: str,
    channel: str,
    message: str,
    thread_ts: str | None = None,
    image: str | None = None,
) -> SlackResponse:
    client = get_slack_client(token)
    if thread_ts is not None:
        res = client.chat_postMessage(
            channel=channel, text=message, thread_ts=thread_ts, link_names=True
        )
        if image is not None:
            client.files_upload(channels=channel, file=image, thread_ts=thread_ts)
    else:
        res = client.chat_postMessage(channel=channel, text=message, link_names=True)
        if image is not None:
            client.files_upload(channels=channel, file=image)
    return res


def update_message(
    token: str,
    channel: str,
    ts: str,
    message: str,
) -> SlackResponse:
    """
    Update an existing Slack message.

    Args:
        token: Slack bot token
        channel: Channel ID or name
        ts: Timestamp of the message to update
        message: New message text

    Returns:
        Slack API response
    """
    client = get_slack_client(token)
    return client.chat_update(channel=channel, ts=ts, text=message, link_names=True)


def get_recent_messages(
    token: str,
    channel: str,
    limit: int = 10,
    oldest: str | None = None,
) -> list[dict]:
    """
    Retrieve recent messages from a Slack channel.

    Args:
        token: Slack bot token
        channel: Channel ID
        limit: Number of messages to retrieve (default: 10)
        oldest: Only messages after this Unix timestamp (optional)

    Returns:
        List of message dictionaries from the channel
    """
    client = get_slack_client(token)
    try:
        if oldest:
            response = client.conversations_history(
                channel=channel, limit=limit, oldest=oldest
            )
        else:
            response = client.conversations_history(channel=channel, limit=limit)

        if not response["ok"]:
            logger.error(f"Failed to retrieve messages: {response.get('error')}")
            return []

        return response.get("messages", [])
    except Exception as e:
        logger.error(f"Failed to get messages from channel {channel}: {e}")
        return []


def _find_text_in_blocks(blocks: list[dict], text: str) -> bool:
    """
    Search for text in Slack message blocks, including buttons.

    Args:
        blocks: List of block elements from a Slack message
        text: Text to search for

    Returns:
        True if text is found in any block or button, False otherwise
    """
    for block in blocks:
        block_type = block.get("type")

        if block_type == "section":
            section_text = block.get("text", {}).get("text", "")
            if text in section_text:
                return True

        elif block_type == "actions":
            elements = block.get("elements", [])
            for element in elements:
                if element.get("type") == "button":
                    button_text = element.get("text", {}).get("text", "")
                    if text in button_text:
                        return True

        elif block_type == "context":
            context_elements = block.get("elements", [])
            for element in context_elements:
                element_text = element.get("text", "")
                if text in element_text:
                    return True

    return False


@with_test_retry()
def check_slack_notification(
    token: str,
    channel_id: str,
    incident_title: str,
    timestamp_seconds: str,
    expected_text: str | None = None,
    limit: int = 20,
) -> dict:
    """
    Check for a Slack notification with specific content.

    Args:
        token: Slack bot token
        channel_id: Channel ID to search in
        incident_title: Title of the incident to find in message
        timestamp_seconds: Unix timestamp to start searching from
        expected_text: Optional text to verify in the message or buttons (e.g., "Reopen Incident")
        limit: Number of messages to retrieve (default: 20)

    Returns:
        The message dictionary if found

    Raises:
        AssertionError: If the notification is not found or doesn't contain expected text
    """
    clock_skew_buffer = get_slack_clock_skew_buffer_seconds()
    search_from_timestamp = str(int(timestamp_seconds) - clock_skew_buffer)

    messages = get_recent_messages(
        token, channel_id, limit=limit, oldest=search_from_timestamp
    )
    logger.info(f"Retrieved {len(messages)} messages from Slack channel")

    found_message = None
    for message in messages:
        message_text = message.get("text", "")
        if incident_title in message_text:
            logger.info(f"Found notification message: {message_text}")
            found_message = message
            break

    assert found_message is not None, (
        f"Slack notification not found for incident '{incident_title}'"
    )

    if expected_text:
        message_text = found_message.get("text", "")
        text_found = expected_text in message_text

        if not text_found:
            blocks = found_message.get("blocks", [])
            text_found = _find_text_in_blocks(blocks, expected_text)

        if not text_found:
            attachments = found_message.get("attachments", [])
            for attachment in attachments:
                attachment_blocks = attachment.get("blocks", [])
                if _find_text_in_blocks(attachment_blocks, expected_text):
                    text_found = True
                    break

        assert text_found, (
            f"Expected text '{expected_text}' not found in message text or blocks. "
            f"Message text: {message_text}, "
            f"Blocks: {found_message.get('blocks', [])}, "
            f"Attachments: {found_message.get('attachments', [])}"
        )
        logger.info(f"Verified message contains expected text: '{expected_text}'")

    return found_message
