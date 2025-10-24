import logging

from slack_sdk import WebClient
from slack_sdk.web import SlackResponse

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
