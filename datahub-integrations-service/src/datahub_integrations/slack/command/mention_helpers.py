from typing import Dict, List, Optional

from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    HumanMessage,
    Message,
)

_TEST_MODE_NO_MESSAGE_READ = get_boolean_env_variable(
    "DATAHUB_SLACK_TEST_MODE_NO_MESSAGE_READ", default=False
)
if _TEST_MODE_NO_MESSAGE_READ:
    logger.info("Running in test mode, with no message read permission")

DATAHUB_INITIAL_THINKING_MESSAGE = "Sure thing! I'm looking through the available data to answer your question. Hold on a second..."
DATAHUB_FEEDBACK_PROMPT = "Was this response helpful?"


def clean_message_text(text: str) -> Optional[str]:
    """
    Clean up message text by removing UI elements and feedback prompts.

    Args:
        text: The message text to clean

    Returns:
        The cleaned message text, or None if the message should be skipped entirely
    """

    # Skip initial thinking message
    if DATAHUB_INITIAL_THINKING_MESSAGE in text:
        return None

    # Skip progress messages (both those that start with progress prefixes and those that end with ⏳)
    progress_prefixes = ["⏳", ":hourglass_flowing_sand:"]
    if any(
        text.startswith(prefix) or text.endswith(prefix) for prefix in progress_prefixes
    ):
        return None

    # Remove the feedback question and buttons
    if DATAHUB_FEEDBACK_PROMPT in text:
        text = text.split(DATAHUB_FEEDBACK_PROMPT)[0].strip()

    # Remove hint text that might appear in different formats
    if "💡 Hint:" in text:
        text = text.split("💡 Hint:")[0].strip()
    if ":bulb: Hint:" in text:
        text = text.split(":bulb: Hint:")[0].strip()

    # Remove Yes/No button text that might appear in different formats
    text = text.replace("👍 Yes button", "").replace("👎 No button", "")
    text = text.replace(":+1: Yes button", "").replace(":-1: No button", "")

    # Remove any trailing divider text that might have been included
    text = text.split("---")[0].strip()

    return text.strip()


class SlackPermissionError(Exception):
    pass


def fetch_thread_history(
    client: WebClient, channel: str, thread_ts: str
) -> Dict[str, Message]:
    if _TEST_MODE_NO_MESSAGE_READ:
        raise SlackPermissionError("No message read in test mode")

    try:
        # Call the Slack API to get the conversation replies
        conversation_replies = client.conversations_replies(
            channel=channel, ts=thread_ts
        )
    except SlackApiError as e:
        error_message = str(e).lower()
        if (
            "not_in_channel" in error_message
            or "missing_scope" in error_message
            or "permission" in error_message
        ):
            logger.debug(f"Permission error when retrieving thread messages: {str(e)}")
            raise SlackPermissionError(str(e)) from e
        else:
            logger.error(f"Error retrieving thread messages: {str(e)}")
            raise

    # Process the messages if needed
    raw_messages: List[dict] = conversation_replies.get("messages", [])
    logger.info(f"Retrieved {len(raw_messages)} messages in thread")

    # Process messages in the thread
    thread_messages: Dict[str, Message] = {}
    for msg in raw_messages:
        message_ts = msg.get("ts")
        if not message_ts:
            continue

        message = parse_thread_message(msg)
        if message:
            thread_messages[message_ts] = message

    logger.debug(f"Processed thread messages: {thread_messages}")
    return thread_messages


def parse_thread_message(msg: dict) -> Optional[Message]:
    # Determine the author (DataHubBot or user ID)
    if msg.get("bot_id"):
        author = "DataHubBot"
    else:
        author = msg.get("user", "unknown")

    raw_text = msg.get("text", "")
    logger.debug(f"Thread message from {author}: {raw_text}")

    # Clean the message text before creating the Message object
    cleaned_text = clean_message_text(raw_text)
    if not cleaned_text:
        logger.debug("Skipping empty / progress message")
        return None

    # Check if this is a follow-up question (they end with :thinking_face:)
    is_follow_up = cleaned_text.endswith(":thinking_face:")
    if is_follow_up:
        # Remove the :thinking_face: emoji from the text
        cleaned_text = cleaned_text.replace(":thinking_face:", "").strip()
        # Follow-up questions should be treated as user messages
        is_assistant = False
    else:
        # Only mark as assistant if it's from the bot and not a follow-up question
        is_assistant = author == "DataHubBot"

    # Create message object with author and text.
    message: Message = (
        AssistantMessage(text=cleaned_text)
        if is_assistant
        else HumanMessage(text=cleaned_text)
    )
    return message
