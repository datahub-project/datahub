import json
from typing import Callable, List, Optional, Tuple

import slack_sdk
import slack_sdk.errors
from loguru import logger
from pydantic import BaseModel
from slack_bolt import App
from slack_sdk import WebClient

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.chat.chat_session import ChatSession, Message
from datahub_integrations.chat.linkify import linkify_slack
from datahub_integrations.chat.mcp_server import mcp
from datahub_integrations.chat.telemetry import track_saas_event
from datahub_integrations.chat.telemetry_models import (
    ChatbotInteractionEvent,
    ChatbotInteractionFeedbackEvent,
    slack_chat_id,
    slack_message_id,
)
from datahub_integrations.slack.constants import (
    ACRYL_SLACK_ICON_URL,
    MESSAGE_LENGTH_HARD_LIMIT,
)
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.slack.utils.time import slack_ts_to_datetime

DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID = "datahub_mention_followup_question"
DATAHUB_MENTION_FEEDBACK_BUTTON_ID = "datahub_mention_feedback"
DATAHUB_THINKING_MESSAGE_PREFIX = "Sure thing! I'm looking through the available data to answer your question. Hold on a second..."


def clean_message_text(text: str) -> str:
    """
    Clean up message text by removing UI elements and feedback prompts.

    Args:
        text: The message text to clean

    Returns:
        The cleaned message text
    """
    # Skip progress messages
    if text.endswith("⏳"):
        return ""

    # Skip initial thinking message
    if text.startswith(DATAHUB_THINKING_MESSAGE_PREFIX):
        return ""

    # Remove the feedback question and buttons
    if "Was this response helpful?" in text:
        text = text.split("Was this response helpful?")[0].strip()

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


class SlackMentionEvent(BaseModel):
    channel_id: str
    message_ts: str
    original_thread_ts: Optional[str]
    user_id: str

    message_text: str

    @property
    def thread_ts(self) -> str:
        return self.original_thread_ts or self.message_ts


def process_app_chat_event_and_send_message(app: App, event: SlackMentionEvent) -> None:
    """
    Process a message event and send a response in a thread.

    Args:
        app: The Slack app instance
        event: The event containing message details (text, channel, user, thread_ts)
    """
    channel_id = event.channel_id

    # Send immediate placeholder response
    placeholder_response = app.client.chat_postMessage(
        channel=channel_id,
        thread_ts=event.thread_ts,  # Reply in the thread
        text=f"{DATAHUB_THINKING_MESSAGE_PREFIX} ⏳",
        icon_url=ACRYL_SLACK_ICON_URL,
        mrkdwn=True,
    )
    response_ts = placeholder_response["ts"]

    def progress_callback(message: str) -> None:
        """Update the placeholder message with progress updates"""
        try:
            app.client.chat_update(
                channel=channel_id,
                ts=response_ts,
                text=f"{message} ⏳",
                icon_url=ACRYL_SLACK_ICON_URL,
                mrkdwn=True,
            )
        except Exception as e:
            logger.error(f"Failed to update progress message: {str(e)}")

    # Process the actual response
    try:
        message, followup_questions = handle_mention(app, event, progress_callback)

        blocks = build_response(
            event.user_id,
            event.thread_ts,
            event.message_ts,
            message,
            followup_questions,
        )

        # Update the placeholder message with the actual response
        app.client.chat_update(
            channel=channel_id,
            ts=response_ts,
            blocks=blocks,
            text=message,  # fallback text used by Slack for notifications
            icon_url=ACRYL_SLACK_ICON_URL,
        )

        track_saas_event(
            ChatbotInteractionEvent(
                chat_id=slack_chat_id(channel_id, event.thread_ts),
                message_id=slack_message_id(channel_id, event.message_ts),
                chatbot="slack",
                slack_thread_id=event.thread_ts,
                slack_message_id=event.message_ts,
                slack_user_id=event.user_id,
                message_contents=event.message_text,
                response_contents=message,
            )
        )
        logger.debug(f"Successfully sent Slack response to channel {channel_id}")
    except Exception as e:
        logger.exception(f"Failed to send slack response to channel: {channel_id}")
        app.client.chat_update(
            channel=channel_id,
            ts=response_ts,
            text=":x: Encountered an internal error",
            icon_url=ACRYL_SLACK_ICON_URL,
            mrkdwn=True,
        )
        track_saas_event(
            ChatbotInteractionEvent(
                chat_id=slack_chat_id(channel_id, event.thread_ts),
                message_id=slack_message_id(channel_id, event.message_ts),
                chatbot="slack",
                slack_thread_id=event.thread_ts,
                slack_message_id=event.message_ts,
                slack_user_id=event.user_id,
                message_contents=event.message_text,
                response_contents=None,
                response_error=f"{type(e).__name__}: {str(e)}",
            )
        )


class SlackPermissionError(Exception):
    pass


def _fetch_thread_history(
    client: WebClient, channel: str, thread_ts: str
) -> List[Message]:
    thread_messages: List[Message] = []
    try:
        # Call the Slack API to get the conversation replies
        conversation_replies = client.conversations_replies(
            channel=channel, ts=thread_ts
        )
    except Exception as e:
        error_message = str(e).lower()
        if (
            "not_in_channel" in error_message
            or "missing_scope" in error_message
            or "permission" in error_message
        ):
            logger.warning(
                f"Permission error when retrieving thread messages: {str(e)}"
            )
            raise SlackPermissionError(str(e)) from e
        else:
            logger.error(f"Error retrieving thread messages: {str(e)}")
            raise

    # Process the messages if needed
    messages: List[dict] = conversation_replies.get("messages", [])
    logger.info(f"Retrieved {len(messages)} messages in thread")
    for msg in messages:
        logger.debug(f"Thread message: {msg.get('text')}")

    # Process messages in the thread
    for msg in messages:
        # Skip our placeholder "looking through data" message
        if msg.get("text", "").startswith(DATAHUB_THINKING_MESSAGE_PREFIX):
            continue

        # Determine the author (DataHubBot or user ID)
        if msg.get("bot_id"):
            author = "DataHubBot"
        else:
            author = msg.get("user", "unknown")

        # Clean the message text before creating the Message object
        cleaned_text = clean_message_text(msg.get("text", ""))

        # Skip empty messages (like progress updates)
        if not cleaned_text:
            continue

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

        # Create message object with author, text, and timestamp
        thread_messages.append(
            Message(
                text=cleaned_text,
                timestamp=slack_ts_to_datetime(msg["ts"]),
                visibility="assistant" if is_assistant else "user",
            )
        )

    logger.debug(f"Processed thread messages: {thread_messages}")
    return thread_messages


def handle_mention(
    app: App, event: SlackMentionEvent, progress_callback: Callable[[str], None]
) -> Tuple[str, List[str]]:
    # Extract the message text from the app mention event
    message_text = event.message_text
    logger.info(f"App mention message: {message_text}")

    # Get the thread_ts (thread ID) from the event
    # If the message is part of a thread, thread_ts will be present
    # Otherwise, use ts (the message's own timestamp) as the root of a new thread
    thread_ts = event.thread_ts or event.message_ts
    logger.info(f"Thread ID: {thread_ts}")

    # If this is part of a thread, get all messages in the thread
    thread_messages = []
    has_permission_error = False
    if thread_ts:
        try:
            thread_messages = _fetch_thread_history(
                app.client, event.channel_id, thread_ts
            )
        except SlackPermissionError:
            has_permission_error = True
            logger.info(
                "Proceeding with just the current message due to permission limitations"
            )

            # Add just the current message to thread_messages.
            thread_messages.append(
                Message(
                    text=message_text,
                    timestamp=slack_ts_to_datetime(event.message_ts),
                    visibility="user",
                )
            )

    chat_session = ChatSession(
        tools=mcp.get_all_tools(),
        user_message_history=thread_messages,
        progress_callback=progress_callback,
    )
    response = chat_session.generate_next_message()

    return linkify_slack(
        DATAHUB_FRONTEND_URL, response.text
    ), response.suggestions if not has_permission_error else []


class FeedbackPayload(BaseModel):
    thread_ts: str
    message_ts: str
    feedback: str


def build_response(
    user_id: str,
    thread_ts: str,
    message_ts: str,
    message: str,
    followup_questions: List[str],
) -> List[dict]:
    # Prepare blocks for the response
    blocks: List[dict] = []

    # Truncate message if it's too long.
    message = truncate(message, max_length=MESSAGE_LENGTH_HARD_LIMIT)

    # Create the main message block with user mention
    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"Hey <@{user_id}>! {message}"},
        }
    )

    # Add a divider before the footer
    blocks.append({"type": "divider"})

    # Add feedback prompt in a context block
    blocks.append(
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "Was this response helpful?"},
            ],
        }
    )

    # Add feedback buttons
    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👍 Yes", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_positive",
                    "value": FeedbackPayload(
                        thread_ts=thread_ts,
                        message_ts=message_ts,
                        feedback="positive",
                    ).json(),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 No", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_negative",
                    "value": FeedbackPayload(
                        thread_ts=thread_ts,
                        message_ts=message_ts,
                        feedback="negative",
                    ).json(),
                },
            ],
        }
    )

    # Add hint text in a separate context block below the buttons
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "💡 Hint: Mention @DataHub in the thread for responses.",
                },
            ],
        }
    )

    # Add follow-up question buttons if available
    if followup_questions and len(followup_questions) > 0:
        # Add a divider before follow-up questions
        blocks.append({"type": "divider"})
        buttons = []
        for i, question in enumerate(followup_questions):
            buttons.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": question, "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID}_{i}",
                    "value": json.dumps({"question": question, "thread_ts": thread_ts}),
                }
            )

        # Add buttons in groups of 2 for better layout
        for i in range(0, len(buttons), 2):
            block_buttons = buttons[i : i + 2]
            blocks.append({"type": "actions", "elements": block_buttons})

    return blocks


def process_feedback(
    app: App,
    channel_id: str,
    user_id: str,
    user_name: str,
    payload: FeedbackPayload,
) -> None:
    # Get the original message content if possible.
    # TODO: Probably should fetch the full message history.
    message_content = None
    try:
        # Fetch the message history to get the original message
        result = app.client.conversations_history(
            channel=channel_id,
            latest=payload.message_ts,
            limit=1,
            inclusive=True,
        )
        message_content = result["messages"][0].get("text", "No content")
        logger.info(
            f"Received {payload.feedback} feedback on message: '{message_content}'"
        )
    except slack_sdk.errors.SlackApiError as e:
        logger.info(
            f"Received {payload.feedback} feedback on message {payload.message_ts}. Could not fetch actual message: {e}"
        )

    track_saas_event(
        ChatbotInteractionFeedbackEvent(
            chat_id=slack_chat_id(channel_id, payload.thread_ts),
            message_id=slack_message_id(channel_id, payload.message_ts),
            chatbot="slack",
            slack_thread_id=payload.thread_ts,
            slack_message_id=payload.message_ts,
            slack_user_id=user_id,
            slack_user_name=user_name,
            feedback=payload.feedback,
            message_contents=message_content,
        )
    )

    # If feedback is negative, prompt for improvement
    if payload.feedback == "negative":
        app.client.chat_postMessage(
            channel=channel_id,
            thread_ts=payload.thread_ts,
            text="What was missing or incorrect in my response? I'd like to help you better! 🤔",
            icon_url=ACRYL_SLACK_ICON_URL,
            mrkdwn=True,
        )
    elif payload.feedback == "positive":
        # For positive feedback, just acknowledge with a reaction
        app.client.reactions_add(
            channel=channel_id,
            timestamp=payload.message_ts,
            name="thumbsup",
        )
