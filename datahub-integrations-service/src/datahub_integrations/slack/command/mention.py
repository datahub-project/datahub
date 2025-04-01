import json
from typing import Callable, List, Tuple

from loguru import logger
from slack_bolt import App

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.chat.chat_session import ChatSession, Message
from datahub_integrations.chat.linkify import linkify_slack
from datahub_integrations.chat.mcp_server import mcp
from datahub_integrations.slack.constants import (
    ACRYL_SLACK_ICON_URL,
    MESSAGE_LENGTH_HARD_LIMIT,
)

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


def process_app_chat_event_and_send_message(app: App, event: dict) -> None:
    """
    Process a message event and send a response in a thread.

    Args:
        app: The Slack app instance
        event: The event containing message details (text, channel, user, thread_ts)
    """
    # Send immediate placeholder response
    thread_ts = event.get("thread_ts", event.get("ts"))
    placeholder_response = app.client.chat_postMessage(
        channel=event["channel"],
        thread_ts=thread_ts,  # Reply in the thread
        text=f"{DATAHUB_THINKING_MESSAGE_PREFIX} ⏳",
        icon_url=ACRYL_SLACK_ICON_URL,
        mrkdwn=True,  # Ensure markdown formatting is respected
    )

    def progress_callback(message: str) -> None:
        """Update the placeholder message with progress updates"""
        try:
            app.client.chat_update(
                channel=event["channel"],
                ts=placeholder_response["ts"],
                text=f"{message} ⏳",
                icon_url=ACRYL_SLACK_ICON_URL,
                mrkdwn=True,
            )
        except Exception as e:
            logger.error(f"Failed to update progress message: {str(e)}")

    # Process the actual response
    message, followup_questions = handle_mention(app, event, progress_callback)
    blocks = build_response(event["user"], thread_ts, message, followup_questions)

    # Extract text content from blocks for fallback
    text = message
    if followup_questions:
        text += "\n\nFollow-up questions:\n" + "\n".join(
            f"• {q}" for q in followup_questions
        )

    try:
        # Update the placeholder message with the actual response
        response = app.client.chat_update(
            channel=event["channel"],
            ts=placeholder_response["ts"],  # Update the placeholder message
            blocks=blocks,
            icon_url=ACRYL_SLACK_ICON_URL,
            mrkdwn=True,  # Ensure markdown formatting is respected
        )

        if response.status_code == 200:
            logger.debug(
                f"Successfully sent Slack response to channel {event['channel']}"
            )
        else:
            logger.error(
                f"Failed to send Slack response to channel {event['channel']}. Error: {response.get('error')}"
            )
    except Exception:
        logger.exception(
            f"Failed to send slack response to channel: {event['channel']}"
        )


def handle_mention(
    app: App, event: dict, progress_callback: Callable[[str], None]
) -> Tuple[str, List[str]]:
    # Extract the message text from the app mention event
    message_text = event.get("text", "")
    logger.info(f"App mention message: {message_text}")

    # Get the thread_ts (thread ID) from the event
    # If the message is part of a thread, thread_ts will be present
    # Otherwise, use ts (the message's own timestamp) as the root of a new thread
    thread_ts = event.get("thread_ts", event.get("ts"))
    logger.info(f"Thread ID: {thread_ts}")

    # If this is part of a thread, get all messages in the thread
    thread_messages = []
    has_permission_error = False
    author = None
    if thread_ts:
        try:
            # Call the Slack API to get the conversation replies
            conversation_replies = app.client.conversations_replies(
                channel=event["channel"], ts=thread_ts
            )

            # Log the conversation replies
            logger.info(
                f"Retrieved {len(conversation_replies.get('messages', []))} messages in thread"
            )

            # Process the messages if needed
            messages: List[dict] = conversation_replies.get("messages", [])
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
                        timestamp=event.get("ts", 0),
                        visibility="assistant" if is_assistant else "user",
                    )
                )

            # Sort messages with latest first (descending order by timestamp)
            thread_messages.sort(key=lambda x: x.timestamp, reverse=True)
            logger.debug(f"Processed thread messages: {thread_messages}")

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
                # Add just the current message to thread_messages instead of returning early
                thread_messages.append(
                    Message(
                        text=message_text,
                        timestamp=event.get("ts", 0),
                        visibility="assistant" if author == "DataHubBot" else "user",
                    )
                )
                has_permission_error = True
                logger.info(
                    "Proceeding with just the current message due to permission limitations"
                )
            else:
                logger.error(f"Error retrieving thread messages: {str(e)}")

        # Sort messages with latest first (descending order by timestamp)
        thread_messages.sort(key=lambda x: x.timestamp, reverse=True)

        logger.debug(f"Processed thread messages: {thread_messages}")

    chat_session = ChatSession(
        tools=mcp.get_all_tools(),
        user_message_history=thread_messages,
        progress_callback=progress_callback,
    )
    response = chat_session.generate_next_message()
    return linkify_slack(
        DATAHUB_FRONTEND_URL, response.text
    ), response.suggestions if not has_permission_error else []


def build_response(
    user_id: str, thread_ts: str, message: str, followup_questions: List[str]
) -> List[dict]:
    # Prepare blocks for the response
    blocks: List[dict] = []

    # Truncate message if it's too long.
    if len(message) > MESSAGE_LENGTH_HARD_LIMIT:
        message = message[: MESSAGE_LENGTH_HARD_LIMIT - 3] + "..."

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
                    "value": json.dumps(
                        {"thread_ts": thread_ts, "feedback": "positive"}
                    ),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 No", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_negative",
                    "value": json.dumps(
                        {"thread_ts": thread_ts, "feedback": "negative"}
                    ),
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
