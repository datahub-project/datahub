from typing import Callable, List, Optional, Tuple

import slack_sdk
import slack_sdk.errors
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from pydantic import BaseModel
from slack_bolt import App
from slack_sdk import WebClient

from datahub_integrations.app import graph
from datahub_integrations.chat.chat_history import AssistantMessage, HumanMessage
from datahub_integrations.chat.chat_session import ChatSession, NextMessage
from datahub_integrations.chat.linkify import slackify_markdown
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.slack.command.mention_helpers import (
    DATAHUB_FEEDBACK_PROMPT,
    DATAHUB_INITIAL_THINKING_MESSAGE,
    SlackPermissionError,
    fetch_thread_history,
)
from datahub_integrations.slack.config import slack_config
from datahub_integrations.slack.constants import (
    DATAHUB_SLACK_ICON_URL,
    MESSAGE_LENGTH_HARD_LIMIT,
)
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.telemetry.chat_events import (
    ChatbotInteractionEvent,
    ChatbotInteractionFeedbackEvent,
    slack_chat_id,
    slack_message_id,
)
from datahub_integrations.telemetry.telemetry import track_saas_event

DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID = "datahub_mention_followup_question"
DATAHUB_MENTION_FEEDBACK_BUTTON_ID = "datahub_mention_feedback"


class SlackMentionEvent(BaseModel):
    channel_id: str
    message_ts: str
    original_thread_ts: Optional[str] = None
    user_id: str

    message_text: str

    @property
    def thread_ts(self) -> str:
        # Get the thread_ts (thread ID) from the event.
        # If the message is part of a thread, thread_ts will be present.
        # Otherwise, use ts (the message's own timestamp) as the root of a new thread.
        return self.original_thread_ts or self.message_ts


def _build_progress_message(steps: List[str]) -> tuple[str, List[dict]]:
    # returns (plain text message fallback, rich Slack message blocks)
    # Current step is always the last one
    current_step = steps[-1]
    # Previous steps are everything except the last
    previous_steps = steps[:-1]

    # Show last 9 previous steps (Slack limit: 10 elements)
    shown_previous = previous_steps[-9:]

    # Build display elements
    elements = [f":white_check_mark: _{step}_" for step in shown_previous]
    elements.append(f":hourglass_flowing_sand: _*{current_step}*_")

    # Render blocks
    blocks = [
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": element} for element in elements],
        }
    ]

    return f":hourglass_flowing_sand: _*{current_step}*_", blocks


def handle_app_mention(app: App, event: SlackMentionEvent) -> None:
    """
    Process a message event and send a response in a thread.

    Args:
        app: The Slack app instance
        event: The event containing message details (text, channel, user, thread_ts)
    """
    channel_id = event.channel_id
    response_ts = None

    try:
        # Send a placeholder message to indicate we're processing
        text, blocks = _build_progress_message([DATAHUB_INITIAL_THINKING_MESSAGE])
        response_ts = app.client.chat_postMessage(
            channel=channel_id,
            thread_ts=event.thread_ts,  # Reply in the thread
            text=text,
            blocks=blocks,
            icon_url=DATAHUB_SLACK_ICON_URL,
            mrkdwn=True,
        )["ts"]

        # Get user info
        user_info = app.client.users_info(user=event.user_id)["user"]
        user_name = user_info["name"]

        def progress_callback(steps: List[str]) -> None:
            try:
                if response_ts is not None and steps:
                    text, blocks = _build_progress_message(steps)
                    app.client.chat_update(
                        channel=channel_id,
                        ts=response_ts,
                        blocks=blocks,
                        text=text,
                        icon_url=DATAHUB_SLACK_ICON_URL,
                        mrkdwn=True,
                    )
            except Exception as e:
                logger.warning(f"Failed to update progress message: {str(e)}")

        # Process the actual response
        message, followup_questions = _generation_mention_response(
            app.client, event, progress_callback, response_ts
        )

        # Build the response blocks
        blocks = _build_response(
            event.user_id,
            event.thread_ts,
            event.message_ts,
            message,
            followup_questions,
        )

        # Update the placeholder message with the actual response
        if response_ts is not None:
            app.client.chat_update(
                channel=channel_id,
                ts=response_ts,
                blocks=blocks,
                text=message,  # fallback text used by Slack for notifications
                icon_url=DATAHUB_SLACK_ICON_URL,
            )

        track_saas_event(
            ChatbotInteractionEvent(
                chat_id=slack_chat_id(channel_id, event.thread_ts),
                message_id=slack_message_id(channel_id, event.message_ts),
                chatbot="slack",
                slack_thread_id=event.thread_ts,
                slack_message_id=event.message_ts,
                slack_user_id=event.user_id,
                slack_user_name=user_name,
                message_contents=event.message_text,
                response_contents=message,
            )
        )
        logger.debug(f"Successfully sent Slack response to channel {channel_id}")
    except Exception as e:
        logger.exception(f"Failed to send slack response to channel: {channel_id}")
        if response_ts is not None:
            app.client.chat_update(
                channel=channel_id,
                ts=response_ts,
                text=":x: Encountered an internal error",
                icon_url=DATAHUB_SLACK_ICON_URL,
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
                slack_user_name=user_name,
                message_contents=event.message_text,
                response_contents=None,
                response_error=f"{type(e).__name__}: {str(e)}",
            )
        )


def _generation_mention_response(
    client: WebClient,
    event: SlackMentionEvent,
    progress_callback: Callable[[List[str]], None],
    response_ts: str,
) -> Tuple[str, List[str]]:
    message_text = event.message_text
    thread_ts = event.thread_ts
    logger.info(f"App mention message: {message_text} in thread {thread_ts}")

    thread_history = slack_config.get_slack_history_cache().get_thread(
        event.channel_id, thread_ts
    )
    try:
        thread_history.set_full_history(
            fetch_thread_history(client, channel=event.channel_id, thread_ts=thread_ts)
        )
    except SlackPermissionError:
        thread_history.add_message(
            event.message_ts,
            HumanMessage(text=event.message_text),
            is_latest_message=True,
        )
    if thread_history.is_limited_history():
        logger.info(
            "Unable to fetch full thread history due to missing permissions. "
            f"Proceeding with {thread_history.message_count()} slack messages"
        )

    history = thread_history.get_chat_history()
    original_history_length = len(history.messages)

    chat_session = ChatSession(
        tools=[mcp],
        client=DataHubClient(graph=graph),
        history=history,
    )
    with chat_session.set_progress_callback(progress_callback):
        response = chat_session.generate_next_message()
    assert isinstance(response, NextMessage)

    # Add the intermediate thinking messages to the history store.
    new_messages = history.messages[original_history_length:]
    thread_history.add_message(response_ts, AssistantMessage(text=response.text))
    thread_history.add_thinking(response_ts, new_messages)

    return slackify_markdown(response.text), response.suggestions


class FeedbackPayload(BaseModel):
    thread_ts: str
    message_ts: str
    feedback: str  # positive or negative


class FollowupQuestionPayload(BaseModel):
    question: str
    thread_ts: str
    user_message_ts: str


def _build_response(
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
                {"type": "mrkdwn", "text": DATAHUB_FEEDBACK_PROMPT},
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
                    ).model_dump_json(),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 No", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_negative",
                    "value": FeedbackPayload(
                        thread_ts=thread_ts,
                        message_ts=message_ts,
                        feedback="negative",
                    ).model_dump_json(),
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
                    "text": {
                        "type": "plain_text",
                        "text": truncate(question, 75),
                        "emoji": True,
                    },
                    "action_id": f"{DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID}_{i}",
                    "value": FollowupQuestionPayload(
                        question=question,
                        thread_ts=thread_ts,
                        user_message_ts=message_ts,
                    ).model_dump_json(),
                }
            )

        # Add buttons in groups of 2 for better layout
        for i in range(0, len(buttons), 2):
            block_buttons = buttons[i : i + 2]
            blocks.append({"type": "actions", "elements": block_buttons})

    return blocks


def handle_followup_question(
    app: App,
    channel_id: str,
    user_id: str,
    payload: FollowupQuestionPayload,
) -> None:
    thread_ts = payload.thread_ts
    question = payload.question

    # First, send the selected question into the thread so the user knows what they selected.
    message_ts = app.client.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        text=f"<@{user_id}> asked: *{question}* 🤔",
        icon_url=DATAHUB_SLACK_ICON_URL,
        mrkdwn=True,
    )["ts"]

    # Create an event object similar to app_mention
    event = SlackMentionEvent(
        channel_id=channel_id,
        message_ts=message_ts,
        original_thread_ts=thread_ts,
        user_id=user_id,
        message_text=question,
    )

    handle_app_mention(app, event)


def handle_feedback(
    app: App,
    channel_id: str,
    bot_message_ts: str,
    user_id: str,
    user_name: str,
    payload: FeedbackPayload,
) -> None:
    # payload.message_ts is the id of the user's message.
    # bot_message_ts is the id of the bot's response message.

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
            icon_url=DATAHUB_SLACK_ICON_URL,
            mrkdwn=True,
        )
    elif payload.feedback == "positive":
        # For positive feedback, just acknowledge with a reaction
        try:
            app.client.reactions_add(
                channel=channel_id,
                timestamp=bot_message_ts,
                name="thumbsup",
            )
        except slack_sdk.errors.SlackApiError as e:
            if "already_reacted" in str(e).lower():
                logger.debug(
                    f"Thumbs up reaction already added to message {bot_message_ts}"
                )
            else:
                raise
