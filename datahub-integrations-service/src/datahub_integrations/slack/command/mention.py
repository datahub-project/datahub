from typing import Callable, List, Optional, Tuple

import slack_sdk
import slack_sdk.errors
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.sdk.main_client import DataHubClient
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger
from pydantic import BaseModel
from slack_bolt import App
from slack_sdk import WebClient

from datahub_integrations.app import graph
from datahub_integrations.chat.agent import (
    AgentMaxTokensExceededError,
    AgentMaxToolCallsExceededError,
    AgentOutputMaxTokensExceededError,
    AgentRunner,
)
from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    HumanMessage,
    Message,
)
from datahub_integrations.chat.types import ChatType, NextMessage
from datahub_integrations.slack.command.mention_helpers import (
    DATAHUB_FEEDBACK_PROMPT,
    DATAHUB_INITIAL_THINKING_MESSAGE,
    SlackPermissionError,
    fetch_thread_history,
)
from datahub_integrations.slack.config import slack_config
from datahub_integrations.slack.constants import DATAHUB_SLACK_ICON_URL
from datahub_integrations.slack.utils.slackify import slackify_markdown
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

# If true, follow-up questions are displayed as text.
# Otherwise, they are displayed as buttons.
# Note that buttons in Slack are visually truncated after 30 characters.
# See https://claude.ai/share/e02f52ca-0972-4f4c-a3ae-cdeeee7729ca
# Because our follow-up questions are typically longer than 30 characters,
# we display them as text by default.
_SLACK_FOLLOWUPS_AS_TEXT = get_boolean_env_variable(
    "DATAHUB_SLACK_FOLLOWUPS_AS_TEXT", default=True
)


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
    """
    Build a progress message for Slack display.

    Returns:
        tuple[str, List[dict]]: (plain text message fallback, rich Slack message blocks)

    Behavior:
    - When the current step is a plan (multi-line with all execution history),
      show only that plan without previous steps since it already contains full context.
    - When the current step is a regular progress update (single concept),
      show last 9 previous steps + current step (Slack limit: 10 elements).
    """
    # Current step is always the last one
    current_step = steps[-1]

    # Check if current step is a plan message (contains newlines AND plan indicators)
    # Plan messages from format_plan_progress contain:
    # - Multiple lines (newlines)
    # - Plan title marker "**Plan:"
    # - Step status icons (✓, ▶, •)
    is_plan_message = "\n" in current_step and (
        "**Plan:" in current_step or "✓" in current_step or "▶" in current_step
    )

    if is_plan_message:
        # Plan message already contains full execution history and formatting.
        # Display it as-is without additional decoration.
        blocks: List[dict] = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": current_step},
            }
        ]
        # For plain text fallback, return the full plan message
        return current_step, blocks
    else:
        # Regular progress update: show last 9 previous + current
        previous_steps = steps[:-1]
        shown_previous = previous_steps[-9:]

        # Build display elements
        elements: List[str] = [
            f":white_check_mark: _{step}_" for step in shown_previous
        ]
        elements.append(f":hourglass_flowing_sand: _*{current_step}*_")

        # Render blocks
        blocks = [
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": element} for element in elements
                ],
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
    user_name = event.user_id
    is_followup_question: Optional[bool] = None
    response_ts = None
    agent = None
    is_limited_history = None

    timer = PerfTimer()
    timer.start()

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

        def progress_callback(steps: List[ProgressUpdate]) -> None:
            try:
                if response_ts is not None and steps:
                    step_texts = [step.text for step in steps]
                    text, blocks = _build_progress_message(step_texts)
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
        agent, is_limited_history = _build_agent(app.client, event)
        is_followup_question = agent.history.is_followup_datahub_ask_question
        message, followup_questions = _generate_mention_response(
            agent, event, progress_callback, response_ts
        )

        # Build the response blocks
        blocks = _build_response(
            event.user_id,
            event.thread_ts,
            event.message_ts,
            message,
            followup_questions,
            user_message_text=event.message_text,
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
                response_length=len(message),
                response_generation_duration_sec=timer.elapsed_seconds(),
                chat_session_id=agent.session_id,
                is_limited_history=is_limited_history,
                num_tool_calls=agent.history.num_tool_calls,
                num_tool_call_errors=agent.history.num_tool_call_errors,
                num_history_messages=len(agent.history.messages),
                full_history=agent.history.json(indent=None),
                reduction_sequence=agent.history.reduction_sequence_json,
                num_reducers_applied=agent.history.num_reducers_applied,
                is_followup_question=is_followup_question,
            )
        )
        logger.debug(f"Successfully sent Slack response to channel {channel_id}")
    except Exception as e:
        if response_ts is not None:
            logger.exception(f"Failed to send successful response {channel_id}: {e}")
            if isinstance(e, AgentMaxToolCallsExceededError):
                text = ":x: Uh, oh ! Looks like your question is too complex. Please try again with a simpler question."
            elif isinstance(
                e, (AgentMaxTokensExceededError, AgentOutputMaxTokensExceededError)
            ):
                # Keeping this for now, however this case should not appear with context reducers.
                text = ":x: Uh, oh ! Looks like I fetched too much information here. Please try asking your question in a new thread."
            else:
                text = ":x: Encountered an internal error"
            app.client.chat_update(
                channel=channel_id,
                ts=response_ts,
                text=text,
                icon_url=DATAHUB_SLACK_ICON_URL,
                mrkdwn=True,
            )
        else:
            logger.exception(
                f"Failed to send Slack response to channel {channel_id}: {e}"
            )
        # Track failed interaction with None defaults, updated if agent exists
        event_data = ChatbotInteractionEvent(
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
            response_generation_duration_sec=timer.elapsed_seconds(),
            chat_session_id=None,
            is_limited_history=is_limited_history,
            is_followup_question=is_followup_question,
        )

        # Update with agent data if available
        if agent:
            event_data.chat_session_id = agent.session_id
            event_data.num_tool_calls = agent.history.num_tool_calls
            event_data.num_tool_call_errors = agent.history.num_tool_call_errors
            event_data.num_history_messages = len(agent.history.messages)
            event_data.full_history = agent.history.json(indent=None)
            event_data.reduction_sequence = agent.history.reduction_sequence_json
            event_data.num_reducers_applied = agent.history.num_reducers_applied

        track_saas_event(event_data)


def _generate_mention_response(
    agent: AgentRunner,
    event: SlackMentionEvent,
    progress_callback: Callable[[List[ProgressUpdate]], None],
    response_ts: str,
) -> Tuple[str, List[str]]:
    original_history_length = len(agent.history.messages)
    response = None
    try:
        with agent.set_progress_callback(progress_callback):
            response = agent.generate_formatted_message()
        assert isinstance(response, NextMessage)
    finally:
        # Add the intermediate thinking messages to the history store.
        new_messages = agent.history.messages[original_history_length:]
        _update_slack_history_cache(event, response_ts, response, new_messages)

    return (
        slackify_markdown(response.text),
        response.suggestions,
    )


def _update_slack_history_cache(
    event: SlackMentionEvent,
    response_ts: str,
    response: Optional[NextMessage],
    new_messages: List[Message],
) -> None:
    thread_history = slack_config.get_slack_history_cache().get_thread(
        event.channel_id, event.thread_ts
    )
    if response is not None:
        thread_history.add_message(response_ts, AssistantMessage(text=response.text))
    thread_history.add_thinking(response_ts, new_messages)


def _build_agent(
    client: WebClient, event: SlackMentionEvent
) -> Tuple[AgentRunner, bool]:
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

    agent = create_data_catalog_explorer_agent(
        client=DataHubClient(graph=graph),
        history=history,
        chat_type=ChatType.SLACK,
    )

    return agent, thread_history.is_limited_history()


class FeedbackPayload(BaseModel):
    thread_ts: str
    message_ts: str
    feedback: str  # positive or negative
    message_contents: Optional[str] = (
        None  # Optional original message text to avoid API call
    )


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
    user_message_text: str,
) -> List[dict]:
    # Prepare blocks for the response
    blocks: List[dict] = []

    # Create the main message block with user mention.
    message = f"Hey <@{user_id}>! {message}"
    if _SLACK_FOLLOWUPS_AS_TEXT and followup_questions:
        followup_message = "**Suggested follow-up questions:**\n"
        for question in followup_questions:
            followup_message += f"\n- {question}"
        followup_message = slackify_markdown(followup_message)
        # With Slack, text within a section block has a hard limit of 3000 characters.
        if len(message) + len(followup_message) < 3000 - 2:  # -2 for the newlines
            message = f"{message}\n\n{followup_message}"
        else:
            logger.debug(
                "Not including follow-up questions due to Slack message length limit"
            )

    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message},
        }
    )

    blocks.append({"type": "divider"})

    # Add feedback prompt + buttons.
    blocks.append(
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": DATAHUB_FEEDBACK_PROMPT},
            ],
        }
    )

    # Prepare message_contents for payload
    # Slack button value has a 2000 character limit for the JSON string
    # So we only include the message contents if it's less than 1000 characters
    message_contents: Optional[str] = (
        user_message_text if len(user_message_text) <= 1000 else None
    )

    # Create payloads for both feedback buttons
    positive_payload = FeedbackPayload(
        thread_ts=thread_ts,
        message_ts=message_ts,
        message_contents=message_contents,
        feedback="positive",
    ).model_dump_json()

    negative_payload = FeedbackPayload(
        thread_ts=thread_ts,
        message_ts=message_ts,
        message_contents=message_contents,
        feedback="negative",
    ).model_dump_json()

    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👍 Yes", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_positive",
                    "value": positive_payload,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 No", "emoji": True},
                    "action_id": f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}_negative",
                    "value": negative_payload,
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
    if not _SLACK_FOLLOWUPS_AS_TEXT and followup_questions:
        blocks.append({"type": "divider"})
        buttons = []
        for i, question in enumerate(followup_questions):
            buttons.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": truncate(question, max_length=75),
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
    # First check if it's already in the payload (avoiding API call)
    message_content = payload.message_contents

    # Only fetch from API if not already in payload
    if message_content is None:
        try:
            # Fetch messages from the specific thread to get the original user message
            # Using conversations_replies to scope search to the thread
            # Using latest parameter to narrow the search window and reduce limit
            result = app.client.conversations_replies(
                channel=channel_id,
                ts=payload.thread_ts,
                latest=payload.message_ts,  # Only get messages up to user's message
                limit=5,
                inclusive=True,  # Include messages at the exact timestamp
            )

            # Find the exact message by matching timestamp
            messages: List[dict] = result.get("messages", [])  # type: ignore[assignment]
            for msg in messages:
                if msg.get("ts") == payload.message_ts:
                    message_content = msg.get("text")
                    break

            if message_content is None:
                logger.warning(
                    f"Could not find user message {payload.message_ts} in thread {payload.thread_ts}"
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
