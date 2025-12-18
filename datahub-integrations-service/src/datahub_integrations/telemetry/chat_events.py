from typing import Literal, Optional, TypeAlias

from datahub_integrations.telemetry.telemetry import BaseEvent

ChatId: TypeAlias = str
MessageId: TypeAlias = str


def slack_chat_id(channel_id: str, thread_ts: str) -> ChatId:
    """Generate a standardized chat ID for telemetry events.

    Args:
        channel_id: The channel ID from Slack
        thread_ts: The timestamp of the thread from Slack

    Returns:
        A standardized chat ID.
    """
    # In Slack, the ts / thread_ts is a timestamp-like string that uniquely
    # identifies a thread within a given channel.
    return f"slack_v1_chat_{channel_id}_{thread_ts}"


def slack_message_id(channel_id: str, message_ts: str) -> MessageId:
    """Generate a standardized message ID for telemetry events.

    Args:
        channel_id: The channel ID from Slack
        message_ts: The timestamp of the message from Slack

    Returns:
        A standardized message ID.
    """
    return f"slack_v1_message_{channel_id}_{message_ts}"


class ChatbotInteractionEvent(BaseEvent):
    """Event representing a chatbot interaction."""

    type: Literal["ChatbotInteraction"] = "ChatbotInteraction"
    chat_id: ChatId
    message_id: MessageId
    chatbot: str = "slack"
    slack_thread_id: str
    slack_message_id: str
    slack_user_id: str
    slack_user_name: str
    # TODO: Add slack_email: Optional[str] = None
    message_contents: str
    response_contents: Optional[str] = None
    response_length: Optional[int] = None
    response_error: Optional[str] = None
    response_generation_duration_sec: float
    chat_session_id: Optional[str] = None  # datahub internal

    is_limited_history: Optional[bool] = None
    is_followup_question: Optional[bool] = None

    # TODO: referenced_urns: List[str] = Field(default_factory=list)

    num_tool_calls: Optional[int] = None
    num_tool_call_errors: Optional[int] = None
    num_history_messages: Optional[int] = None
    full_history: Optional[str] = None

    num_reducers_applied: Optional[int] = None
    reduction_sequence: Optional[str] = None


class ChatbotInteractionFeedbackEvent(BaseEvent):
    """Event representing feedback on a chatbot interaction."""

    type: Literal["ChatbotInteractionFeedback"] = "ChatbotInteractionFeedback"
    chat_id: ChatId
    message_id: MessageId
    chatbot: str = "slack"
    slack_thread_id: str
    slack_message_id: str
    slack_user_id: str
    slack_user_name: str
    # TODO: Add email + resolved actor URN

    feedback: str  # "positive" or "negative"
    message_contents: Optional[str] = None


class ChatbotToolCallEvent(BaseEvent):
    """Event representing a tool call within a chatbot interaction."""

    type: Literal["ChatbotToolCall"] = "ChatbotToolCall"
    chat_session_id: str

    # Tool-specific fields
    tool_name: str
    tool_input: Optional[dict] = None  # Input dictionary/arguments passed to the tool
    tool_execution_duration_sec: float
    tool_result_length: Optional[int] = None
    tool_result_is_error: bool = False
    tool_error: Optional[str] = None
