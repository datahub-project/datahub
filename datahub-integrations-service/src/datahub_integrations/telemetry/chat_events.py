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


def teams_chat_id(conversation_id: str, activity_id: str) -> ChatId:
    """Generate a standardized chat ID for Teams telemetry events.

    Args:
        conversation_id: The conversation ID from Teams
        activity_id: The activity ID from Teams Bot Framework

    Returns:
        A standardized chat ID.
    """
    return f"teams_v1_chat_{conversation_id}_{activity_id}"


def teams_message_id(conversation_id: str, activity_id: str) -> MessageId:
    """Generate a standardized message ID for Teams telemetry events.

    Args:
        conversation_id: The conversation ID from Teams
        activity_id: The activity ID from Teams Bot Framework

    Returns:
        A standardized message ID.
    """
    return f"teams_v1_message_{conversation_id}_{activity_id}"


def ui_chat_id(conversation_urn: str, timestamp: str) -> ChatId:
    """Generate a standardized chat ID for DataHub UI telemetry events.

    Args:
        conversation_urn: The conversation URN from DataHub
        timestamp: The timestamp of the message

    Returns:
        A standardized chat ID.
    """
    return f"ui_v1_chat_{conversation_urn}_{timestamp}"


def ui_message_id(conversation_urn: str, timestamp: str) -> MessageId:
    """Generate a standardized message ID for DataHub UI telemetry events.

    Args:
        conversation_urn: The conversation URN from DataHub
        timestamp: The timestamp of the message

    Returns:
        A standardized message ID.
    """
    return f"ui_v1_message_{conversation_urn}_{timestamp}"


class ChatbotInteractionEvent(BaseEvent):
    """Event representing a chatbot interaction."""

    type: Literal["ChatbotInteraction"] = "ChatbotInteraction"
    chat_id: ChatId
    message_id: MessageId
    chatbot: str = "slack"
    agent_name: Optional[str] = None
    slack_thread_id: Optional[str] = None
    slack_message_id: Optional[str] = None
    slack_user_id: Optional[str] = None
    slack_user_name: Optional[str] = None
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

    # Teams-specific fields
    teams_user_id: Optional[str] = None
    teams_user_name: Optional[str] = None
    teams_conversation_id: Optional[str] = None
    teams_activity_id: Optional[str] = None

    # DataHub UI-specific fields
    ui_user_urn: Optional[str] = None
    ui_conversation_urn: Optional[str] = None

    # Tool context snapshot (captures view preferences and other context items)
    tool_context: Optional[str] = None


class ChatbotInteractionFeedbackEvent(BaseEvent):
    """Event representing feedback on a chatbot interaction."""

    type: Literal["ChatbotInteractionFeedback"] = "ChatbotInteractionFeedback"
    chat_id: ChatId
    message_id: MessageId
    chatbot: str = "slack"
    slack_thread_id: Optional[str] = None
    slack_message_id: Optional[str] = None
    slack_user_id: Optional[str] = None
    slack_user_name: Optional[str] = None
    # TODO: Add email + resolved actor URN

    feedback: str  # "positive" or "negative"
    message_contents: Optional[str] = None

    # Teams-specific fields
    teams_user_id: Optional[str] = None
    teams_user_name: Optional[str] = None
    teams_conversation_id: Optional[str] = None
    teams_activity_id: Optional[str] = None

    # DataHub UI-specific fields
    ui_user_urn: Optional[str] = None
    ui_conversation_urn: Optional[str] = None


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

    # External plugin context (populated for external MCP plugin tools)
    is_external: bool = False
    plugin_id: Optional[str] = None
    plugin_name: Optional[str] = None
