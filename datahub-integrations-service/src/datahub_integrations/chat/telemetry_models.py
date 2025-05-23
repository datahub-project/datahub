from datetime import datetime, timezone
from typing import Literal, Optional, TypeAlias

from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    """Base class for all telemetry events."""

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the event occurred",
    )
    type: str


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
    slack_email: Optional[str] = None
    message_contents: str
    response_contents: Optional[str] = None
    response_error: Optional[str] = None
    # TODO: referenced_urns: List[str] = Field(default_factory=list)


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
