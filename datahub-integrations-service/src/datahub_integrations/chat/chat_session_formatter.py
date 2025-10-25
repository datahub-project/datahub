"""
Chat session message formatter.

This module provides simple message formatting functionality for different chat types
with appropriate length limits and validation.
"""

from loguru import logger

from datahub_integrations.chat.types import ChatType


def format_message(text: str, chat_type: ChatType) -> str:
    """
    Format message text based on chat type with appropriate length limits.

    Args:
        text: The message text to format
        chat_type: The type of chat context (DataHub UI, Slack, etc.)

    Returns:
        Formatted message text with appropriate length limits applied
    """
    # Define hard limits for different chat types
    limits = {
        ChatType.DATAHUB_UI: 10000,  # Higher limit for DataHub UI
        ChatType.SLACK: 2900,  # Slack's section block limit
        ChatType.TEAMS: 4000,  # Teams can handle longer messages
        ChatType.DEFAULT: 2900,  # Conservative default
    }

    hard_limit = limits.get(chat_type, limits[ChatType.DEFAULT])

    # Apply hard limit by truncating if necessary
    if len(text) > hard_limit:
        truncated_text = text[: hard_limit - 3] + "..."
        logger.warning(
            f"Message truncated to {hard_limit} characters for {chat_type.value} chat"
        )
        return truncated_text

    return text
