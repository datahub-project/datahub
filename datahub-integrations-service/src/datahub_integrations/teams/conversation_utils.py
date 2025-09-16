"""
Teams conversation utilities for proper conversation ID handling.

This module provides utilities to extract the correct conversation ID
for Teams conversations, handling both direct chats and channel threads.
"""

from typing import Any, Dict, Optional

from loguru import logger


def get_teams_conversation_id(activity: Any) -> Optional[str]:
    """
    Extract the proper conversation ID for Teams conversations.

    For Teams conversations, we need to handle two scenarios:
    1. Direct chat (1-on-1 with bot): Use conversation.id as-is
    2. Channel conversation: Use thread ID from channelData if available

    Args:
        activity: Bot Framework Activity object

    Returns:
        Proper conversation ID for the conversation context, or None if not available
    """
    if not activity or not activity.conversation:
        logger.warning("No conversation information available in activity")
        return None

    # Handle both dict and object forms of conversation
    if hasattr(activity.conversation, "id"):
        conversation_id = activity.conversation.id
    elif isinstance(activity.conversation, dict):
        conversation_id = activity.conversation.get("id")
    else:
        logger.warning(f"Unexpected conversation format: {type(activity.conversation)}")
        return None

    # Check if this is a channel conversation by looking at channelData
    channel_data = getattr(activity, "channel_data", None)

    if channel_data and isinstance(channel_data, dict):
        # This is a channel conversation
        thread_id = channel_data.get("threadId")

        if thread_id:
            # Use thread ID for channel conversations to scope to the specific thread
            logger.debug(f"Channel conversation detected, using thread ID: {thread_id}")
            return f"{conversation_id}:thread:{thread_id}"
        else:
            # Channel conversation but no thread ID - use conversation ID with channel marker
            logger.debug(
                f"Channel conversation without thread ID, using conversation ID: {conversation_id}"
            )
            return f"{conversation_id}:channel"
    else:
        # This is a direct chat (1-on-1 with bot)
        logger.debug(
            f"Direct chat conversation, using conversation ID: {conversation_id}"
        )
        return conversation_id


def is_channel_conversation(activity: Any) -> bool:
    """
    Check if the activity is from a channel conversation.

    Args:
        activity: Bot Framework Activity object

    Returns:
        True if this is a channel conversation, False if direct chat
    """
    channel_data = getattr(activity, "channel_data", None)
    return channel_data is not None and isinstance(channel_data, dict)


def is_direct_chat_conversation(activity: Any) -> bool:
    """
    Check if the activity is from a direct chat conversation.

    Args:
        activity: Bot Framework Activity object

    Returns:
        True if this is a direct chat conversation, False if channel conversation
    """
    return not is_channel_conversation(activity)


def get_conversation_context_info(activity: Any) -> Dict[str, Any]:
    """
    Get detailed conversation context information.

    Args:
        activity: Bot Framework Activity object

    Returns:
        Dictionary with conversation context information
    """
    if not activity or not activity.conversation:
        return {}

    channel_data = getattr(activity, "channel_data", None)

    context = {
        "conversation_id": activity.conversation.id,
        "conversation_type": "channel"
        if is_channel_conversation(activity)
        else "direct_chat",
        "is_channel": is_channel_conversation(activity),
        "is_direct_chat": is_direct_chat_conversation(activity),
    }

    if channel_data and isinstance(channel_data, dict):
        context.update(
            {
                "channel_id": channel_data.get("channelId"),
                "thread_id": channel_data.get("threadId"),
                "team_id": channel_data.get("teamId"),
                "tenant_id": channel_data.get("tenantId"),
            }
        )

    return context
