"""Utilities for analyzing conversation health and quality."""

from typing import List, Dict, Any, Optional
from api.models import ConversationHealthStatus, ArchivedMessageModel


def compute_health_status(messages: List[ArchivedMessageModel]) -> ConversationHealthStatus:
    """
    Analyze conversation messages and compute health metrics.

    Args:
        messages: List of ArchivedMessageModel objects

    Returns:
        ConversationHealthStatus with computed metrics
    """
    if not messages:
        return ConversationHealthStatus(
            is_abandoned=False,
            completion_rate=1.0,
            unanswered_questions_count=0,
            has_errors=False,
        )

    # Group messages by turn (user message starts a new turn)
    turns = []
    current_turn = {"user_msg": None, "assistant_msgs": []}

    for msg in messages:
        if msg.role == "user":
            # Save previous turn if it exists
            if current_turn["user_msg"]:
                turns.append(current_turn)

            # Start new turn
            current_turn = {"user_msg": msg, "assistant_msgs": []}
        elif msg.role == "assistant":
            current_turn["assistant_msgs"].append(msg)

    # Don't forget last turn
    if current_turn["user_msg"]:
        turns.append(current_turn)

    if not turns:
        return ConversationHealthStatus(
            is_abandoned=False,
            completion_rate=1.0,
            unanswered_questions_count=0,
            has_errors=False,
        )

    # Analyze turns
    unanswered_questions = []
    turns_with_responses = 0
    has_errors = False

    for turn in turns:
        assistant_msgs = turn["assistant_msgs"]

        # Check if turn has a TEXT response
        has_text_response = any(msg.message_type == "TEXT" for msg in assistant_msgs)

        # Check if there's any thinking/tool activity
        has_thinking = any(
            msg.message_type in ["THINKING", "TOOL_CALL"] for msg in assistant_msgs
        )

        # Check for errors
        for msg in assistant_msgs:
            if msg.message_type == "TOOL_RESULT":
                # Parse tool result to check is_error field
                try:
                    import json
                    result_data = json.loads(msg.content)
                    # Check if is_error field is true
                    if result_data.get("is_error") is True:
                        has_errors = True
                    # Also check if result contains error field with content
                    elif isinstance(result_data.get("result"), dict):
                        if result_data["result"].get("error"):
                            has_errors = True
                except:
                    # Fallback to string matching if JSON parsing fails
                    # But be more specific - look for actual error indicators
                    content_lower = msg.content.lower()
                    if ('"is_error": true' in content_lower or
                        '"error":' in content_lower and 'null' not in content_lower):
                        has_errors = True

        if has_text_response:
            turns_with_responses += 1
        else:
            unanswered_questions.append(
                {"has_thinking": has_thinking, "user_msg": turn["user_msg"]}
            )

    # Analyze last turn for abandonment
    last_turn = turns[-1]
    last_assistant_msgs = last_turn["assistant_msgs"]

    has_last_text_response = any(
        msg.message_type == "TEXT" for msg in last_assistant_msgs
    )
    has_last_thinking = any(
        msg.message_type in ["THINKING", "TOOL_CALL"] for msg in last_assistant_msgs
    )

    # Determine if abandoned
    is_abandoned = False
    abandonment_reason = None
    last_msg = messages[-1]

    if last_msg.role == "user":
        # Last message is user question - bot never responded
        is_abandoned = True
        abandonment_reason = "no_response_at_all"
    elif not has_last_text_response and has_last_thinking:
        # Bot started working but never finished
        is_abandoned = True
        abandonment_reason = "incomplete_response"
    elif not has_last_text_response and not has_last_thinking:
        # This shouldn't happen but let's track it
        is_abandoned = True
        abandonment_reason = "no_assistant_activity"

    # Calculate completion rate
    completion_rate = (
        turns_with_responses / len(turns) if len(turns) > 0 else 1.0
    )

    return ConversationHealthStatus(
        is_abandoned=is_abandoned,
        abandonment_reason=abandonment_reason,
        unanswered_questions_count=len(unanswered_questions),
        completion_rate=round(completion_rate, 2),
        has_errors=has_errors,
        last_message_role=last_msg.role,
    )
