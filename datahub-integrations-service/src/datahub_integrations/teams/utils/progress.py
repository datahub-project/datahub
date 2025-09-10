"""Teams progress message utilities for live AI reasoning updates."""

from typing import Any, Dict, List, Optional


def build_teams_progress_message(steps: List[str]) -> Dict[str, Any]:
    """
    Build a Teams message with progress indicators for AI reasoning steps.

    Args:
        steps: List of reasoning steps, with the last being the current step

    Returns:
        Teams message dict with progress visualization
    """
    if not steps:
        return {"type": "message", "text": "🤔 Starting to process your question..."}

    # Current step is always the last one
    current_step = steps[-1]
    # Previous steps are everything except the last
    previous_steps = steps[:-1]

    # Show last 8 previous steps to avoid message length limits
    shown_previous = previous_steps[-8:]

    # Build text representation
    progress_lines = []

    # Add completed steps
    for step in shown_previous:
        progress_lines.append(f"✅ *{step}*")

    # Add current step
    progress_lines.append(f"⏳ *{current_step}*")

    # Join with double newlines for Teams (better line separation)
    progress_text = "\n\n".join(progress_lines)

    return {"type": "message", "text": progress_text}


def build_teams_initial_progress_message(
    initial_message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build initial Teams message to show AI is processing.

    Args:
        initial_message: Optional custom initial message

    Returns:
        Teams message dict for initial processing state
    """
    message = (
        initial_message
        or "Sure thing! I'm looking through the available data to answer your question. Hold on a second..."
    )

    return {"type": "message", "text": f"⏳ *{message}*"}
