"""
Progress tracking utilities for agent execution.

This module provides push-based progress tracking for agent execution.
Messages are pushed to the tracker as they are added to history, and
the tracker notifies the callback with accumulated progress updates.
"""

import re
from typing import Callable, List, Literal, Optional

from datahub_integrations.chat.agent.conversational_parser import ConversationalParser
from datahub_integrations.chat.chat_history import (
    Message,
    ReasoningMessage,
)
from datahub_integrations.chat.utils import PlanGetter
from datahub_integrations.slack.utils.string import truncate


class ProgressUpdate:
    """
    Structured progress update with both text content and message type.

    This replaces the old string-only progress updates to avoid losing
    type information and having to reconstruct it later.
    """

    def __init__(
        self,
        text: str,
        message_type: Literal["THINKING", "TOOL_CALL", "TOOL_RESULT", "TEXT"],
    ):
        self.text = text
        self.message_type = message_type

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProgressUpdate):
            return False
        return self.text == other.text and self.message_type == other.message_type

    def __repr__(self) -> str:
        return f"ProgressUpdate(type={self.message_type}, text={self.text[:50]}...)"


ProgressCallback = Callable[[List[ProgressUpdate]], None]


class ProgressTracker:
    """
    Push-based progress tracking for agent execution.

    Messages are pushed via `on_message()` as they are added to history.
    The tracker accumulates progress updates and notifies the callback
    whenever a new trackable message is received.

    Currently only tracks ReasoningMessage (shown as "THINKING" updates).
    """

    def __init__(
        self,
        progress_callback: Optional[ProgressCallback],
        get_plan: Optional[PlanGetter] = None,
        conversational_parser: Optional[ConversationalParser] = None,
    ):
        """
        Initialize progress tracker.

        Args:
            progress_callback: Optional callback for progress updates.
                Called with List[ProgressUpdate] whenever new updates are available.
            get_plan: Optional callback to retrieve plans by ID.
                Used for formatting plan progress in reasoning messages.
            conversational_parser: Optional parser for reasoning messages.
                If None, uses PlainTextParser (returns text as-is).
        """
        self.progress_callback = progress_callback
        self.get_plan = get_plan

        # Set up conversational parser
        if conversational_parser is not None:
            self.parser = conversational_parser
        else:
            # Default to plain text parser
            from datahub_integrations.chat.agent.conversational_parser import (
                PlainTextParser,
            )

            self.parser = PlainTextParser()

        # Accumulated progress updates
        self._updates: List[ProgressUpdate] = []

    @classmethod
    def _sanitize_progress_step(cls, step: str) -> str:
        """Replace trailing colon (with optional whitespace) with a period."""
        return re.sub(r":\s*$", ".", step).strip()

    def on_message(self, message: Message) -> None:
        """
        Process a new message and notify callback if it's trackable.

        Currently only ReasoningMessage produces progress updates.
        Other message types (ToolCallRequest, ToolResult, etc.) are ignored.

        Args:
            message: The message that was just added to history.
        """
        if isinstance(message, ReasoningMessage):
            # Use conversational parser to extract user-visible text
            user_visible_text = self.parser.parse_message(message.text, self.get_plan)

            # Sanitize and truncate progress messages
            # Max 1000 chars per step: generous buffer since parsed messages are
            # typically 50-200 chars. Even with 10 steps (10K chars total), this
            # stays well within Slack's 3K recommended limit and Teams' 28KB limit.
            sanitized_text = self._sanitize_progress_step(user_visible_text)
            text = truncate(sanitized_text, max_length=1000)

            self._updates.append(ProgressUpdate(text=text, message_type="THINKING"))

            # Notify callback with accumulated updates
            if self.progress_callback:
                self.progress_callback(list(self._updates))

    def reset(self) -> None:
        """
        Clear accumulated updates.

        Call this when starting a new generation cycle (e.g., in set_progress_callback).
        """
        self._updates = []

    def get_updates(self) -> List[ProgressUpdate]:
        """
        Get current accumulated updates.

        Returns:
            Copy of the accumulated ProgressUpdate list.
        """
        return list(self._updates)
