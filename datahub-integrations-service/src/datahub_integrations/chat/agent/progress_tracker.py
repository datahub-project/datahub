"""
Progress tracking utilities for agent execution.

This module provides generalized progress tracking extracted from ChatSession's
FilteredProgressListener, making it reusable across different agent implementations.
"""

import re
from typing import TYPE_CHECKING, Callable, List, Literal, Optional

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.slack.utils.string import truncate

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.agent_runner import AgentRunner
    from datahub_integrations.chat.agent.conversational_parser import (
        ConversationalParser,
    )


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

    def __eq__(self, other):
        if not isinstance(other, ProgressUpdate):
            return False
        return self.text == other.text and self.message_type == other.message_type

    def __repr__(self):
        return f"ProgressUpdate(type={self.message_type}, text={self.text[:50]}...)"


ProgressCallback = Callable[[List[ProgressUpdate]], None]


class ProgressTracker:
    """
    Generalized progress tracking for agent execution.

    Converts chat history messages into user-friendly progress updates
    and ensures the progress callback is only called when things change.

    This is extracted from ChatSession's FilteredProgressListener to be
    reusable across different agent implementations.
    """

    def __init__(
        self,
        history: ChatHistory,
        progress_callback: Optional[ProgressCallback],
        agent: Optional["AgentRunner"] = None,
        start_offset: int = 0,
        conversational_parser: Optional["ConversationalParser"] = None,
    ):
        """
        Initialize progress tracker.

        Args:
            history: Chat history to track
            progress_callback: Optional callback for progress updates
            agent: Optional agent instance for context in progress messages
            start_offset: Starting index in history to track from
            conversational_parser: Optional parser for reasoning messages.
                                  If None, uses PlainTextParser (returns text as-is)
        """
        self.history = history
        self.progress_callback = progress_callback
        self.agent = agent
        self.start_offset = start_offset

        # Set up conversational parser
        if conversational_parser is not None:
            self.parser = conversational_parser
        else:
            # Default to plain text parser
            from datahub_integrations.chat.agent.conversational_parser import (
                PlainTextParser,
            )

            self.parser = PlainTextParser()

        self._last_progress_updates: Optional[List[ProgressUpdate]] = None

    @classmethod
    def _sanitize_progress_step(cls, step: str) -> str:
        """Replace trailing colon (with optional whitespace) with a period"""
        return re.sub(r":\s*$", ".", step).strip()

    def get_progress_updates(self) -> List[ProgressUpdate]:
        """
        Get current progress updates derived from chat history with type information.

        Returns:
            List of ProgressUpdate objects with text and message type
        """
        updates = []

        for message in self.history.messages[self.start_offset :]:
            # Determine message type
            message_type: Literal["THINKING", "TOOL_CALL", "TOOL_RESULT", "TEXT"]

            if isinstance(message, ReasoningMessage):
                message_type = "THINKING"
                # Use conversational parser to extract user-visible text
                user_visible_text = self.parser.parse_message(message.text, self.agent)

                # Sanitize and truncate progress messages
                # Max 1000 chars per step: generous buffer since parsed messages are
                # typically 50-200 chars. Even with 10 steps (10K chars total), this
                # stays well within Slack's 3K recommended limit and Teams' 28KB limit.
                sanitized_text = self._sanitize_progress_step(user_visible_text)
                text = truncate(sanitized_text, max_length=1000)

                updates.append(ProgressUpdate(text=text, message_type=message_type))

            elif isinstance(message, ToolCallRequest):
                message_type = "TOOL_CALL"
                # Could add tool call details here if needed
                # For now, skip to avoid clutter
                pass

            elif isinstance(message, (ToolResult, ToolResultError)):
                message_type = "TOOL_RESULT"
                # Could add tool result details here if needed
                # For now, skip to avoid clutter
                pass

        return updates

    def handle_history_updated(self) -> None:
        """
        Check for new progress updates and call callback if changed.

        This should be called after each message is added to history.
        """
        current_updates = self.get_progress_updates()
        if current_updates != self._last_progress_updates:
            self._last_progress_updates = current_updates
            if self.progress_callback:
                self.progress_callback(current_updates)
