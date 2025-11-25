"""
Conversational parser protocol and implementations.

This module defines how agents parse their conversational messages (thinking, reasoning)
into user-visible progress text. Different agents can use different message formats.
"""

from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.agent_runner import AgentRunner


class ConversationalParser(Protocol):
    """
    Protocol for parsing conversational messages into user-visible progress text.

    Different agents may use different message formats to communicate their thinking:
    - XML with structured tags (DataHub's <reasoning> format)
    - JSON with structured fields
    - Plain text
    - Custom formats

    This protocol allows customization while keeping progress tracking generic.
    """

    def parse_message(
        self, message_text: str, agent: Optional["AgentRunner"] = None
    ) -> str:
        """
        Parse a message and extract user-visible progress text.

        Args:
            message_text: Raw message text from LLM
            agent: Optional agent instance for context (e.g., accessing plan state)

        Returns:
            User-friendly text to display as progress
        """
        ...


class PlainTextParser:
    """
    Simple parser that returns text as-is without special formatting.

    This is the default parser for agents that don't use structured
    reasoning messages.
    """

    def parse_message(
        self, message_text: str, agent: Optional["AgentRunner"] = None
    ) -> str:
        """Return the message text with basic sanitization."""
        return message_text.strip()


class XmlReasoningParser:
    """
    Parser for DataHub's XML reasoning format.

    Parses <reasoning> tags with structured fields:
    - <action>, <rationale> - What and why
    - <plan_id>, <plan_step> - Plan execution context
    - <warning>, <confidence> - Important caveats
    - <user_requested>, <what_found>, <exact_match> - Entity matching

    If plan fields are present and agent has plan_cache, formats with
    plan progress indicators showing completed/in-progress/pending steps.
    """

    def parse_message(
        self, message_text: str, agent: Optional["AgentRunner"] = None
    ) -> str:
        """
        Parse XML reasoning message and format for user display.

        Args:
            message_text: XML reasoning message
            agent: Agent instance (used for plan formatting if available)

        Returns:
            Formatted user-visible text
        """
        from datahub_integrations.chat.utils import parse_reasoning_message

        parsed = parse_reasoning_message(message_text)

        # If agent has plan_cache (planning capability), pass for plan formatting
        # Use hasattr check since not all AgentRunners may have planning enabled
        if agent and hasattr(agent, "plan_cache"):
            # Pass agent directly - to_user_visible_message will access plan_cache
            return parsed.to_user_visible_message(session=agent)  # type: ignore
        else:
            # No plan formatting
            return parsed.to_user_visible_message(session=None)
