"""Utility functions for chat functionality."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from bs4 import BeautifulSoup
from loguru import logger

if TYPE_CHECKING:
    from datahub_integrations.chat.chat_session import ChatSession
    from datahub_integrations.chat.planner.tools import get_plan_by_id
else:
    # Import at runtime to avoid circular dependency
    def get_plan_by_id(*args, **kwargs):
        from datahub_integrations.chat.planner.tools import (
            get_plan_by_id as _get_plan_by_id,
        )

        return _get_plan_by_id(*args, **kwargs)


@dataclass(frozen=True)
class ParsedReasoning:
    """
    Represents a parsed reasoning message with structured fields.

    Attributes:
        action: Brief description of the action being taken
        rationale: Explanation of why this action is needed
        user_requested: What the user originally asked for
        what_found: What was actually found (if different from user request)
        exact_match: Whether the found entity exactly matches what user requested
        discrepancies: Specific differences between requested and found
        proof_of_relation: Evidence that entities are related
        confidence: Confidence level (high/medium/low)
        warning: Important caveats or warnings
        user_visible_text: Text outside <reasoning> tags intended for users
        plan_id: Plan identifier if executing a multi-step plan
        plan_step: Current step ID being worked on
        step_status: Status of the current step
        raw_text: Original unparsed text
    """

    raw_text: str = ""
    action: Optional[str] = None
    rationale: Optional[str] = None
    user_requested: Optional[str] = None
    what_found: Optional[str] = None
    exact_match: Optional[bool] = None
    discrepancies: Optional[str] = None
    proof_of_relation: Optional[str] = None
    confidence: Optional[str] = None
    warning: Optional[str] = None
    user_visible_text: Optional[str] = None
    plan_id: Optional[str] = None
    plan_step: Optional[str] = None
    step_status: Optional[str] = None

    def to_user_visible_message(self, session: Optional["ChatSession"] = None) -> str:
        """
        Create a user-visible message from the parsed reasoning.

        If plan fields are present and session is provided, formats the message
        as a plan progress display with step indicators.

        Otherwise, prioritizes showing:
        1. User-visible text OR action (whichever is longer)
        2. Warning if present (especially for mismatches)
        3. Confidence level if medium or low

        Args:
            session: Optional ChatSession to retrieve plan information

        Returns:
            A human-readable string suitable for showing to users
        """
        # If plan fields are present and we have a session, show plan progress
        if self.plan_id and session:
            # Get basic reasoning message to show under current step
            parts = self._format_message_parts()
            basic_reasoning = " - ".join(parts) if parts else self.raw_text.strip()

            # Format with plan progress display
            return format_plan_progress(
                plan_id=self.plan_id,
                current_step_id=self.plan_step,
                step_status=self.step_status,
                session=session,
                reasoning_message=basic_reasoning,
            )

        # Default formatting (no plan context)
        parts = self._format_message_parts()

        # Add confidence if not high
        if self.confidence and self.confidence.lower() in ["medium", "low"]:
            parts.append(f"[Confidence: {self.confidence}]")

        # If we have nothing useful, fall back to raw text
        if not parts:
            return self.raw_text.strip()

        return " - ".join(parts)

    def has_entity_mismatch(self) -> bool:
        """Check if there's a mismatch between what user requested and what was found."""
        return self.exact_match is False

    def _format_message_parts(self) -> list[str]:
        """
        Extract common message formatting logic for user-visible text.

        Returns a list of message parts including:
        1. User-visible text OR action (whichever is longer)
        2. Warning (explicit or auto-generated from discrepancies)

        Returns:
            List of formatted message parts
        """
        parts = []

        # Start with either user_visible_text or action (whichever is longer)
        # Design decision: We use string length as a heuristic because both fields
        # typically describe the same action - one is structured (action) and one is
        # natural language (user_visible_text). The longer version is usually more
        # descriptive and informative for users. For example:
        #   action: "Search"
        #   user_visible_text: "I'm going to search for the dataset now"
        # In this case, the user_visible_text is more helpful.
        user_visible_text = (self.user_visible_text or "").strip()
        action = (self.action or "").strip()
        # Use whichever is longer and non-empty
        if user_visible_text or action:
            parts.append(
                user_visible_text if len(user_visible_text) > len(action) else action
            )

        # Add warning if present (important for entity mismatches)
        if self.warning:
            parts.append(f"⚠️ {self.warning}")
        elif self.exact_match is False and self.discrepancies:
            # Auto-generate warning from discrepancies if not explicitly set
            parts.append(f"⚠️ Note: {self.discrepancies}")

        return parts


def parse_reasoning_message(text: str) -> ParsedReasoning:
    """
    Parse a reasoning message that may contain XML structure.

    Uses BeautifulSoup to handle potentially malformed XML gracefully.
    If XML parsing fails or no XML is found, returns the raw text.

    Args:
        text: The reasoning message text, potentially containing XML

    Returns:
        ParsedReasoning object with extracted fields

    Example:
        >>> text = '''<reasoning>
        ...   <action>Search for dataset</action>
        ...   <rationale>Need to find the URN</rationale>
        ...   <user_requested>PROD_DB.table</user_requested>
        ...   <what_found>TEST_DB.table</what_found>
        ...   <exact_match>false</exact_match>
        ...   <discrepancies>Different database</discrepancies>
        ...   <confidence>medium</confidence>
        ... </reasoning>'''
        >>> parsed = parse_reasoning_message(text)
        >>> parsed.action
        'Search for dataset'
        >>> parsed.confidence
        'medium'
        >>> parsed.exact_match
        False
        >>> parsed.has_entity_mismatch()
        True
    """
    # Try to parse as XML using BeautifulSoup
    try:
        # Use html.parser as it's more tolerant of malformed XML from LLMs
        # and doesn't require lxml dependency
        soup = BeautifulSoup(text, "html.parser")

        # Look for <reasoning> tag
        reasoning_tag = soup.find("reasoning")

        if reasoning_tag:
            # Extract fields from <reasoning> tag
            action = _get_tag_text(reasoning_tag, "action")
            rationale = _get_tag_text(reasoning_tag, "rationale")
            user_requested = _get_tag_text(reasoning_tag, "user_requested")
            what_found = _get_tag_text(reasoning_tag, "what_found")
            discrepancies = _get_tag_text(reasoning_tag, "discrepancies")
            proof_of_relation = _get_tag_text(reasoning_tag, "proof_of_relation")
            confidence = _get_tag_text(reasoning_tag, "confidence")
            warning = _get_tag_text(reasoning_tag, "warning")
            plan_id = _get_tag_text(reasoning_tag, "plan_id")
            plan_step = _get_tag_text(reasoning_tag, "plan_step")
            step_status = _get_tag_text(reasoning_tag, "step_status")

            # Parse exact_match as boolean (lenient parsing for LLM variations)
            exact_match_text = _get_tag_text(reasoning_tag, "exact_match")
            exact_match = None
            if exact_match_text:
                exact_match_lower = exact_match_text.lower().strip()
                # Accept common true values
                if exact_match_lower in ("true", "yes", "1", "t", "y"):
                    exact_match = True
                # Accept common false values
                elif exact_match_lower in ("false", "no", "0", "f", "n"):
                    exact_match = False
                else:
                    # Log unexpected value for debugging
                    logger.warning(
                        f"Unexpected exact_match value: {exact_match_text!r}. "
                        f"Expected true/false/yes/no/1/0. Defaulting to None."
                    )

            # Extract text outside <reasoning> tags (intended for user display)
            # Remove the reasoning tag and get remaining text
            reasoning_tag.extract()  # Remove from soup
            user_visible_text = soup.get_text().strip()

            return ParsedReasoning(
                action=action,
                rationale=rationale,
                user_requested=user_requested,
                what_found=what_found,
                exact_match=exact_match,
                discrepancies=discrepancies,
                proof_of_relation=proof_of_relation,
                confidence=confidence,
                warning=warning,
                user_visible_text=user_visible_text if user_visible_text else None,
                plan_id=plan_id,
                plan_step=plan_step,
                step_status=step_status,
                raw_text=text,
            )
    except Exception as e:
        # If parsing fails completely, fall back to raw text
        logger.info(f"Failed to parse reasoning XML, using raw text: {e}")

    # No XML found or parsing failed - return raw text
    return ParsedReasoning(raw_text=text)


def _get_tag_text(parent_tag: Any, tag_name: str) -> Optional[str]:
    """
    Extract text content from a tag, returning None if not found.

    Args:
        parent_tag: BeautifulSoup tag to search within
        tag_name: Name of the child tag to find

    Returns:
        Stripped text content or None
    """
    tag = parent_tag.find(tag_name)
    if tag:
        text = tag.get_text().strip()
        return text if text else None
    return None


def format_plan_progress(
    plan_id: str,
    current_step_id: Optional[str],
    step_status: Optional[str],
    session: "ChatSession",
    reasoning_message: str,
) -> str:
    """
    Format plan progress with visual step indicators.

    Shows all plan steps with status icons:
    - ✓ for completed steps (all steps before current)
    - ▶ for in-progress step (current step)
    - • for pending steps (steps after current)

    If no current step specified, all steps shown as pending.

    Args:
        plan_id: The plan identifier
        current_step_id: ID of the currently executing step (e.g., "s1")
        step_status: Status of current step (e.g., "in_progress", "completed")
        session: ChatSession to retrieve the plan from
        reasoning_message: The reasoning message to display under the current step

    Returns:
        Formatted string with plan progress display
    """
    # Retrieve the plan
    plan = get_plan_by_id(plan_id, session)
    if not plan:
        # Plan not found, just return the reasoning message
        return reasoning_message

    lines = []

    # Add plan title
    lines.append(f"**Plan: {plan.title}**")

    # Determine current step index
    # Note: If current_step_id is hallucinated/invalid, current_step_index stays None
    # and all steps are safely rendered as pending (•) - no errors thrown
    current_step_index = None
    if current_step_id:
        for idx, step in enumerate(plan.steps):
            if step.id == current_step_id:
                current_step_index = idx
                break

    # Format each step
    for idx, step in enumerate(plan.steps):
        if current_step_index is None:
            # No current step - all steps are pending
            icon = "•"
            lines.append(f"{icon} {step.description}")
        elif idx < current_step_index:
            # Steps before current are completed
            icon = "✓"
            lines.append(f"{icon} {step.description}")
        elif idx == current_step_index:
            # Current step is in progress
            icon = "▶"
            lines.append(f"{icon} {step.description}")
            # Add indented reasoning message under current step (blockquote + italic for visual distinction)
            if reasoning_message:
                lines.append(f"> _{reasoning_message}_")
        else:
            # Steps after current are pending
            icon = "•"
            lines.append(f"{icon} {step.description}")

    return "\n\n".join(lines)
