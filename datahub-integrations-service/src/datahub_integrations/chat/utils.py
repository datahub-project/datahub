"""Utility functions for chat functionality."""

from dataclasses import dataclass
from typing import Any, Optional

from bs4 import BeautifulSoup
from loguru import logger


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

    def to_user_visible_message(self) -> str:
        """
        Create a user-visible message from the parsed reasoning.

        Prioritizes showing:
        1. User-visible text OR action (whichever is longer)
        2. Warning if present (especially for mismatches)
        3. Confidence level if medium or low

        Returns:
            A human-readable string suitable for showing to users
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
