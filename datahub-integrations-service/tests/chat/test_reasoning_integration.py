"""Integration tests for reasoning message parsing in chat flow."""

from datahub_integrations.chat.utils import parse_reasoning_message


def test_reasoning_message_user_display() -> None:
    """Test that reasoning messages are converted to user-friendly format."""
    # Simulate a reasoning message with XML from the LLM
    reasoning_xml = """<reasoning>
  <action>Search for dataset OPERATION_DB.ANALYST_TASKS.table</action>
  <user_requested>OPERATION_DB.ANALYST_TASKS.table</user_requested>
  <what_found>analytics.test.table</what_found>
  <exact_match>false</exact_match>
  <discrepancies>Different database and schema</discrepancies>
  <confidence>medium</confidence>
</reasoning>"""

    # Parse it
    parsed = parse_reasoning_message(reasoning_xml)

    # Get user-visible version
    user_visible = parsed.to_user_visible_message()

    # Exact assertion for complete user-visible content
    expected = (
        "Search for dataset OPERATION_DB.ANALYST_TASKS.table - "
        "⚠️ Note: Different database and schema - "
        "[Confidence: medium]"
    )
    assert user_visible == expected

    # Additional verification that XML tags are not present
    assert "<reasoning>" not in user_visible
    assert "</reasoning>" not in user_visible
    assert "<action>" not in user_visible


def test_plain_reasoning_message() -> None:
    """Test that plain text reasoning (no XML) still works."""
    plain_text = "I'm going to search for the dataset now."

    parsed = parse_reasoning_message(plain_text)
    user_visible = parsed.to_user_visible_message()

    # Should return the plain text as-is
    assert user_visible == plain_text


def test_reasoning_with_high_confidence() -> None:
    """Test that high confidence doesn't show confidence level."""
    reasoning_xml = """<reasoning>
  <action>Get entity details</action>
  <confidence>high</confidence>
</reasoning>"""

    parsed = parse_reasoning_message(reasoning_xml)
    user_visible = parsed.to_user_visible_message()

    # Exact assertion for complete user-visible content
    expected = "Get entity details"
    assert user_visible == expected

    # High confidence is not shown
    assert "Confidence" not in user_visible


def test_reasoning_with_warning() -> None:
    """Test that explicit warnings are displayed prominently."""
    reasoning_xml = """<reasoning>
  <action>Respond to user</action>
  <warning>Entity not found in DataHub</warning>
  <confidence>low</confidence>
</reasoning>"""

    parsed = parse_reasoning_message(reasoning_xml)
    user_visible = parsed.to_user_visible_message()

    # Exact assertion for complete user-visible content
    expected = "Respond to user - ⚠️ Entity not found in DataHub - [Confidence: low]"
    assert user_visible == expected
