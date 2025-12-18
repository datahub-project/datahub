"""Unit tests for chat utility functions."""

import pytest

from datahub_integrations.chat.chat_api import ChatContext
from datahub_integrations.chat.utils import (
    ParsedReasoning,
    combine_contexts,
    parse_reasoning_message,
)


class TestParsedReasoning:
    """Tests for the ParsedReasoning dataclass."""

    def test_creation_with_defaults(self) -> None:
        """Test creating ParsedReasoning with default values."""
        parsed = ParsedReasoning()
        assert parsed.raw_text == ""
        assert parsed.action is None
        assert parsed.rationale is None
        assert parsed.exact_match is None

    def test_creation_with_values(self) -> None:
        """Test creating ParsedReasoning with specific values."""
        parsed = ParsedReasoning(
            action="Search for entity",
            confidence="high",
            exact_match=True,
            raw_text="<reasoning>test</reasoning>",
        )
        assert parsed.action == "Search for entity"
        assert parsed.confidence == "high"
        assert parsed.exact_match is True
        assert parsed.raw_text == "<reasoning>test</reasoning>"

    def test_immutability(self) -> None:
        """Test that ParsedReasoning is immutable (frozen)."""
        parsed = ParsedReasoning(action="Test action")

        # Frozen dataclasses raise AttributeError (specifically FrozenInstanceError)
        with pytest.raises(AttributeError):
            parsed.action = "Modified"  # type: ignore[misc]  # Testing immutability

    def test_hashable(self) -> None:
        """Test that ParsedReasoning is hashable."""
        parsed1 = ParsedReasoning(action="Test", confidence="high")
        parsed2 = ParsedReasoning(action="Test", confidence="high")

        # Can be added to a set
        reasoning_set = {parsed1, parsed2}
        assert len(reasoning_set) == 1  # Same values should hash the same

        # Can be used as dict key
        reasoning_dict = {parsed1: "value"}
        assert reasoning_dict[parsed2] == "value"

    def test_has_entity_mismatch_true(self) -> None:
        """Test has_entity_mismatch returns True when exact_match is False."""
        parsed = ParsedReasoning(exact_match=False)
        assert parsed.has_entity_mismatch() is True

    def test_has_entity_mismatch_false(self) -> None:
        """Test has_entity_mismatch returns False when exact_match is True."""
        parsed = ParsedReasoning(exact_match=True)
        assert parsed.has_entity_mismatch() is False

    def test_has_entity_mismatch_none(self) -> None:
        """Test has_entity_mismatch returns False when exact_match is None."""
        parsed = ParsedReasoning(exact_match=None)
        assert parsed.has_entity_mismatch() is False

    def test_to_user_visible_message_with_action(self) -> None:
        """Test user-visible message with just action."""
        parsed = ParsedReasoning(action="Search for dataset")
        assert parsed.to_user_visible_message() == "Search for dataset"

    def test_to_user_visible_message_with_warning(self) -> None:
        """Test user-visible message with explicit warning."""
        parsed = ParsedReasoning(
            action="Get entity details",
            warning="Entity not found",
        )
        message = parsed.to_user_visible_message()
        assert "Get entity details" in message
        assert "⚠️ Entity not found" in message

    def test_to_user_visible_message_with_mismatch(self) -> None:
        """Test user-visible message auto-generates warning for mismatch."""
        parsed = ParsedReasoning(
            action="Get entity details",
            exact_match=False,
            discrepancies="Different database",
        )
        message = parsed.to_user_visible_message()
        assert "Get entity details" in message
        assert "⚠️ Note: Different database" in message

    def test_to_user_visible_message_with_medium_confidence(self) -> None:
        """Test user-visible message includes medium confidence."""
        parsed = ParsedReasoning(
            action="Search",
            confidence="medium",
        )
        message = parsed.to_user_visible_message()
        assert "Search" in message
        assert "[Confidence: medium]" in message

    def test_to_user_visible_message_with_low_confidence(self) -> None:
        """Test user-visible message includes low confidence."""
        parsed = ParsedReasoning(
            action="Search",
            confidence="low",
        )
        message = parsed.to_user_visible_message()
        assert "[Confidence: low]" in message

    def test_to_user_visible_message_with_high_confidence(self) -> None:
        """Test user-visible message excludes high confidence."""
        parsed = ParsedReasoning(
            action="Search",
            confidence="high",
        )
        message = parsed.to_user_visible_message()
        assert "Confidence" not in message

    def test_to_user_visible_message_fallback_to_raw(self) -> None:
        """Test user-visible message falls back to raw text when no fields."""
        parsed = ParsedReasoning(raw_text="Some plain text reasoning")
        assert parsed.to_user_visible_message() == "Some plain text reasoning"


class TestParseReasoningMessage:
    """Tests for the parse_reasoning_message function."""

    def test_parse_well_formed_xml(self) -> None:
        """Test parsing well-formed XML reasoning."""
        xml_text = """<reasoning>
  <action>Search for dataset</action>
  <rationale>Need to find the URN</rationale>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Search for dataset"
        assert parsed.rationale == "Need to find the URN"
        assert parsed.confidence == "high"
        assert parsed.raw_text == xml_text

    def test_parse_xml_with_entity_mismatch(self) -> None:
        """Test parsing XML with entity mismatch fields."""
        xml_text = """<reasoning>
  <action>Get entity details</action>
  <user_requested>OPERATION_DB.ANALYST_TASKS.table</user_requested>
  <what_found>analytics.test.table</what_found>
  <exact_match>false</exact_match>
  <discrepancies>Different database and schema</discrepancies>
  <confidence>medium</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Get entity details"
        assert parsed.user_requested == "OPERATION_DB.ANALYST_TASKS.table"
        assert parsed.what_found == "analytics.test.table"
        assert parsed.exact_match is False
        assert parsed.discrepancies == "Different database and schema"
        assert parsed.confidence == "medium"

    def test_parse_xml_exact_match_true(self) -> None:
        """Test parsing XML with exact_match=true."""
        xml_text = """<reasoning>
  <exact_match>true</exact_match>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)
        assert parsed.exact_match is True

    def test_parse_xml_exact_match_variations(self) -> None:
        """Test that exact_match accepts common boolean variations."""
        # Test true variations
        for value in ["true", "True", "TRUE", "yes", "Yes", "1", "t", "y"]:
            xml_text = f"""<reasoning>
  <exact_match>{value}</exact_match>
</reasoning>"""
            parsed = parse_reasoning_message(xml_text)
            assert parsed.exact_match is True, (
                f"Expected True for {value!r}, got {parsed.exact_match}"
            )

        # Test false variations
        for value in ["false", "False", "FALSE", "no", "No", "0", "f", "n"]:
            xml_text = f"""<reasoning>
  <exact_match>{value}</exact_match>
</reasoning>"""
            parsed = parse_reasoning_message(xml_text)
            assert parsed.exact_match is False, (
                f"Expected False for {value!r}, got {parsed.exact_match}"
            )

    def test_parse_xml_all_fields(self) -> None:
        """Test parsing XML with all possible fields."""
        xml_text = """<reasoning>
  <action>Respond to user</action>
  <rationale>Have all needed information</rationale>
  <user_requested>Entity A</user_requested>
  <what_found>Entity B</what_found>
  <exact_match>false</exact_match>
  <discrepancies>Different entity</discrepancies>
  <proof_of_relation>SIMILAR names only</proof_of_relation>
  <confidence>low</confidence>
  <warning>This is a different entity</warning>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Respond to user"
        assert parsed.rationale == "Have all needed information"
        assert parsed.user_requested == "Entity A"
        assert parsed.what_found == "Entity B"
        assert parsed.exact_match is False
        assert parsed.discrepancies == "Different entity"
        assert parsed.proof_of_relation == "SIMILAR names only"
        assert parsed.confidence == "low"
        assert parsed.warning == "This is a different entity"

    def test_parse_malformed_xml_unclosed_tag(self) -> None:
        """Test parsing malformed XML with unclosed tag."""
        malformed = """<reasoning>
  <action>Search without closing
  <confidence>high</confidence>
</reasoning>"""

        # BeautifulSoup's html.parser should handle this gracefully
        parsed = parse_reasoning_message(malformed)

        # Should at least get the confidence
        assert parsed.confidence == "high"

    def test_parse_plain_text_no_xml(self) -> None:
        """Test parsing plain text without XML structure."""
        plain_text = "I'm going to search for the dataset. [++]"

        parsed = parse_reasoning_message(plain_text)

        # Should return raw text when no XML found
        assert parsed.raw_text == plain_text
        assert parsed.action is None
        assert parsed.confidence is None

    def test_parse_empty_string(self) -> None:
        """Test parsing empty string."""
        parsed = parse_reasoning_message("")

        assert parsed.raw_text == ""
        assert parsed.action is None

    def test_parse_xml_with_empty_tags(self) -> None:
        """Test parsing XML with empty tags."""
        xml_text = """<reasoning>
  <action></action>
  <confidence>medium</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # Empty tags should be treated as None
        assert parsed.action is None
        assert parsed.confidence == "medium"

    def test_parse_xml_with_whitespace(self) -> None:
        """Test parsing XML strips whitespace from tag content."""
        xml_text = """<reasoning>
  <action>  Search for dataset  </action>
  <confidence>  high  </confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Search for dataset"
        assert parsed.confidence == "high"

    def test_parse_xml_case_insensitive_tags(self) -> None:
        """Test that tag names are case-insensitive (HTML parser behavior)."""
        xml_text = """<reasoning>
  <Action>First action</Action>
  <action>Second action</action>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # html.parser normalizes tags to lowercase, so finds the first one
        # This is expected behavior for HTML parsing
        assert parsed.action == "First action"

    def test_parse_nested_reasoning_tags(self) -> None:
        """Test handling of nested reasoning tags (uses first one)."""
        xml_text = """<reasoning>
  <action>First action</action>
</reasoning>
<reasoning>
  <action>Second action</action>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # Should use the first reasoning tag found
        assert parsed.action == "First action"

    def test_parse_xml_without_reasoning_wrapper(self) -> None:
        """Test parsing XML without <reasoning> wrapper."""
        xml_text = """<action>Direct action</action>
<confidence>high</confidence>"""

        parsed = parse_reasoning_message(xml_text)

        # Should return raw text when no <reasoning> tag found
        assert parsed.raw_text == xml_text
        assert parsed.action is None

    def test_parse_xml_with_nested_tags(self) -> None:
        """Test parsing XML with nested tags (e.g., emphasis in action text)."""
        xml_text = """<reasoning>
  <action>Search for <em>important</em> dataset</action>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # Should extract all text content, not fail on nested tags
        assert parsed.action == "Search for important dataset"
        assert parsed.confidence == "high"

    def test_parse_xml_with_user_visible_text_outside(self) -> None:
        """Test parsing XML with user-visible text outside <reasoning> tags."""
        xml_text = """I'm going to search for the dataset now.

<reasoning>
  <action>Search</action>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Search"
        assert parsed.confidence == "high"
        assert parsed.user_visible_text == "I'm going to search for the dataset now."

    def test_user_visible_message_prefers_longer_text(self) -> None:
        """Test that longer user-visible text is preferred over action."""
        xml_text = """I'm going to search for the dataset OPERATION_DB.ANALYST_TASKS.table to find its downstream dependencies.

<reasoning>
  <action>Search</action>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)
        message = parsed.to_user_visible_message()

        # Should use the longer user-visible text, not "Search"
        assert (
            message
            == "I'm going to search for the dataset OPERATION_DB.ANALYST_TASKS.table to find its downstream dependencies."
        )

    def test_user_visible_message_uses_action_when_no_external_text(self) -> None:
        """Test that action is used when there's no text outside <reasoning>."""
        xml_text = """<reasoning>
  <action>Search for dataset</action>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)
        message = parsed.to_user_visible_message()

        assert message == "Search for dataset"

    def test_integration_parse_and_use(self) -> None:
        """Integration test: parse and use the result."""
        xml_text = """<reasoning>
  <action>Search for entity</action>
  <user_requested>db.schema.table</user_requested>
  <what_found>db2.schema2.table</what_found>
  <exact_match>false</exact_match>
  <discrepancies>Different database</discrepancies>
  <confidence>medium</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # Check methods work correctly
        assert parsed.has_entity_mismatch() is True

        message = parsed.to_user_visible_message()
        assert "Search for entity" in message
        assert "⚠️" in message
        assert "Different database" in message
        assert "[Confidence: medium]" in message

    def test_parse_xml_with_plan_fields(self) -> None:
        """Test parsing XML with plan-related fields."""
        xml_text = """<reasoning>
  <action>Search for dataset</action>
  <rationale>Need to find the entity</rationale>
  <plan_id>plan_abc123</plan_id>
  <plan_step>s0</plan_step>
  <step_status>in_progress</step_status>
  <confidence>high</confidence>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Search for dataset"
        assert parsed.rationale == "Need to find the entity"
        assert parsed.plan_id == "plan_abc123"
        assert parsed.plan_step == "s0"
        assert parsed.step_status == "in_progress"
        assert parsed.confidence == "high"

    def test_parse_xml_with_only_plan_id(self) -> None:
        """Test parsing XML with only plan_id (no step info)."""
        xml_text = """<reasoning>
  <action>Create plan</action>
  <plan_id>plan_xyz789</plan_id>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Create plan"
        assert parsed.plan_id == "plan_xyz789"
        assert parsed.plan_step is None
        assert parsed.step_status is None

    def test_parse_xml_with_complete_plan_info(self) -> None:
        """Test parsing XML with all plan fields populated."""
        xml_text = """<reasoning>
  <action>Get downstream lineage</action>
  <rationale>Need to find affected dashboards</rationale>
  <plan_id>plan_def456</plan_id>
  <plan_step>s2</plan_step>
  <step_status>completed</step_status>
  <confidence>high</confidence>
  <warning>Found 100+ downstream assets</warning>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.action == "Get downstream lineage"
        assert parsed.rationale == "Need to find affected dashboards"
        assert parsed.plan_id == "plan_def456"
        assert parsed.plan_step == "s2"
        assert parsed.step_status == "completed"
        assert parsed.confidence == "high"
        assert parsed.warning == "Found 100+ downstream assets"

    def test_parse_xml_plan_fields_with_whitespace(self) -> None:
        """Test that plan fields are trimmed of whitespace."""
        xml_text = """<reasoning>
  <action>Execute step</action>
  <plan_id>  plan_abc123  </plan_id>
  <plan_step>  s1  </plan_step>
  <step_status>  started  </step_status>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        assert parsed.plan_id == "plan_abc123"
        assert parsed.plan_step == "s1"
        assert parsed.step_status == "started"

    def test_parse_xml_empty_plan_fields(self) -> None:
        """Test that empty plan field tags are treated as None."""
        xml_text = """<reasoning>
  <action>Test action</action>
  <plan_id></plan_id>
  <plan_step></plan_step>
  <step_status></step_status>
</reasoning>"""

        parsed = parse_reasoning_message(xml_text)

        # Empty tags should be None, not empty strings
        assert parsed.plan_id is None
        assert parsed.plan_step is None
        assert parsed.step_status is None

    def test_parsed_reasoning_with_plan_fields_in_constructor(self) -> None:
        """Test creating ParsedReasoning with plan fields directly."""
        parsed = ParsedReasoning(
            action="Search",
            plan_id="plan_test123",
            plan_step="s0",
            step_status="in_progress",
            raw_text="<reasoning>test</reasoning>",
        )

        assert parsed.action == "Search"
        assert parsed.plan_id == "plan_test123"
        assert parsed.plan_step == "s0"
        assert parsed.step_status == "in_progress"


class TestCombineContexts:
    """Tests for the combine_contexts function."""

    def test_combine_both_contexts_provided(self) -> None:
        conversation_context = "User is editing an ingestion source"
        message_context = ChatContext(
            text="Current step: Configure Recipe. Source type: MySQL",
            entity_urns=["urn:li:dataSource:123"],
        )

        result = combine_contexts(conversation_context, message_context)

        assert result == (
            "User is editing an ingestion source\n\n"
            "Current Context: Current step: Configure Recipe. Source type: MySQL"
        )

    def test_combine_only_conversation_context(self) -> None:
        conversation_context = "User is viewing ingestion run details"
        message_context = None

        result = combine_contexts(conversation_context, message_context)

        assert result == "User is viewing ingestion run details"

    def test_combine_only_message_context(self) -> None:
        conversation_context = None
        message_context = ChatContext(
            text="Current step: Test Connection",
        )

        result = combine_contexts(conversation_context, message_context)

        assert result == "Current step: Test Connection"

    def test_combine_neither_context(self) -> None:
        conversation_context = None
        message_context = None

        result = combine_contexts(conversation_context, message_context)

        assert result is None

    def test_combine_empty_conversation_context(self) -> None:
        conversation_context = ""
        message_context = ChatContext(text="Current step: Review")

        result = combine_contexts(conversation_context, message_context)

        # Empty string is falsy, so only message context should be returned
        assert result == "Current step: Review"

    def test_combine_empty_message_context_text(self) -> None:
        conversation_context = "Base context"
        message_context = ChatContext(text="")

        result = combine_contexts(conversation_context, message_context)

        # Empty message context text means only conversation context
        assert result == "Base context"

    def test_combine_message_context_with_entity_urns(self) -> None:
        conversation_context = "Editing source"
        message_context = ChatContext(
            text="Step: Configure",
            entity_urns=[
                "urn:li:dataSource:123",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            ],
        )

        result = combine_contexts(conversation_context, message_context)

        # entity_urns don't affect the text combination
        assert result == "Editing source\n\nCurrent Context: Step: Configure"
