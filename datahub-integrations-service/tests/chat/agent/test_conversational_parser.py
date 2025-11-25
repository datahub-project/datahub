"""Unit tests for conversational parser implementations."""

from unittest.mock import Mock, patch

from datahub_integrations.chat.agent.conversational_parser import (
    PlainTextParser,
    XmlReasoningParser,
)


class TestPlainTextParser:
    """Test PlainTextParser implementation."""

    def test_parse_simple_text(self) -> None:
        """Test parsing simple text message."""
        parser = PlainTextParser()
        message = "This is a simple message"

        result = parser.parse_message(message)

        assert result == "This is a simple message"

    def test_parse_strips_whitespace(self) -> None:
        """Test that parser strips leading/trailing whitespace."""
        parser = PlainTextParser()
        message = "  Message with whitespace  \n\t"

        result = parser.parse_message(message)

        assert result == "Message with whitespace"

    def test_parse_multiline_text(self) -> None:
        """Test parsing multiline text."""
        parser = PlainTextParser()
        message = """Line 1
Line 2
Line 3"""

        result = parser.parse_message(message)

        # Should preserve internal structure but strip outer whitespace
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result

    def test_parse_empty_string(self) -> None:
        """Test parsing empty string."""
        parser = PlainTextParser()

        result = parser.parse_message("")

        assert result == ""

    def test_parse_ignores_agent_parameter(self) -> None:
        """Test that agent parameter is not required."""
        parser = PlainTextParser()
        mock_agent = Mock()

        result = parser.parse_message("Test message", agent=mock_agent)

        assert result == "Test message"


class TestXmlReasoningParser:
    """Test XmlReasoningParser implementation."""

    def test_parse_basic_reasoning_message(self) -> None:
        """Test parsing a basic XML reasoning message."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Search for dataset</action>
  <rationale>Need to find the requested dataset</rationale>
</reasoning>"""

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            # Mock the parsed result
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = (
                "Searching for the dataset"
            )
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            assert result == "Searching for the dataset"
            mock_parse.assert_called_once_with(message)
            mock_parsed.to_user_visible_message.assert_called_once_with(session=None)

    def test_parse_with_agent_and_plan_cache(self) -> None:
        """Test parsing with agent that has plan_cache attribute."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Execute step 1</action>
  <plan_id>plan_123</plan_id>
  <plan_step>s0</plan_step>
</reasoning>"""

        # Create mock agent with plan_cache
        mock_agent = Mock()
        mock_agent.plan_cache = {
            "plan_123": {"steps": [{"id": "s0", "status": "completed"}]}
        }

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = (
                "[1/3 ✓] Executing step 1"
            )
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=mock_agent)

            # Should pass agent to to_user_visible_message for plan formatting
            mock_parsed.to_user_visible_message.assert_called_once_with(
                session=mock_agent
            )
            assert "[1/3 ✓]" in result or result == "[1/3 ✓] Executing step 1"

    def test_parse_without_agent(self) -> None:
        """Test parsing without agent (no plan formatting)."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Check entity</action>
</reasoning>"""

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = "Checking entity"
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            # Should pass None for session (no plan formatting)
            mock_parsed.to_user_visible_message.assert_called_once_with(session=None)
            assert result == "Checking entity"

    def test_parse_with_agent_without_plan_cache(self) -> None:
        """Test parsing with agent that doesn't have plan_cache."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Search entities</action>
</reasoning>"""

        # Create agent without plan_cache attribute
        mock_agent = Mock(spec=[])  # spec=[] means no attributes

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = "Searching entities"
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=mock_agent)

            # Should pass None since agent doesn't have plan_cache
            mock_parsed.to_user_visible_message.assert_called_once_with(session=None)
            assert result == "Searching entities"

    def test_parse_complex_reasoning_with_multiple_fields(self) -> None:
        """Test parsing reasoning message with multiple fields."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Compare schemas</action>
  <rationale>User requested schema comparison</rationale>
  <user_requested>prod.users</user_requested>
  <what_found>prod.user_accounts</what_found>
  <exact_match>false</exact_match>
  <confidence>medium</confidence>
  <warning>Found similar but not exact match</warning>
</reasoning>"""

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = (
                "Comparing schemas (note: found similar match)"
            )
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            assert "Comparing schemas" in result
            mock_parse.assert_called_once_with(message)

    def test_parse_malformed_xml_handled_gracefully(self) -> None:
        """Test that malformed XML is handled by parse_reasoning_message."""
        parser = XmlReasoningParser()
        message = "<reasoning>Missing closing tag"

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            # Simulate parse_reasoning_message handling malformed XML
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = "Parsing error"
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            # Should still return something (parse_reasoning_message handles errors)
            assert isinstance(result, str)
            mock_parse.assert_called_once_with(message)

    def test_parse_empty_reasoning_message(self) -> None:
        """Test parsing empty reasoning message."""
        parser = XmlReasoningParser()
        message = "<reasoning></reasoning>"

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = ""
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            assert isinstance(result, str)


class TestConversationalParserProtocol:
    """Test that both parsers adhere to the ConversationalParser protocol."""

    def test_plain_text_parser_has_parse_message(self) -> None:
        """Test PlainTextParser has parse_message method."""
        parser = PlainTextParser()
        assert hasattr(parser, "parse_message")
        assert callable(parser.parse_message)

    def test_xml_reasoning_parser_has_parse_message(self) -> None:
        """Test XmlReasoningParser has parse_message method."""
        parser = XmlReasoningParser()
        assert hasattr(parser, "parse_message")
        assert callable(parser.parse_message)

    def test_parsers_accept_same_signature(self) -> None:
        """Test both parsers accept the same method signature."""
        plain_parser = PlainTextParser()
        xml_parser = XmlReasoningParser()

        message = "Test message"
        mock_agent = Mock()

        # Both should accept message and optional agent parameter
        plain_result = plain_parser.parse_message(message, agent=mock_agent)
        assert isinstance(plain_result, str)

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = "Test"
            mock_parse.return_value = mock_parsed

            xml_result = xml_parser.parse_message(message, agent=mock_agent)
            assert isinstance(xml_result, str)


class TestParserEdgeCases:
    """Test edge cases and error handling for parsers."""

    def test_plain_text_parser_with_special_characters(self) -> None:
        """Test PlainTextParser handles special characters."""
        parser = PlainTextParser()
        message = "Special chars: <>&\"'`@#$%^&*()"

        result = parser.parse_message(message)

        assert result == message.strip()

    def test_plain_text_parser_with_unicode(self) -> None:
        """Test PlainTextParser handles unicode characters."""
        parser = PlainTextParser()
        message = "Unicode: 你好 🚀 café"

        result = parser.parse_message(message)

        assert "你好" in result
        assert "🚀" in result
        assert "café" in result

    def test_xml_parser_with_nested_tags(self) -> None:
        """Test XmlReasoningParser with nested XML tags."""
        parser = XmlReasoningParser()
        message = """<reasoning>
  <action>Process <strong>important</strong> entity</action>
</reasoning>"""

        with patch(
            "datahub_integrations.chat.utils.parse_reasoning_message"
        ) as mock_parse:
            mock_parsed = Mock()
            mock_parsed.to_user_visible_message.return_value = "Processing entity"
            mock_parse.return_value = mock_parsed

            result = parser.parse_message(message, agent=None)

            assert isinstance(result, str)
            mock_parse.assert_called_once()
