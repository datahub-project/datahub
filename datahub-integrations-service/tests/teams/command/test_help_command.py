from unittest.mock import MagicMock

import pytest

from datahub_integrations.teams.command.help import handle_help_command_teams


class TestHelpCommand:
    """Test cases for the Teams help command handler."""

    @pytest.mark.asyncio
    async def test_help_command_returns_message_with_attachments(self) -> None:
        """Test that help command returns a valid message with adaptive card attachment."""
        mock_graph = MagicMock()

        result = await handle_help_command_teams(mock_graph, "urn:li:user:test")

        assert result["type"] == "message"
        assert "attachments" in result
        assert isinstance(result["attachments"], list)
        assert len(result["attachments"]) > 0

    @pytest.mark.asyncio
    async def test_help_command_contains_all_commands(self) -> None:
        """Test that help includes all available commands."""
        mock_graph = MagicMock()

        result = await handle_help_command_teams(mock_graph, "urn:li:user:test")

        help_text = str(result)

        # Check for all major commands
        assert "search" in help_text.lower()
        assert "get" in help_text.lower()
        assert "help" in help_text.lower()
        assert "ask" in help_text.lower()

    @pytest.mark.asyncio
    async def test_help_command_with_none_user(self) -> None:
        """Test that help command works with None user_urn."""
        mock_graph = MagicMock()

        result = await handle_help_command_teams(mock_graph, None)

        assert result["type"] == "message"
        assert "attachments" in result

    @pytest.mark.asyncio
    async def test_help_command_includes_examples(self) -> None:
        """Test that help command includes usage examples."""
        mock_graph = MagicMock()

        result = await handle_help_command_teams(mock_graph, "urn:li:user:test")

        help_text = str(result)

        # Check for example usage patterns
        assert "/datahub" in help_text
        assert "tip" in help_text.lower() or "search" in help_text.lower()

    @pytest.mark.asyncio
    async def test_help_command_formatting(self) -> None:
        """Test that help command has proper formatting elements."""
        mock_graph = MagicMock()

        result = await handle_help_command_teams(mock_graph, "urn:li:user:test")

        # Check that it returns a message with attachments
        assert result["type"] == "message"
        assert "attachments" in result

        # The attachment should be an adaptive card
        attachment = result["attachments"][0]
        assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
        assert "content" in attachment

        # The content should have adaptive card structure
        card_content = attachment["content"]
        assert card_content["type"] == "AdaptiveCard"
        assert "$schema" in card_content
        assert "version" in card_content
        assert "body" in card_content

        # Should have text blocks or other content
        body_types = [block.get("type") for block in card_content["body"]]
        assert "TextBlock" in body_types or "Container" in body_types
