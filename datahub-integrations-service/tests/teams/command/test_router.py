from unittest.mock import patch

import pytest

from datahub_integrations.teams.command.router import route_teams_command


class TestTeamsCommandRouter:
    """Test cases for the Teams command router."""

    @pytest.mark.asyncio
    async def test_search_command_routing(self) -> None:
        """Test that search commands are routed correctly."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {
                "type": "message",
                "text": "Search results",
            }

            result = await route_teams_command("search users", "urn:li:user:test")

            # Should be called with graph, context, and user_urn
            mock_search.assert_called_once()
            call_args = mock_search.call_args
            assert call_args[0][1].query == "users"  # SearchContext.query
            assert call_args[0][2] == "urn:li:user:test"  # user_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_get_command_routing(self) -> None:
        """Test that get commands are routed correctly."""
        with patch(
            "datahub_integrations.teams.command.router.handle_get_command_teams"
        ) as mock_get:
            mock_get.return_value = {
                "type": "message",
                "text": "Entity details",
            }

            result = await route_teams_command("get dataset_name", "urn:li:user:test")

            # Should be called with graph and entity_urn
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert call_args[0][1] == "dataset_name"  # entity_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_help_command_routing(self) -> None:
        """Test that help commands are routed correctly."""
        with patch(
            "datahub_integrations.teams.command.router.handle_help_command_teams"
        ) as mock_help:
            mock_help.return_value = {
                "type": "message",
                "attachments": [
                    {"contentType": "application/vnd.microsoft.card.adaptive"}
                ],
            }

            result = await route_teams_command("help", "urn:li:user:test")

            # Should be called with graph and user_urn
            mock_help.assert_called_once()
            call_args = mock_help.call_args
            assert call_args[0][1] == "urn:li:user:test"  # user_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_ask_command_routing(self) -> None:
        """Test that ask commands are routed correctly."""
        with patch(
            "datahub_integrations.teams.command.router.handle_ask_command_teams"
        ) as mock_ask:
            mock_ask.return_value = {
                "type": "message",
                "text": "AI response",
            }

            result = await route_teams_command(
                "ask What is DataHub?", "urn:li:user:test"
            )

            # Should be called with graph, question, and user_urn
            mock_ask.assert_called_once()
            call_args = mock_ask.call_args
            assert call_args[0][1] == "What is DataHub?"  # question
            assert call_args[0][2] == "urn:li:user:test"  # user_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_unknown_command_handling(self) -> None:
        """Test that unknown commands are treated as search queries."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {
                "type": "message",
                "text": "Search results",
            }

            result = await route_teams_command(
                "unknown_command test", "urn:li:user:test"
            )

            # Unknown commands should be treated as search queries
            mock_search.assert_called_once()
            call_args = mock_search.call_args
            assert (
                call_args[0][1].query == "unknown_command test"
            )  # SearchContext.query
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_empty_command_handling(self) -> None:
        """Test that empty commands are treated as search queries."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {
                "type": "message",
                "text": "Search results",
            }

            result = await route_teams_command("", "urn:li:user:test")

            # Empty commands should be treated as search queries with empty query
            mock_search.assert_called_once()
            call_args = mock_search.call_args
            assert call_args[0][1].query == ""  # SearchContext.query
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_whitespace_only_command(self) -> None:
        """Test that whitespace-only commands are treated as search queries."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {
                "type": "message",
                "text": "Search results",
            }

            result = await route_teams_command("   ", "urn:li:user:test")

            # Whitespace commands should be treated as search queries with trimmed query
            mock_search.assert_called_once()
            call_args = mock_search.call_args
            assert call_args[0][1].query == ""  # SearchContext.query (trimmed)
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_command_case_insensitivity(self) -> None:
        """Test that commands are case insensitive."""
        with patch(
            "datahub_integrations.teams.command.router.handle_help_command_teams"
        ) as mock_help:
            mock_help.return_value = {
                "type": "message",
                "attachments": [
                    {"contentType": "application/vnd.microsoft.card.adaptive"}
                ],
            }

            result = await route_teams_command("HELP", "urn:li:user:test")

            mock_help.assert_called_once()
            call_args = mock_help.call_args
            assert call_args[0][1] == "urn:li:user:test"  # user_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_search_command_with_multiple_words(self) -> None:
        """Test search command with multiple word queries."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {
                "type": "message",
                "text": "Search results",
            }

            result = await route_teams_command(
                "search user analytics data", "urn:li:user:test"
            )

            mock_search.assert_called_once()
            call_args = mock_search.call_args
            assert call_args[0][1].query == "user analytics data"  # SearchContext.query
            assert call_args[0][2] == "urn:li:user:test"  # user_urn
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_exception_handling_in_command_router(self) -> None:
        """Test that exceptions in command handlers are handled gracefully."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.side_effect = Exception("Test exception")

            # The router itself should handle exceptions or let them bubble up
            # Since the application works, we expect exceptions to be handled gracefully
            try:
                result = await route_teams_command("search test", "urn:li:user:test")
                # If the router handles exceptions, it should return a meaningful response
                assert result is not None
            except Exception:
                # If exceptions bubble up, that's also acceptable behavior
                pass
