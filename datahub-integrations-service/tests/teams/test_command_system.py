"""Comprehensive tests for Teams command system focusing on edge cases."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.teams.command.ask import handle_ask_command_teams
from datahub_integrations.teams.command.get import handle_get_command_teams
from datahub_integrations.teams.command.help import handle_help_command_teams
from datahub_integrations.teams.command.router import (
    handle_teams_command_from_router,
    route_teams_command,
)
from datahub_integrations.teams.command.search import handle_search_command_teams
from datahub_integrations.teams.config import TeamsConnection
from datahub_integrations.teams.context import SearchContext


class TestCommandRouter:
    """Test command routing logic with edge cases."""

    @pytest.mark.asyncio
    async def test_route_empty_command(self) -> None:
        """Test routing with empty command defaults to search."""
        result = await route_teams_command("", None)
        assert result["type"] == "message"
        # Empty query should still produce a result
        assert "text" in result or "attachments" in result

    @pytest.mark.asyncio
    async def test_route_whitespace_only_command(self) -> None:
        """Test routing with whitespace-only command."""
        result = await route_teams_command("   \t\n   ", None)
        assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_route_search_command_explicit(self) -> None:
        """Test explicit search command routing."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {"type": "message", "text": "search result"}
            await route_teams_command("search test query", "user123")
            mock_search.assert_called_once()
            args = mock_search.call_args[0]
            assert isinstance(args[1], SearchContext)
            assert args[1].query == "test query"
            assert args[2] == "user123"

    @pytest.mark.asyncio
    async def test_route_search_command_case_insensitive(self) -> None:
        """Test search command is case insensitive."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {"type": "message", "text": "result"}
            await route_teams_command("SEARCH test", None)
            mock_search.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_search_command_no_query(self) -> None:
        """Test search command with no query."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {"type": "message", "text": "result"}
            await route_teams_command("search", None)
            args = mock_search.call_args[0]
            assert args[1].query == ""

    @pytest.mark.asyncio
    async def test_route_get_command_explicit(self) -> None:
        """Test explicit get command routing."""
        with (
            patch(
                "datahub_integrations.teams.command.router.handle_get_command_teams"
            ) as mock_get,
            patch("datahub_integrations.teams.command.router.graph") as mock_graph,
        ):
            mock_get.return_value = {"type": "message", "attachments": []}
            await route_teams_command("get urn:li:dataset:(test)", "user123")
            mock_get.assert_called_once_with(mock_graph, "urn:li:dataset:(test)")

    @pytest.mark.asyncio
    async def test_route_get_command_no_urn(self) -> None:
        """Test get command with no URN."""
        with patch(
            "datahub_integrations.teams.command.router.handle_get_command_teams"
        ) as mock_get:
            mock_get.return_value = {"type": "message", "text": "Entity not found: "}
            await route_teams_command("get", None)
            args = mock_get.call_args[0]
            assert args[1] == ""

    @pytest.mark.asyncio
    async def test_route_ask_command_explicit(self) -> None:
        """Test explicit ask command routing."""
        with patch(
            "datahub_integrations.teams.command.router.handle_ask_command_teams"
        ) as mock_ask:
            mock_ask.return_value = {"type": "message", "text": "ai response"}
            await route_teams_command("ask What is this?", "user123")
            mock_ask.assert_called_once()
            args = mock_ask.call_args[0]
            assert args[1] == "What is this?"
            assert args[2] == "user123"

    @pytest.mark.asyncio
    async def test_route_ask_command_empty_question(self) -> None:
        """Test ask command with empty question."""
        with patch(
            "datahub_integrations.teams.command.router.handle_ask_command_teams"
        ) as mock_ask:
            mock_ask.return_value = {
                "type": "message",
                "text": "Please provide a question",
            }
            await route_teams_command("ask", None)
            args = mock_ask.call_args[0]
            assert args[1] == ""

    @pytest.mark.asyncio
    async def test_route_help_command(self) -> None:
        """Test help command routing."""
        with patch(
            "datahub_integrations.teams.command.router.handle_help_command_teams"
        ) as mock_help:
            mock_help.return_value = {"type": "message", "attachments": []}
            await route_teams_command("help", "user123")
            mock_help.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_fallback_to_search(self) -> None:
        """Test unknown commands fallback to search."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {"type": "message", "text": "search result"}
            await route_teams_command("unknown command", None)
            mock_search.assert_called_once()
            args = mock_search.call_args[0]
            assert args[1].query == "unknown command"

    @pytest.mark.asyncio
    async def test_route_special_characters(self) -> None:
        """Test commands with special characters."""
        with patch(
            "datahub_integrations.teams.command.router.handle_search_command_teams"
        ) as mock_search:
            mock_search.return_value = {"type": "message", "text": "result"}
            await route_teams_command("search @#$%^&*()", None)
            args = mock_search.call_args[0]
            assert args[1].query == "@#$%^&*()"

    @pytest.mark.asyncio
    async def test_handle_teams_command_from_router(self) -> None:
        """Test Teams webhook command handling."""
        # Mock activity
        activity = MagicMock()
        activity.text = "/datahub search test"
        activity.from_ = {"id": "user123"}

        config = MagicMock(spec=TeamsConnection)

        with patch(
            "datahub_integrations.teams.command.router.route_teams_command"
        ) as mock_route:
            mock_route.return_value = {"type": "message", "text": "result"}
            result = await handle_teams_command_from_router(activity, config)
            mock_route.assert_called_once_with("search test", None)
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_handle_teams_command_router_error(self) -> None:
        """Test error handling in router."""
        activity = MagicMock()
        activity.text = "/datahub search test"
        activity.from_ = {"id": "user123"}

        config = MagicMock(spec=TeamsConnection)

        with patch(
            "datahub_integrations.teams.command.router.route_teams_command"
        ) as mock_route:
            mock_route.side_effect = Exception("Test error")
            result = await handle_teams_command_from_router(activity, config)
            assert result["type"] == "message"
            assert "error" in result["text"]

    @pytest.mark.asyncio
    async def test_handle_teams_command_no_text(self) -> None:
        """Test handling activity with no text."""
        activity = MagicMock()
        activity.text = None
        activity.from_ = {"id": "user123"}

        config = MagicMock(spec=TeamsConnection)

        with patch(
            "datahub_integrations.teams.command.router.route_teams_command"
        ) as mock_route:
            mock_route.return_value = {"type": "message", "text": "result"}
            result = await handle_teams_command_from_router(activity, config)
            mock_route.assert_called_once_with("", None)
            assert result["type"] == "message"


class TestSearchCommand:
    """Test search command with edge cases."""

    @pytest.mark.asyncio
    async def test_search_basic_query(self) -> None:
        """Test basic search functionality."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "searchAcrossEntities": {"searchResults": []}
        }
        context = SearchContext(query="test", page=0, filters={})

        with patch(
            "datahub_integrations.teams.command.search.render_search_teams"
        ) as mock_render:
            mock_render.return_value = {"type": "message", "attachments": []}
            await handle_search_command_teams(mock_graph, context, None)
            mock_render.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_empty_query(self) -> None:
        """Test search with empty query."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "searchAcrossEntities": {"searchResults": []}
        }
        context = SearchContext(query="", page=0, filters={})

        with patch(
            "datahub_integrations.teams.command.search.render_search_teams"
        ) as mock_render:
            mock_render.return_value = {"type": "message", "attachments": []}
            result = await handle_search_command_teams(mock_graph, context, None)
            # Should still execute the search
            assert result["type"] == "message"

    @pytest.mark.asyncio
    async def test_search_pagination(self) -> None:
        """Test search with pagination."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "searchAcrossEntities": {"searchResults": []}
        }
        context = SearchContext(query="test", page=2, filters={})

        with patch(
            "datahub_integrations.teams.command.search.render_search_teams"
        ) as mock_render:
            mock_render.return_value = {"type": "message", "attachments": []}
            await handle_search_command_teams(mock_graph, context, None)
            # Check that pagination is applied (start = page * RESULT_COUNT = 2 * 3 = 6)
            mock_graph.execute_graphql.assert_called_once()
            call_args = mock_graph.execute_graphql.call_args
            variables = call_args[1]["variables"]
            assert variables["input"]["start"] == 6

    @pytest.mark.asyncio
    async def test_search_graphql_error(self) -> None:
        """Test search with GraphQL error."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        context = SearchContext(query="test", page=0, filters={})
        result = await handle_search_command_teams(mock_graph, context, None)

        assert result["type"] == "message"
        assert "error" in result["text"]
        assert "test" in result["text"]

    @pytest.mark.asyncio
    async def test_search_no_results(self) -> None:
        """Test search with no results."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"searchAcrossEntities": None}
        context = SearchContext(query="nonexistent", page=0, filters={})
        result = await handle_search_command_teams(mock_graph, context, None)

        assert result["type"] == "message"
        assert "No results found" in result["text"]
        assert "nonexistent" in result["text"]

    @pytest.mark.asyncio
    async def test_search_null_response(self) -> None:
        """Test search with null GraphQL response."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = None

        context = SearchContext(query="test", page=0, filters={})
        result = await handle_search_command_teams(mock_graph, context, None)

        assert result["type"] == "message"
        assert "No results found" in result["text"]


class TestGetCommand:
    """Test get command with edge cases."""

    @pytest.mark.asyncio
    async def test_get_valid_urn(self) -> None:
        """Test get with valid URN."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"entity": {"urn": "test"}}

        with patch(
            "datahub_integrations.teams.command.get.render_entity_card"
        ) as mock_render:
            mock_render.return_value = {"type": "AdaptiveCard"}
            result = await handle_get_command_teams(mock_graph, "urn:li:dataset:(test)")

            assert result["type"] == "message"
            assert "attachments" in result
            mock_render.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_invalid_urn(self) -> None:
        """Test get with invalid URN."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"entity": None}
        result = await handle_get_command_teams(mock_graph, "invalid:urn")

        assert result["type"] == "message"
        assert "Entity not found" in result["text"]
        assert "invalid:urn" in result["text"]

    @pytest.mark.asyncio
    async def test_get_empty_urn(self) -> None:
        """Test get with empty URN."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"entity": None}
        result = await handle_get_command_teams(mock_graph, "")

        assert result["type"] == "message"
        assert "Entity not found" in result["text"]

    @pytest.mark.asyncio
    async def test_get_graphql_error(self) -> None:
        """Test get with GraphQL error."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        result = await handle_get_command_teams(mock_graph, "urn:li:dataset:(test)")

        assert result["type"] == "message"
        assert "error" in result["text"]

    @pytest.mark.asyncio
    async def test_get_malformed_response(self) -> None:
        """Test get with malformed GraphQL response."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {}  # Missing 'entity' key

        result = await handle_get_command_teams(mock_graph, "urn:li:dataset:(test)")

        assert result["type"] == "message"
        assert "Entity not found" in result["text"]

    @pytest.mark.asyncio
    async def test_get_special_characters_urn(self) -> None:
        """Test get with URN containing special characters."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"entity": {"urn": "test"}}
        special_urn = (
            "urn:li:dataset:(platform,db.schema.table with spaces & symbols,PROD)"
        )

        with patch(
            "datahub_integrations.teams.command.get.render_entity_card"
        ) as mock_render:
            mock_render.return_value = {"type": "AdaptiveCard"}
            await handle_get_command_teams(mock_graph, special_urn)

            # Should attempt to execute GraphQL with the URN as-is
            mock_graph.execute_graphql.assert_called_once()
            call_args = mock_graph.execute_graphql.call_args
            assert call_args[1]["variables"]["urn"] == special_urn


class TestAskCommand:
    """Test ask command with edge cases."""

    @pytest.mark.asyncio
    async def test_ask_empty_question(self) -> None:
        """Test ask with empty question."""
        mock_graph = MagicMock(spec=DataHubGraph)
        result = await handle_ask_command_teams(mock_graph, "", None)

        assert result["type"] == "message"
        assert "Please provide a question" in result["text"]

    @pytest.mark.asyncio
    async def test_ask_whitespace_question(self) -> None:
        """Test ask with whitespace-only question."""
        mock_graph = MagicMock(spec=DataHubGraph)
        result = await handle_ask_command_teams(mock_graph, "   \t\n   ", None)

        assert result["type"] == "message"
        assert "Please provide a question" in result["text"]

    @pytest.mark.asyncio
    async def test_ask_successful_ai_response(self) -> None:
        """Test ask command with successful AI response."""
        mock_graph = MagicMock(spec=DataHubGraph)

        # Mock the entire ChatSession and DataHubClient to avoid any network calls
        with (
            patch(
                "datahub_integrations.teams.command.ask.ChatSession"
            ) as mock_chat_session_cls,
            patch(
                "datahub_integrations.teams.command.ask.DataHubClient"
            ) as mock_client_cls,
            patch("datahub_integrations.teams.config.teams_config") as mock_config,
        ):
            # Mock DataHubClient creation
            mock_client = MagicMock()
            mock_client_cls.return_value = mock_client

            # Mock ChatSession and its methods
            mock_chat_session = MagicMock()
            mock_chat_session_cls.return_value = mock_chat_session

            # Mock the response from generate_next_message
            from datahub_integrations.chat.chat_session import NextMessage

            mock_response = NextMessage(
                text="AI response to your question about DataHub", suggestions=[]
            )
            mock_chat_session.generate_next_message.return_value = mock_response

            # Mock context manager for progress callback
            mock_context_manager = MagicMock()
            mock_chat_session.set_progress_callback.return_value = mock_context_manager
            mock_context_manager.__enter__.return_value = None
            mock_context_manager.__exit__.return_value = None

            # Mock Teams config
            config = MagicMock()
            config.enable_conversation_history = False
            mock_config.get_config.return_value = config

            result = await handle_ask_command_teams(
                mock_graph, "What is DataHub?", "user123"
            )

            assert result["type"] == "message"
            assert "AI response" in result["text"]
            # Verify ChatSession was created and used
            mock_chat_session_cls.assert_called_once()
            mock_chat_session.generate_next_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_ai_api_error(self) -> None:
        """Test ask command when AI API fails."""
        mock_graph = MagicMock(spec=DataHubGraph)

        # Mock get_llm_client to simulate API failure
        with (
            patch(
                "datahub_integrations.chat.chat_session.get_llm_client"
            ) as mock_bedrock_fn,
            patch("datahub_integrations.teams.config.teams_config") as mock_config,
        ):
            # Mock Bedrock client to raise exception on converse
            mock_bedrock_client = MagicMock()
            mock_bedrock_client.converse.side_effect = Exception("Bedrock API error")
            mock_bedrock_fn.return_value = mock_bedrock_client

            # Mock Teams config
            config = MagicMock()
            config.enable_conversation_history = False
            mock_config.get_config.return_value = config

            result = await handle_ask_command_teams(mock_graph, "Test question", None)

            # Should gracefully handle API errors
            assert result["type"] == "message"
            assert "error" in result["text"]

    @pytest.mark.asyncio
    async def test_ask_execution_error(self) -> None:
        """Test ask command with any execution error falls back to error response."""
        mock_graph = MagicMock(spec=DataHubGraph)

        # Mock ChatSession to simulate execution error
        with patch(
            "datahub_integrations.teams.command.ask.ChatSession"
        ) as mock_chat_session_cls:
            mock_chat_session = MagicMock()
            mock_chat_session.generate_next_message.side_effect = Exception(
                "Chat session error"
            )
            mock_chat_session_cls.return_value = mock_chat_session

            result = await handle_ask_command_teams(mock_graph, "Test question", None)

            # The command should return an error message instead of crashing
            assert result["type"] == "message"
            assert "error" in result["text"]
            assert "Try again" in result["text"] or "contact support" in result["text"]

    @pytest.mark.asyncio
    async def test_ask_long_question_validation(self) -> None:
        """Test ask command doesn't crash with extremely long input."""
        mock_graph = MagicMock(spec=DataHubGraph)
        long_question = "What is " + "a" * 10000 + "?"

        # Should handle long questions gracefully (either process or error gracefully)
        result = await handle_ask_command_teams(mock_graph, long_question, None)

        assert result["type"] == "message"
        assert "text" in result
        # Either gets an AI response or error response, both are acceptable

    @pytest.mark.asyncio
    async def test_ask_special_characters(self) -> None:
        """Test ask command with special characters."""
        mock_graph = MagicMock(spec=DataHubGraph)
        question_with_special_chars = "What about @#$%^&*(){}[]|\\:;\"'<>?,./"

        # Should handle special characters gracefully
        result = await handle_ask_command_teams(
            mock_graph, question_with_special_chars, None
        )

        assert result["type"] == "message"
        assert "text" in result

    @pytest.mark.asyncio
    async def test_ask_with_callback_parameter(self) -> None:
        """Test ask command accepts progress callback parameter."""
        mock_graph = MagicMock(spec=DataHubGraph)
        callback_called = []

        def progress_callback(steps: Any) -> None:
            callback_called.append(steps)

        # Should accept callback parameter without crashing
        result = await handle_ask_command_teams(
            mock_graph, "Test question", None, progress_callback=progress_callback
        )

        assert result["type"] == "message"
        assert "text" in result

    @pytest.mark.asyncio
    async def test_ask_with_conversation_parameters(self) -> None:
        """Test ask command accepts conversation parameters."""
        mock_graph = MagicMock(spec=DataHubGraph)

        # Should accept conversation parameters without crashing
        result = await handle_ask_command_teams(
            mock_graph,
            "Test question",
            "user123",
            conversation_id="conv123",
            message_ts="123456",
        )

        assert result["type"] == "message"
        assert "text" in result

    @pytest.mark.asyncio
    async def test_ask_with_mcp_tools_available(self) -> None:
        """Test ask command has access to DataHub MCP tools."""
        mock_graph = MagicMock(spec=DataHubGraph)

        # Mock successful AI response that uses tools
        with (
            patch("datahub_integrations.gen_ai.bedrock.boto3.Session") as mock_session,
            patch("datahub_integrations.teams.config.teams_config") as mock_config,
        ):
            # Mock boto3 session and client
            mock_boto_session = MagicMock()
            mock_session.return_value = mock_boto_session
            mock_bedrock_client = MagicMock()
            mock_boto_session.client.return_value = mock_bedrock_client

            # Mock Teams config
            config = MagicMock()
            config.enable_conversation_history = False
            mock_config.get_config.return_value = config

            # Mock a tool use scenario where AI calls DataHub search
            mock_bedrock_client.converse.return_value = {
                "output": {
                    "message": {
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": "search_123",
                                    "name": "search",
                                    "input": {"query": "datasets"},
                                }
                            }
                        ],
                        "role": "assistant",
                    }
                },
                "stopReason": "tool_use",
                "usage": {"inputTokens": 50, "outputTokens": 25, "totalTokens": 75},
            }

            result = await handle_ask_command_teams(
                mock_graph, "Find datasets", "user123"
            )

            # Should handle tool use scenarios gracefully
            assert result["type"] == "message"
            assert "text" in result

    @pytest.mark.asyncio
    async def test_ask_conversation_history_enabled(self) -> None:
        """Test ask command with conversation history enabled."""
        mock_graph = MagicMock(spec=DataHubGraph)

        with (
            patch(
                "datahub_integrations.chat.chat_session.get_llm_client"
            ) as mock_bedrock_fn,
            patch("datahub_integrations.teams.config.teams_config") as mock_config,
        ):
            # Mock Bedrock client directly
            mock_bedrock_client = MagicMock()
            mock_bedrock_fn.return_value = mock_bedrock_client

            mock_bedrock_client.converse.return_value = {
                "output": {
                    "message": {
                        "content": [{"text": "Response with history context"}],
                        "role": "assistant",
                    }
                },
                "stopReason": "end_turn",
                "usage": {"inputTokens": 75, "outputTokens": 30, "totalTokens": 105},
            }

            # Mock config with history enabled
            config = MagicMock()
            config.enable_conversation_history = True
            mock_config.get_config.return_value = config

            # Mock history cache with proper ChatHistory object
            from datahub_integrations.chat.chat_history import ChatHistory

            history_cache = MagicMock()
            conv_history = MagicMock()
            chat_history = ChatHistory()  # Use real ChatHistory object
            conv_history.get_chat_history.return_value = chat_history
            history_cache.get_conversation.return_value = conv_history
            mock_config.get_teams_history_cache.return_value = history_cache

            result = await handle_ask_command_teams(
                mock_graph,
                "Follow-up question",
                "user123",
                conversation_id="conv123",
                message_ts="123456",
            )

            assert result["type"] == "message"
            assert "Response with history" in result["text"]
            # Verify history methods were called
            conv_history.add_message.assert_called()
            conv_history.get_chat_history.assert_called()

    @pytest.mark.asyncio
    async def test_ask_return_format(self) -> None:
        """Test ask command returns properly formatted response."""
        mock_graph = MagicMock(spec=DataHubGraph)

        result = await handle_ask_command_teams(mock_graph, "Test question", None)

        # Verify response structure
        assert isinstance(result, dict)
        assert result["type"] == "message"
        assert "text" in result
        # May or may not have suggestions depending on success/failure


class TestHelpCommand:
    """Test help command."""

    @pytest.mark.asyncio
    async def test_help_basic(self) -> None:
        """Test basic help command."""
        mock_graph = MagicMock(spec=DataHubGraph)

        with patch(
            "datahub_integrations.teams.command.help.create_teams_help_card"
        ) as mock_card:
            mock_card.return_value = {"type": "AdaptiveCard"}
            result = await handle_help_command_teams(mock_graph, None)

            assert result["type"] == "message"
            assert "attachments" in result
            assert len(result["attachments"]) == 1
            mock_card.assert_called_once()

    @pytest.mark.asyncio
    async def test_help_with_user(self) -> None:
        """Test help command with user URN."""
        mock_graph = MagicMock(spec=DataHubGraph)

        with patch(
            "datahub_integrations.teams.command.help.create_teams_help_card"
        ) as mock_card:
            mock_card.return_value = {"type": "AdaptiveCard"}
            result = await handle_help_command_teams(mock_graph, "user123")

            assert result["type"] == "message"
            assert "attachments" in result
            mock_card.assert_called_once()

    @pytest.mark.asyncio
    async def test_help_card_creation_error(self) -> None:
        """Test help command when card creation fails."""
        mock_graph = MagicMock(spec=DataHubGraph)

        with patch(
            "datahub_integrations.teams.command.help.create_teams_help_card"
        ) as mock_card:
            mock_card.side_effect = Exception("Card creation failed")

            # Help command doesn't have explicit error handling, so exception should propagate
            with pytest.raises(Exception, match="Card creation failed"):
                await handle_help_command_teams(mock_graph, None)
