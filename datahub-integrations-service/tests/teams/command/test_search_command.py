from unittest.mock import MagicMock

import pytest

from datahub_integrations.teams.command.search import handle_search_command_teams
from datahub_integrations.teams.context import SearchContext


class TestSearchCommand:
    """Test cases for the Teams search command handler."""

    @pytest.mark.asyncio
    async def test_search_command_success(self) -> None:
        """Test successful search command execution."""
        # Mock GraphQL response
        mock_search_response = {
            "searchAcrossEntities": {
                "count": 1,
                "total": 1,
                "start": 0,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
                            "type": "DATASET",
                            "properties": {
                                "name": "test_table",
                                "description": "A test table",
                            },
                        }
                    }
                ],
            }
        }

        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = mock_search_response

        context = SearchContext(query="test table", page=0, filters={})
        result = await handle_search_command_teams(
            mock_graph, context, "urn:li:user:test"
        )

        assert result["type"] == "message"

        # Verify GraphQL was called
        mock_graph.execute_graphql.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_command_empty_query(self) -> None:
        """Test search command with empty query."""
        mock_graph = MagicMock()

        context = SearchContext(query="", page=0, filters={})
        result = await handle_search_command_teams(
            mock_graph, context, "urn:li:user:test"
        )

        assert result["type"] == "message"
        result_text = str(result)
        assert "search" in result_text.lower()

    @pytest.mark.asyncio
    async def test_search_command_no_results(self) -> None:
        """Test search command when no results are found."""
        mock_search_response = {
            "searchAcrossEntities": {
                "count": 0,
                "total": 0,
                "start": 0,
                "searchResults": [],
            }
        }

        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = mock_search_response

        context = SearchContext(query="nonexistent", page=0, filters={})
        result = await handle_search_command_teams(
            mock_graph, context, "urn:li:user:test"
        )

        assert result["type"] == "message"
        result_text = str(result)
        assert "no results" in result_text.lower() or "not found" in result_text.lower()

    @pytest.mark.asyncio
    async def test_search_command_exception_handling(self) -> None:
        """Test search command handles exceptions gracefully."""
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = Exception("Search failed")

        context = SearchContext(query="test", page=0, filters={})
        result = await handle_search_command_teams(
            mock_graph, context, "urn:li:user:test"
        )

        assert result["type"] == "message"
        result_text = str(result)
        assert "error" in result_text.lower()

    @pytest.mark.asyncio
    async def test_search_command_multiple_results_formatting(self) -> None:
        """Test search command formats multiple results correctly."""
        mock_search_response = {
            "searchAcrossEntities": {
                "count": 2,
                "total": 2,
                "start": 0,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table1,PROD)",
                            "type": "DATASET",
                            "properties": {
                                "name": "table1",
                                "description": "First table",
                            },
                        }
                    },
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table2,PROD)",
                            "type": "DATASET",
                            "properties": {
                                "name": "table2",
                                "description": "Second table",
                            },
                        }
                    },
                ],
            }
        }

        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = mock_search_response

        context = SearchContext(query="table", page=0, filters={})
        result = await handle_search_command_teams(
            mock_graph, context, "urn:li:user:test"
        )

        assert result["type"] == "message"
        result_text = str(result)

        # Should contain both results
        assert "table1" in result_text
        assert "table2" in result_text

    @pytest.mark.asyncio
    async def test_search_command_with_none_user(self) -> None:
        """Test search command works with None user_urn."""
        mock_search_response = {
            "searchAcrossEntities": {
                "count": 0,
                "total": 0,
                "start": 0,
                "searchResults": [],
            }
        }

        mock_graph = MagicMock()
        mock_graph.execute_graphql.return_value = mock_search_response

        context = SearchContext(query="test", page=0, filters={})
        result = await handle_search_command_teams(mock_graph, context, None)

        assert result["type"] == "message"
