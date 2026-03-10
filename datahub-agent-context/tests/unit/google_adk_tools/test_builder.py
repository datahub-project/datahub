"""Tests for Google ADK tools builder with context management."""

from typing import Callable
from unittest.mock import Mock

import pytest

from datahub_agent_context import get_datahub_client
from datahub_agent_context.google_adk_tools.builder import build_google_adk_tools

READ_ONLY_TOOLS = {
    "search_documents",
    "grep_documents",
    "get_entities",
    "list_schema_fields",
    "get_me",
    "get_lineage",
    "get_lineage_paths_between",
    "get_dataset_queries",
    "get_dataset_assertions",
    "search",
}

MUTATION_TOOLS = {
    "update_description",
    "set_domains",
    "remove_domains",
    "add_owners",
    "remove_owners",
    "add_structured_properties",
    "remove_structured_properties",
    "add_tags",
    "remove_tags",
    "add_glossary_terms",
    "remove_glossary_terms",
    "save_document",
}


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    mock.graph.frontend_base_url = "http://localhost:9002"
    return mock


def test_build_google_adk_tools_returns_list_of_callables(mock_client):
    """Test that build_google_adk_tools returns a list of callables."""
    tools = build_google_adk_tools(mock_client)

    assert isinstance(tools, list)
    assert len(tools) > 0
    for t in tools:
        assert callable(t)


def test_build_google_adk_tools_without_mutations(mock_client):
    """Test that build_google_adk_tools excludes mutations by default."""
    tools = build_google_adk_tools(mock_client, include_mutations=False)

    tool_names = {t.__name__ for t in tools}

    assert tool_names == READ_ONLY_TOOLS


def test_build_google_adk_tools_with_mutations(mock_client):
    """Test that build_google_adk_tools includes mutations when requested."""
    tools = build_google_adk_tools(mock_client, include_mutations=True)

    tool_names = {t.__name__ for t in tools}

    assert tool_names == READ_ONLY_TOOLS | MUTATION_TOOLS


def test_build_google_adk_tools_preserves_function_names(mock_client):
    """Test that wrapped tools preserve original function names."""
    tools = build_google_adk_tools(mock_client)

    for t in tools:
        assert t.__name__ in READ_ONLY_TOOLS


def test_build_google_adk_tools_context_managed_automatically(mock_client):
    """Test that tools manage DataHubClient context automatically when called."""
    mock_client._graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "total": 1,
            "searchResults": [{"entity": {"urn": "test:urn"}}],
        }
    }

    tools = build_google_adk_tools(mock_client)
    search_tool = next(t for t in tools if t.__name__ == "search")

    # Context not set before call
    with pytest.raises(RuntimeError, match="No DataHubClient in context"):
        get_datahub_client()

    result = search_tool(query="test")

    assert result is not None

    # Context reset after call
    with pytest.raises(RuntimeError, match="No DataHubClient in context"):
        get_datahub_client()


def test_build_google_adk_tools_context_reset_on_exception(mock_client):
    """Test that context is reset even when a tool raises an exception."""
    mock_client._graph.execute_graphql.side_effect = RuntimeError("network error")

    tools = build_google_adk_tools(mock_client)
    search_tool = next(t for t in tools if t.__name__ == "search")

    with pytest.raises(RuntimeError, match="network error"):
        search_tool(query="test")

    # Context should still be cleaned up
    with pytest.raises(RuntimeError, match="No DataHubClient in context"):
        get_datahub_client()


def test_build_google_adk_tools_not_langchain_basetools(mock_client):
    """Test that Google ADK tools are plain callables, not LangChain BaseTool instances."""
    tools = build_google_adk_tools(mock_client)

    for t in tools:
        assert isinstance(t, Callable)  # type: ignore[arg-type]
        # Plain functions don't have LangChain's _run method or name attribute as a property
        assert not hasattr(t, "_run")
