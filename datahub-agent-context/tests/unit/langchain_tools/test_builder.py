"""Tests for LangChain tools builder with context management."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import get_graph
from datahub_agent_context.langchain_tools.builder import (
    build_langchain_tools,
    create_context_wrapper,
)


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock._graph.execute_graphql = Mock()
    mock._graph.frontend_base_url = "http://localhost:9002"
    return mock


def test_create_context_wrapper_sets_and_resets_context(mock_client):
    """Test that create_context_wrapper manages context correctly."""

    def sample_tool():
        """Sample tool that retrieves graph from context."""
        return get_graph()

    wrapped = create_context_wrapper(sample_tool, mock_client)

    # Context should not be set initially
    with pytest.raises(RuntimeError, match="No DataHubGraph in context"):
        get_graph()

    # Call wrapped function - it should set and reset context
    result = wrapped()

    # Should return the client's graph
    assert result == mock_client._graph

    # Context should be reset after function returns
    with pytest.raises(RuntimeError, match="No DataHubGraph in context"):
        get_graph()


def test_create_context_wrapper_resets_on_exception(mock_client):
    """Test that context is reset even when function raises."""

    def failing_tool():
        """Tool that checks context then raises."""
        graph = get_graph()
        assert graph == mock_client._graph
        raise ValueError("Test error")

    wrapped = create_context_wrapper(failing_tool, mock_client)

    # Function should raise the error
    with pytest.raises(ValueError, match="Test error"):
        wrapped()

    # Context should still be reset after exception
    with pytest.raises(RuntimeError, match="No DataHubGraph in context"):
        get_graph()


def test_create_context_wrapper_preserves_function_metadata(mock_client):
    """Test that wrapper preserves function name and docstring."""

    def sample_tool(arg1: str, arg2: int = 10):
        """Sample tool docstring."""
        return f"{get_graph()} {arg1} {arg2}"

    wrapped = create_context_wrapper(sample_tool, mock_client)

    assert wrapped.__name__ == "sample_tool"
    assert wrapped.__doc__ == "Sample tool docstring."


def test_build_langchain_tools_returns_tools(mock_client):
    """Test that build_langchain_tools returns a list of BaseTool instances."""
    tools = build_langchain_tools(mock_client)

    assert isinstance(tools, list)
    assert len(tools) > 0

    # Check that tools are LangChain BaseTool instances
    for tool in tools:
        assert hasattr(tool, "name")
        assert hasattr(tool, "description")
        assert callable(tool._run)


def test_build_langchain_tools_without_mutations(mock_client):
    """Test that build_langchain_tools excludes mutations by default."""
    tools = build_langchain_tools(mock_client, include_mutations=False)

    tool_names = {tool.name for tool in tools}

    # Should include read-only tools
    assert "search" in tool_names
    assert "get_entities" in tool_names

    # Should NOT include mutation tools
    assert "update_description" not in tool_names
    assert "add_tags" not in tool_names
    assert "remove_tags" not in tool_names


def test_build_langchain_tools_with_mutations(mock_client):
    """Test that build_langchain_tools includes mutations when requested."""
    tools = build_langchain_tools(mock_client, include_mutations=True)

    tool_names = {tool.name for tool in tools}

    # Should include read-only tools
    assert "search" in tool_names
    assert "get_entities" in tool_names

    # Should include mutation tools
    assert "update_description" in tool_names
    assert "add_tags" in tool_names
    assert "remove_tags" in tool_names


def test_wrapped_tools_manage_context_automatically(mock_client):
    """Test that tools manage context automatically when called."""
    # Setup mock to return search results
    mock_client._graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "total": 1,
            "searchResults": [{"entity": {"urn": "test:urn"}}],
        }
    }

    tools = build_langchain_tools(mock_client)

    # Find the search tool
    search_tool = next(t for t in tools if t.name == "search")

    # Context should not be set initially
    with pytest.raises(RuntimeError, match="No DataHubGraph in context"):
        get_graph()

    # Call the tool - context should be managed automatically
    # Use invoke() which is the public API for LangChain tools
    result = search_tool.invoke({"query": "test"})

    # Should have returned results
    assert result is not None

    # Context should be reset after tool execution
    with pytest.raises(RuntimeError, match="No DataHubGraph in context"):
        get_graph()
