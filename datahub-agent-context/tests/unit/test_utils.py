"""Tests for datahub_agent_context.utils."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import get_datahub_client
from datahub_agent_context.utils import create_context_wrapper


@pytest.fixture
def mock_client():
    mock = Mock()
    mock._graph = Mock()
    return mock


def test_sets_client_in_context_during_call(mock_client):
    def tool():
        return get_datahub_client()

    wrapped = create_context_wrapper(tool, mock_client)
    result = wrapped()
    assert result is mock_client


def test_resets_context_after_call(mock_client):
    wrapped = create_context_wrapper(lambda: None, mock_client)
    wrapped()
    with pytest.raises(RuntimeError, match="No DataHubClient in context"):
        get_datahub_client()


def test_resets_context_on_exception(mock_client):
    def failing_tool():
        raise ValueError("boom")

    wrapped = create_context_wrapper(failing_tool, mock_client)
    with pytest.raises(ValueError, match="boom"):
        wrapped()

    with pytest.raises(RuntimeError, match="No DataHubClient in context"):
        get_datahub_client()


def test_preserves_function_metadata(mock_client):
    def my_tool(x: int, y: str = "default") -> str:
        """My tool docstring."""
        return f"{x} {y}"

    wrapped = create_context_wrapper(my_tool, mock_client)
    assert wrapped.__name__ == "my_tool"
    assert wrapped.__doc__ == "My tool docstring."


def test_passes_args_and_kwargs(mock_client):
    def tool(a, b, c=10):
        return (a, b, c)

    wrapped = create_context_wrapper(tool, mock_client)
    assert wrapped(1, 2, c=3) == (1, 2, 3)
