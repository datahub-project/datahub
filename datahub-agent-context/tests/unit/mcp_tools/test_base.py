"""Tests for base MCP tools module."""

import os
from unittest.mock import Mock, patch

import pytest

from datahub_agent_context.mcp_tools.base import (
    _is_datahub_cloud,
    _user_view_cache,
    fetch_global_default_view,
    fetch_user_default_view,
    resolve_default_view,
)


@pytest.fixture
def mock_client_with_frontend_url():
    """Create a mock DataHubClient with frontend_base_url set."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.frontend_base_url = "https://mycompany.acryl.io"
    return mock


@pytest.fixture
def mock_client_without_frontend_url():
    """Create a mock DataHubClient without frontend_base_url."""
    mock = Mock()
    mock._graph = Mock()
    # Explicitly make hasattr return False for frontend_base_url
    del mock.graph.frontend_base_url
    return mock


@pytest.fixture
def mock_client_with_empty_frontend_url():
    """Create a mock DataHubClient with empty frontend_base_url."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.frontend_base_url = None
    return mock


def test_is_datahub_cloud_with_frontend_url(mock_client_with_frontend_url):
    """Test cloud detection returns True when frontend_base_url is set."""
    result = _is_datahub_cloud(mock_client_with_frontend_url.graph)
    assert result is True


def test_is_datahub_cloud_without_frontend_url(mock_client_without_frontend_url):
    """Test cloud detection returns False when frontend_base_url is not present."""
    result = _is_datahub_cloud(mock_client_without_frontend_url.graph)
    assert result is False


def test_is_datahub_cloud_with_empty_frontend_url(mock_client_with_empty_frontend_url):
    """Test cloud detection returns False when frontend_base_url is None."""
    result = _is_datahub_cloud(mock_client_with_empty_frontend_url.graph)
    assert result is False


def test_is_datahub_cloud_with_env_var_disabled(mock_client_with_frontend_url):
    """Test cloud detection returns False when DISABLE_NEWER_GMS_FIELD_DETECTION is set."""
    with patch.dict(os.environ, {"DISABLE_NEWER_GMS_FIELD_DETECTION": "true"}):
        result = _is_datahub_cloud(mock_client_with_frontend_url.graph)
        assert result is False


def test_is_datahub_cloud_with_value_error(mock_client_with_frontend_url):
    """Test cloud detection returns False when ValueError is raised."""
    # Configure the mock to raise ValueError when accessing frontend_base_url
    type(mock_client_with_frontend_url.graph).frontend_base_url = property(
        lambda self: (_ for _ in ()).throw(ValueError("test error"))
    )

    result = _is_datahub_cloud(mock_client_with_frontend_url.graph)
    assert result is False


# --- Default view resolution tests ---


@pytest.fixture(autouse=True)
def _clear_view_caches():
    """Clear all view caches before each test to avoid cross-test pollution."""
    fetch_global_default_view.cache.clear()  # type: ignore[attr-defined]
    _user_view_cache.clear()


@pytest.fixture
def mock_graph():
    mock = Mock()
    mock.frontend_base_url = None
    mock.execute_graphql = Mock()
    return mock


def test_fetch_user_default_view_returns_urn(mock_graph):
    mock_graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "settings": {
                    "views": {"defaultView": {"urn": "urn:li:dataHubView:my-view"}}
                }
            }
        }
    }
    assert fetch_user_default_view(mock_graph) == "urn:li:dataHubView:my-view"


def test_fetch_user_default_view_returns_none_when_not_set(mock_graph):
    mock_graph.execute_graphql.return_value = {
        "me": {"corpUser": {"settings": {"views": {"defaultView": None}}}}
    }
    assert fetch_user_default_view(mock_graph) is None


def test_fetch_user_default_view_returns_none_on_error(mock_graph):
    mock_graph.execute_graphql.side_effect = Exception("network error")
    assert fetch_user_default_view(mock_graph) is None


@patch("datahub_agent_context.mcp_tools.base.DISABLE_DEFAULT_VIEW", True)
def test_fetch_user_default_view_disabled(mock_graph):
    _user_view_cache.clear()
    assert fetch_user_default_view(mock_graph) is None
    mock_graph.execute_graphql.assert_not_called()


def test_resolve_default_view_prefers_user_over_global(mock_graph):
    """User personal view takes priority over org global view."""
    with (
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_user_default_view",
            return_value="urn:li:dataHubView:user-view",
        ),
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_global_default_view",
            return_value="urn:li:dataHubView:global-view",
        ),
    ):
        result = resolve_default_view(mock_graph)
    assert result == "urn:li:dataHubView:user-view"


def test_resolve_default_view_falls_back_to_global(mock_graph):
    """When no user view, falls back to org global view."""
    with (
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_user_default_view",
            return_value=None,
        ),
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_global_default_view",
            return_value="urn:li:dataHubView:global-view",
        ),
    ):
        result = resolve_default_view(mock_graph)
    assert result == "urn:li:dataHubView:global-view"


def test_resolve_default_view_returns_none_when_neither_set(mock_graph):
    with (
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_user_default_view",
            return_value=None,
        ),
        patch(
            "datahub_agent_context.mcp_tools.base.fetch_global_default_view",
            return_value=None,
        ),
    ):
        result = resolve_default_view(mock_graph)
    assert result is None
