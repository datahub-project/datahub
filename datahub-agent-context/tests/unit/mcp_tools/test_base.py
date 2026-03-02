"""Tests for base MCP tools module."""

import os
from unittest.mock import Mock, patch

import pytest

from datahub_agent_context.mcp_tools.base import _is_datahub_cloud


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
