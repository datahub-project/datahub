"""
Tests for version-based tool filtering.

Test scenarios:
1. Tools without version requirements are always included
2. Cloud server with sufficient version -> version-gated tools included
3. Cloud server with old version -> version-gated tools excluded
4. OSS server with sufficient version -> version-gated tools included
5. OSS server with old version -> version-gated tools excluded
6. Tool with cloud-only requirement -> excluded on OSS
7. Tool with oss-only requirement -> excluded on Cloud
8. Error fetching server version -> fail open (all tools returned)
9. @min_version decorator sets function attribute correctly
10. _register_tool captures version requirements
11. _parse_version handles various formats
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datahub_integrations.mcp.version_requirements import (
    TOOL_VERSION_REQUIREMENTS,
    VersionFilterMiddleware,
    VersionRequirement,
    _is_tool_compatible,
    _parse_version,
    filter_tools_by_version,
    min_version,
)


class TestParseVersion:
    """Tests for _parse_version."""

    def test_three_part_version(self):
        assert _parse_version("1.4.0") == (1, 4, 0, 0)

    def test_four_part_version(self):
        assert _parse_version("0.3.16.1") == (0, 3, 16, 1)

    def test_v_prefix(self):
        assert _parse_version("v1.4.0") == (1, 4, 0, 0)

    def test_rc_suffix(self):
        assert _parse_version("1.4.0rc3") == (1, 4, 0, 0)

    def test_dash_suffix(self):
        assert _parse_version("1.4.0-beta.1") == (1, 4, 0, 0)

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid version format"):
            _parse_version("not-a-version")


class TestMinVersionDecorator:
    """Tests for the @min_version decorator."""

    def test_sets_version_requirement_attribute(self):
        @min_version(cloud="0.3.16", oss="1.4.0")
        def my_tool():
            pass

        assert hasattr(my_tool, "_version_requirement")
        req = my_tool._version_requirement
        assert isinstance(req, VersionRequirement)
        assert req.cloud_min == (0, 3, 16, 0)
        assert req.oss_min == (1, 4, 0, 0)

    def test_cloud_only(self):
        @min_version(cloud="0.3.16")
        def my_tool():
            pass

        req = my_tool._version_requirement
        assert req.cloud_min == (0, 3, 16, 0)
        assert req.oss_min is None

    def test_oss_only(self):
        @min_version(oss="1.4.0")
        def my_tool():
            pass

        req = my_tool._version_requirement
        assert req.cloud_min is None
        assert req.oss_min == (1, 4, 0, 0)

    def test_preserves_function_identity(self):
        @min_version(cloud="0.3.16")
        def my_tool():
            """My docstring."""
            return 42

        assert my_tool() == 42
        assert my_tool.__doc__ == "My docstring."
        assert my_tool.__name__ == "my_tool"


class TestIsToolCompatible:
    """Tests for the _is_tool_compatible helper."""

    def test_cloud_compatible(self):
        req = VersionRequirement(cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0))
        assert _is_tool_compatible(req, is_cloud=True, server_version=(0, 3, 16, 0))

    def test_cloud_newer_version(self):
        req = VersionRequirement(cloud_min=(0, 3, 16, 0))
        assert _is_tool_compatible(req, is_cloud=True, server_version=(0, 3, 20, 0))

    def test_cloud_older_version(self):
        req = VersionRequirement(cloud_min=(0, 3, 16, 0))
        assert not _is_tool_compatible(req, is_cloud=True, server_version=(0, 3, 15, 0))

    def test_oss_compatible(self):
        req = VersionRequirement(cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0))
        assert _is_tool_compatible(req, is_cloud=False, server_version=(1, 4, 0, 0))

    def test_oss_newer_version(self):
        req = VersionRequirement(oss_min=(1, 4, 0, 0))
        assert _is_tool_compatible(req, is_cloud=False, server_version=(1, 5, 0, 0))

    def test_oss_older_version(self):
        req = VersionRequirement(oss_min=(1, 4, 0, 0))
        assert not _is_tool_compatible(req, is_cloud=False, server_version=(1, 3, 0, 0))

    def test_cloud_only_tool_on_oss(self):
        req = VersionRequirement(cloud_min=(0, 3, 16, 0), oss_min=None)
        assert not _is_tool_compatible(req, is_cloud=False, server_version=(1, 5, 0, 0))

    def test_oss_only_tool_on_cloud(self):
        req = VersionRequirement(cloud_min=None, oss_min=(1, 4, 0, 0))
        assert not _is_tool_compatible(req, is_cloud=True, server_version=(0, 3, 20, 0))


class TestFilterToolsByVersion:
    """Tests for filter_tools_by_version."""

    @pytest.fixture
    def mock_tools(self):
        return [
            SimpleNamespace(name="search"),
            SimpleNamespace(name="get_entities"),
            SimpleNamespace(name="add_tags"),
            SimpleNamespace(name="search_documents"),
            SimpleNamespace(name="get_me"),
        ]

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Save and restore TOOL_VERSION_REQUIREMENTS around each test."""
        original = dict(TOOL_VERSION_REQUIREMENTS)
        TOOL_VERSION_REQUIREMENTS.clear()
        yield
        TOOL_VERSION_REQUIREMENTS.clear()
        TOOL_VERSION_REQUIREMENTS.update(original)

    @pytest.fixture
    def mock_client(self):
        """Mock get_datahub_client to return a client with a GMS server URL."""
        client = MagicMock()
        client._graph._gms_server = "http://localhost:8080"
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=client,
        ):
            yield client

    def test_no_requirements_returns_all(self, mock_tools):
        """When TOOL_VERSION_REQUIREMENTS is empty, all tools are returned."""
        result = filter_tools_by_version(mock_tools)
        assert len(result) == 5

    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    def test_cloud_new_enough_keeps_tools(
        self, mock_version_info, mock_tools, mock_client
    ):
        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        mock_version_info.return_value = (True, (0, 3, 20, 0))

        result = filter_tools_by_version(mock_tools)
        assert len(result) == 5
        assert any(t.name == "add_tags" for t in result)

    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    def test_cloud_too_old_filters_tools(
        self, mock_version_info, mock_tools, mock_client
    ):
        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        TOOL_VERSION_REQUIREMENTS["search_documents"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0)
        )
        mock_version_info.return_value = (True, (0, 3, 10, 0))

        result = filter_tools_by_version(mock_tools)
        tool_names = {t.name for t in result}
        assert "add_tags" not in tool_names
        assert "search_documents" not in tool_names
        # Unrestricted tools still present
        assert "search" in tool_names
        assert "get_entities" in tool_names
        assert "get_me" in tool_names

    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    def test_oss_new_enough_keeps_tools(
        self, mock_version_info, mock_tools, mock_client
    ):
        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        mock_version_info.return_value = (False, (1, 5, 0, 0))

        result = filter_tools_by_version(mock_tools)
        assert any(t.name == "add_tags" for t in result)

    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    def test_oss_too_old_filters_tools(
        self, mock_version_info, mock_tools, mock_client
    ):
        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        mock_version_info.return_value = (False, (1, 3, 0, 0))

        result = filter_tools_by_version(mock_tools)
        assert not any(t.name == "add_tags" for t in result)

    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    def test_cloud_only_tool_excluded_on_oss(
        self, mock_version_info, mock_tools, mock_client
    ):
        """Tool with cloud_min set but oss_min=None should be excluded on OSS."""
        TOOL_VERSION_REQUIREMENTS["search_documents"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0)
        )
        mock_version_info.return_value = (False, (1, 5, 0, 0))

        result = filter_tools_by_version(mock_tools)
        assert not any(t.name == "search_documents" for t in result)

    def test_error_fails_open(self, mock_tools):
        """On error fetching server version (e.g., no client), return all tools."""
        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        # No mock_client fixture -> get_datahub_client() raises LookupError -> fail open
        result = filter_tools_by_version(mock_tools)
        assert len(result) == 5


class TestVersionFilterMiddleware:
    """Tests for the VersionFilterMiddleware."""

    @pytest.fixture
    def middleware(self):
        return VersionFilterMiddleware()

    @pytest.fixture
    def mock_tools(self):
        return [
            SimpleNamespace(name="search"),
            SimpleNamespace(name="add_tags"),
            SimpleNamespace(name="get_me"),
        ]

    @pytest.fixture
    def mock_context(self):
        return MagicMock()

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        original = dict(TOOL_VERSION_REQUIREMENTS)
        TOOL_VERSION_REQUIREMENTS.clear()
        yield
        TOOL_VERSION_REQUIREMENTS.clear()
        TOOL_VERSION_REQUIREMENTS.update(original)

    @pytest.mark.asyncio
    @patch("datahub_integrations.mcp.version_requirements._get_server_version_info")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    async def test_middleware_filters_incompatible_tools(
        self, mock_get_client, mock_version_info, middleware, mock_tools, mock_context
    ):
        mock_client = MagicMock()
        mock_client._graph._gms_server = "http://localhost:8080"
        mock_get_client.return_value = mock_client

        TOOL_VERSION_REQUIREMENTS["add_tags"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        TOOL_VERSION_REQUIREMENTS["get_me"] = VersionRequirement(
            cloud_min=(0, 3, 16, 0), oss_min=(1, 4, 0, 0)
        )
        mock_version_info.return_value = (True, (0, 3, 10, 0))
        mock_call_next = AsyncMock(return_value=mock_tools)

        result = await middleware.on_list_tools(mock_context, mock_call_next)

        tool_names = {t.name for t in result}
        assert tool_names == {"search"}

    @pytest.mark.asyncio
    async def test_middleware_passes_through_when_no_requirements(
        self, middleware, mock_tools, mock_context
    ):
        mock_call_next = AsyncMock(return_value=mock_tools)
        result = await middleware.on_list_tools(mock_context, mock_call_next)
        assert len(result) == 3
