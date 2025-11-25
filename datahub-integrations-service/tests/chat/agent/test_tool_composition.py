"""Unit tests for tool composition utilities."""

from unittest.mock import Mock

from fastmcp import FastMCP

from datahub_integrations.chat.agent.tool_composition import (
    exclude_tools_by_names,
    filter_tools_by_names,
    flatten_tools,
    tools_from_fastmcp,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


class TestToolsFromFastMCP:
    """Test tools_from_fastmcp function."""

    def test_extract_tools_from_mcp_server(self) -> None:
        """Test extracting tools from FastMCP server."""
        # Create a mock FastMCP server
        mcp_server = Mock(spec=FastMCP)
        mcp_server.name = "test_server"

        # Mock the tool manager with some tools
        mock_tool_1 = Mock()
        mock_tool_1.name = "tool1"
        mock_tool_2 = Mock()
        mock_tool_2.name = "tool2"

        mcp_server._tool_manager = Mock()
        mcp_server._tool_manager._tools = {
            "tool1": mock_tool_1,
            "tool2": mock_tool_2,
        }

        # Extract tools
        result = tools_from_fastmcp(mcp_server)

        # Verify
        assert len(result) == 2
        assert all(isinstance(tool, ToolWrapper) for tool in result)
        # Tools should be named with the server prefix
        assert result[0].name.startswith("test_server__")
        assert result[1].name.startswith("test_server__")

    def test_empty_mcp_server(self) -> None:
        """Test extracting from MCP server with no tools."""
        mcp_server = Mock(spec=FastMCP)
        mcp_server.name = "empty_server"
        mcp_server._tool_manager = Mock()
        mcp_server._tool_manager._tools = {}

        result = tools_from_fastmcp(mcp_server)

        assert result == []


class TestFlattenTools:
    """Test flatten_tools function."""

    def test_flatten_mixed_toolwrapper_and_fastmcp(self) -> None:
        """Test flattening a mix of ToolWrapper and FastMCP objects."""
        # Create a direct ToolWrapper
        direct_tool = Mock(spec=ToolWrapper)
        direct_tool.name = "direct_tool"

        # Create a mock FastMCP server with tools
        mcp_server = Mock(spec=FastMCP)
        mcp_server.name = "mcp"
        mock_mcp_tool = Mock()
        mcp_server._tool_manager = Mock()
        mcp_server._tool_manager._tools = {"mcp_tool": mock_mcp_tool}

        # Flatten
        tools = [direct_tool, mcp_server]
        result = flatten_tools(tools)  # type: ignore[arg-type]

        # Should have 2 tools: 1 direct + 1 from MCP
        assert len(result) == 2
        assert result[0] == direct_tool
        assert isinstance(result[1], ToolWrapper)

    def test_flatten_only_toolwrappers(self) -> None:
        """Test flattening list with only ToolWrapper objects."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        result = flatten_tools([tool1, tool2])

        assert len(result) == 2
        assert result[0] == tool1
        assert result[1] == tool2

    def test_flatten_only_fastmcp(self) -> None:
        """Test flattening list with only FastMCP objects."""
        mcp1 = Mock(spec=FastMCP)
        mcp1.name = "mcp1"
        mcp1._tool_manager = Mock()
        mcp1._tool_manager._tools = {"tool1": Mock()}

        mcp2 = Mock(spec=FastMCP)
        mcp2.name = "mcp2"
        mcp2._tool_manager = Mock()
        mcp2._tool_manager._tools = {"tool2": Mock(), "tool3": Mock()}

        result = flatten_tools([mcp1, mcp2])

        # Should have 3 tools total (1 from mcp1, 2 from mcp2)
        assert len(result) == 3
        assert all(isinstance(tool, ToolWrapper) for tool in result)

    def test_flatten_empty_list(self) -> None:
        """Test flattening empty list."""
        result = flatten_tools([])
        assert result == []


class TestFilterToolsByNames:
    """Test filter_tools_by_names function."""

    def test_filter_keeps_allowed_tools(self) -> None:
        """Test filtering keeps only tools with allowed names."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "search"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "get_entities"
        tool3 = Mock(spec=ToolWrapper)
        tool3.name = "delete_entity"

        tools = [tool1, tool2, tool3]
        allowed = ["search", "get_entities"]

        result = filter_tools_by_names(tools, allowed)  # type: ignore[arg-type]

        assert len(result) == 2
        assert tool1 in result
        assert tool2 in result
        assert tool3 not in result

    def test_filter_with_no_matches(self) -> None:
        """Test filtering when no tools match allowed names."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        result = filter_tools_by_names([tool1, tool2], ["tool3", "tool4"])

        assert result == []

    def test_filter_with_all_matches(self) -> None:
        """Test filtering when all tools match."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        tools = [tool1, tool2]
        result = filter_tools_by_names(tools, ["tool1", "tool2", "tool3"])  # type: ignore[arg-type]

        assert len(result) == 2
        assert tool1 in result
        assert tool2 in result

    def test_filter_empty_allowed_list(self) -> None:
        """Test filtering with empty allowed list."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"

        result = filter_tools_by_names([tool1], [])

        assert result == []

    def test_filter_empty_tools_list(self) -> None:
        """Test filtering empty tools list."""
        result = filter_tools_by_names([], ["tool1", "tool2"])
        assert result == []


class TestExcludeToolsByNames:
    """Test exclude_tools_by_names function."""

    def test_exclude_removes_specified_tools(self) -> None:
        """Test excluding tools by name."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "search"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "get_entities"
        tool3 = Mock(spec=ToolWrapper)
        tool3.name = "delete_entity"

        tools = [tool1, tool2, tool3]
        excluded = ["delete_entity"]

        result = exclude_tools_by_names(tools, excluded)  # type: ignore[arg-type]

        assert len(result) == 2
        assert tool1 in result
        assert tool2 in result
        assert tool3 not in result

    def test_exclude_multiple_tools(self) -> None:
        """Test excluding multiple tools."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"
        tool3 = Mock(spec=ToolWrapper)
        tool3.name = "tool3"

        result = exclude_tools_by_names([tool1, tool2, tool3], ["tool1", "tool3"])  # type: ignore[arg-type]

        assert len(result) == 1
        assert result[0] == tool2

    def test_exclude_with_no_matches(self) -> None:
        """Test excluding when no tools match excluded names."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        tools = [tool1, tool2]
        result = exclude_tools_by_names(tools, ["tool3", "tool4"])  # type: ignore[arg-type]

        assert len(result) == 2
        assert tool1 in result
        assert tool2 in result

    def test_exclude_all_tools(self) -> None:
        """Test excluding all tools."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        result = exclude_tools_by_names([tool1, tool2], ["tool1", "tool2"])  # type: ignore[arg-type]

        assert result == []

    def test_exclude_empty_exclusion_list(self) -> None:
        """Test excluding with empty exclusion list."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "tool2"

        tools = [tool1, tool2]
        result = exclude_tools_by_names(tools, [])  # type: ignore[arg-type]

        assert len(result) == 2
        assert tool1 in result
        assert tool2 in result

    def test_exclude_empty_tools_list(self) -> None:
        """Test excluding from empty tools list."""
        result = exclude_tools_by_names([], ["tool1"])
        assert result == []


class TestToolCompositionIntegration:
    """Integration tests combining multiple tool composition functions."""

    def test_flatten_then_filter(self) -> None:
        """Test flattening and then filtering tools."""
        # Create direct tools
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "search"
        tool2 = Mock(spec=ToolWrapper)
        tool2.name = "delete"

        # For this integration test, just test with ToolWrappers
        # (testing MCP flattening is covered in other tests)
        flattened = flatten_tools([tool1, tool2])
        assert len(flattened) == 2

        # Filter
        filtered = filter_tools_by_names(flattened, ["search"])
        assert len(filtered) == 1
        assert filtered[0].name == "search"

    def test_flatten_then_exclude(self) -> None:
        """Test flattening and then excluding tools."""
        tool1 = Mock(spec=ToolWrapper)
        tool1.name = "tool1"

        mcp = Mock(spec=FastMCP)
        mcp.name = "mcp"
        mcp._tool_manager = Mock()
        mcp._tool_manager._tools = {"tool2": Mock(), "tool3": Mock()}

        # Flatten
        flattened = flatten_tools([tool1, mcp])
        assert len(flattened) == 3

        # Exclude
        result = exclude_tools_by_names(flattened, ["tool1"])
        assert len(result) == 2
