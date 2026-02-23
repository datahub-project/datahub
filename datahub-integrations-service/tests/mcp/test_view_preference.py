# mypy: disable-error-code="type-abstract"
from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.mcp.mcp_server import (
    MCPContext,
    get_datahub_client,
    get_mcp_context,
    with_datahub_client,
)
from datahub_integrations.mcp.tool_context import ToolContext
from datahub_integrations.mcp.view_preference import (
    CustomView,
    NoView,
    UseDefaultView,
    ViewPreference,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def mock_client(mock_graph: MagicMock) -> MagicMock:
    client = MagicMock(spec=DataHubClient)
    client._graph = mock_graph
    return client


class TestViewPreference:
    def test_no_view_returns_none(self, mock_graph: MagicMock) -> None:
        assert NoView().get_view(mock_graph) is None

    def test_custom_view_returns_urn(self, mock_graph: MagicMock) -> None:
        urn = "urn:li:dataHubView:my-view"
        assert CustomView(urn=urn).get_view(mock_graph) == urn

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    def test_use_default_view_delegates(
        self, mock_fetch: MagicMock, mock_graph: MagicMock
    ) -> None:
        mock_fetch.return_value = "urn:li:dataHubView:global-default"
        result = UseDefaultView().get_view(mock_graph)
        assert result == "urn:li:dataHubView:global-default"
        mock_fetch.assert_called_once_with(mock_graph)

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    def test_use_default_view_returns_none_when_no_default(
        self, mock_fetch: MagicMock, mock_graph: MagicMock
    ) -> None:
        mock_fetch.return_value = None
        assert UseDefaultView().get_view(mock_graph) is None

    def test_custom_view_is_frozen(self) -> None:
        view = CustomView(urn="urn:li:dataHubView:test")
        with pytest.raises(AttributeError):
            view.urn = "something-else"  # type: ignore[misc]

    def test_no_view_is_frozen(self) -> None:
        view = NoView()
        with pytest.raises(AttributeError):
            view.foo = "bar"  # type: ignore[attr-defined]


class TestToolContext:
    def test_get_by_base_type(self) -> None:
        ctx = ToolContext([NoView()])
        result = ctx.get(ViewPreference)
        assert isinstance(result, NoView)

    def test_first_match_wins(self) -> None:
        ctx = ToolContext([NoView(), CustomView(urn="urn:li:dataHubView:second")])
        result = ctx.get(ViewPreference)
        assert isinstance(result, NoView)

    def test_returns_default_when_missing(self) -> None:
        ctx = ToolContext([])
        default = UseDefaultView()
        result = ctx.get(ViewPreference, default)
        assert result is default

    def test_returns_none_when_missing_no_default(self) -> None:
        ctx = ToolContext([])
        assert ctx.get(ViewPreference) is None

    def test_empty_bag(self) -> None:
        ctx = ToolContext()
        assert ctx.get(ViewPreference) is None

    def test_exact_type_match(self) -> None:
        view = CustomView(urn="urn:li:dataHubView:test")
        ctx = ToolContext([view])
        assert ctx.get(CustomView) is view

    def test_unrelated_types_not_found(self) -> None:
        ctx = ToolContext(["a string", 42])
        assert ctx.get(ViewPreference) is None


class TestMCPContext:
    def test_defaults_to_empty_tool_context(self, mock_client: MagicMock) -> None:
        ctx = MCPContext(client=mock_client)
        assert ctx.tool_context.get(ViewPreference) is None

    def test_accepts_tool_context_with_view(self, mock_client: MagicMock) -> None:
        view = NoView()
        ctx = MCPContext(client=mock_client, tool_context=ToolContext([view]))
        assert ctx.tool_context.get(ViewPreference) is view


class TestWithDatahubClient:
    def test_sets_and_resets_context(self, mock_client: MagicMock) -> None:
        with with_datahub_client(mock_client):
            ctx = get_mcp_context()
            assert ctx.client is mock_client
            assert ctx.tool_context.get(ViewPreference) is None
        with pytest.raises(LookupError):
            get_mcp_context()

    def test_passes_tool_context_through(self, mock_client: MagicMock) -> None:
        tc = ToolContext([NoView()])
        with with_datahub_client(mock_client, tool_context=tc):
            ctx = get_mcp_context()
            assert isinstance(ctx.tool_context.get(ViewPreference), NoView)

    def test_get_datahub_client_returns_client(self, mock_client: MagicMock) -> None:
        with with_datahub_client(mock_client):
            assert get_datahub_client() is mock_client

    def test_defaults_to_empty_tool_context_when_none(
        self, mock_client: MagicMock
    ) -> None:
        with with_datahub_client(mock_client, tool_context=None):
            ctx = get_mcp_context()
            assert ctx.tool_context.get(ViewPreference) is None

    def test_nested_contexts_restore_correctly(self, mock_client: MagicMock) -> None:
        outer_tc = ToolContext([NoView()])
        inner_tc = ToolContext([CustomView(urn="urn:li:dataHubView:inner")])

        with with_datahub_client(mock_client, tool_context=outer_tc):
            assert isinstance(
                get_mcp_context().tool_context.get(ViewPreference), NoView
            )

            with with_datahub_client(mock_client, tool_context=inner_tc):
                assert isinstance(
                    get_mcp_context().tool_context.get(ViewPreference), CustomView
                )

            assert isinstance(
                get_mcp_context().tool_context.get(ViewPreference), NoView
            )
