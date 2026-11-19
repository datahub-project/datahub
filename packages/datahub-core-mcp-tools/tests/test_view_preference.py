# mypy: disable-error-code="type-abstract"
from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.view_helpers import (
    _global_view_cache,
    _user_view_cache,
    fetch_global_default_view,
    fetch_user_default_view,
)
from datahub_core_mcp_tools.view_preference import (
    CustomView,
    NoView,
    UseDefaultView,
    resolve_view_urn,
)

_PATCH_USER = "datahub_core_mcp_tools.view_preference.fetch_user_default_view"
_PATCH_GLOBAL = "datahub_core_mcp_tools.view_preference.fetch_global_default_view"
_PATCH_GQL = "datahub_core_mcp_tools.view_helpers.graphql_helpers"


@pytest.fixture(autouse=True)
def _clear_caches() -> None:
    _user_view_cache.clear()
    _global_view_cache.clear()


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


def _graph(server: str, token: str | None) -> MagicMock:
    graph = MagicMock(spec=DataHubGraph)
    graph.config = MagicMock(server=server, token=token, auth=None)
    return graph


def _me_response(view_urn: str | None) -> dict:
    default_view = {"urn": view_urn} if view_urn else None
    return {"me": {"corpUser": {"settings": {"views": {"defaultView": default_view}}}}}


@pytest.mark.parametrize(
    ("user", "global_", "expected"),
    [
        ("urn:li:dataHubView:mine", "urn:li:dataHubView:org", "urn:li:dataHubView:mine"),
        (None, "urn:li:dataHubView:org", "urn:li:dataHubView:org"),
        (None, None, None),
    ],
)  # fmt: skip
def test_default_view_prefers_the_users_own(mock_graph, user, global_, expected):
    with (
        patch(_PATCH_USER, return_value=user),
        patch(_PATCH_GLOBAL, return_value=global_),
    ):
        assert UseDefaultView().get_view(mock_graph) == expected


def test_explicit_preferences_ignore_the_defaults(mock_graph):
    with patch(_PATCH_USER) as user, patch(_PATCH_GLOBAL) as global_:
        assert NoView().get_view(mock_graph) is None
        assert CustomView(urn="urn:li:dataHubView:x").get_view(mock_graph) == (
            "urn:li:dataHubView:x"
        )
    user.assert_not_called()
    global_.assert_not_called()


def test_resolve_view_urn_uses_the_bound_preference_else_the_default(mock_graph):
    client = MagicMock(spec=DataHubClient)
    with patch(_PATCH_USER, return_value="urn:li:dataHubView:mine"):
        with with_datahub_client(client, tool_context=ToolContext()):
            assert resolve_view_urn(mock_graph) == "urn:li:dataHubView:mine"
        bound = ToolContext([CustomView(urn="urn:li:dataHubView:override")])
        with with_datahub_client(client, tool_context=bound):
            assert resolve_view_urn(mock_graph) == "urn:li:dataHubView:override"


@pytest.mark.parametrize(
    "response",
    [_me_response(None), {"me": None}, {"me": {"corpUser": {"settings": None}}}],
)
def test_a_user_without_a_default_view_has_none(mock_graph, response):
    with patch(_PATCH_GQL) as gql:
        gql.execute_graphql.return_value = response
        assert fetch_user_default_view(mock_graph) is None


def test_an_unreadable_user_view_is_skipped(mock_graph):
    with patch(_PATCH_GQL) as gql:
        gql.execute_graphql.side_effect = RuntimeError("unauthorized")
        assert fetch_user_default_view(mock_graph) is None


def test_disabled_default_views_make_no_calls(mock_graph):
    with (
        patch("datahub_core_mcp_tools.view_helpers.DISABLE_DEFAULT_VIEW", True),
        patch(_PATCH_GQL) as gql,
    ):
        assert fetch_user_default_view(mock_graph) is None
        assert fetch_global_default_view(mock_graph) is None
    gql.execute_graphql.assert_not_called()


def test_views_are_cached_per_caller_not_per_client():
    # A multi-user host builds a new client per request: the same caller's next
    # client must hit the cache, and another caller's must not.
    with patch(_PATCH_GQL) as gql:
        gql.execute_graphql.side_effect = [
            _me_response("urn:li:dataHubView:a"),
            _me_response("urn:li:dataHubView:b"),
        ]
        assert fetch_user_default_view(_graph("http://gms", "token-a")) == (
            "urn:li:dataHubView:a"
        )
        assert fetch_user_default_view(_graph("http://gms", "token-a")) == (
            "urn:li:dataHubView:a"
        )
        assert fetch_user_default_view(_graph("http://gms", "token-b")) == (
            "urn:li:dataHubView:b"
        )
    assert gql.execute_graphql.call_count == 2


def test_global_view_is_cached(mock_graph):
    with patch(_PATCH_GQL) as gql:
        gql.execute_graphql.return_value = {
            "globalViewsSettings": {"defaultView": "urn:li:dataHubView:org"}
        }
        assert fetch_global_default_view(mock_graph) == "urn:li:dataHubView:org"
        assert fetch_global_default_view(mock_graph) == "urn:li:dataHubView:org"
    gql.execute_graphql.assert_called_once()
