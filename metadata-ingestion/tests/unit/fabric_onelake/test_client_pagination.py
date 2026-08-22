"""Unit tests for pagination in OneLakeClient and BaseFabricClient."""

from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.onelake.client import (
    ONELAKE_TABLE_API_BASE_URL,
    OneLakeClient,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeClientReport


def _make_response(data: Any, status_code: int = 200) -> Mock:
    """Build a minimal mock requests.Response."""
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status = Mock()
    return resp


def _make_http_error(status_code: int, text: str = "") -> requests.exceptions.HTTPError:
    error_response = Mock()
    error_response.status_code = status_code
    error_response.text = text
    err = requests.exceptions.HTTPError(response=error_response)
    return err


@pytest.fixture
def mock_auth() -> MagicMock:
    auth = MagicMock(spec=FabricAuthHelper)
    auth.get_authorization_header.return_value = "Bearer mock-token"
    return auth


@pytest.fixture
def mock_report() -> MagicMock:
    return MagicMock(spec=FabricOneLakeClientReport)


@pytest.fixture
def client(mock_auth: MagicMock, mock_report: MagicMock) -> OneLakeClient:
    return OneLakeClient(auth_helper=mock_auth, report=mock_report)


# ---------------------------------------------------------------------------
# TestPaginateFabricApi — _paginate() on BaseFabricClient
# ---------------------------------------------------------------------------


class TestPaginateFabricApi:
    def test_single_page(self, client: OneLakeClient) -> None:
        page1 = {"value": [{"id": "a"}, {"id": "b"}]}
        with patch.object(
            client, "get", return_value=_make_response(page1)
        ) as mock_get:
            items = list(client._paginate("workspaces"))
        assert items == [{"id": "a"}, {"id": "b"}]
        mock_get.assert_called_once_with("workspaces", params={})

    def test_multi_page(self, client: OneLakeClient) -> None:
        page1 = {"value": [{"id": "a"}], "continuationToken": "tok1"}
        page2 = {"value": [{"id": "b"}], "continuationToken": "tok2"}
        page3 = {"value": [{"id": "c"}]}
        responses = [
            _make_response(page1),
            _make_response(page2),
            _make_response(page3),
        ]
        with patch.object(client, "get", side_effect=responses) as mock_get:
            items = list(client._paginate("workspaces"))
        assert items == [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        assert mock_get.call_count == 3
        # Second call must include the continuation token
        mock_get.assert_any_call("workspaces", params={"continuationToken": "tok1"})
        mock_get.assert_any_call("workspaces", params={"continuationToken": "tok2"})

    def test_empty_page(self, client: OneLakeClient) -> None:
        page: Dict[str, Any] = {"value": []}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("workspaces"))
        assert items == []

    def test_custom_items_key(self, client: OneLakeClient) -> None:
        page = {"data": [{"name": "tbl1"}, {"name": "tbl2"}]}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("some/endpoint", items_key="data"))
        assert items == [{"name": "tbl1"}, {"name": "tbl2"}]

    def test_custom_items_key_multi_page(self, client: OneLakeClient) -> None:
        page1 = {"data": [{"name": "tbl1"}], "continuationToken": "ct1"}
        page2 = {"data": [{"name": "tbl2"}]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            items = list(client._paginate("endpoint", items_key="data"))
        assert [i["name"] for i in items] == ["tbl1", "tbl2"]

    def test_default_items_key_unchanged(self, client: OneLakeClient) -> None:
        """Default items_key="value" must keep existing behaviour for workspaces."""
        page = {"value": [{"id": "ws-1"}]}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("workspaces"))
        assert items == [{"id": "ws-1"}]

    def test_repeated_continuation_token_breaks(self, client: OneLakeClient) -> None:
        """A non-advancing continuationToken must stop pagination without emitting
        the repeated page twice."""
        page: Dict[str, Any] = {"value": [{"id": "a"}], "continuationToken": "stuck"}
        with patch.object(client, "get", return_value=_make_response(page)) as mock_get:
            items = list(client._paginate("workspaces"))
        # Page 1 yields and records the token; page 2 detects the repeat before
        # yielding and breaks, so the item is emitted once.
        assert mock_get.call_count == 2
        assert items == [{"id": "a"}]


# ---------------------------------------------------------------------------
# TestListItemsPagination — _list_items() (lakehouses & warehouses)
# ---------------------------------------------------------------------------


class TestListItemsPagination:
    def _lakehouse_item(self, n: int) -> Dict[str, str]:
        return {
            "id": f"lh-{n}",
            "displayName": f"Lakehouse{n}",
            "description": f"desc{n}",
        }

    def test_list_lakehouses_single_page(self, client: OneLakeClient) -> None:
        page = {"value": [self._lakehouse_item(1), self._lakehouse_item(2)]}
        with patch.object(client, "get", return_value=_make_response(page)):
            lakehouses = list(client.list_lakehouses("ws-1"))
        assert len(lakehouses) == 2
        assert lakehouses[0].id == "lh-1"
        assert lakehouses[1].id == "lh-2"

    def test_list_lakehouses_multi_page(self, client: OneLakeClient) -> None:
        page1 = {
            "value": [self._lakehouse_item(i) for i in range(1, 4)],
            "continuationToken": "ct",
        }
        page2 = {"value": [self._lakehouse_item(4)]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            lakehouses = list(client.list_lakehouses("ws-1"))
        assert len(lakehouses) == 4
        assert [lh.id for lh in lakehouses] == ["lh-1", "lh-2", "lh-3", "lh-4"]

    def test_list_warehouses_multi_page(self, client: OneLakeClient) -> None:
        def _wh(n: int) -> Dict[str, str]:
            return {"id": f"wh-{n}", "displayName": f"Warehouse{n}"}

        page1 = {"value": [_wh(1)], "continuationToken": "ct"}
        page2 = {"value": [_wh(2)]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            warehouses = list(client.list_warehouses("ws-1"))
        assert [wh.id for wh in warehouses] == ["wh-1", "wh-2"]


# ---------------------------------------------------------------------------
# TestListLakehouseTablesPagination — non-schema branch ("data" key)
# ---------------------------------------------------------------------------


class TestListLakehouseTablesPagination:
    def _table_item(self, name: str) -> Dict[str, str]:
        return {"name": name, "description": f"desc_{name}"}

    def _setup_schemas_disabled(self, client: OneLakeClient) -> None:
        """Patch _is_lakehouse_schemas_enabled to return False."""
        client._is_lakehouse_schemas_enabled = Mock(return_value=False)  # type: ignore[method-assign]

    def test_single_page(self, client: OneLakeClient) -> None:
        self._setup_schemas_disabled(client)
        page = {"data": [self._table_item("my_table")]}
        with patch.object(client, "get", return_value=_make_response(page)):
            tables = list(client.list_lakehouse_tables("ws-1", "lh-1"))
        assert len(tables) == 1
        assert tables[0].name == "my_table"

    def test_multi_page(self, client: OneLakeClient) -> None:
        self._setup_schemas_disabled(client)
        page1 = {
            "data": [self._table_item("tbl1"), self._table_item("tbl2")],
            "continuationToken": "ct",
        }
        page2 = {"data": [self._table_item("tbl3")]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            tables = list(client.list_lakehouse_tables("ws-1", "lh-1"))
        assert [t.name for t in tables] == ["tbl1", "tbl2", "tbl3"]

    def test_400_schemas_enabled_fallback_still_works(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """400 UnsupportedOperationForSchemasEnabledLakehouse must trigger schema fallback."""
        self._setup_schemas_disabled(client)
        http_err = _make_http_error(
            400, "UnsupportedOperationForSchemasEnabledLakehouse"
        )

        # First call (tables endpoint) raises the 400 error
        # Subsequent calls are for OneLake Table API (session.get)
        schema_resp = _make_response({"schemas": [{"name": "dbo"}]})
        table_resp = _make_response({"tables": [{"name": "t1"}]})

        with (
            patch.object(client, "get", side_effect=http_err),
            patch.object(client._session, "get", side_effect=[schema_resp, table_resp]),
        ):
            tables = list(client.list_lakehouse_tables("ws-1", "lh-1"))

        assert len(tables) == 1
        assert tables[0].name == "t1"
        assert tables[0].schema_name == "dbo"


# ---------------------------------------------------------------------------
# TestListWarehouseTablesPagination
# ---------------------------------------------------------------------------


class TestListWarehouseTablesPagination:
    def _table_item(self, schema: str, name: str) -> Dict[str, str]:
        return {"name": f"{schema}.{name}"}

    def test_single_page(self, client: OneLakeClient) -> None:
        page = {"value": [self._table_item("dbo", "orders")]}
        with patch.object(client, "get", return_value=_make_response(page)):
            tables = list(client.list_warehouse_tables("ws-1", "wh-1"))
        assert len(tables) == 1
        assert tables[0].name == "orders"
        assert tables[0].schema_name == "dbo"

    def test_multi_page(self, client: OneLakeClient) -> None:
        page1 = {
            "value": [self._table_item("dbo", "t1"), self._table_item("dbo", "t2")],
            "continuationToken": "ct",
        }
        page2 = {"value": [self._table_item("sales", "t3")]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            tables = list(client.list_warehouse_tables("ws-1", "wh-1"))
        assert [t.name for t in tables] == ["t1", "t2", "t3"]
        assert tables[2].schema_name == "sales"

    def test_404_returns_empty(self, client: OneLakeClient) -> None:
        """Staging warehouses return 404 — must yield nothing, not raise."""
        http_err = _make_http_error(404)
        with patch.object(client, "get", side_effect=http_err):
            tables = list(client.list_warehouse_tables("ws-1", "staging-wh"))
        assert tables == []

    def test_non_404_http_error_propagates(self, client: OneLakeClient) -> None:
        http_err = _make_http_error(500, "internal server error")
        with patch.object(client, "get", side_effect=http_err):
            with pytest.raises(requests.exceptions.HTTPError):
                list(client.list_warehouse_tables("ws-1", "wh-1"))


# ---------------------------------------------------------------------------
# TestOneLakeTableApiPagination — _paginate_onelake_table_api()
#
# Both the request parameter "page_token" and response field "next_page_token"
# are defined by the Unity Catalog API spec that the OneLake Table API implements
# (listSchemas / listTables). Should an endpoint ignore page_token, the seen-token
# guard breaks the loop instead of spinning forever (test_repeated_page_token_breaks).
# ---------------------------------------------------------------------------


class TestOneLakeTableApiPagination:
    """Tests for _paginate_onelake_table_api()."""

    def test_schemas_single_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        resp = _make_response({"schemas": [{"name": "dbo"}, {"name": "sales"}]})
        with patch.object(client._session, "get", return_value=resp):
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert schemas == ["dbo", "sales"]

    def test_schemas_multi_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """Validate that next_page_token drives a second request."""
        resp1 = _make_response({"schemas": [{"name": "s1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"schemas": [{"name": "s2"}]})
        with patch.object(
            client._session, "get", side_effect=[resp1, resp2]
        ) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))

        assert schemas == ["s1", "s2"]
        assert mock_get.call_count == 2
        # Second call must include page_token
        second_call_kwargs = mock_get.call_args_list[1]
        assert second_call_kwargs.kwargs.get("params", {}).get("page_token") == "pt1"

    def test_tables_single_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        resp = _make_response({"tables": [{"name": "orders"}, {"name": "products"}]})
        with patch.object(client._session, "get", return_value=resp):
            tables = list(
                client._list_tables_per_schema_via_onelake_api("ws-1", "lh-1", "dbo")
            )
        assert [t.name for t in tables] == ["orders", "products"]
        assert all(t.schema_name == "dbo" for t in tables)

    def test_tables_multi_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        resp1 = _make_response({"tables": [{"name": "t1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"tables": [{"name": "t2"}, {"name": "t3"}]})
        with patch.object(client._session, "get", side_effect=[resp1, resp2]):
            tables = list(
                client._list_tables_per_schema_via_onelake_api("ws-1", "lh-1", "sales")
            )
        assert [t.name for t in tables] == ["t1", "t2", "t3"]

    def test_tables_uses_comment_field_for_description(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """OneLake Table API uses 'comment' not 'description' for table descriptions."""
        resp = _make_response({"tables": [{"name": "t1", "comment": "my comment"}]})
        with patch.object(client._session, "get", return_value=resp):
            tables = list(
                client._list_tables_per_schema_via_onelake_api("ws-1", "lh-1", "dbo")
            )
        assert tables[0].description == "my comment"

    def test_auth_header_uses_storage_scope(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """_paginate_onelake_table_api must use ONELAKE_STORAGE_SCOPE, not the default scope."""
        from datahub.ingestion.source.fabric.common.auth import ONELAKE_STORAGE_SCOPE

        resp = _make_response({"schemas": []})
        with patch.object(client._session, "get", return_value=resp):
            list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))

        mock_auth.get_authorization_header.assert_called_with(
            scope=ONELAKE_STORAGE_SCOPE
        )

    def test_http_error_propagates(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        err_resp = Mock()
        err_resp.status_code = 403
        err_resp.text = "Forbidden"
        http_err = requests.exceptions.HTTPError(response=err_resp)

        with patch.object(client._session, "get", side_effect=http_err):
            with pytest.raises(requests.exceptions.HTTPError):
                list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))

    def test_no_next_page_token_stops_after_one_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """When next_page_token is absent, exactly one request is made."""
        resp = _make_response({"schemas": [{"name": "dbo"}]})
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert schemas == ["dbo"]
        assert mock_get.call_count == 1

    def test_null_next_page_token_stops_after_one_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """next_page_token=null (None) must also stop pagination."""
        resp = _make_response({"schemas": [{"name": "dbo"}], "next_page_token": None})
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert schemas == ["dbo"]
        assert mock_get.call_count == 1

    def test_repeated_page_token_breaks(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """An endpoint that ignores page_token repeats next_page_token; the guard
        must stop pagination instead of looping forever."""
        resp = _make_response(
            {"schemas": [{"name": "dbo"}], "next_page_token": "stuck"}
        )
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        # Page 1 yields and records the token; page 2 detects the repeat before
        # yielding and breaks, so the schema is emitted once.
        assert mock_get.call_count == 2
        assert schemas == ["dbo"]

    def test_url_construction_schemas(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        resp = _make_response({"schemas": []})
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            list(client._list_schemas_via_onelake_api("ws-abc", "lh-xyz"))

        expected_url = (
            f"{ONELAKE_TABLE_API_BASE_URL}/delta/ws-abc/lh-xyz"
            "/api/2.1/unity-catalog/schemas"
        )
        actual_url = mock_get.call_args[0][0]
        assert actual_url == expected_url

    def test_url_construction_tables(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        resp = _make_response({"tables": []})
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            list(
                client._list_tables_per_schema_via_onelake_api(
                    "ws-abc", "lh-xyz", "my_schema"
                )
            )

        expected_url = (
            f"{ONELAKE_TABLE_API_BASE_URL}/delta/ws-abc/lh-xyz"
            "/api/2.1/unity-catalog/tables"
        )
        actual_url = mock_get.call_args[0][0]
        assert actual_url == expected_url
