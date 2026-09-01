"""Unit tests for pagination in OneLakeClient and BaseFabricClient.

Design notes shared across these tests:

- The repeated-token guard tests use a *bounded* ``side_effect`` list rather
  than ``return_value``: if the guard ever regresses, the mock runs dry and
  the test fails fast with ``StopIteration`` instead of hanging the CI job in
  an infinite pagination loop.
- A fetched page is never dropped: when an endpoint repeats a pagination
  token, the page is yielded *before* the guard breaks the loop. Re-emitting
  a page at worst duplicates idempotent-per-URN metadata, while dropping one
  loses entities — so the guard tests deliberately assert the duplicate
  emission, and assert the truncation is recorded on the client report.
"""

from typing import Any, Dict, List
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
        # Later calls must carry the continuation token — full-dict equality
        # so a rebuilt/lost params dict cannot slip through.
        assert mock_get.call_args_list[1].kwargs["params"] == {
            "continuationToken": "tok1"
        }
        assert mock_get.call_args_list[2].kwargs["params"] == {
            "continuationToken": "tok2"
        }

    def test_multi_page_preserves_initial_params(self, client: OneLakeClient) -> None:
        """Page 2+ must carry the caller's original filters alongside the token.

        A regression that rebuilt the params dict instead of extending it would
        page against the wrong filter — plausible-but-wrong results, which is
        worse than truncation.
        """
        page1 = {"value": [{"id": "a"}], "continuationToken": "tok1"}
        page2 = {"value": [{"id": "b"}]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ) as mock_get:
            items = list(
                client._paginate(
                    "workspaces/ws-1/items", params={"type": "DataPipeline"}
                )
            )
        assert [i["id"] for i in items] == ["a", "b"]
        assert mock_get.call_args_list[0].kwargs["params"] == {"type": "DataPipeline"}
        assert mock_get.call_args_list[1].kwargs["params"] == {
            "type": "DataPipeline",
            "continuationToken": "tok1",
        }

    def test_caller_params_not_mutated(self, client: OneLakeClient) -> None:
        params = {"type": "DataPipeline"}
        page1 = {"value": [{"id": "a"}], "continuationToken": "tok1"}
        page2 = {"value": [{"id": "b"}]}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            list(client._paginate("endpoint", params=params))
        assert params == {"type": "DataPipeline"}

    def test_empty_page_is_not_a_parse_failure(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A legitimately-empty page ({"value": []}) yields nothing quietly."""
        page: Dict[str, Any] = {"value": []}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("workspaces"))
        assert items == []
        mock_report.report_parse_failure.assert_not_called()

    def test_null_items_value_treated_as_empty(self, client: OneLakeClient) -> None:
        """{"value": null} must be an empty page, not a TypeError."""
        page = {"value": None}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("workspaces"))
        assert items == []

    def test_token_only_terminal_page_is_not_a_parse_failure(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A terminal page carrying only the pagination envelope (items array
        omitted entirely) is a legitimate empty page — e.g.
        {"continuationToken": null} — not a malformed response."""
        page1 = {"value": [{"id": "a"}], "continuationToken": "tok1"}
        page2: Dict[str, Any] = {"continuationToken": None, "continuationUri": None}
        with patch.object(
            client, "get", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            items = list(client._paginate("workspaces"))
        assert items == [{"id": "a"}]
        mock_report.report_parse_failure.assert_not_called()

    def test_fallback_warning_fires_once_per_run(
        self, client: OneLakeClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A multi-page listing served entirely under the fallback key warns
        once, not once per page."""
        page1 = {"value": [{"name": "t1"}], "continuationToken": "ct1"}
        page2 = {"value": [{"name": "t2"}]}
        with (
            caplog.at_level("WARNING"),
            patch.object(
                client,
                "get",
                side_effect=[_make_response(page1), _make_response(page2)],
            ),
        ):
            items = list(
                client._paginate(
                    "endpoint", items_key="data", fallback_items_keys=("value",)
                )
            )
        assert [i["name"] for i in items] == ["t1", "t2"]
        fallback_warnings = [
            r for r in caplog.records if "fallback key" in r.getMessage()
        ]
        assert len(fallback_warnings) == 1

    def test_missing_items_key_reports_parse_failure(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A non-empty response without the expected items key must be loud:
        it would otherwise ingest zero items with no signal."""
        page = {"unexpected": [{"id": "a"}]}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(client._paginate("workspaces"))
        assert items == []
        mock_report.report_parse_failure.assert_called_once()

    def test_fallback_items_key_used(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """When the primary key is absent, a configured fallback key still
        serves the items (and is not a parse failure)."""
        page = {"value": [{"name": "tbl1"}]}
        with patch.object(client, "get", return_value=_make_response(page)):
            items = list(
                client._paginate(
                    "endpoint", items_key="data", fallback_items_keys=("value",)
                )
            )
        assert items == [{"name": "tbl1"}]
        mock_report.report_parse_failure.assert_not_called()

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

    def test_repeated_continuation_token_breaks(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A non-advancing continuationToken stops pagination after the second
        request. The second page is yielded before the guard fires (never drop
        a fetched page; the duplicate is tolerated), and the truncation is
        recorded on the client report."""
        page: Dict[str, Any] = {"value": [{"id": "a"}], "continuationToken": "stuck"}
        responses = [_make_response(page) for _ in range(5)]
        with patch.object(client, "get", side_effect=responses) as mock_get:
            items = list(client._paginate("workspaces"))
        assert mock_get.call_count == 2
        assert items == [{"id": "a"}, {"id": "a"}]
        mock_report.report_pagination_truncated.assert_called_once()


# ---------------------------------------------------------------------------
# TestPaginatePostFabricApi — _paginate_post() on BaseFabricClient
#
# The only production caller is query_activity_runs (data_factory/client.py),
# whose integration tests patch the method out wholesale — so the paginator
# body is pinned here.
# ---------------------------------------------------------------------------


class TestPaginatePostFabricApi:
    BODY: Dict[str, Any] = {
        "filters": [],
        "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
    }

    def test_single_page(self, client: OneLakeClient) -> None:
        page1 = {"value": [{"activityName": "a"}]}
        with patch.object(
            client, "post", return_value=_make_response(page1)
        ) as mock_post:
            items = list(client._paginate_post("runs", dict(self.BODY)))
        assert items == [{"activityName": "a"}]
        mock_post.assert_called_once_with("runs", json=self.BODY)

    def test_multi_page_body_carries_token_and_original_fields(
        self, client: OneLakeClient
    ) -> None:
        """Page 2's body must be the original body plus the token — full-dict
        equality so a rebuilt/lost body cannot slip through."""
        page1 = {"value": [{"activityName": "a"}], "continuationToken": "ct1"}
        page2 = {"value": [{"activityName": "b"}]}
        with patch.object(
            client, "post", side_effect=[_make_response(page1), _make_response(page2)]
        ) as mock_post:
            items = list(client._paginate_post("runs", dict(self.BODY)))
        assert [i["activityName"] for i in items] == ["a", "b"]
        assert mock_post.call_args_list[0].kwargs["json"] == self.BODY
        assert mock_post.call_args_list[1].kwargs["json"] == {
            **self.BODY,
            "continuationToken": "ct1",
        }

    def test_caller_body_not_mutated(self, client: OneLakeClient) -> None:
        """Pagination state must never be written into the caller's dict —
        a reused body would otherwise carry a stale token into an unrelated
        request."""
        body = dict(self.BODY)
        page1 = {"value": [{"activityName": "a"}], "continuationToken": "ct1"}
        page2 = {"value": [{"activityName": "b"}]}
        with patch.object(
            client, "post", side_effect=[_make_response(page1), _make_response(page2)]
        ):
            list(client._paginate_post("runs", body))
        assert body == self.BODY

    def test_list_shaped_response(self, client: OneLakeClient) -> None:
        """A bare-array response is yielded as-is and stops after one page."""
        page: List[Dict[str, str]] = [{"activityName": "a"}, {"activityName": "b"}]
        with patch.object(
            client, "post", return_value=_make_response(page)
        ) as mock_post:
            items = list(client._paginate_post("runs", dict(self.BODY)))
        assert items == page
        assert mock_post.call_count == 1

    def test_repeated_continuation_token_breaks(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        page = {"value": [{"activityName": "a"}], "continuationToken": "stuck"}
        responses = [_make_response(page) for _ in range(5)]
        with patch.object(client, "post", side_effect=responses) as mock_post:
            items = list(client._paginate_post("runs", dict(self.BODY)))
        assert mock_post.call_count == 2
        assert items == [{"activityName": "a"}, {"activityName": "a"}]
        mock_report.report_pagination_truncated.assert_called_once()


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
# TestListLakehouseTablesPagination — non-schema branch ("data" key, with a
# loud "value" fallback)
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

    def test_value_key_fallback_still_ingests(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A response serving tables under "value" (the pre-pagination code
        read both keys) must still ingest every table, not silently zero."""
        self._setup_schemas_disabled(client)
        page = {"value": [self._table_item("tbl1"), self._table_item("tbl2")]}
        with patch.object(client, "get", return_value=_make_response(page)):
            tables = list(client.list_lakehouse_tables("ws-1", "lh-1"))
        assert [t.name for t in tables] == ["tbl1", "tbl2"]
        mock_report.report_parse_failure.assert_not_called()

    def test_unrecognized_items_key_reports_parse_failure(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A non-empty response under neither "data" nor "value" must surface
        as a parse failure instead of quietly ingesting nothing."""
        self._setup_schemas_disabled(client)
        page = {"tables": [self._table_item("tbl1")]}
        with patch.object(client, "get", return_value=_make_response(page)):
            tables = list(client.list_lakehouse_tables("ws-1", "lh-1"))
        assert tables == []
        mock_report.report_parse_failure.assert_called_once()

    def test_http_error_after_first_page_records_truncation(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A failure on page 2+ means earlier pages were already consumed —
        record the truncation before propagating."""
        self._setup_schemas_disabled(client)
        page1 = _make_response(
            {"data": [self._table_item("tbl1")], "continuationToken": "ct"}
        )
        with patch.object(
            client, "get", side_effect=[page1, _make_http_error(500, "boom")]
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                list(client.list_lakehouse_tables("ws-1", "lh-1"))
        mock_report.report_pagination_truncated.assert_called_once()

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

    def test_400_after_first_page_is_truncation_not_fallback(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """The schema fallback re-lists the whole lakehouse, so it is only
        taken from a clean start; after a page has been yielded a 400 is a
        truncated listing and propagates."""
        self._setup_schemas_disabled(client)
        page1 = _make_response(
            {"data": [self._table_item("tbl1")], "continuationToken": "ct"}
        )
        http_err = _make_http_error(
            400, "UnsupportedOperationForSchemasEnabledLakehouse"
        )
        with patch.object(client, "get", side_effect=[page1, http_err]):
            with pytest.raises(requests.exceptions.HTTPError):
                list(client.list_lakehouse_tables("ws-1", "lh-1"))
        mock_report.report_pagination_truncated.assert_called_once()


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

    def test_404_returns_empty(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """Staging warehouses return 404 on the first request — must yield
        nothing, not raise, and is not a truncation."""
        http_err = _make_http_error(404)
        with patch.object(client, "get", side_effect=http_err):
            tables = list(client.list_warehouse_tables("ws-1", "staging-wh"))
        assert tables == []
        mock_report.report_pagination_truncated.assert_not_called()

    def test_404_after_first_page_keeps_tables_and_records_truncation(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A 404 on page 2+ is a listing cut short, not a staging warehouse:
        the tables already yielded are kept and the truncation is recorded."""
        page1 = _make_response(
            {"value": [self._table_item("dbo", "t1")], "continuationToken": "ct"}
        )
        with patch.object(client, "get", side_effect=[page1, _make_http_error(404)]):
            tables = list(client.list_warehouse_tables("ws-1", "wh-1"))
        assert [t.name for t in tables] == ["t1"]
        mock_report.report_pagination_truncated.assert_called_once()

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
        """Validate that next_page_token drives a second request that carries
        both the original catalog filter and the token."""
        resp1 = _make_response({"schemas": [{"name": "s1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"schemas": [{"name": "s2"}]})
        with patch.object(
            client._session, "get", side_effect=[resp1, resp2]
        ) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))

        assert schemas == ["s1", "s2"]
        assert mock_get.call_count == 2
        # Full-dict equality: a rebuilt params dict that lost catalog_name
        # would page against the wrong lakehouse with the suite still green.
        second_call = mock_get.call_args_list[1]
        assert second_call.kwargs["params"] == {
            "catalog_name": "lh-1",
            "page_token": "pt1",
        }

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

    def test_auth_header_fetched_per_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """The Authorization header is fetched once per page (the helper
        caches, so this is cheap) — a single fetch hoisted above the loop
        could expire during a long pagination run."""
        resp1 = _make_response({"schemas": [{"name": "s1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"schemas": [{"name": "s2"}], "next_page_token": "pt2"})
        resp3 = _make_response({"schemas": [{"name": "s3"}]})
        with patch.object(client._session, "get", side_effect=[resp1, resp2, resp3]):
            list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert mock_auth.get_authorization_header.call_count == 3

    def test_requests_counted_on_report(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """Every page fetched via the OneLake Table API counts toward
        request_count — this path cannot go through _request()."""
        resp1 = _make_response({"schemas": [{"name": "s1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"schemas": [{"name": "s2"}]})
        with patch.object(client._session, "get", side_effect=[resp1, resp2]):
            list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert mock_report.report_request.call_count == 2

    def test_http_error_propagates_and_counts_once(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        err_resp = Mock()
        err_resp.status_code = 403
        err_resp.text = "Forbidden"
        http_err = requests.exceptions.HTTPError(response=err_resp)

        with patch.object(client._session, "get", side_effect=http_err):
            with pytest.raises(requests.exceptions.HTTPError):
                list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        # Counted at the request layer inside the paginator, and only there.
        assert mock_report.report_error.call_count == 1

    def test_no_next_page_token_stops_after_one_page(
        self, client: OneLakeClient, mock_auth: MagicMock
    ) -> None:
        """When next_page_token is absent (or null), exactly one request is made."""
        resp = _make_response({"schemas": [{"name": "dbo"}], "next_page_token": None})
        with patch.object(client._session, "get", return_value=resp) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert schemas == ["dbo"]
        assert mock_get.call_count == 1

    def test_token_only_terminal_page_is_not_a_parse_failure(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """Unity Catalog-style APIs may omit the items array on an empty/terminal
        page and serialize only {"next_page_token": null} — that page is empty,
        not malformed."""
        resp1 = _make_response({"schemas": [{"name": "s1"}], "next_page_token": "pt1"})
        resp2 = _make_response({"next_page_token": None})
        with patch.object(client._session, "get", side_effect=[resp1, resp2]):
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert schemas == ["s1"]
        mock_report.report_parse_failure.assert_not_called()

    def test_malformed_json_body_counts_as_error(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """A 200 whose body fails JSON decoding is a failed request: it must
        count on the report and propagate."""
        resp = Mock(spec=requests.Response)
        resp.status_code = 200
        resp.raise_for_status = Mock()
        resp.json.side_effect = requests.exceptions.JSONDecodeError(
            "Expecting value", "not json", 0
        )
        with patch.object(client._session, "get", return_value=resp):
            with pytest.raises(requests.exceptions.JSONDecodeError):
                list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert mock_report.report_error.call_count == 1

    def test_repeated_page_token_breaks(
        self, client: OneLakeClient, mock_report: MagicMock
    ) -> None:
        """An endpoint that ignores page_token repeats next_page_token; the
        guard stops pagination after the second request. The second page is
        yielded before the guard fires (never drop a fetched page), and the
        truncation is recorded on the client report."""
        resp_data = {"schemas": [{"name": "dbo"}], "next_page_token": "stuck"}
        responses = [_make_response(resp_data) for _ in range(5)]
        with patch.object(client._session, "get", side_effect=responses) as mock_get:
            schemas = list(client._list_schemas_via_onelake_api("ws-1", "lh-1"))
        assert mock_get.call_count == 2
        assert schemas == ["dbo", "dbo"]
        mock_report.report_pagination_truncated.assert_called_once()

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
