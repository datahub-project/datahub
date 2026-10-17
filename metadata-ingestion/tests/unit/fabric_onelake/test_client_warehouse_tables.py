"""Warehouse table listing uses the OneLake catalog, not query history."""

from unittest.mock import MagicMock

import requests

from datahub.ingestion.source.fabric.onelake.client import OneLakeClient


def _client() -> OneLakeClient:
    client = OneLakeClient.__new__(OneLakeClient)
    client.timeout = 30
    client.report = MagicMock()
    client.auth_helper = MagicMock()
    client.auth_helper.get_authorization_header.return_value = "Bearer token"
    client._session = MagicMock()
    return client


def test_list_warehouse_tables_uses_onelake_and_skips_system_schemas() -> None:
    client = _client()

    schema_resp = MagicMock()
    schema_resp.raise_for_status.return_value = None
    schema_resp.json.return_value = {
        "schemas": [
            {"name": "dbo"},
            {"name": "sys"},
            {"name": "INFORMATION_SCHEMA"},
            {"name": "queryinsights"},
        ]
    }

    table_resp = MagicMock()
    table_resp.raise_for_status.return_value = None
    table_resp.json.return_value = {
        "tables": [
            {"name": "orders", "comment": "order facts"},
            {"name": "customers"},
        ]
    }

    def get(url: str, headers=None, params=None, timeout=None):
        if url.endswith("/schemas"):
            return schema_resp
        return table_resp

    client._session.get.side_effect = get

    tables = list(client.list_warehouse_tables("ws-1", "wh-1"))

    assert [(table.schema_name, table.name) for table in tables] == [
        ("dbo", "orders"),
        ("dbo", "customers"),
    ]
    assert tables[0].description == "order facts"
    table_calls = [
        call
        for call in client._session.get.call_args_list
        if str(call.args[0]).endswith("/tables")
    ]
    assert len(table_calls) == 1
    assert table_calls[0].kwargs["params"]["catalog_name"] == "wh-1"
    assert table_calls[0].kwargs["params"]["schema_name"] == "dbo"


def _page(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


def test_unity_catalog_listing_follows_next_page_token() -> None:
    client = _client()

    schema_pages = [
        _page({"schemas": [{"name": "dbo"}], "next_page_token": "s2"}),
        _page({"schemas": [{"name": "sales"}]}),
    ]
    table_pages = {
        "dbo": [
            _page({"tables": [{"name": "t1"}], "next_page_token": "p2"}),
            _page({"tables": [{"name": "t2"}], "next_page_token": "p3"}),
            _page({"tables": [{"name": "t3"}]}),
        ],
        "sales": [_page({"tables": [{"name": "t4"}]})],
    }
    requests_seen: list[dict] = []

    def get(url: str, headers=None, params=None, timeout=None):
        requests_seen.append(dict(params))
        if url.endswith("/schemas"):
            return schema_pages.pop(0)
        return table_pages[params["schema_name"]].pop(0)

    client._session.get.side_effect = get

    tables = list(client.list_warehouse_tables("ws-1", "wh-1"))

    assert [(t.schema_name, t.name) for t in tables] == [
        ("dbo", "t1"),
        ("dbo", "t2"),
        ("dbo", "t3"),
        ("sales", "t4"),
    ]
    schema_requests = [p for p in requests_seen if "schema_name" not in p]
    assert [p.get("page_token") for p in schema_requests] == [None, "s2"]
    dbo_requests = [p for p in requests_seen if p.get("schema_name") == "dbo"]
    assert [p.get("page_token") for p in dbo_requests] == [None, "p2", "p3"]
    assert all("max_results" in p for p in requests_seen)


def test_unity_catalog_listing_stops_on_repeated_page_token() -> None:
    client = _client()
    client._session.get.return_value = _page(
        {"schemas": [{"name": "dbo"}], "next_page_token": "same"}
    )

    schemas = list(
        client._list_schemas_via_onelake_api("ws-1", "lh-1", item_label="lakehouse")
    )

    assert schemas == ["dbo", "dbo"]
    assert client._session.get.call_count == 2


def test_list_warehouse_tables_404_yields_nothing() -> None:
    client = _client()
    response = MagicMock()
    response.status_code = 404
    response.text = "not found"
    error = requests.exceptions.HTTPError(response=response)
    schema_resp = MagicMock()
    schema_resp.raise_for_status.side_effect = error
    client._session.get.return_value = schema_resp

    assert list(client.list_warehouse_tables("ws-1", "wh-1")) == []
    client.report.report_error.assert_not_called()
