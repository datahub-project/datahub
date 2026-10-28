"""OneLakeClient listing branches: schemas-enabled vs schemas-disabled lakehouses,
the 400 fallback to the Delta Table API, and warehouse table-name parsing."""

import json
from typing import Any, Dict, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.fabric.common.auth import ONELAKE_STORAGE_SCOPE
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.constants import (
    FABRIC_SQL_DEFAULT_SCHEMA,
)


def _response(status: int, body: Optional[object] = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body if body is not None else {}).encode()
    response.url = "https://api.fabric.microsoft.com/v1/x"
    return response


def _client() -> OneLakeClient:
    auth = MagicMock()
    auth.get_authorization_header.return_value = "Bearer placeholder-token"
    return OneLakeClient(auth_helper=auth)


_SCHEMAS_ENABLED = {"properties": {"defaultSchema": "dbo"}}
_SCHEMAS_DISABLED: Dict[str, Any] = {"properties": {}}


def test_lakehouses_and_warehouses_carry_their_item_type() -> None:
    client = _client()
    page = _response(200, {"value": [{"id": "i1", "displayName": "main"}]})
    with patch.object(client._session, "request", return_value=page) as request:
        lakehouses = list(client.list_lakehouses("ws"))
        warehouses = list(client.list_warehouses("ws"))

    assert request.call_args_list[0].kwargs["url"].endswith("/ws/lakehouses")
    assert request.call_args_list[1].kwargs["url"].endswith("/ws/warehouses")
    assert (lakehouses[0].type, lakehouses[0].workspace_id) == ("Lakehouse", "ws")
    assert warehouses[0].type == "Warehouse"


def test_schemas_disabled_lakehouse_reads_the_data_key_with_an_empty_schema() -> None:
    client = _client()
    pages = [
        _response(200, _SCHEMAS_DISABLED),
        _response(
            200,
            {"data": [{"name": "orders", "description": "d"}, {"name": "fin.refunds"}]},
        ),
    ]
    with patch.object(client._session, "request", side_effect=pages):
        tables = list(client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name, t.description) for t in tables] == [
        ("", "orders", "d"),
        ("fin", "refunds", None),
    ]


def test_schemas_enabled_lakehouse_lists_tables_per_schema_with_storage_scope() -> None:
    client = _client()
    delta_pages = [
        _response(200, {"schemas": [{"name": "dbo"}, {"name": ""}]}),
        _response(200, {"tables": [{"name": "t1", "comment": "from delta"}]}),
    ]
    with (
        patch.object(
            client._session, "request", return_value=_response(200, _SCHEMAS_ENABLED)
        ),
        patch.object(client._session, "get", side_effect=delta_pages) as get,
    ):
        tables = list(client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name, t.description) for t in tables] == [
        ("dbo", "t1", "from delta")
    ]
    # The nameless schema entry is skipped, so only one table listing happens.
    assert get.call_count == 2
    assert get.call_args.kwargs["params"] == {
        "catalog_name": "lh",
        "schema_name": "dbo",
    }
    scopes = {
        call.kwargs.get("scope")
        for call in cast(
            MagicMock, client.auth_helper
        ).get_authorization_header.call_args_list
    }
    assert ONELAKE_STORAGE_SCOPE in scopes


def test_schema_detection_failure_falls_back_to_the_rest_tables_endpoint() -> None:
    client = _client()
    pages = [_response(500), _response(200, {"data": [{"name": "t"}]})]
    with patch.object(client._session, "request", side_effect=pages):
        tables = list(client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name) for t in tables] == [("", "t")]


def test_unsupported_operation_400_falls_back_to_the_delta_api() -> None:
    client = _client()
    rest_pages = [
        _response(200, _SCHEMAS_DISABLED),
        _response(400, {"errorCode": "UnsupportedOperationForSchemasEnabledLakehouse"}),
    ]
    delta_pages = [
        _response(200, {"schemas": [{"name": "sales"}]}),
        _response(200, {"tables": [{"name": "facts"}]}),
    ]
    with (
        patch.object(client._session, "request", side_effect=rest_pages),
        patch.object(client._session, "get", side_effect=delta_pages),
    ):
        tables = list(client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name) for t in tables] == [("sales", "facts")]


def test_other_rest_errors_on_lakehouse_tables_are_raised_and_counted() -> None:
    client = _client()
    pages = [_response(200, _SCHEMAS_DISABLED), _response(500)]
    with (
        patch.object(client._session, "request", side_effect=pages),
        pytest.raises(requests.HTTPError),
    ):
        list(client.list_lakehouse_tables("ws", "lh"))

    assert client.report.error_count >= 1


def test_warehouse_tables_default_bare_names_to_the_default_schema() -> None:
    client = _client()
    page = _response(200, {"value": [{"name": "dbo.facts"}, {"name": "bare"}]})
    with patch.object(client._session, "request", return_value=page):
        tables = list(client.list_warehouse_tables("ws", "wh"))

    assert [(t.schema_name, t.name, t.item_id) for t in tables] == [
        ("dbo", "facts", "wh"),
        (FABRIC_SQL_DEFAULT_SCHEMA, "bare", "wh"),
    ]


def test_warehouse_tables_non_404_errors_are_raised() -> None:
    client = _client()
    seen: List[str] = []
    with (
        patch.object(client._session, "request", return_value=_response(500)),
        pytest.raises(requests.HTTPError),
    ):
        list(client.list_warehouse_tables("ws", "wh", on_degraded=seen.append))

    assert seen == []


def test_workspaces_are_read_across_continuation_pages() -> None:
    client = _client()
    pages = [
        _response(
            200,
            {
                "value": [{"id": "w1", "displayName": "sales"}],
                "continuationToken": "p2",
            },
        ),
        _response(200, {"value": [{"id": "w2", "displayName": "hr"}]}),
    ]
    with patch.object(client._session, "request", side_effect=pages) as request:
        workspaces = list(client.list_workspaces())

    assert [(w.id, w.name) for w in workspaces] == [("w1", "sales"), ("w2", "hr")]
    assert request.call_args_list[1].kwargs["params"] == {"continuationToken": "p2"}
