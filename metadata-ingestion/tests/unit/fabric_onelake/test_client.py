"""OneLakeClient listing branches: schemas-enabled vs schemas-disabled lakehouses,
the 400 fallback to the Delta Table API, and warehouse table-name parsing."""

from typing import Any, Dict, List
from unittest.mock import NonCallableMagicMock, patch

import pytest
import requests

from datahub.ingestion.source.fabric.common.auth import ONELAKE_STORAGE_SCOPE
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.constants import (
    FABRIC_SQL_DEFAULT_SCHEMA,
)
from tests.unit.fabric_common.conftest import fabric_response

_SCHEMAS_ENABLED = {"properties": {"defaultSchema": "dbo"}}
_SCHEMAS_DISABLED: Dict[str, Any] = {"properties": {}}


def test_lakehouses_and_warehouses_carry_their_item_type(
    onelake_client: OneLakeClient,
) -> None:
    page = fabric_response(200, {"value": [{"id": "i1", "displayName": "main"}]})
    with patch.object(onelake_client._session, "request", return_value=page) as request:
        lakehouses = list(onelake_client.list_lakehouses("ws"))
        warehouses = list(onelake_client.list_warehouses("ws"))

    assert request.call_args_list[0].kwargs["url"].endswith("/ws/lakehouses")
    assert request.call_args_list[1].kwargs["url"].endswith("/ws/warehouses")
    assert (lakehouses[0].type, lakehouses[0].workspace_id) == ("Lakehouse", "ws")
    assert warehouses[0].type == "Warehouse"


def test_schemas_disabled_lakehouse_reads_the_data_key_with_an_empty_schema(
    onelake_client: OneLakeClient,
) -> None:
    """Without schemas the REST tables endpoint answers under `data`, and a
    dotted name is how a schema reaches us at all."""
    pages = [
        fabric_response(200, _SCHEMAS_DISABLED),
        fabric_response(
            200,
            {"data": [{"name": "orders", "description": "d"}, {"name": "fin.refunds"}]},
        ),
    ]
    with patch.object(onelake_client._session, "request", side_effect=pages):
        tables = list(onelake_client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name, t.description) for t in tables] == [
        ("", "orders", "d"),
        ("fin", "refunds", None),
    ]


def test_schemas_enabled_lakehouse_lists_tables_per_schema_with_storage_scope(
    onelake_client: OneLakeClient, auth_helper: NonCallableMagicMock
) -> None:
    """Schemas-enabled lakehouses are read through the OneLake Delta API,
    which needs a storage-scoped token rather than the Fabric API one."""
    delta_pages = [
        fabric_response(200, {"schemas": [{"name": "dbo"}, {"name": ""}]}),
        fabric_response(200, {"tables": [{"name": "t1", "comment": "from delta"}]}),
    ]
    with (
        patch.object(
            onelake_client._session,
            "request",
            return_value=fabric_response(200, _SCHEMAS_ENABLED),
        ),
        patch.object(onelake_client._session, "get", side_effect=delta_pages) as get,
    ):
        tables = list(onelake_client.list_lakehouse_tables("ws", "lh"))

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
        for call in auth_helper.get_authorization_header.call_args_list
    }
    assert ONELAKE_STORAGE_SCOPE in scopes


def test_schema_detection_failure_falls_back_to_the_rest_tables_endpoint(
    onelake_client: OneLakeClient,
) -> None:
    pages = [fabric_response(500), fabric_response(200, {"data": [{"name": "t"}]})]
    with patch.object(onelake_client._session, "request", side_effect=pages):
        tables = list(onelake_client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name) for t in tables] == [("", "t")]


def test_unsupported_operation_400_falls_back_to_the_delta_api(
    onelake_client: OneLakeClient,
) -> None:
    """A lakehouse whose properties don't say schemas are enabled can still
    refuse the REST listing with this error code; the Delta API is the
    only way to its tables."""
    rest_pages = [
        fabric_response(200, _SCHEMAS_DISABLED),
        fabric_response(
            400, {"errorCode": "UnsupportedOperationForSchemasEnabledLakehouse"}
        ),
    ]
    delta_pages = [
        fabric_response(200, {"schemas": [{"name": "sales"}]}),
        fabric_response(200, {"tables": [{"name": "facts"}]}),
    ]
    with (
        patch.object(onelake_client._session, "request", side_effect=rest_pages),
        patch.object(onelake_client._session, "get", side_effect=delta_pages),
    ):
        tables = list(onelake_client.list_lakehouse_tables("ws", "lh"))

    assert [(t.schema_name, t.name) for t in tables] == [("sales", "facts")]


def test_other_rest_errors_on_lakehouse_tables_are_raised_and_counted(
    onelake_client: OneLakeClient,
) -> None:
    pages = [fabric_response(200, _SCHEMAS_DISABLED), fabric_response(500)]
    with (
        patch.object(onelake_client._session, "request", side_effect=pages),
        pytest.raises(requests.HTTPError),
    ):
        list(onelake_client.list_lakehouse_tables("ws", "lh"))

    assert onelake_client.report.error_count >= 1


def test_warehouse_tables_default_bare_names_to_the_default_schema(
    onelake_client: OneLakeClient,
) -> None:
    page = fabric_response(200, {"value": [{"name": "dbo.facts"}, {"name": "bare"}]})
    with patch.object(onelake_client._session, "request", return_value=page):
        tables = list(onelake_client.list_warehouse_tables("ws", "wh"))

    assert [(t.schema_name, t.name, t.item_id) for t in tables] == [
        ("dbo", "facts", "wh"),
        (FABRIC_SQL_DEFAULT_SCHEMA, "bare", "wh"),
    ]


def test_warehouse_tables_non_404_errors_are_raised(
    onelake_client: OneLakeClient,
) -> None:
    """Only a 404 degrades to an empty listing; anything else is an error
    the caller must see, not a degraded result."""
    seen: List[str] = []
    with (
        patch.object(
            onelake_client._session, "request", return_value=fabric_response(500)
        ),
        pytest.raises(requests.HTTPError),
    ):
        list(onelake_client.list_warehouse_tables("ws", "wh", on_degraded=seen.append))

    assert seen == []


def test_workspaces_map_display_name_to_name(onelake_client: OneLakeClient) -> None:
    # Pagination itself is BaseFabricClient's, pinned in fabric_common.
    page = fabric_response(
        200, {"value": [{"id": "w1", "displayName": "sales", "capacityId": "c1"}]}
    )
    with patch.object(onelake_client._session, "request", return_value=page):
        workspaces = list(onelake_client.list_workspaces())

    assert [(w.id, w.name, w.capacity_id) for w in workspaces] == [
        ("w1", "sales", "c1")
    ]
