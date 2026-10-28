"""BaseFabricClient / FabricCoreClient against a stubbed HTTP session: pagination,
error accounting, and the per-endpoint response parsing the connectors rely on."""

import base64
import json
from typing import Dict, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient


def _response(status: int, body: Optional[object] = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body if body is not None else {}).encode()
    response.url = "https://api.fabric.microsoft.com/v1/x"
    return response


def _client() -> FabricCoreClient:
    auth = MagicMock()
    auth.get_authorization_header.return_value = "Bearer placeholder-token"
    return FabricCoreClient(auth_helper=auth)


def _request_params(mock: MagicMock, call_index: int) -> Dict[str, str]:
    return mock.call_args_list[call_index].kwargs["params"] or {}


def test_paginate_follows_continuation_tokens() -> None:
    client = _client()
    pages = iter(
        [
            _response(200, {"value": [{"id": "w1"}], "continuationToken": "tok-1"}),
            _response(200, {"value": [{"id": "w2"}]}),
        ]
    )
    # The client reuses one params dict across pages, so snapshot each call.
    sent_params: List[Dict[str, str]] = []

    def _request(**kwargs: object) -> requests.Response:
        params = kwargs["params"]
        assert isinstance(params, dict)
        sent_params.append(dict(params))
        return next(pages)

    with patch.object(client._session, "request", side_effect=_request):
        ids = [ws.id for ws in client.list_workspaces()]

    assert ids == ["w1", "w2"]
    assert sent_params == [{}, {"continuationToken": "tok-1"}]
    assert client.report.request_count == 2


def test_paginate_post_handles_dict_and_bare_list_pages() -> None:
    client = _client()
    pages = [
        _response(200, {"value": [{"id": 1}], "continuationToken": "tok-1"}),
        _response(200, [{"id": 2}]),
    ]
    body: Dict[str, str] = {"filter": "x"}
    with patch.object(client._session, "request", side_effect=pages) as request:
        items = list(client._paginate_post("workspaces/ws/query", body))

    assert [item["id"] for item in items] == [1, 2]
    assert request.call_args_list[1].kwargs["json"]["continuationToken"] == "tok-1"


def test_http_error_is_raised_and_counted() -> None:
    client = _client()
    with (
        patch.object(client._session, "request", return_value=_response(403)),
        pytest.raises(requests.HTTPError),
    ):
        list(client.list_workspaces())

    assert client.report.request_count == 1
    assert client.report.error_count == 1


def test_transport_error_is_raised_and_counted() -> None:
    client = _client()
    with (
        patch.object(
            client._session,
            "request",
            side_effect=requests.ConnectionError("unreachable"),
        ),
        pytest.raises(requests.ConnectionError),
    ):
        list(client.list_workspaces())

    assert client.report.error_count == 1


def test_malformed_workspace_is_skipped_and_reported() -> None:
    client = _client()
    page = _response(
        200, {"value": [{"id": "w1", "displayName": "ws"}, {"displayName": "no-id"}]}
    )
    with patch.object(client._session, "request", return_value=page):
        workspaces = list(client.list_workspaces())

    assert [(ws.id, ws.name) for ws in workspaces] == [("w1", "ws")]
    assert len(client.report.api_parse_failures) == 1


def test_list_items_filters_by_type_and_skips_malformed_items() -> None:
    client = _client()
    page = _response(
        200,
        {
            "value": [
                {"id": "i1", "displayName": "pipe", "type": "DataPipeline"},
                {"displayName": "no-id"},
            ]
        },
    )
    with patch.object(client._session, "request", return_value=page) as request:
        items = list(client.list_items("ws-1", item_type="DataPipeline"))

    assert _request_params(request, 0) == {"type": "DataPipeline"}
    assert [(i.id, i.type, i.workspace_id) for i in items] == [
        ("i1", "DataPipeline", "ws-1")
    ]
    assert len(client.report.api_parse_failures) == 1


def test_item_definition_decodes_json_parts_and_skips_undecodable_ones() -> None:
    client = _client()
    good = base64.b64encode(json.dumps({"activities": []}).encode()).decode()
    page = _response(
        200,
        {
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": good},
                    {"path": "broken.json", "payload": "####"},
                ]
            }
        },
    )
    with patch.object(client._session, "request", return_value=page) as request:
        parts = client.get_item_definition("ws-1", "item-1")

    assert request.call_args.kwargs["method"] == "POST"
    assert parts == [{"path": "pipeline-content.json", "content": {"activities": []}}]
    assert len(client.report.api_parse_failures) == 1


def test_item_connections_without_an_id_are_skipped() -> None:
    client = _client()
    page = _response(
        200,
        {
            "value": [
                {
                    "id": "c1",
                    "displayName": "sql-conn",
                    "connectionDetails": {"type": "SQL", "path": "server.example"},
                },
                {"displayName": "automatic-sso"},
            ]
        },
    )
    with patch.object(client._session, "request", return_value=page):
        connections = list(client.list_item_connections("ws-1", "item-1"))

    assert len(connections) == 1
    assert connections[0].connection_type == "SQL"
    assert connections[0].connection_path == "server.example"


def test_job_instances_extract_failure_message_and_skip_malformed() -> None:
    client = _client()
    page = _response(
        200,
        {
            "value": [
                {
                    "id": "j1",
                    "status": "Failed",
                    "failureReason": {"message": "boom", "errorCode": "E1"},
                },
                {"id": "j2", "status": "Completed"},
                {"status": "Completed"},
            ]
        },
    )
    with patch.object(client._session, "request", return_value=page):
        jobs = list(client.list_item_job_instances("ws-1", "item-1"))

    failures: List[Optional[str]] = [job.failure_reason for job in jobs]
    assert [job.id for job in jobs] == ["j1", "j2"]
    assert failures == ["boom", None]
    assert len(client.report.api_parse_failures) == 1


def test_context_manager_closes_the_session() -> None:
    client = _client()
    with patch.object(client._session, "close") as close:
        with client as entered:
            assert entered is client
    close.assert_called_once_with()


def test_auth_failure_surfaces_before_any_request_is_sent() -> None:
    client = _client()
    cast(
        MagicMock, client.auth_helper
    ).get_authorization_header.side_effect = RuntimeError("no token")
    with patch.object(client._session, "request") as request:
        with pytest.raises(RuntimeError):
            list(client.list_workspaces())
    request.assert_not_called()
