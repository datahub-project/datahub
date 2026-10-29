"""BaseFabricClient / FabricCoreClient against a stubbed HTTP session: pagination,
error accounting, and the per-endpoint response parsing the connectors rely on."""

import base64
import json
from typing import Dict, List, Optional
from unittest.mock import NonCallableMagicMock, patch

import pytest
import requests

from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient
from tests.unit.fabric_common.conftest import fabric_response


def test_paginate_follows_continuation_tokens(core_client: FabricCoreClient) -> None:
    """A dropped continuation token would silently truncate every Fabric
    listing to its first page."""
    pages = iter(
        [
            fabric_response(
                200, {"value": [{"id": "w1"}], "continuationToken": "tok-1"}
            ),
            fabric_response(200, {"value": [{"id": "w2"}]}),
        ]
    )
    # The client reuses one params dict across pages, so snapshot each call.
    sent_params: List[Dict[str, str]] = []

    def _request(**kwargs: object) -> requests.Response:
        params = kwargs["params"]
        assert isinstance(params, dict)
        sent_params.append(dict(params))
        return next(pages)

    with patch.object(core_client._session, "request", side_effect=_request):
        ids = [ws.id for ws in core_client.list_workspaces()]

    assert ids == ["w1", "w2"]
    assert sent_params == [{}, {"continuationToken": "tok-1"}]
    assert core_client.report.request_count == 2


def test_paginate_post_handles_dict_and_bare_list_pages(
    core_client: FabricCoreClient,
) -> None:
    """POST listings answer either `{"value": [...]}` or a bare list; the
    continuation token goes back in the JSON body, not the query string."""
    pages = [
        fabric_response(200, {"value": [{"id": 1}], "continuationToken": "tok-1"}),
        fabric_response(200, [{"id": 2}]),
    ]
    with patch.object(core_client._session, "request", side_effect=pages) as request:
        items = list(core_client._paginate_post("workspaces/ws/query", {"filter": "x"}))

    assert [item["id"] for item in items] == [1, 2]
    assert request.call_args_list[1].kwargs["json"]["continuationToken"] == "tok-1"


def test_http_error_is_raised_and_counted(core_client: FabricCoreClient) -> None:
    with (
        patch.object(
            core_client._session, "request", return_value=fabric_response(403)
        ),
        pytest.raises(requests.HTTPError),
    ):
        list(core_client.list_workspaces())

    assert core_client.report.request_count == 1
    assert core_client.report.error_count == 1


def test_transport_error_is_raised_and_counted(core_client: FabricCoreClient) -> None:
    with (
        patch.object(
            core_client._session,
            "request",
            side_effect=requests.ConnectionError("unreachable"),
        ),
        pytest.raises(requests.ConnectionError),
    ):
        list(core_client.list_workspaces())

    assert core_client.report.error_count == 1


def test_malformed_workspace_is_skipped_and_reported(
    core_client: FabricCoreClient,
) -> None:
    page = fabric_response(
        200, {"value": [{"id": "w1", "displayName": "ws"}, {"displayName": "no-id"}]}
    )
    with patch.object(core_client._session, "request", return_value=page):
        workspaces = list(core_client.list_workspaces())

    assert [(ws.id, ws.name) for ws in workspaces] == [("w1", "ws")]
    assert len(core_client.report.api_parse_failures) == 1


def test_list_items_filters_by_type_and_skips_malformed_items(
    core_client: FabricCoreClient,
) -> None:
    page = fabric_response(
        200,
        {
            "value": [
                {"id": "i1", "displayName": "pipe", "type": "DataPipeline"},
                {"displayName": "no-id"},
            ]
        },
    )
    with patch.object(core_client._session, "request", return_value=page) as request:
        items = list(core_client.list_items("ws-1", item_type="DataPipeline"))

    assert request.call_args_list[0].kwargs["params"] == {"type": "DataPipeline"}
    assert [(i.id, i.type, i.workspace_id) for i in items] == [
        ("i1", "DataPipeline", "ws-1")
    ]
    assert len(core_client.report.api_parse_failures) == 1


def test_item_definition_decodes_json_parts_and_skips_undecodable_ones(
    core_client: FabricCoreClient,
) -> None:
    """Definition parts arrive base64-encoded; one undecodable part must not
    cost the pipeline its other parts."""
    good = base64.b64encode(json.dumps({"activities": []}).encode()).decode()
    page = fabric_response(
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
    with patch.object(core_client._session, "request", return_value=page) as request:
        parts = core_client.get_item_definition("ws-1", "item-1")

    assert request.call_args.kwargs["method"] == "POST"
    assert parts == [{"path": "pipeline-content.json", "content": {"activities": []}}]
    assert len(core_client.report.api_parse_failures) == 1


def test_item_connections_without_an_id_are_skipped(
    core_client: FabricCoreClient,
) -> None:
    page = fabric_response(
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
    with patch.object(core_client._session, "request", return_value=page):
        connections = list(core_client.list_item_connections("ws-1", "item-1"))

    assert [(c.connection_type, c.connection_path) for c in connections] == [
        ("SQL", "server.example")
    ]


def test_job_instances_extract_failure_message_and_skip_malformed(
    core_client: FabricCoreClient,
) -> None:
    page = fabric_response(
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
    with patch.object(core_client._session, "request", return_value=page):
        jobs = list(core_client.list_item_job_instances("ws-1", "item-1"))

    failures: List[Optional[str]] = [job.failure_reason for job in jobs]
    assert [job.id for job in jobs] == ["j1", "j2"]
    assert failures == ["boom", None]
    assert len(core_client.report.api_parse_failures) == 1


def test_context_manager_closes_the_session(core_client: FabricCoreClient) -> None:
    with patch.object(core_client._session, "close") as close:
        with core_client as entered:
            assert entered is core_client
    close.assert_called_once_with()


def test_auth_failure_surfaces_before_any_request_is_sent(
    core_client: FabricCoreClient, auth_helper: NonCallableMagicMock
) -> None:
    """A token failure must not turn into an unauthenticated request."""
    auth_helper.get_authorization_header.side_effect = RuntimeError("no token")
    with patch.object(core_client._session, "request") as request:
        with pytest.raises(RuntimeError):
            list(core_client.list_workspaces())
    request.assert_not_called()
