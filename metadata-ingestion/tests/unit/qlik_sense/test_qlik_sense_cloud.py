from typing import List
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import Sheet
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource

CLOUD_CONFIG = {"tenant_hostname": "tenant.example.com", "api_key": "key"}


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_load_model_failure_leaves_app_handle_on_top(
    mock_session_cls: MagicMock,
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )
    websocket = MagicMock()
    websocket.handle = [-1, 1]
    replies: List[object] = [
        {"qReturn": {"qHandle": 2, "qType": "LoadModel"}},
        RuntimeError("Access denied"),
    ]

    def send(method: str, params: object = None) -> object:
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        websocket.handle.append(2)
        return reply

    websocket.websocket_send_request.side_effect = send

    assert api._get_app_used_tables(websocket, "app-1") == []
    assert websocket.handle == [-1, 1]


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_missing_load_model_leaves_app_handle_on_top(
    mock_session_cls: MagicMock,
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )
    websocket = MagicMock()
    websocket.handle = [-1, 1]

    def send(method: str, params: object = None) -> object:
        websocket.handle.append(2)
        return {"qReturn": {"qHandle": 2, "qType": None}}

    websocket.websocket_send_request.side_effect = send

    assert api._get_app_used_tables(websocket, "app-1") == []
    assert websocket.handle == [-1, 1]


@pytest.mark.parametrize(
    "call, unread",
    [
        ("get_spaces", "spaces"),
        ("get_data_connections", "data connections"),
        ("get_items", "items of space space-1"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_failed_request_is_recorded_as_unread(
    mock_session_cls: MagicMock, call: str, unread: str
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    mock_session_cls.return_value.get.side_effect = requests.ConnectionError("down")

    getattr(api, call)(*(["space-1"] if call == "get_items" else []))

    assert api.unread_items == [unread]


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_partly_read_app_is_marked_incomplete(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = MagicMock(tables=[])
    mock_app_cls.model_validate.return_value = app

    def unreadable_sheets(*_: object) -> List[Sheet]:
        api._log_http_error("Unable to fetch sheets")
        return []

    with (
        patch.object(api, "_get_app_sheets", side_effect=unreadable_sheets),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "_get_app_script", return_value=None),
    ):
        assert api._get_app("app-1") is app

    assert app.read_complete is False


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_items_are_reported_as_failures(mock_api_cls: MagicMock) -> None:
    mock_api_cls.return_value.get_spaces.return_value = []
    mock_api_cls.return_value.unread_items = ["spaces", "data connections"]
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    list(source.get_workunits_internal())

    assert [failure.title for failure in source.reporter.failures] == [
        "Unable to read from Qlik"
    ]
