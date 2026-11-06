from typing import List
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI


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


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_close_error_keeps_loaded_app(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )
    mock_websocket_cls.return_value.close_websocket.side_effect = OSError("closed")
    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "_get_app_script", return_value=None),
    ):
        app = api._get_app("app-1")

    assert app is mock_app_cls.model_validate.return_value
