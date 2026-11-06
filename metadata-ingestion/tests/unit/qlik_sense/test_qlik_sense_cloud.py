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
