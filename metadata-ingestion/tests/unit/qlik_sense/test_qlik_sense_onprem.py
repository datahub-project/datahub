import json
import ssl
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection

ONPREM_CONFIG = {
    "deployment": "on_prem",
    "server": "qlik.example.com",
    "client_cert": "/secrets/client.pem",
    "client_key": "/secrets/client_key.pem",
    "root_cert": "/secrets/root.pem",
}

STREAM = {
    "id": "stream-1",
    "name": "Everyone",
    "createdDate": "2024-01-09T18:05:36Z",
    "modifiedDate": "2024-01-15T11:07:00.333Z",
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
}

APP = {
    "id": "app-1",
    "name": "Sales",
    "description": "",
    "createdDate": "2024-01-09T18:05:36.545Z",
    "modifiedDate": "2024-01-15T11:07:00.333Z",
    "stream": {"id": "stream-1", "name": "Everyone"},
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
}

SHEET_META = {
    "title": "Overview",
    "description": "",
    "id": "sheet-1",
    "ownerId": "user-1",
    "createdDate": "2024-01-15T11:01:49.704Z",
    "modifiedDate": "2024-01-29T12:23:46.868Z",
}


class _Response:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.status_code = 200

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        return None


def _qrs_get(url, params=None, **_):
    assert params is not None
    assert len(params["xrfkey"]) == 16
    if url.endswith("/about"):
        return _Response({"buildVersion": "14.129.3"})
    if url.endswith("/stream/full"):
        return _Response([STREAM])
    if url.endswith("/app/full"):
        assert params["filter"] == "stream.id eq stream-1"
        return _Response([APP])
    raise AssertionError(url)


def _engine_calls():
    return [
        {"qReturn": {"qHandle": 1}},
        {"qList": [{"qInfo": {"qId": "sheet-1", "qType": "sheet"}}]},
        {"qReturn": {"qHandle": 2, "qType": "GenericObject"}},
        {
            "qLayout": {
                "qMeta": SHEET_META,
                "qChildList": {"qItems": []},
            }
        },
    ]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_lists_stream_and_opens_app(mock_session_cls, mock_engine) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = _engine_calls()
    mock_engine.return_value = websocket

    config = QlikSourceConfig.model_validate(ONPREM_CONFIG)
    api = QlikOnPremAPI(config)

    assert session.cert == ("/secrets/client.pem", "/secrets/client_key.pem")
    assert session.verify == "/secrets/root.pem"
    assert session.headers["X-Qlik-Xrfkey"] == api.xrfkey
    assert session.headers["X-Qlik-User"] == (
        "UserDirectory=INTERNAL; UserId=sa_repository"
    )

    spaces = api.get_spaces()
    assert [space.name for space in spaces] == ["Everyone"]
    assert api.get_user_name("user-1") == "Ada"

    items = api.get_items("stream-1")
    assert len(items) == 1
    assert items[0].qTitle == "Sales"
    assert [sheet.title for sheet in items[0].sheets] == ["Overview"]
    mock_engine.assert_called_once()
    assert mock_engine.call_args.kwargs["port"] == 4747
    assert mock_engine.call_args.kwargs["user_header"] == (
        "UserDirectory=INTERNAL; UserId=sa_engine"
    )


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_keeps_app_when_engine_is_unreachable(
    mock_session_cls, mock_engine
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    mock_engine.side_effect = OSError("engine port closed")

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    items = api.get_items("stream-1")

    assert len(items) == 1
    assert items[0].sheets == []


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_onprem_source_uses_onprem_client(mock_cloud, mock_onprem) -> None:
    QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )

    mock_onprem.assert_called_once()
    mock_cloud.assert_not_called()


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_onprem_websocket_skips_notifications(mock_connect) -> None:
    socket = MagicMock()
    socket.recv.side_effect = [
        json.dumps(
            {
                "method": "OnConnected",
                "params": {"qSessionState": "SESSION_CREATED"},
            }
        ),
        json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"qReturn": {"qHandle": 1}}}),
    ]
    mock_connect.return_value = socket

    connection = WebsocketConnection.for_on_prem(
        host="qlik.example.com",
        port=4747,
        app_id="app-1",
        user_header="UserDirectory=INTERNAL; UserId=sa_engine",
        client_cert="/secrets/client.pem",
        client_key="/secrets/client_key.pem",
        root_cert="/secrets/root.pem",
    )
    result = connection.websocket_send_request(
        method="OpenDoc", params={"qDocName": "app-1"}
    )

    assert result["qReturn"]["qHandle"] == 1
    assert mock_connect.call_args.args[0] == "wss://qlik.example.com:4747/app/app-1"
    assert mock_connect.call_args.kwargs["sslopt"]["cert_reqs"] == ssl.CERT_REQUIRED
    assert mock_connect.call_args.kwargs["header"]["X-Qlik-User"].endswith("sa_engine")
