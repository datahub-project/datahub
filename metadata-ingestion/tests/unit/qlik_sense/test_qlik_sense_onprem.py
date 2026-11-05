import json
import ssl
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import infer_data_connection
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


LOAD_SCRIPT = """
LIB CONNECT TO 'Sales SQL';
Orders:
SQL SELECT order_id, amount FROM dbo.orders;
"""

DATA_CONNECTION = {
    "name": "Sales SQL",
    "connectionstring": (
        "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Data Source=db.example.com;"
        "Initial Catalog=sales_db]"
    ),
    "type": "OLEDB",
}


class _Response:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.status_code = 200

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        return None


def _qrs_get(
    url: str, params: Optional[Dict[str, str]] = None, **_: object
) -> _Response:
    assert params is not None
    assert len(params["xrfkey"]) == 16
    if url.endswith("/about"):
        return _Response({"buildVersion": "14.129.3"})
    if url.endswith("/stream/full"):
        return _Response([STREAM])
    if url.endswith("/app/full"):
        assert params["filter"] == "stream.id eq stream-1"
        return _Response([APP])
    if url.endswith("/dataconnection/full"):
        return _Response([DATA_CONNECTION])
    raise AssertionError(url)


def _engine_calls() -> List[Dict[str, object]]:
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
def test_onprem_lists_stream_and_opens_app(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = _engine_calls()
    websocket.get_script.return_value = LOAD_SCRIPT
    mock_engine.return_value = websocket

    config = QlikSourceConfig.model_validate(ONPREM_CONFIG)
    api = QlikOnPremAPI(config)

    assert session.cert == ("/secrets/client.pem", "/secrets/client_key.pem")
    assert session.verify == "/secrets/root.pem"
    assert session.get.call_args.kwargs["verify"] == "/secrets/root.pem"
    assert session.headers["X-Qlik-Xrfkey"] == api.xrfkey
    assert session.headers["X-Qlik-User"] == (
        "UserDirectory=INTERNAL; UserId=sa_repository"
    )

    spaces = api.get_spaces()
    assert [space.name for space in spaces] == ["Everyone"]
    assert api.get_user_name("user-1") == "Ada"

    items = api.get_items("stream-1")
    assert len(items) == 1
    app = items[0]
    assert isinstance(app, App)
    assert app.qTitle == "Sales"
    assert [sheet.title for sheet in app.sheets] == ["Overview"]
    assert app.script == LOAD_SCRIPT
    open_doc = websocket.websocket_send_request.call_args_list[0].kwargs
    assert open_doc["params"]["qNoData"] is True
    connection = api.get_data_connections()["Sales SQL"]
    assert (connection.platform, connection.database) == ("mssql", "sales_db")
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
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    mock_engine.side_effect = OSError("engine port closed")

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    items = api.get_items("stream-1")

    assert len(items) == 1
    app = items[0]
    assert isinstance(app, App)
    assert app.sheets == []


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_onprem_source_uses_onprem_client(
    mock_cloud: MagicMock, mock_onprem: MagicMock
) -> None:
    QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )

    mock_onprem.assert_called_once()
    mock_cloud.assert_not_called()


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_tables_get_upstream_lineage(mock_onprem: MagicMock) -> None:
    mock_onprem.return_value.get_data_connections.return_value = {
        "Sales SQL": infer_data_connection(
            "Sales SQL", str(DATA_CONNECTION["connectionstring"])
        )
    }
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "qUsage": "ANALYTICS",
            "ownerId": "user-1",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "stream-1",
            "script": LOAD_SCRIPT,
        }
    )

    tables = source._build_script_tables(app)

    assert [table.identifier for table in tables] == ["app-1.orders"]
    assert tables[0].upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.orders,PROD)"
    ]
    assert set(tables[0].column_upstreams) == {"order_id", "amount"}


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_server_cert_hostname_overrides_tls_name(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    config = QlikSourceConfig.model_validate(
        {**ONPREM_CONFIG, "server_cert_hostname": "qlik-central.example.com"}
    )

    api = QlikOnPremAPI(config)

    prefix, adapter = session.mount.call_args.args
    assert prefix == api.qrs_url
    assert adapter._assert_hostname == "qlik-central.example.com"
    assert config.ui_hostname() == "qlik-central.example.com"


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_onprem_websocket_uses_cert_hostname(mock_connect: MagicMock) -> None:
    mock_connect.return_value = MagicMock()

    WebsocketConnection.for_on_prem(
        host="10.0.0.10",
        port=4747,
        app_id="app-1",
        user_header="UserDirectory=INTERNAL; UserId=sa_engine",
        client_cert="/secrets/client.pem",
        client_key="/secrets/client_key.pem",
        root_cert="/secrets/root.pem",
        assert_hostname="qlik-central.example.com",
    )

    sslopt = mock_connect.call_args.kwargs["sslopt"]
    assert sslopt["server_hostname"] == "qlik-central.example.com"
    assert sslopt["cert_reqs"] == ssl.CERT_REQUIRED


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_cloud_websocket_raises_engine_error(mock_connect: MagicMock) -> None:
    socket = MagicMock()
    socket.recv.return_value = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": 5, "message": "Access denied"}}
    )
    mock_connect.return_value = socket
    connection = WebsocketConnection("tenant.example.com", "key", "app-1")
    connection.handle.append(1)

    with pytest.raises(RuntimeError, match="Access denied"):
        connection.get_script()


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_onprem_websocket_skips_notifications(mock_connect: MagicMock) -> None:
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
