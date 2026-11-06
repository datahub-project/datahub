import json
import ssl
import threading
import time
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App, Chart, QlikTable
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ScriptTableLineage,
    infer_data_connection,
)
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
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
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
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.exceptions.HTTPError(f"{self.status_code} Client Error")
            error.response = self  # type: ignore[assignment]
            raise error


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

    items = list(api.get_items("stream-1"))
    assert len(items) == 1
    app = items[0]
    assert isinstance(app, App)
    assert app.qTitle == "Sales"
    assert [(s.id, s.title, s.ownerId) for s in app.sheets] == [
        ("sheet-1", "Overview", "user-1")
    ]
    assert app.script == LOAD_SCRIPT
    websocket.close_websocket.assert_called_once()
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
    items = list(api.get_items("stream-1"))

    assert len(items) == 1
    app = items[0]
    assert isinstance(app, App)
    assert app.sheets == []
    assert not app.read_complete

    with patch(
        "datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI",
        return_value=api,
    ):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(ONPREM_CONFIG),
            PipelineContext(run_id="test-onprem"),
        )
        list(source.get_workunits_internal())

    assert [failure.title for failure in source.reporter.failures] == [
        "Unable to fully read Qlik app"
    ]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_closes_engine_session_when_app_cannot_open(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = RuntimeError("Access denied")
    mock_engine.return_value = websocket

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    items = list(api.get_items("stream-1"))

    assert [item.id for item in items] == ["app-1"]
    websocket.close_websocket.assert_called_once()


def _chart_layout(chart_id: str) -> Dict[str, object]:
    return {
        "qLayout": {
            "qInfo": {"qId": chart_id, "qType": "barchart"},
            "visualization": "barchart",
            "title": f"Chart {chart_id}",
            "subtitle": "",
            "qHyperCube": {"qDimensionInfo": [], "qMeasureInfo": []},
        }
    }


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_loads_apps_in_parallel(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    app_ids = [f"app-{i}" for i in range(6)]
    lock = threading.Lock()
    in_flight = 0
    peak = 0

    def qrs_get(
        url: str, params: Optional[Dict[str, str]] = None, **kwargs: object
    ) -> _Response:
        if url.endswith("/app/full"):
            return _Response([{**APP, "id": app_id} for app_id in app_ids])
        return _qrs_get(url, params, **kwargs)

    def get_script() -> str:
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        time.sleep(0.05)
        with lock:
            in_flight -= 1
        return "// script"

    def open_engine(app_id: str, **_: object) -> MagicMock:
        if app_id == "app-2":
            raise OSError("engine session limit")
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = _engine_calls()
        websocket.get_script.side_effect = get_script
        return websocket

    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = qrs_get
    mock_engine.side_effect = open_engine

    api = QlikOnPremAPI(
        QlikSourceConfig.model_validate({**ONPREM_CONFIG, "max_workers": 3})
    )
    api.get_spaces()
    items = {item.id: item for item in api.get_items("stream-1")}

    assert sorted(items) == app_ids
    assert 1 < peak <= 3
    for app_id, item in items.items():
        assert isinstance(item, App)
        expected_sheets = [] if app_id == "app-2" else ["Overview"]
        assert [sheet.title for sheet in item.sheets] == expected_sheets


def test_chart_keeps_dimension_in_error_state() -> None:
    layout = _chart_layout("broken-dimension")["qLayout"]
    layout["qHyperCube"] = {  # type: ignore[index]
        "qDimensionInfo": [
            {"qFallbackTitle": "Region", "qMin": 0, "qMax": 9},
            {"qError": {"qErrorCode": 7005}},
        ],
        "qMeasureInfo": [],
    }

    chart = Chart.model_validate(layout)

    assert [(d.Title, d.Min) for d in chart.qDimension] == [
        ("Region", "0"),
        (None, None),
    ]


def test_chart_without_title_is_kept() -> None:
    layout = _chart_layout("untitled")["qLayout"]
    del layout["title"]  # type: ignore[attr-defined]
    del layout["subtitle"]  # type: ignore[attr-defined]

    chart = Chart.model_validate(layout)

    assert (chart.qId, chart.title, chart.subtitle) == ("untitled", "", "")


class _ScriptedEngine(WebsocketConnection):
    """Replays Engine replies in order, keeping the real handle stack logic."""

    def __init__(self, replies: List[object]) -> None:
        self.replies = replies
        self.handle = [-1]
        self.socket_connection = None
        self.sent_on: List[Tuple[str, int]] = []

    def websocket_send_request(
        self, method: str, params: Optional[Union[Dict, List]] = None
    ) -> Dict:
        self.sent_on.append((method, self.handle[-1]))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        assert isinstance(reply, dict)
        handle = reply.get("qReturn", {}).get("qHandle")
        if handle:
            self.handle.append(handle)
        return reply

    def get_script(self) -> str:
        return LOAD_SCRIPT


def _sheet_replies(chart_ids: List[str]) -> List[object]:
    return [
        {"qReturn": {"qHandle": 1}},
        {"qList": [{"qInfo": {"qId": "sheet-1", "qType": "sheet"}}]},
        {"qReturn": {"qHandle": 2, "qType": "GenericObject"}},
        {
            "qLayout": {
                "qMeta": SHEET_META,
                "qChildList": {
                    "qItems": [{"qInfo": {"qId": chart_id}} for chart_id in chart_ids]
                },
            }
        },
    ]


def _read_onprem_app(
    mock_session_cls: MagicMock, mock_engine: MagicMock, engine: _ScriptedEngine
) -> App:
    mock_engine.return_value = engine
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    (app,) = api.get_items("stream-1")
    assert isinstance(app, App)
    return app


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_reads_charts_after_a_failed_chart(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    engine = _ScriptedEngine(
        _sheet_replies(["broken", "ok"])
        + [
            RuntimeError("Access denied"),
            {"qReturn": {"qHandle": 3, "qType": "GenericObject"}},
            _chart_layout("ok"),
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["ok"]
    assert not app.read_complete
    assert engine.sent_on[4:] == [("GetChild", 2), ("GetChild", 2), ("GetLayout", 3)]
    assert engine.handle == [-1, 1]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_reads_charts_inside_containers(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    untitled = _chart_layout("in-tab")
    untitled["qLayout"]["title"] = ""  # type: ignore[index]
    engine = _ScriptedEngine(
        _sheet_replies(["button", "tabs"])
        + [
            {"qReturn": {"qHandle": 3}},
            {"qLayout": {"qInfo": {"qId": "button", "qType": "action-button"}}},
            {"qReturn": {"qHandle": 4}},
            {
                "qLayout": {
                    "qInfo": {"qId": "tabs", "qType": "container"},
                    "children": [{"refId": "t1", "label": "Monthly"}],
                    "qChildList": {
                        "qItems": [
                            {
                                "qInfo": {"qId": "in-tab"},
                                "qData": {"containerChildId": "t1"},
                            },
                            {"qInfo": {"qId": "titled"}, "qData": {}},
                        ]
                    },
                }
            },
            {"qReturn": {"qHandle": 5}},
            untitled,
            {"qReturn": {"qHandle": 6}},
            _chart_layout("titled"),
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [(c.qId, c.title) for c in app.sheets[0].charts] == [
        ("in-tab", "Monthly"),
        ("titled", "Chart titled"),
    ]
    assert app.read_complete
    assert ("GetChild", 4) in engine.sent_on
    assert engine.handle == [-1, 1]


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


def _script_tables(
    mock_onprem: MagicMock,
    extra_config: Dict,
    script: str = LOAD_SCRIPT,
    connections: Optional[Dict[str, str]] = None,
    tables: Optional[List[QlikTable]] = None,
) -> Tuple[QlikSenseSource, List[ScriptTableLineage]]:
    if connections is None:
        connections = {"Sales SQL": str(DATA_CONNECTION["connectionstring"])}
    mock_onprem.return_value.get_data_connections.return_value = {
        name: infer_data_connection(name, connection_string)
        for name, connection_string in connections.items()
    }
    source = QlikSenseSource(
        QlikSourceConfig.model_validate({**ONPREM_CONFIG, **extra_config}),
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
            "script": script,
        }
    )
    app.tables = tables or []
    return source, source._build_script_tables(app)


@pytest.mark.parametrize(
    "extra_config, expected_upstream",
    [
        ({}, "mssql,sales_db.dbo.orders,PROD"),
        (
            {
                "env": "DEV",
                "data_connection_to_platform_instance": {
                    "Sales SQL": {"database": "sales_db"}
                },
            },
            "mssql,sales_db.dbo.orders,DEV",
        ),
        (
            {
                "env": "DEV",
                "data_connection_to_platform_instance": {"Sales SQL": {"env": "QA"}},
            },
            "mssql,sales_db.dbo.orders,QA",
        ),
        (
            {
                "data_connection_to_platform_instance": {
                    "Sales SQL": {
                        "platform": "postgres",
                        "database": "analytics",
                        "default_schema": "public",
                    }
                }
            },
            "postgres,analytics.dbo.orders,PROD",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_tables_get_upstream_lineage(
    mock_onprem: MagicMock, extra_config: Dict, expected_upstream: str
) -> None:
    _, tables = _script_tables(mock_onprem, extra_config)

    assert [table.identifier for table in tables] == ["app-1.orders"]
    assert tables[0].upstream_urns == [
        f"urn:li:dataset:(urn:li:dataPlatform:{expected_upstream})"
    ]
    assert set(tables[0].column_upstreams) == {"order_id", "amount"}


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_mapping_default_schema_qualifies_bare_tables(
    mock_onprem: MagicMock,
) -> None:
    _, tables = _script_tables(
        mock_onprem,
        {
            "data_connection_to_platform_instance": {
                "Sales SQL": {
                    "platform": "postgres",
                    "database": "analytics",
                    "default_schema": "public",
                }
            }
        },
        script="LIB CONNECT TO 'Sales SQL';\nOrders:\nSQL SELECT order_id FROM orders;",
    )

    assert tables[0].upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"
    ]


def _qlik_table(
    name: str, qri: Optional[str] = None, alias: Optional[str] = None
) -> QlikTable:
    return QlikTable.model_validate(
        {
            "tableName": name,
            "boxType": "blackbox",
            "tableAlias": alias or name,
            "dataconnectorName": "Sales SQL",
            "connectionInfo": {
                "id": "conn-1",
                "sourceConnectorID": "sqlserver",
                "space": "stream-1",
            },
            "fields": [],
            "tableQri": qri,
        }
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_script_skips_tables_the_data_model_already_traces(
    mock_onprem: MagicMock,
) -> None:
    script = (
        "LIB CONNECT TO 'Sales SQL';\n"
        "Orders:\nSQL SELECT order_id FROM dbo.orders;\n"
        "Customers:\nSQL SELECT customer_id FROM dbo.customers;\n"
        "Products:\nSQL SELECT product_id FROM dbo.products;"
    )
    _, tables = _script_tables(
        mock_onprem,
        {},
        script=script,
        tables=[
            _qlik_table("dbo.orders", "qri:db:orders", alias="ORDERS"),
            _qlik_table("Products", "qri:db:products"),
            _qlik_table("Customers"),
        ],
    )

    assert [table.label for table in tables] == ["Customers"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_table_without_known_connection_is_reported(
    mock_onprem: MagicMock,
) -> None:
    source, tables = _script_tables(mock_onprem, {}, connections={})

    assert tables == []
    assert list(source.reporter.unresolved_data_connections) == ["app-1: Sales SQL"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_table_without_upstream_is_skipped(
    mock_onprem: MagicMock,
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script="LIB CONNECT TO 'Sales SQL';\nOne:\nSQL SELECT 1 AS one;",
    )

    assert tables == []
    assert source.reporter.warnings


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_qrs_errors_return_no_streams_or_apps(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}

    def get(url: str, **kwargs: object) -> _Response:
        if url.endswith("/about"):
            return _qrs_get(url, **kwargs)  # type: ignore[arg-type]
        return _Response({}, status_code=401)

    session.get.side_effect = get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))

    assert api.get_spaces() == []
    assert list(api.get_items("stream-1")) == []
    assert api.get_data_connections() == {}


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_qrs_rejecting_the_connection_check_fails_fast(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.return_value = _Response({}, status_code=401)

    with pytest.raises(requests.exceptions.HTTPError):
        QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api._presented_certificate")
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_logs_presented_certificate_when_tls_fails(
    mock_session_cls: MagicMock,
    mock_presented: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = requests.exceptions.SSLError("certificate verify failed")
    mock_presented.return_value = "-----BEGIN CERTIFICATE-----"

    with pytest.raises(requests.exceptions.SSLError):
        QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))

    mock_presented.assert_called_once_with(
        "qlik.example.com", 4242, "/secrets/client.pem", "/secrets/client_key.pem"
    )
    assert "-----BEGIN CERTIFICATE-----" in caplog.text


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_verify_server_hostname_false_skips_name_checks(
    mock_session_cls: MagicMock, mock_connect: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    config = QlikSourceConfig.model_validate(
        {**ONPREM_CONFIG, "verify_server_hostname": False}
    )

    api = QlikOnPremAPI(config)
    api._open_engine("app-1")

    assert config.tls_assert_hostname() is False
    assert session.verify == "/secrets/root.pem"
    _, adapter = session.mount.call_args.args
    assert adapter._assert_hostname is False
    sslopt = mock_connect.call_args.kwargs["sslopt"]
    assert sslopt["check_hostname"] is False
    assert sslopt["cert_reqs"] == ssl.CERT_REQUIRED


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


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
def test_cloud_app_read_failure_closes_engine_session(
    mock_engine: MagicMock, mock_session_cls: MagicMock
) -> None:
    websocket = mock_engine.return_value
    websocket.websocket_send_request.side_effect = RuntimeError("Access denied")
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )

    assert (
        api._load_item(
            {Constant.RESOURCETYPE: Constant.APP, Constant.RESOURCEID: "app-1"}
        )
        is None
    )
    websocket.close_websocket.assert_called_once()
    assert api.unread_items == ["app-1"]


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
