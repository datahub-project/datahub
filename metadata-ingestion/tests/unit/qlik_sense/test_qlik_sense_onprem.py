import json
import ssl
import threading
import time
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Chart,
    QlikTable,
    SchemaField,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import (
    UNPUBLISHED_APPS_NAME,
    QlikOnPremAPI,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ScriptTableLineage,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    InputFieldsClass,
    SubTypesClass,
    UpstreamLineageClass,
)

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
        # A failed read must not leak into other apps read on the same thread.
        assert item.read_complete is (app_id != "app-2")


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unexpected_app_error_records_app_id(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = _qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()

    with patch.object(api, "_read_app", side_effect=RuntimeError("boom")):
        assert list(api.get_items("stream-1")) == []

    assert api.unread_items == [APP["id"]]


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
    # Neither dimension lists its fields, so the chart may read any of them.
    assert not chart.fields_complete


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


def _sheet_replies(
    chart_ids: List[str], chart_properties: Optional[List[Dict]] = None
) -> List[object]:
    # Without chart_properties the property tree is empty, so each chart's
    # properties are read on their own.
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
        {
            "qPropEntry": {
                "qProperty": {"qInfo": {"qId": "sheet-1"}},
                "qChildren": [
                    {"qProperty": prop, "qChildren": []}
                    for prop in chart_properties or []
                ],
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
            {"qProp": {}},
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["ok"]
    assert not app.read_complete
    assert engine.sent_on[4:] == [
        ("GetFullPropertyTree", 2),
        ("GetChild", 2),
        ("GetChild", 2),
        ("GetLayout", 3),
        ("GetProperties", 3),
    ]
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
            {"qProp": {}},
            {"qReturn": {"qHandle": 6}},
            _chart_layout("titled"),
            {"qProp": {}},
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


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_does_not_open_objects_that_are_not_charts(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    engine = _ScriptedEngine(
        _sheet_replies(
            ["filters", "sales"],
            [
                {"qInfo": {"qId": "filters", "qType": "filterpane"}},
                {"qInfo": {"qId": "sales"}, "qHyperCubeDef": {"qMeasures": []}},
            ],
        )
        + [{"qReturn": {"qHandle": 3}}, _chart_layout("sales")]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["sales"]
    assert app.read_complete
    assert [sent for sent in engine.sent_on if sent[0] == "GetChild"] == [
        ("GetChild", 2)
    ]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_reads_linked_master_visualization(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    layout = _chart_layout("linked")
    layout["qLayout"]["qHyperCube"]["qMeasureInfo"] = [  # type: ignore[index]
        {"qFallbackTitle": "Sales"}
    ]
    engine = _ScriptedEngine(
        _sheet_replies(
            ["linked"],
            [{"qInfo": {"qId": "linked"}, "qExtendsId": "master-viz"}],
        )
        + [
            {"qReturn": {"qHandle": 3}},
            layout,
            {
                "qProp": {
                    "qHyperCubeDef": {"qMeasures": [{"qDef": {"qDef": "Sum(Sales)"}}]}
                }
            },
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    (chart,) = app.sheets[0].charts
    assert chart.field_expressions == ["Sum(Sales)"]
    assert chart.fields_complete
    assert ("GetEffectiveProperties", 3) in engine.sent_on


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_container_that_holds_itself_is_read_once(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    container = {
        "qLayout": {
            "qInfo": {"qId": "tabs", "qType": "container"},
            "qChildList": {
                "qItems": [{"qInfo": {"qId": "tabs"}}, {"qInfo": {"qId": "chart"}}]
            },
        }
    }
    engine = _ScriptedEngine(
        _sheet_replies(["tabs"])
        + [
            {"qReturn": {"qHandle": 3}},
            container,
            {"qReturn": {"qHandle": 4}},
            _chart_layout("chart"),
            {"qProp": {}},
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["chart"]
    assert app.read_complete
    assert [sent for sent in engine.sent_on if sent[0] == "GetChild"] == [
        ("GetChild", 2),
        ("GetChild", 3),
    ]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_reads_chart_field_expressions(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    layout = _chart_layout("sales")
    layout["qLayout"]["qHyperCube"] = {  # type: ignore[index]
        "qDimensionInfo": [{"qFallbackTitle": "Region", "qGroupFieldDefs": ["Region"]}],
        "qMeasureInfo": [{"qFallbackTitle": "Sales"}, {"qFallbackTitle": "Orders"}],
    }
    engine = _ScriptedEngine(
        _sheet_replies(
            ["sales", "count"],
            [
                {
                    "qInfo": {"qId": "sales"},
                    "qHyperCubeDef": {
                        "qMeasures": [
                            {"qDef": {"qDef": "Sum([Net Sales])"}},
                            {"qLibraryId": "orders", "qDef": {"qDef": ""}},
                        ]
                    },
                },
                {
                    "qInfo": {"qId": "count"},
                    "qHyperCubeDef": {"qMeasures": [{"qLibraryId": "orders"}]},
                },
            ],
        )
        + [
            {"qReturn": {"qHandle": 3}},
            layout,
            {"qReturn": {"qHandle": 5}},
            _chart_layout("count"),
            {"qReturn": {"qHandle": 4, "qType": "GenericObject"}},
            {
                "qLayout": {
                    "qMeasureList": {
                        "qItems": [
                            {
                                "qInfo": {"qId": "orders"},
                                "qData": {"measure": {"qDef": "Count(order_id)"}},
                            }
                        ]
                    }
                }
            },
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.field_expressions for chart in app.sheets[0].charts] == [
        ["Region", "Sum([Net Sales])", "Count(order_id)"],
        ["Count(order_id)"],
    ]
    assert app.read_complete
    assert not {"GetProperties", "GetMeasure"} & {
        method for method, _ in engine.sent_on
    }
    assert engine.handle == [-1, 1]


def test_chart_input_fields_match_app_fields() -> None:
    orders = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    customers = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.customers,PROD)"
    app_fields = QlikSenseSource._index_app_fields(
        {
            orders: [SchemaField(name="order_id"), SchemaField(name="Net Sales")],
            customers: [
                SchemaField(name="order_id"),
                SchemaField(name="Region"),
                SchemaField(name="Segment"),
            ],
        }
    )
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.field_expressions = [
        "Net Sales",
        "=Upper(region)",
        "Sum({<Region={\"Segment\", 'Net Sales'}>} [Net Sales]) / Count(ORDER_ID)",
        # Segment is called as a function here, not read as a field.
        "=Sum(Segment (order_id))",
    ]

    assert [
        (field.dataset_urn, field.field.name)
        for field in QlikSenseSource._get_chart_input_fields(chart, app_fields)
    ] == [
        (orders, "Net Sales"),
        (customers, "Region"),
        (orders, "order_id"),
        (customers, "order_id"),
    ]


@pytest.mark.parametrize(
    "expressions, fields_complete, unknown_schema, matched_fields, keeps_all_tables",
    [
        pytest.param(
            ["Sum(unknown_field)"], True, False, [], True, id="no_field_matches"
        ),
        pytest.param(
            ["Region", "Sum($(vSales))"],
            True,
            False,
            ["Region"],
            True,
            id="variable_measure",
        ),
        pytest.param(
            ["Count({<Year={$(vYear)}>} Region)"],
            True,
            False,
            ["Region"],
            False,
            id="variable_in_set_modifier",
        ),
        pytest.param(
            ["Count({<Year={$(=Only({1} Year))}>} Region)"],
            True,
            False,
            ["Region"],
            False,
            id="nested_expansion_in_set_modifier",
        ),
        pytest.param(["Region"], False, False, ["Region"], True, id="unread_measure"),
        pytest.param(
            ["Region", "Sum(amount)"],
            True,
            True,
            ["Region"],
            True,
            id="unmatched_name_with_unknown_schema",
        ),
        pytest.param(
            ["Region"],
            True,
            True,
            ["Region"],
            False,
            id="all_names_matched_with_unknown_schema",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_chart_inputs_when_fields_are_unknown(
    mock_onprem: MagicMock,
    expressions: List[str],
    fields_complete: bool,
    unknown_schema: bool,
    matched_fields: List[str],
    keeps_all_tables: bool,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.field_expressions = expressions
    chart.fields_complete = fields_complete
    app_tables = [
        "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.customers,PROD)",
    ]
    # A SQL SELECT * script table has no known columns.
    app_fields = QlikSenseSource._index_app_fields(
        {app_tables[0]: [], app_tables[1]: [SchemaField(name="Region")]}
    )

    workunits = list(
        source._gen_charts_workunit(
            [chart], app_tables, app_fields, unknown_schema, "app-1"
        )
    )
    (chart_info,) = filter(
        None, (wu.get_aspect_of_type(ChartInfoClass) for wu in workunits)
    )
    (input_fields,) = filter(
        None, (wu.get_aspect_of_type(InputFieldsClass) for wu in workunits)
    )

    assert chart_info.inputs == (app_tables if keeps_all_tables else app_tables[1:])
    assert [
        field.schemaField.fieldPath
        for field in input_fields.fields
        if field.schemaField
    ] == matched_fields


@pytest.mark.parametrize(
    "reply, fields_complete",
    [
        pytest.param(RuntimeError("Access denied"), False, id="properties_unread"),
        pytest.param(
            {"qProp": {"qHyperCubeDef": {"qMeasures": [{"qDef": {}}]}}},
            False,
            id="measure_without_definition",
        ),
        pytest.param(
            {
                "qProp": {
                    "qHyperCubeDef": {"qMeasures": [{"qDef": {"qDef": "Sum(Sales)"}}]}
                }
            },
            True,
            id="measure_expression",
        ),
    ],
)
def test_measure_definitions_mark_unread_measures(
    reply: object, fields_complete: bool
) -> None:
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    engine = _ScriptedEngine([reply])

    engine._add_measure_definitions(chart, None, "sheet-1", on_error=lambda _: None)

    assert chart.fields_complete is fields_complete


@pytest.mark.parametrize(
    "replies",
    [
        pytest.param(
            [
                {"qReturn": {"qHandle": 3}},
                {"qLayout": {"qMeasureList": {"qItems": []}}},
            ],
            id="missing_from_list",
        ),
        pytest.param(
            [
                RuntimeError("Session objects not allowed"),
                {"qReturn": {"qType": "GenericMeasure", "qHandle": None}},
            ],
            id="no_handle",
        ),
    ],
)
def test_deleted_master_measure_is_not_a_read_error(replies: List[object]) -> None:
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.measure_library_ids = ["deleted"]
    engine = _ScriptedEngine(replies)
    errors: List[str] = []

    engine.add_master_measure_definitions([MagicMock(charts=[chart])], errors.append)

    assert errors == []
    assert not chart.fields_complete
    assert engine.replies == []
    assert engine.handle == [-1]


def test_unread_master_measure_marks_chart_fields_incomplete() -> None:
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.measure_library_ids = ["measure-1", "measure-2"]
    engine = _ScriptedEngine(
        [
            RuntimeError("Session objects not allowed"),
            RuntimeError("Access denied"),
            {"qReturn": {"qHandle": 3}},
            {"qLayout": {"qMeasure": {"qDef": "Sum(Sales)"}}},
        ]
    )

    engine.add_master_measure_definitions(
        [MagicMock(charts=[chart])], on_error=lambda _: None
    )

    assert "Sum(Sales)" in chart.field_expressions
    assert not chart.fields_complete


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


@pytest.mark.parametrize(
    "sql, warns",
    [
        ("SELECT 1 AS one", False),
        ("SELECT FROM WHERE", True),
        (
            'SELECT "id" FROM JSON (wrap on) "root" PK "__KEY_root" '
            'WITH CONNECTION (URL "https://example.com/api")',
            False,
        ),
    ],
    ids=["no_tables", "unparsable", "connector_sql"],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_table_without_upstream_is_skipped(
    mock_onprem: MagicMock, sql: str, warns: bool
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script=f"LIB CONNECT TO 'Sales SQL';\nOne:\nSQL {sql};",
    )

    assert tables == []
    assert bool(source.reporter.warnings) == warns


@pytest.mark.parametrize(
    "condition",
    [
        "ON o.id = d.order_id\r\n$(vWhere)\r\nAND (d.code LIKE 'A%')",
        "ON o.id = d.order_id WHERE $(vWhere) AND d.code LIKE 'A%'",
    ],
    ids=["after_join", "after_where"],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_unset_condition_variable_keeps_lineage(
    mock_onprem: MagicMock, condition: str
) -> None:
    _, tables = _script_tables(
        mock_onprem,
        {},
        script=(
            "LIB CONNECT TO 'Sales SQL';\nOrders:\nSQL SELECT o.id, d.code "
            f"FROM sales_db.dbo.orders o LEFT JOIN sales_db.dbo.details d {condition};"
        ),
    )

    assert sorted(tables[0].upstream_urns) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.details,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.orders,PROD)",
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_unset_variable_in_table_name_is_skipped(
    mock_onprem: MagicMock,
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script=(
            "LIB CONNECT TO 'Sales SQL';\nOrders:\n"
            "SQL SELECT id FROM sales_db.dbo.orders_$(vYear) WHERE id > 1 AND id < 9;"
        ),
    )

    assert tables == []
    assert source.reporter.warnings


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_bracket_identifiers_parse_on_backtick_platforms(
    mock_onprem: MagicMock,
) -> None:
    _, tables = _script_tables(
        mock_onprem,
        {
            "data_connection_to_platform_instance": {
                "Sales SQL": {"platform": "databricks", "database": "main"}
            }
        },
        script=(
            "LIB CONNECT TO 'Sales SQL';\nAccounts:\n"
            "SQL SELECT account_id AS [Account id], tags[0] AS first_tag "
            "FROM `sales`.`accounts`;"
        ),
    )

    assert tables[0].upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:databricks,main.sales.accounts,PROD)"
    ]


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
    connection = WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")
    connection.handle.append(1)

    with pytest.raises(RuntimeError, match="Access denied"):
        connection.get_script()


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
def test_cloud_app_read_failure_closes_engine_session(
    mock_engine: MagicMock, mock_session_cls: MagicMock
) -> None:
    websocket = mock_engine.for_cloud.return_value
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


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_tables_workunits(mock_onprem: MagicMock) -> None:
    source, tables = _script_tables(mock_onprem, {})

    workunits = list(source._gen_script_tables_workunit(tables, "app-1"))

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.orders,PROD)"
    (lineage,) = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(UpstreamLineageClass))
    ]
    assert [upstream.dataset for upstream in lineage.upstreams] == [upstream_urn]
    assert sorted(
        (fgl.upstreams or [], fgl.downstreams or [])
        for fgl in lineage.fineGrainedLineages or []
    ) == [
        (
            [f"urn:li:schemaField:({upstream_urn},amount)"],
            [f"urn:li:schemaField:({dataset_urn},amount)"],
        ),
        (
            [f"urn:li:schemaField:({upstream_urn},order_id)"],
            [f"urn:li:schemaField:({dataset_urn},order_id)"],
        ),
    ]
    (subtypes,) = [
        subtype for wu in workunits if (subtype := wu.get_aspect_of_type(SubTypesClass))
    ]
    assert subtypes.typeNames == [DatasetSubTypes.QLIK_DATASET]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_bare_connection_name_is_not_guessed(
    mock_onprem: MagicMock,
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script="LIB CONNECT TO 'Snowflake_Finance';\nOrders:\nSQL SELECT id FROM db.s.orders;",
    )

    assert tables == []
    assert list(source.reporter.unresolved_data_connections) == [
        "app-1: Snowflake_Finance"
    ]


INLINE_CREDENTIALS = "UID=svc_reader;PWD=hunter2-secret"


@pytest.mark.parametrize(
    "driver, resolves",
    [("ODBC Driver 17 for SQL Server", True), ("Unknown Driver", False)],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_inline_connection_credentials_are_not_emitted(
    mock_onprem: MagicMock, driver: str, resolves: bool
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script=(
            f"ODBC CONNECT TO [Driver={{{driver}}};Server=db.example.com;"
            f"Database=sales_db;{INLINE_CREDENTIALS}];\n"
            "Orders:\nSQL SELECT order_id FROM dbo.orders;"
        ),
        connections={},
    )

    workunits = list(source._gen_script_tables_workunit(tables, "app-1"))
    emitted = json.dumps([wu.metadata.to_obj() for wu in workunits])  # type: ignore[union-attr]
    reported = json.dumps(source.reporter.as_obj(), default=str)

    assert bool(tables) == resolves
    for secret in ("svc_reader", "hunter2-secret"):
        assert secret not in emitted
        assert secret not in reported
    assert "Database=sales_db" in (emitted if resolves else reported)


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_onprem_script_table_without_database_is_dropped(
    mock_onprem: MagicMock,
) -> None:
    source, tables = _script_tables(
        mock_onprem,
        {},
        script=(
            "LIB CONNECT TO 'Sales SQL';\n"
            "Orders:\nSQL SELECT o.id FROM orders o JOIN sales_db.dbo.items i ON o.id = i.id;\n"
            "Customers:\nSQL SELECT id FROM customers;"
        ),
    )

    assert [(table.label, table.upstream_urns) for table in tables] == [
        (
            "Orders",
            ["urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.items,PROD)"],
        )
    ]
    assert [warning.title for warning in source.reporter.warnings] == [
        "Qlik load script table is missing its database or schema"
    ]


@pytest.mark.parametrize("ingest_unpublished", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unpublished_apps(
    mock_session_cls: MagicMock, ingest_unpublished: bool
) -> None:
    filters: List[str] = []

    def get(
        url: str, params: Optional[Dict[str, str]] = None, **_: object
    ) -> _Response:
        if url.endswith("/app/full"):
            assert params is not None
            filters.append(params["filter"])
            return _Response([])
        return _qrs_get(url, params)

    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = get
    api = QlikOnPremAPI(
        QlikSourceConfig.model_validate(
            {**ONPREM_CONFIG, "ingest_unpublished_apps": ingest_unpublished}
        )
    )

    spaces = api.get_spaces()
    for space in spaces:
        list(api.get_items(space.id))

    assert [space.name for space in spaces] == (
        ["Everyone", UNPUBLISHED_APPS_NAME] if ingest_unpublished else ["Everyone"]
    )
    assert filters == (
        ["stream.id eq stream-1", "published eq false"]
        if ingest_unpublished
        else ["stream.id eq stream-1"]
    )
