import json
import ssl
import threading
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
    Sheet,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import (
    UNPUBLISHED_APPS_NAME,
    QlikOnPremAPI,
    _QrsAdapter,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ScriptTableLineage,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    EngineError,
    WebsocketConnection,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ChartInfoClass,
    DashboardInfoClass,
    InputFieldsClass,
    MetadataChangeProposalClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from tests.unit.qlik_sense.qlik_fakes import (
    APP,
    DATA_CONNECTION,
    LOAD_SCRIPT,
    ON_PREM,
    ONPREM_CONFIG,
    SHEET_META,
    STREAM,
    FakeResponse,
    fake_engine_calls,
    fake_qrs_get,
)


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_lists_stream_and_opens_app(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = fake_engine_calls()
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
    # The user id joins with users ingested from AD or LDAP; the name would not.
    assert api.get_user_name("user-1") == "ada"

    items = list(api.get_items(["stream-1"]))
    assert len(items) == 1
    app = items[0]
    assert isinstance(app, App)
    assert app.qTitle == "Sales"
    assert [(s.id, s.title, s.ownerId) for s in app.sheets] == [
        ("sheet-1", "Overview", "user-1")
    ]
    assert app.script == LOAD_SCRIPT
    assert not app.tables_read_failed
    websocket.close_websocket.assert_called_once()
    open_doc = websocket.websocket_send_request.call_args_list[0].kwargs
    assert open_doc["params"]["qNoData"] is True
    connection = api.get_data_connections()["Sales SQL"]
    assert (connection.platform, connection.database) == ("mssql", "sales_db")
    mock_engine.assert_called_once()
    assert mock_engine.call_args.kwargs["port"] == 4747
    assert mock_engine.call_args.kwargs["timeout"] == 300
    assert {call.kwargs["timeout"] for call in session.get.call_args_list} == {300}
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
    session.get.side_effect = fake_qrs_get
    mock_engine.side_effect = OSError("engine port closed")

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    items = list(api.get_items(["stream-1"]))

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
    session.get.side_effect = fake_qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = RuntimeError("Access denied")
    mock_engine.return_value = websocket

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    items = list(api.get_items(["stream-1"]))

    assert [item.id for item in items] == ["app-1"]
    assert isinstance(items[0], App) and not items[0].sheets_complete
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
    ) -> FakeResponse:
        if url.endswith("/app/full"):
            return FakeResponse([{**APP, "id": app_id} for app_id in app_ids])
        return fake_qrs_get(url, params, **kwargs)

    # The first read waits until a second one is running, so the reads only
    # finish when they really overlap.
    overlapping = threading.Event()

    def get_script() -> str:
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
            if in_flight > 1:
                overlapping.set()
        overlapping.wait(timeout=10)
        with lock:
            in_flight -= 1
        return "// script"

    def open_engine(app_id: str, **_: object) -> MagicMock:
        if app_id == "app-2":
            raise OSError("engine session limit")
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = fake_engine_calls()
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
    items = {item.id: item for item in api.get_items(["stream-1"])}

    assert sorted(items) == app_ids
    assert overlapping.is_set()
    assert 1 < peak <= 3
    for app_id, item in items.items():
        assert isinstance(item, App)
        expected_sheets = [] if app_id == "app-2" else ["Overview"]
        assert [sheet.title for sheet in item.sheets] == expected_sheets
        # A failed read must not leak into other apps read on the same thread.
        assert item.read_complete is (app_id != "app-2")


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_reads_apps_of_different_streams_in_parallel(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    streams = [
        {**STREAM, "id": "stream-1"},
        {**STREAM, "id": "stream-2", "name": "Finance"},
        {**STREAM, "id": "stream-3", "name": "Sales"},
    ]

    def qrs_get(
        url: str, params: Optional[Dict[str, str]] = None, **kwargs: object
    ) -> FakeResponse:
        if url.endswith("/stream/full"):
            return FakeResponse(streams)
        if url.endswith("/app/full"):
            assert params is not None
            stream_id = params["filter"].rsplit(" ", 1)[-1]
            if stream_id == "stream-3":
                return FakeResponse({}, status_code=503)
            count = 5 if stream_id == "stream-1" else 1
            return FakeResponse(
                [
                    {
                        **APP,
                        "id": f"app-{i}-of-{stream_id}",
                        "stream": {"id": stream_id},
                    }
                    for i in range(count)
                ]
            )
        return fake_qrs_get(url, params, **kwargs)

    # Apps of the large stream-1 only finish once the app of stream-2 has
    # started, so the reads fail if stream-2 waits behind all of stream-1.
    other_stream_started = threading.Event()

    def get_script(app_id: str) -> str:
        if app_id.endswith("stream-2"):
            other_stream_started.set()
        elif not other_stream_started.wait(timeout=10):
            raise TimeoutError(app_id)
        return ""

    def open_engine(app_id: str, **_: object) -> MagicMock:
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = fake_engine_calls()
        websocket.get_script.side_effect = lambda: get_script(app_id)
        return websocket

    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = qrs_get
    mock_engine.side_effect = open_engine

    api = QlikOnPremAPI(
        QlikSourceConfig.model_validate({**ONPREM_CONFIG, "max_workers": 2})
    )
    api.get_spaces()
    items = list(api.get_items(["stream-1", "stream-2", "stream-3"]))

    assert sorted(item.id for item in items) == sorted(
        [*(f"app-{i}-of-stream-1" for i in range(5)), "app-0-of-stream-2"]
    )
    assert all(isinstance(item, App) and item.read_complete for item in items)
    assert [unread.name for unread in api.unread_items] == ["apps of stream stream-3"]


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_items_of_next_space_are_listed_only_after_first_item_is_taken(
    mock_session_cls: MagicMock,
) -> None:
    listed: List[str] = []

    def list_items(space_id: str) -> List[Dict]:
        listed.append(space_id)
        return [{"id": f"{space_id}-{i}"} for i in range(2)]

    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    with patch.object(api, "_list_items", side_effect=list_items):
        items = api._items_in_turn(["stream-1", "stream-2"])
        assert next(items) == ("stream-1", {"id": "stream-1-0"})
        assert listed == ["stream-1"]
        assert [item["id"] for _, item in items] == [
            "stream-2-0",
            "stream-1-1",
            "stream-2-1",
        ]


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unexpected_app_error_records_app_id(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()

    with patch.object(api, "_read_app", side_effect=RuntimeError("boom")):
        assert list(api.get_items(["stream-1"])) == []

    assert api.unread_items == [UnreadItem(name=APP["id"], cause="boom")]


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
        self._handles = [-1]
        self.socket_connection = None
        self.sent_on: List[Tuple[str, int]] = []

    def websocket_send_request(
        self, method: str, params: Optional[Union[Dict, List]] = None
    ) -> Dict:
        self.sent_on.append((method, self._handles[-1]))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        assert isinstance(reply, dict)
        handle = reply.get("qReturn", {}).get("qHandle")
        if handle:
            self._handles.append(handle)
        return reply

    def get_script(self) -> str:
        return LOAD_SCRIPT


class _ScriptlessEngine(_ScriptedEngine):
    def get_script(self) -> str:
        raise RuntimeError("Engine closed the session")


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unread_load_script_leaves_app_tables_unknown(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    engine = _ScriptlessEngine(_sheet_replies([]))
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert app.script is None
    assert app.tables_read_failed
    assert not app.read_complete


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
    session.get.side_effect = fake_qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    api.get_spaces()
    (app,) = api.get_items(["stream-1"])
    assert isinstance(app, App)
    return app


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_unreadable_property_tree_falls_back_without_failing_the_app(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    replies = _sheet_replies(["ok"])
    replies[-1] = RuntimeError("Method not found")
    engine = _ScriptedEngine(
        replies
        + [
            {"qReturn": {"qHandle": 3, "qType": "GenericObject"}},
            _chart_layout("ok"),
            {"qProp": {"qHyperCubeDef": {"qMeasures": []}}},
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["ok"]
    assert app.read_complete
    assert len(app.read_fallbacks) == 1


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_engine_read_fallbacks_are_reported_as_info(mock_onprem: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "stream-1",
        }
    )
    app.read_fallbacks = ["Sheet sheet-1: unable to read the property tree"]
    space = MagicMock()
    space.name = "Everyone"

    with patch.object(source, "_gen_app_workunit", return_value=[]):
        list(source._gen_item_workunits(app, space, False))

    assert [info.title for info in source.reporter.infos] == [
        "Qlik Engine batch read unavailable"
    ]
    assert not source.reporter.failures


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
    assert not app.sheets[0].charts_complete
    assert not app.read_complete
    assert engine.sent_on[4:] == [
        ("GetFullPropertyTree", 2),
        ("GetChild", 2),
        ("GetChild", 2),
        ("GetLayout", 3),
        ("GetProperties", 3),
        # The app's variables, read on the app handle.
        ("CreateSessionObject", 1),
    ]
    assert engine.handles == [-1, 1]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unread_measures_keep_the_sheet_chart_list_complete(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    # The chart is listed, so the sheet's chart list can still be written.
    engine = _ScriptedEngine(
        _sheet_replies(["ok"])
        + [
            {"qReturn": {"qHandle": 3, "qType": "GenericObject"}},
            _chart_layout("ok"),
            RuntimeError("Access denied"),
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert [chart.qId for chart in app.sheets[0].charts] == ["ok"]
    assert app.sheets[0].charts[0].fields_unread
    assert app.sheets[0].charts_complete


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
    assert engine.handles == [-1, 1]


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
    assert engine.handles == [-1, 1]


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
        "=Upper(Region)",
        # Data model field names are case-sensitive, so ORDER_ID is not order_id.
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
            # Every table's fields are known, so the chart reads none of them.
            ["Sum(unknown_field)"],
            True,
            False,
            [],
            False,
            id="no_field_matches",
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
            [],
            True,
            False,
            [],
            False,
            id="text_object_without_fields",
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
            ["Region.autoCalendar.Year"],
            True,
            False,
            ["Region"],
            False,
            id="derived_field_of_known_field",
        ),
        pytest.param(
            ["[Region].autoCalendar.Year"],
            True,
            False,
            ["Region"],
            False,
            id="bracketed_derived_field_of_known_field",
        ),
        pytest.param(
            ["OrderDate.autoCalendar.Year", "Region"],
            True,
            False,
            ["Region"],
            True,
            id="derived_field_of_unknown_field",
        ),
        pytest.param(
            ["Region//$(vRegion)"],
            True,
            False,
            ["Region"],
            False,
            id="variable_in_adjacent_comment",
        ),
        pytest.param(
            ["Region // $(vRegion)", "Count(Region) /* $(vOld) */"],
            True,
            False,
            ["Region"],
            False,
            id="variable_in_comment",
        ),
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
        pytest.param(
            ["Region", "Count(DISTINCT Region)"],
            True,
            True,
            ["Region"],
            False,
            id="keyword_with_unknown_schema",
        ),
        pytest.param(
            ["Count({<Year={2024}+{$(vYear)}>} Region)"],
            True,
            False,
            ["Region"],
            False,
            id="variable_in_combined_set_values",
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

    inputs = source._chart_inputs([chart], {}, app_fields, app_tables, unknown_schema)
    workunits = list(source._gen_charts_workunit([chart], inputs, "app-1"))
    (chart_info,) = filter(
        None, (wu.get_aspect_of_type(ChartInfoClass) for wu in workunits)
    )
    (input_fields,) = filter(
        None, (wu.get_aspect_of_type(InputFieldsClass) for wu in workunits)
    )

    # Region is the only field, of customers; a chart reading none has no input.
    read_tables = app_tables[1:] if matched_fields else []
    assert chart_info.inputs == (app_tables if keeps_all_tables else read_tables)
    assert [
        field.schemaField.fieldPath
        for field in input_fields.fields
        if field.schemaField
    ] == matched_fields


def test_field_names_match_in_any_case_only_for_script_tables() -> None:
    model_a = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.a,PROD)"
    model_b = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.b,PROD)"
    script = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    app_fields = QlikSenseSource._index_app_fields(
        {
            model_a: [SchemaField(name="ID")],
            model_b: [SchemaField(name="id")],
            script: [SchemaField(name="amount")],
        },
        script_table_urns={script},
    )
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.field_expressions = ["ID", "Id", "Sum(Amount)"]

    assert [
        (field.dataset_urn, field.field.name)
        for field in QlikSenseSource._get_chart_input_fields(chart, app_fields)
    ] == [(model_a, "ID"), (script, "amount")]


def test_exact_match_does_not_hide_script_table_in_other_case() -> None:
    model = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.a,PROD)"
    script = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    app_fields = QlikSenseSource._index_app_fields(
        {model: [SchemaField(name="order_id")], script: [SchemaField(name="ORDER_ID")]},
        script_table_urns={script},
    )
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.field_expressions = ["Count(order_id)"]

    assert [
        (field.dataset_urn, field.field.name)
        for field in QlikSenseSource._get_chart_input_fields(chart, app_fields)
    ] == [(model, "order_id"), (script, "ORDER_ID")]


def test_qualified_field_name_matches_before_derived_field() -> None:
    table = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    app_fields = QlikSenseSource._index_app_fields(
        {table: [SchemaField(name="Orders.Region"), SchemaField(name="Orders")]}
    )

    assert QlikSenseSource._app_field_key("Orders.Region", app_fields) == (
        "Orders.Region"
    )
    assert QlikSenseSource._app_field_key("Orders.autoCalendar.Year", app_fields) == (
        "Orders"
    )
    assert QlikSenseSource._app_field_key("Customers.Region", app_fields) is None
    # A field whose own name contains a dot keeps it when a suffix is derived.
    assert (
        QlikSenseSource._app_field_key("Orders.Region.autoCalendar.Year", app_fields)
        == "Orders.Region"
    )


@pytest.mark.parametrize(
    "reply, fields_complete, fields_unread",
    [
        pytest.param(
            RuntimeError("Access denied"), False, True, id="properties_unread"
        ),
        pytest.param(
            {"qProp": {"qHyperCubeDef": {"qMeasures": []}}},
            False,
            True,
            id="fewer_measures_than_layout",
        ),
        pytest.param(
            {"qProp": {"qHyperCubeDef": {"qMeasures": [{"qDef": {}}]}}},
            False,
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
            False,
            id="measure_expression",
        ),
    ],
)
def test_measure_definitions_mark_unread_measures(
    reply: object, fields_complete: bool, fields_unread: bool
) -> None:
    layout = _chart_layout("sales")["qLayout"]
    layout["qHyperCube"]["qMeasureInfo"] = [{"qFallbackTitle": "Sales"}]  # type: ignore[index]
    chart = Chart.model_validate(layout)
    engine = _ScriptedEngine([reply])

    engine._add_measure_definitions(chart, None, "sheet-1", on_error=lambda _: None)

    assert chart.fields_complete is fields_complete
    assert chart.fields_unread is fields_unread


@pytest.mark.parametrize("fields_unread", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_input_fields_are_kept_when_measures_were_unread(
    mock_onprem: MagicMock, fields_unread: bool
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.field_expressions = ["Region"]
    # A deleted master measure is incomplete too, but will not come back.
    chart.fields_complete = False
    chart.fields_unread = fields_unread
    table = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.customers,PROD)"
    app_fields = QlikSenseSource._index_app_fields(
        {table: [SchemaField(name="Region")]}
    )

    inputs = source._chart_inputs([chart], {}, app_fields, [table], False)
    workunits = list(source._gen_charts_workunit([chart], inputs, "app-1"))

    assert any(wu.get_aspect_of_type(ChartInfoClass) for wu in workunits)
    assert (
        any(wu.get_aspect_of_type(InputFieldsClass) for wu in workunits)
        is not fields_unread
    )


def test_measure_definitions_follow_linked_master_visualization() -> None:
    chart = Chart.model_validate(_chart_layout("linked")["qLayout"])
    engine = _ScriptedEngine(
        [
            {"qProp": {"qInfo": {"qId": "linked"}, "qExtendsId": "master-viz"}},
            {
                "qProp": {
                    "qHyperCubeDef": {"qMeasures": [{"qDef": {"qDef": "Sum(Sales)"}}]}
                }
            },
        ]
    )

    engine._add_measure_definitions(chart, None, "sheet-1", on_error=lambda _: None)

    assert [method for method, _ in engine.sent_on] == [
        "GetProperties",
        "GetEffectiveProperties",
    ]
    assert chart.field_expressions == ["Sum(Sales)"]
    assert chart.fields_complete


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
    list_unreadable = isinstance(replies[0], Exception)
    engine = _ScriptedEngine(replies)
    errors: List[str] = []
    fallbacks: List[str] = []

    engine.add_master_measure_definitions(
        [MagicMock(charts=[chart])], errors.append, fallbacks.append
    )

    assert errors == []
    assert len(fallbacks) == list_unreadable
    assert not chart.fields_unread
    assert not chart.fields_complete
    assert engine.replies == []
    assert engine.handles == [-1]


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
        [MagicMock(charts=[chart])], on_error=lambda _: None, on_fallback=lambda _: None
    )

    assert "Sum(Sales)" in chart.field_expressions
    assert not chart.fields_complete
    assert chart.fields_unread


def test_unreadable_master_measure_list_entry_falls_back_to_reading_one_by_one() -> (
    None
):
    chart = Chart.model_validate(_chart_layout("sales")["qLayout"])
    chart.measure_library_ids = ["measure-1"]
    engine = _ScriptedEngine(
        [
            {"qReturn": {"qHandle": 3}},
            {"qLayout": {"qMeasureList": {"qItems": [{"qData": {}}]}}},
            {"qReturn": {"qHandle": 4}},
            {"qLayout": {"qMeasure": {"qDef": "Sum(Sales)"}}},
        ]
    )
    fallbacks: List[str] = []

    engine.add_master_measure_definitions(
        [MagicMock(charts=[chart])],
        on_error=lambda _: None,
        on_fallback=fallbacks.append,
    )

    assert len(fallbacks) == 1
    assert chart.field_expressions[-1] == "Sum(Sales)"
    assert chart.fields_complete


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
def test_onprem_script_table_columns_take_preceding_load_names(
    mock_onprem: MagicMock,
) -> None:
    script = """
LIB CONNECT TO 'Sales SQL';
Orders:
LOAD order_id AS OrderID, amount AS [Net Sales];
SQL SELECT order_id, amount FROM dbo.orders;
"""
    _, (table,) = _script_tables(mock_onprem, {}, script=script)

    assert {
        field: [urn.rsplit(",", 1)[-1] for urn in upstreams]
        for field, upstreams in table.column_upstreams.items()
    } == {"OrderID": ["order_id)"], "Net Sales": ["amount)"]}


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

    def get(url: str, **kwargs: object) -> FakeResponse:
        if url.endswith("/about"):
            return fake_qrs_get(url, **kwargs)  # type: ignore[arg-type]
        return FakeResponse({}, status_code=401)

    session.get.side_effect = get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))

    assert api.get_spaces() == []
    assert list(api.get_items(["stream-1"])) == []
    assert api.get_data_connections() == {}


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_qrs_rejecting_the_connection_check_fails_fast(
    mock_session_cls: MagicMock,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.return_value = FakeResponse({}, status_code=401)

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
        "qlik.example.com", 4242, "/secrets/client.pem", "/secrets/client_key.pem", None
    )
    assert "-----BEGIN CERTIFICATE-----" in caplog.text


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_verify_server_hostname_false_skips_name_checks(
    mock_session_cls: MagicMock, mock_connect: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    config = QlikSourceConfig.model_validate(
        {"on_prem": {**ON_PREM, "verify_server_hostname": False}}
    )

    api = QlikOnPremAPI(config)
    api._open_engine("app-1")

    assert config.on_prem is not None
    assert config.on_prem.tls_assert_hostname() is False
    assert session.verify == "/secrets/root.pem"
    _, adapter = session.mount.call_args.args
    assert adapter.poolmanager.connection_pool_kw["assert_hostname"] is False
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
        {"on_prem": {**ON_PREM, "server_cert_hostname": "qlik-central.example.com"}}
    )

    api = QlikOnPremAPI(config)

    prefix, adapter = session.mount.call_args.args
    assert prefix == api.qrs_url
    assert (
        adapter.poolmanager.connection_pool_kw["assert_hostname"]
        == "qlik-central.example.com"
    )
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
    socket.recv.side_effect = [
        json.dumps({"jsonrpc": "2.0", "method": "OnConnected", "params": {}}),
        json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"qReturn": {"qHandle": 1}}}),
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "error": {"code": 5, "message": "Access denied"},
            }
        ),
    ]
    mock_connect.return_value = socket
    connection = WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")
    connection.websocket_send_request(method="OpenDoc", params={"qDocName": "app-1"})

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
            {Constant.RESOURCETYPE: Constant.APP, Constant.RESOURCEID: "app-1"}, None
        )
        is None
    )
    websocket.close_websocket.assert_called_once()
    assert api.unread_items == [UnreadItem(name="app-1", cause="Access denied")]


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
    assert subtypes.typeNames == [DatasetSubTypes.QLIK_TABLE]


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
    ) -> FakeResponse:
        if url.endswith("/app/full"):
            assert params is not None
            filters.append(params["filter"])
            return FakeResponse([])
        return fake_qrs_get(url, params)

    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = get
    api = QlikOnPremAPI(
        QlikSourceConfig.model_validate(
            {"on_prem": {**ON_PREM, "ingest_unpublished_apps": ingest_unpublished}}
        )
    )

    spaces = api.get_spaces()
    for space in spaces:
        list(api.get_items([space.id]))

    assert [space.name for space in spaces] == (
        ["Everyone", UNPUBLISHED_APPS_NAME] if ingest_unpublished else ["Everyone"]
    )
    assert filters == (
        ["stream.id eq stream-1", "published eq false"]
        if ingest_unpublished
        else ["stream.id eq stream-1"]
    )


@pytest.mark.parametrize(
    "charts_complete, tables_read_failed",
    [
        pytest.param(True, False, id="complete"),
        pytest.param(False, False, id="chart_unread"),
        pytest.param(True, True, id="tables_unread"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_partly_read_app_does_not_overwrite_relationships(
    mock_onprem: MagicMock, charts_complete: bool, tables_read_failed: bool
) -> None:
    mock_onprem.return_value.get_data_connections.return_value = {}
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    sheet = Sheet.model_validate(
        {
            "id": "sheet-1",
            "title": "Overview",
            "description": "",
            "createdDate": "2024-01-15T11:01:49.704Z",
            "modifiedDate": "2024-01-29T12:23:46.868Z",
        }
    )
    sheet.charts = [Chart.model_validate(_chart_layout("sales")["qLayout"])]
    sheet.charts_complete = charts_complete
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "stream-1",
        }
    )
    app.sheets = [sheet]
    app.tables_read_failed = tables_read_failed

    workunits = list(source._gen_app_workunit(app))

    def emitted(aspect: type) -> bool:
        return any(wu.get_aspect_of_type(aspect) for wu in workunits)

    sheet_urn = source._gen_dashboard_urn(sheet.id)
    assert (
        any(
            wu.get_urn() == sheet_urn and wu.get_aspect_of_type(DashboardInfoClass)
            for wu in workunits
        )
        is charts_complete
    )
    assert emitted(ChartInfoClass) is not tables_read_failed
    assert emitted(InputFieldsClass) is not tables_read_failed
    chart_info_patches = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.changeType == ChangeTypeClass.PATCH
        and wu.metadata.aspectName == ChartInfoClass.ASPECT_NAME
    ]
    # Without known inputs the chart is patched, keeping its last inputs.
    assert len(chart_info_patches) == int(tables_read_failed)
    assert all(
        patch.aspect is not None
        and b"inputs" not in patch.aspect.value
        and b"lastModified" not in patch.aspect.value
        for patch in chart_info_patches
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_user_lookup_is_cached_and_failures_recorded_once(
    mock_session_cls: MagicMock,
) -> None:
    def qrs_get(url: str, **kwargs: object) -> FakeResponse:
        if url.endswith("/user/user-2"):
            return FakeResponse({"name": "Grace", "userId": "grace"})
        if url.endswith("/user/user-3"):
            return FakeResponse({}, status_code=503)
        return fake_qrs_get(url, **kwargs)  # type: ignore[arg-type]

    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))

    assert [api.get_user_name("user-2") for _ in range(2)] == ["grace", "grace"]
    assert [api.get_user_name("user-3") for _ in range(2)] == [None, None]

    user_calls = [
        call.args[0] for call in session.get.call_args_list if "/user/" in call.args[0]
    ]
    assert len(user_calls) == 2
    assert list(api.failed_user_lookups) == ["user-3"]


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_unreadable_sheet_marks_app_partly_read(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    engine = _ScriptedEngine(
        [
            {"qReturn": {"qHandle": 1}},
            {"qList": [{"qInfo": {"qId": "sheet-1", "qType": "sheet"}}]},
            RuntimeError("Object not found"),
        ]
    )
    app = _read_onprem_app(mock_session_cls, mock_engine, engine)

    assert app.sheets == []
    assert not app.read_complete
    assert not app.sheets_complete
    assert engine.handles == [-1, 1]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_script_sql_errors_do_not_report_secrets(mock_onprem: MagicMock) -> None:
    script = """LIB CONNECT TO 'Sales SQL';
T: SQL SELECT a -- don't
FROM sales_db.dbo.t1;
ODBC CONNECT TO [Driver={ODBC Driver 17 for SQL Server};Server=h;Database=d;UID=svc;PWD=Hunter2Secret];
U: SQL SELECT * FROM d.dbo.u1;
V: SQL SELECT FROM WHERE 'Hunter2Secret';
"""
    source, tables = _script_tables(mock_onprem, {}, script=script)

    assert [table.label for table in tables] == ["T", "U"]
    assert [warning.title for warning in source.reporter.warnings] == [
        "Unable to parse Qlik load script SQL"
    ]
    assert "Hunter2Secret" not in str(source.reporter.as_obj())


def _sheet_layout(sheet_id: str, **meta: object) -> List[object]:
    return [
        {"qReturn": {"qHandle": 2, "qType": "GenericObject"}},
        {"qLayout": {"qMeta": {**SHEET_META, **meta}}},
    ]


@pytest.mark.parametrize("unpublished_app", [False, True])
@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_private_sheets_of_published_apps_are_skipped(
    mock_session_cls: MagicMock, mock_engine: MagicMock, unpublished_app: bool
) -> None:
    engine = _ScriptedEngine(
        [
            {"qReturn": {"qHandle": 1}},
            {
                "qList": [
                    {"qInfo": {"qId": "base", "qType": "sheet"}},
                    {"qInfo": {"qId": "mine", "qType": "sheet"}},
                ]
            },
            # QRS can leave out fractional seconds.
            *_sheet_layout("base", published=True, createdDate="2024-01-15T11:01:49Z"),
            *_sheet_layout("mine", published=False),
        ]
    )
    mock_engine.return_value = engine
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    api = QlikOnPremAPI(QlikSourceConfig.model_validate(ONPREM_CONFIG))
    app_dict = {**APP, "stream": None} if unpublished_app else APP

    app = api._read_app(app_dict)

    assert app is not None
    assert {sheet.id for sheet in app.sheets} == (
        {"base", "mine"} if unpublished_app else {"base"}
    )
    assert app.read_complete


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_engine_reply_wait_is_bounded(mock_connect: MagicMock) -> None:
    socket = MagicMock()
    notification = json.dumps({"method": "OnConnected", "params": {}})
    socket.recv.return_value = notification
    mock_connect.return_value = socket
    connection = WebsocketConnection.for_on_prem(
        host="qlik.example.com",
        port=4747,
        app_id="app-1",
        user_header="UserDirectory=INTERNAL; UserId=sa_engine",
        client_cert="/secrets/client.pem",
        client_key="/secrets/client_key.pem",
        root_cert="/secrets/root.pem",
        key_password="key-password",
        timeout=5,
    )

    with (
        patch(
            "datahub.ingestion.source.qlik_sense.websocket_connection.time.monotonic",
            side_effect=[0, 1, 6],
        ),
        pytest.raises(TimeoutError),
    ):
        connection.websocket_send_request(method="OpenDoc")

    assert mock_connect.call_args.kwargs["timeout"] == 5
    assert mock_connect.call_args.kwargs["sslopt"]["password"] == "key-password"
    # The receive after a notification only waits for the time that is left.
    socket.settimeout.assert_called_once_with(4)


def test_qrs_adapter_passes_the_client_key_password() -> None:
    adapter = _QrsAdapter(assert_hostname=None, key_password="key-password")

    assert adapter.poolmanager.connection_pool_kw["key_password"] == "key-password"
    assert "assert_hostname" not in adapter.poolmanager.connection_pool_kw


@pytest.mark.parametrize("engine_reachable", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.socket.create_connection")
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_test_connection_checks_the_engine_port(
    mock_session_cls: MagicMock, mock_connect: MagicMock, engine_reachable: bool
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    if not engine_reachable:
        mock_connect.side_effect = ConnectionRefusedError("refused")

    report = QlikSenseSource.test_connection(ONPREM_CONFIG)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is engine_reachable
    assert mock_connect.call_args.args[0] == ("qlik.example.com", 4747)


@pytest.mark.parametrize("mapping_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_mapping_table_does_not_hide_data_table_with_same_label(
    mock_onprem: MagicMock, mapping_first: bool
) -> None:
    mapping_load = "Regions:\nMAPPING SQL SELECT region_code, region_name FROM sales_db.dbo.regions;"
    data_load = (
        "Regions:\nSQL SELECT region_code, manager FROM sales_db.dbo.region_managers;"
    )
    loads = [mapping_load, data_load] if mapping_first else [data_load, mapping_load]
    source, tables = _script_tables(
        mock_onprem, {}, script="\n".join(["LIB CONNECT TO 'Sales SQL';", *loads])
    )

    (table,) = tables
    assert not table.mapping
    assert table.upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.region_managers,PROD)"
    ]
    assert all(
        "dbo.regions," not in urn
        for urns in table.column_upstreams.values()
        for urn in urns
    )
    assert [
        field.name
        for fields in source._app_table_fields([], tables).values()
        for field in fields
    ] == ["region_code", "manager"]


@pytest.mark.parametrize("mapping_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_labels_differing_only_in_case_are_one_table(
    mock_onprem: MagicMock, mapping_first: bool
) -> None:
    mapping_load = "REGIONS:\nMAPPING SQL SELECT region_code, region_name FROM sales_db.dbo.regions;"
    data_loads = [
        "Regions:\nSQL SELECT region_code, manager FROM sales_db.dbo.region_managers;",
        "regions:\nSQL SELECT region_code, office FROM sales_db.dbo.region_offices;",
    ]
    loads = (
        [mapping_load, *data_loads] if mapping_first else [*data_loads, mapping_load]
    )
    _, tables = _script_tables(
        mock_onprem, {}, script="\n".join(["LIB CONNECT TO 'Sales SQL';", *loads])
    )

    (table,) = tables
    assert table.identifier == "app-1.regions"
    assert table.upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.region_managers,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.region_offices,PROD)",
    ]
    assert list(table.column_upstreams) == ["region_code", "manager", "office"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_mapping_tables_are_not_chart_inputs(mock_onprem: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(ONPREM_CONFIG),
        PipelineContext(run_id="test-onprem"),
    )
    tables = [
        ScriptTableLineage(
            label="Map",
            identifier="app-1.map",
            column_upstreams={"k": []},
            mapping=True,
        ),
        ScriptTableLineage(
            label="Orders", identifier="app-1.orders", column_upstreams={"k": []}
        ),
    ]

    assert list(source._app_table_fields([], tables)) == [
        "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    ]


@pytest.mark.parametrize(
    "select",
    [
        pytest.param("region, jan, region", id="column_selected_twice"),
        pytest.param("*", id="star"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_crosstable_on_select_of_unknown_column_order_keeps_table_lineage(
    mock_onprem: MagicMock, select: str
) -> None:
    _, tables = _script_tables(
        mock_onprem,
        {},
        script="LIB CONNECT TO 'Sales SQL';\n"
        f"T: CrossTable(Month, Sales) SQL SELECT {select} FROM sales_db.dbo.sales;",
    )

    (table,) = tables
    assert table.upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.sales,PROD)"
    ]
    assert table.column_upstreams == {}


@pytest.mark.parametrize(
    "load, expected_fields",
    [
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD region, jan, feb;",
            {"region": ["region"], "Month": [], "Sales": ["jan", "feb"]},
            id="crosstable",
        ),
        pytest.param("T: Generic LOAD region, jan, feb;", {}, id="generic"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_reshaped_script_tables_keep_only_traceable_fields(
    mock_onprem: MagicMock, load: str, expected_fields: Dict[str, List[str]]
) -> None:
    _, tables = _script_tables(
        mock_onprem,
        {},
        script="LIB CONNECT TO 'Sales SQL';\n"
        f"{load}\n"
        "SQL SELECT region, jan, feb FROM sales_db.dbo.sales;",
    )

    (table,) = tables
    upstream = "urn:li:dataset:(urn:li:dataPlatform:mssql,sales_db.dbo.sales,PROD)"
    assert table.upstream_urns == [upstream]
    assert table.column_upstreams == {
        field: [f"urn:li:schemaField:({upstream},{column})" for column in columns]
        for field, columns in expected_fields.items()
    }


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_opens_app_with_data_to_read_its_data_model(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = fake_engine_calls()
    websocket.get_script.return_value = LOAD_SCRIPT
    websocket.get_data_model_tables.return_value = [
        {
            "qName": "Orders",
            "qFields": [
                {"qName": "order_id", "qTags": ["$numeric", "$integer"]},
                {"qName": "amount", "qTags": ["$numeric"]},
            ],
        },
        {"qName": "$Syn 1 Table", "qIsSynthetic": True},
    ]
    mock_engine.return_value = websocket
    config = QlikSourceConfig.model_validate(
        {"on_prem": {**ON_PREM, "read_data_model": True}}
    )

    api = QlikOnPremAPI(config)
    api.get_spaces()
    [app] = list(api.get_items(["stream-1"]))

    open_doc = websocket.websocket_send_request.call_args_list[0].kwargs
    assert open_doc["params"]["qNoData"] is False
    assert isinstance(app, App)
    assert [
        (table.name, [(f.name, f.dataType) for f in table.fields])
        for table in app.data_model_tables
    ] == [("Orders", [("order_id", "INTEGER"), ("amount", "DOUBLE")])]
    assert app.read_complete


@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_app_whose_data_model_cant_be_read_is_still_complete(
    mock_session_cls: MagicMock, mock_engine: MagicMock
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    websocket = MagicMock()
    websocket.websocket_send_request.side_effect = fake_engine_calls()
    websocket.get_script.return_value = LOAD_SCRIPT
    websocket.get_variables.return_value = {}
    websocket.get_data_model_tables.side_effect = TimeoutError("timed out")
    mock_engine.return_value = websocket
    config = QlikSourceConfig.model_validate(
        {"on_prem": {**ON_PREM, "read_data_model": True}}
    )

    api = QlikOnPremAPI(config)
    api.get_spaces()
    [app] = list(api.get_items(["stream-1"]))

    # As when opening with data fails: the script, sheets and QVDs stand, and
    # the next run reads the data model again.
    assert isinstance(app, App)
    assert app.read_complete
    assert app.data_load_failed
    assert app.script == LOAD_SCRIPT
    assert not app.data_model_tables
    assert len(api.opened_without_data) == 1


@pytest.mark.parametrize(
    "error",
    [
        EngineError({"code": 5, "message": "Access denied"}),
        TimeoutError("Connection timed out"),
    ],
)
@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_onprem_app_whose_data_cant_be_opened_is_read_without_it(
    mock_session_cls: MagicMock, mock_engine: MagicMock, error: Exception
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    # Section access denies the data, or the data load times out.
    with_data = MagicMock()
    with_data.websocket_send_request.side_effect = error
    without_data = MagicMock()
    without_data.websocket_send_request.side_effect = fake_engine_calls()
    without_data.get_script.return_value = LOAD_SCRIPT
    without_data.get_variables.return_value = {}
    mock_engine.side_effect = [with_data, without_data]
    config = QlikSourceConfig.model_validate(
        {"on_prem": {**ON_PREM, "read_data_model": True}}
    )

    api = QlikOnPremAPI(config)
    api.get_spaces()
    [app] = list(api.get_items(["stream-1"]))

    open_doc = without_data.websocket_send_request.call_args_list[0].kwargs
    assert open_doc["params"]["qNoData"] is True
    assert isinstance(app, App)
    assert app.read_complete
    assert app.data_load_failed
    assert app.script == LOAD_SCRIPT
    assert not app.data_model_tables
    without_data.get_data_model_tables.assert_not_called()
    assert len(api.opened_without_data) == 1
