import json
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dashboard_urn,
    make_dataset_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Chart,
    ChartInputs,
    Sheet,
)
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ContainerClass,
    DashboardInfoClass,
    EdgeClass,
    MetadataChangeProposalClass,
    SubTypesClass,
)
from tests.unit.qlik_sense.qlik_fakes import LOAD_SCRIPT, ONPREM_CONFIG, fake_qrs_get

CLOUD_CONFIG = {"tenant_hostname": "tenant.example.com", "api_key": "key"}


def _sheet(sheet_id: str) -> Sheet:
    return Sheet.model_validate(
        {
            "id": sheet_id,
            "title": sheet_id,
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
        }
    )


def _app(read_errors: list, sheets_complete: bool = True) -> App:
    return App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "qri": "qri:app:sense://app-1",
            "description": "Sales app",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "sheets": [_sheet("sheet-1"), _sheet("sheet-2")],
            "read_errors": read_errors,
            "sheets_complete": sheets_complete,
        }
    )


TABLES = [
    make_dataset_urn("qlik-sense", "app-1.orders"),
    make_dataset_urn("qlik-sense", "app-1.customers"),
]


@pytest.mark.parametrize(
    "sheets_complete, table_urns, written_whole",
    [
        # Another part of the app failed, but its sheets and tables are known.
        (True, TABLES, True),
        (False, TABLES, False),
        (True, None, False),
    ],
)
def test_app_is_a_dashboard_of_its_sheets_and_tables(
    sheets_complete: bool, table_urns: Optional[List[str]], written_whole: bool
) -> None:
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(CLOUD_CONFIG),
            PipelineContext(run_id="test-app-dashboard"),
        )
    app = _app(["chart-1: unable to read"], sheets_complete=sheets_complete)
    workunits = list(source._gen_app_dashboard_workunit(app, None, table_urns))

    app_urn = make_dashboard_urn("qlik-sense", "app-1")
    sheets = [
        make_dashboard_urn("qlik-sense", "sheet-1"),
        make_dashboard_urn("qlik-sense", "sheet-2"),
    ]
    assert {wu.get_urn() for wu in workunits} == {app_urn}
    infos = [
        info for wu in workunits if (info := wu.get_aspect_of_type(DashboardInfoClass))
    ]
    if written_whole:
        [info] = infos
        assert info.title == "Sales"
        assert [edge.destinationUrn for edge in info.dashboards or []] == sheets
        assert [edge.destinationUrn for edge in info.datasetEdges or []] == TABLES
    else:
        # What was read is added to the edges of the last full read.
        assert infos == []
        [patch_mcp] = [
            wu.metadata
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == DashboardInfoClass.ASPECT_NAME
        ]
        assert patch_mcp.aspect is not None
        paths = [op["path"] for op in json.loads(patch_mcp.aspect.value)]
        assert sum(path.startswith("/dashboards/") for path in paths) == 2
        assert sum(path.startswith("/datasetEdges/") for path in paths) == len(
            table_urns or []
        )
        # The app's own fields are known even when some edges are not.
        assert {
            "/title",
            "/description",
            "/dashboardUrl",
            "/lastModified",
            "/customProperties/qri",
        } <= set(paths)
    [subtypes] = [
        types for wu in workunits if (types := wu.get_aspect_of_type(SubTypesClass))
    ]
    assert subtypes.typeNames == ["App"]
    [container] = [
        c for wu in workunits if (c := wu.get_aspect_of_type(ContainerClass))
    ]
    assert container.container == make_container_urn(source._gen_app_key("app-1"))


@pytest.mark.parametrize("sheets_complete", [True, False])
def test_patched_app_dashboard_drops_sheets_no_longer_ingested(
    sheets_complete: bool,
) -> None:
    removed = make_dashboard_urn("qlik-sense", "removed-sheet")
    graph = MagicMock()
    graph.get_aspect.return_value = DashboardInfoClass(
        title="Sales",
        description="",
        lastModified=ChangeAuditStampsClass(),
        dashboards=[
            EdgeClass(destinationUrn=make_dashboard_urn("qlik-sense", "sheet-1")),
            EdgeClass(destinationUrn=removed),
        ],
    )
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(CLOUD_CONFIG),
            PipelineContext(run_id="test-app-dashboard", graph=graph),
        )
    # The tables are unknown, so the dashboard is patched.
    app = _app([], sheets_complete=sheets_complete)
    workunits = list(source._gen_app_dashboard_workunit(app, None, None))

    # One patch, so the edges it adds and removes apply to the same aspect.
    [patch_mcp] = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == DashboardInfoClass.ASPECT_NAME
    ]
    assert patch_mcp.aspect is not None
    removed_paths = [
        op["path"] for op in json.loads(patch_mcp.aspect.value) if op["op"] == "remove"
    ]
    # Only when every sheet is known can one be told to be gone.
    assert removed_paths == (
        [f"/dashboards/{removed.replace('/', '~1')}"] if sheets_complete else []
    )


def test_app_without_ingested_sheets_links_its_last_known_tables() -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = None
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(
                {**CLOUD_CONFIG, "include_sheets_and_charts": False}
            ),
            PipelineContext(run_id="test-app-dashboard", graph=graph),
        )
    app = _app([])
    app.sheets = []
    with patch.object(
        source,
        "_usable_checkpoint",
        return_value=AppCheckpoint(
            urns=TABLES, table_urns_by_qri={"qri-1": TABLES[0], "qri-2": TABLES[1]}
        ),
    ):
        # The tables are unknown this run, so the dashboard is patched.
        workunits = list(source._gen_app_dashboard_workunit(app, None, None))

    added = [
        op["path"]
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == DashboardInfoClass.ASPECT_NAME
        and wu.metadata.aspect is not None
        for op in json.loads(wu.metadata.aspect.value)
        if op["op"] == "add" and op["path"].startswith("/datasetEdges/")
    ]
    # Without sheets, the app reaches its tables only through these edges.
    assert added == [f"/datasetEdges/{urn.replace('/', '~1')}" for urn in TABLES]


def test_onprem_app_without_ingested_sheets_links_its_last_read_tables() -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = None
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(
                {**ONPREM_CONFIG, "include_sheets_and_charts": False}
            ),
            PipelineContext(run_id="test-app-dashboard", graph=graph),
        )
    app = _app([])
    app.sheets = []
    # On-prem script tables have no QRI; the last read recorded their URNs.
    other_app_table = make_dataset_urn("qlik-sense", "app-10.orders")
    with patch.object(
        source,
        "_usable_checkpoint",
        return_value=AppCheckpoint(
            urns=[
                *TABLES,
                other_app_table,
                make_dashboard_urn("qlik-sense", "app-1"),
            ],
            table_urns_by_qri={},
        ),
    ):
        workunits = list(source._gen_app_dashboard_workunit(app, None, None))

    added = [
        op["path"]
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == DashboardInfoClass.ASPECT_NAME
        and wu.metadata.aspect is not None
        for op in json.loads(wu.metadata.aspect.value)
        if op["op"] == "add" and op["path"].startswith("/datasetEdges/")
    ]
    assert added == [f"/datasetEdges/{urn.replace('/', '~1')}" for urn in TABLES]


TWO_TABLE_SCRIPT = (
    LOAD_SCRIPT + "Customers:\nSQL SELECT customer_id, region FROM dbo.customers;\n"
)


@pytest.mark.parametrize("include_sheets_and_charts", [True, False])
@patch(
    "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem"
)
@patch("datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session")
def test_app_without_sheets_and_charts_links_to_all_its_tables(
    mock_session_cls: MagicMock,
    mock_engine: MagicMock,
    include_sheets_and_charts: bool,
) -> None:
    session = mock_session_cls.return_value
    session.headers = {}
    session.get.side_effect = fake_qrs_get
    engine = MagicMock()
    # The app has a sheet, whose chart reads the orders table.
    engine.websocket_send_request.side_effect = lambda method, **_: (
        {"qList": [{"qInfo": {"qId": "sheet-1", "qType": "sheet"}}]}
        if method == "GetObjects"
        else {"qReturn": {"qHandle": 1}}
    )
    engine.get_script.return_value = TWO_TABLE_SCRIPT
    engine.get_variables.return_value = {}
    mock_engine.return_value = engine
    sheet = _sheet("sheet-1")
    chart = Chart.model_validate(
        {
            "qInfo": {"qId": "chart-1"},
            "visualization": "barchart",
            "qHyperCube": {"qDimensionInfo": [], "qMeasureInfo": []},
        }
    )
    chart.field_expressions = ["Sum(amount)"]
    sheet.charts = [chart]
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {**ONPREM_CONFIG, "include_sheets_and_charts": include_sheets_and_charts}
        ),
        PipelineContext(run_id="test-app-dashboard"),
    )

    with patch.object(QlikOnPremAPI, "_get_sheet", return_value=sheet):
        workunits = list(source.get_workunits_internal())

    methods = [
        call.kwargs["method"] for call in engine.websocket_send_request.mock_calls
    ]
    app_info = next(
        info
        for wu in workunits
        if wu.get_urn() == make_dashboard_urn("qlik-sense", "app-1")
        and (info := wu.get_aspect_of_type(DashboardInfoClass))
    )
    charts = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:chart:")
    }
    orders = make_dataset_urn("qlik-sense", "app-1.orders")
    customers = make_dataset_urn("qlik-sense", "app-1.customers")
    linked = [edge.destinationUrn for edge in app_info.datasetEdges or []]
    if include_sheets_and_charts:
        # Orders reaches the app through the chart and its sheet.
        assert "GetObjects" in methods
        assert charts and len(app_info.dashboards) == 1
        assert linked == [customers]
    else:
        # The Engine is never asked for sheets, so the app links all its tables.
        assert "GetObjects" not in methods
        assert not charts and app_info.dashboards == []
        assert linked == [orders, customers]


def _chart(chart_id: str) -> Chart:
    return Chart.model_validate(
        {
            "qInfo": {"qId": chart_id},
            "visualization": "barchart",
            "qHyperCube": {"qDimensionInfo": [], "qMeasureInfo": []},
        }
    )


@pytest.mark.parametrize("charts_complete", [True, False])
def test_app_links_tables_its_charts_cannot(charts_complete: bool) -> None:
    sheet = _sheet("sheet-1")
    sheet.charts = [_chart("chart-1")]
    # Another chart of the sheet could not be read, so the sheet is not
    # written with its charts and chart-1's table has no path to the app.
    sheet.charts_complete = charts_complete
    app = _app([])
    app.sheets = [sheet]
    inputs = {"chart-1": ChartInputs(fields=[], tables=[TABLES[0]])}

    assert QlikSenseSource._tables_without_chart_path(app, inputs, TABLES) == (
        TABLES[1:] if charts_complete else TABLES
    )
