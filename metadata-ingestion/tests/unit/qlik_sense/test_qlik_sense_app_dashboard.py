import json
from typing import List, Optional
from unittest.mock import patch

import pytest

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dashboard_urn,
    make_dataset_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Chart,
    ChartInputs,
    Sheet,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.metadata.schema_classes import (
    ContainerClass,
    DashboardInfoClass,
    MetadataChangeProposalClass,
    SubTypesClass,
)

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
