from unittest.mock import patch

import pytest

from datahub.emitter.mce_builder import make_container_urn, make_dashboard_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App, Sheet
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.metadata.schema_classes import (
    ContainerClass,
    DashboardInfoClass,
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


def _app(read_errors: list) -> App:
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
        }
    )


@pytest.mark.parametrize("read_errors", [[], ["sheet-3: unable to read"]])
def test_app_is_a_dashboard_of_its_sheets(read_errors: list) -> None:
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(CLOUD_CONFIG),
            PipelineContext(run_id="test-app-dashboard"),
        )
    app = _app(read_errors)
    workunits = list(source._gen_app_dashboard_workunit(app, None))

    app_urn = make_dashboard_urn("qlik-sense", "app-1")
    assert {wu.get_urn() for wu in workunits} == {app_urn}
    infos = [
        info for wu in workunits if (info := wu.get_aspect_of_type(DashboardInfoClass))
    ]
    if read_errors:
        # A partly read app may miss sheets, so its last full read is kept.
        assert infos == []
    else:
        [info] = infos
        assert info.title == "Sales"
        assert [edge.destinationUrn for edge in info.dashboards or []] == [
            make_dashboard_urn("qlik-sense", "sheet-1"),
            make_dashboard_urn("qlik-sense", "sheet-2"),
        ]
    [subtypes] = [
        types for wu in workunits if (types := wu.get_aspect_of_type(SubTypesClass))
    ]
    assert subtypes.typeNames == ["Qlik App"]
    [container] = [
        c for wu in workunits if (c := wu.get_aspect_of_type(ContainerClass))
    ]
    assert container.container == make_container_urn(source._gen_space_key("space-1"))
