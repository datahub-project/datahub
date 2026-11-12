from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    QvdReference,
    parse_script,
)
from tests.unit.qlik_sense.qlik_fakes import ONPREM_CONFIG


def test_recipe_values_fill_sql_variables_the_script_does_not_set() -> None:
    parsed = parse_script(
        "LIB CONNECT TO 'db';\n"
        "Orders: SQL SELECT id FROM $(vSchema).orders;\n"
        "SET vSchema = 'staging';\n"
        "Staged: SQL SELECT id FROM $(vSchema).orders;",
        script_variables={"vSchema": "sales"},
    )

    # A SET in the script takes precedence from where it runs.
    assert [table.sql.strip() for table in parsed.tables] == [
        "SELECT id FROM sales.orders",
        "SELECT id FROM staging.orders",
    ]


def test_app_variables_take_precedence_over_recipe_values_in_paths() -> None:
    parsed = parse_script(
        "Orders: LOAD id FROM [lib://$(vConn)/$(vYear)/orders.qvd] (qvd);",
        app_variables={"vConn": "QVD Data"},
        script_variables={"vConn": "Other", "vYear": "2026"},
    )

    [load] = parsed.qvd_loads
    assert load.file == QvdReference(connection="QVD Data", path="2026/orders.qvd")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI")
def test_recipe_values_reach_the_apps_qvd_paths(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {**ONPREM_CONFIG, "script_variables": {"vMonth": "2026-10"}}
        ),
        PipelineContext(run_id="test-script-variables"),
    )
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "script": "Orders: LOAD id FROM [lib://QVD Data/$(vMonth)/orders.qvd] (qvd);",
        }
    )

    [load] = source._parsed_script(app).qvd_loads
    assert load.file.path == "2026-10/orders.qvd"
    assert source._app_variables(app) == {"vMonth": "2026-10"}
