from typing import Dict, List
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    QvdReference,
    parse_script,
)
from tests.unit.qlik_sense.qlik_fakes import ONPREM_CONFIG


def _sql(script: str, variables: Dict[str, str]) -> List[str]:
    return [table.sql.strip() for table in parse_script(script, variables).tables]


def _app(script: str, variables: Dict[str, str]) -> App:
    return App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "description": "",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "script": script,
            "variables": variables,
        }
    )


def _source(script_variables: Dict[str, str]) -> QlikSenseSource:
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikOnPremAPI"):
        return QlikSenseSource(
            QlikSourceConfig.model_validate(
                {**ONPREM_CONFIG, "script_variables": script_variables}
            ),
            PipelineContext(run_id="test-script-variables"),
        )


def test_fallback_values_fill_sql_until_the_script_sets_the_variable() -> None:
    assert _sql(
        "LIB CONNECT TO 'db';\n"
        "Orders: SQL SELECT id FROM $(vSchema).orders;\n"
        "SET vSchema = 'staging';\n"
        "Staged: SQL SELECT id FROM $(vSchema).orders;\n"
        "LET vSchema = Lower(vSource);\n"
        "Computed: SQL SELECT id FROM $(vSchema).orders;",
        {"vSchema": "sales"},
    ) == [
        "SELECT id FROM sales.orders",
        "SELECT id FROM staging.orders",
        # LET from an expression can't be evaluated, so the reload's value stands.
        "SELECT id FROM sales.orders",
    ]


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "FOR EACH vTable IN 'a', 'b'\n"
            "T: SQL SELECT id FROM $(vTable);\n"
            "NEXT vTable;",
            id="loop variable",
        ),
        pytest.param(
            "SUB LoadTable(vTable)\nT: SQL SELECT id FROM $(vTable);\nEND SUB;\n"
            "CALL LoadTable(Lower(vSource));",
            id="sub parameter from an expression",
        ),
        pytest.param(
            "IF vEnv = 'prod' THEN\nSET vTable = 'a';\nELSE\nSET vTable = 'b';\n"
            "END IF;\nT: SQL SELECT id FROM $(vTable);",
            id="branches that disagree",
        ),
        pytest.param(
            "FOR EACH vT IN 'a', 'b'\nLET vTable = Upper(vT);\n"
            "T: SQL SELECT id FROM $(vTable);\nNEXT vT;",
            id="LET in a loop",
        ),
        pytest.param(
            "SUB LoadTable\nLET vTable = Upper(vSource);\n"
            "T: SQL SELECT id FROM $(vTable);\nEND SUB;\nCALL LoadTable;",
            id="LET in a sub",
        ),
        pytest.param(
            "IF vEnv = 'A' THEN\nLET vTable = Lower('SALES_' & vRegion);\n"
            "A: SQL SELECT id FROM $(vTable);\nELSE\n"
            "LET vTable = Lower('HIST_' & vRegion);\n"
            "B: SQL SELECT id FROM $(vTable);\nEND IF;",
            id="LET in IF branches",
        ),
        pytest.param(
            "SWITCH vEnv\nCASE 'A'\nLET vTable = Lower('SALES_' & vRegion);\n"
            "A: SQL SELECT id FROM $(vTable);\nEND SWITCH;",
            id="LET in a SWITCH case",
        ),
        pytest.param(
            "LET vTable = Lower('SALES_' & vRegion);\n"
            "T: SQL SELECT id FROM $(vTable);\n"
            "LET vTable = Lower('HIST_' & vRegion);",
            id="LET set again later",
        ),
        pytest.param(
            "FOR EACH vT IN 'a', 'b'\nSET vTable = $(vT);\n"
            "T: SQL SELECT id FROM $(vTable);\nNEXT vT;",
            id="SET copying a loop variable",
        ),
        pytest.param(
            "FOR EACH vTable IN 'a', 'b'\n$(Include=lib://Scripts/common.qvs);\n"
            "T: SQL SELECT id FROM $(vTable);\nNEXT vTable;",
            id="loop variable after an include",
        ),
    ],
)
def test_fallback_values_never_fill_a_variable_the_script_made_unknown(
    script: str,
) -> None:
    # The app keeps only the last value, so lineage from it would be wrong.
    sql = _sql(f"LIB CONNECT TO 'db';\n{script}", {"vTable": "b"})
    assert sql and set(sql) == {"SELECT id FROM $(vTable)"}


def test_expression_values_are_not_expanded() -> None:
    assert _sql(
        "LIB CONNECT TO 'db';\nT: SQL SELECT id FROM $(vSchema).orders;",
        {"vSchema": "=Lower('Sales')"},
    ) == ["SELECT id FROM $(vSchema).orders"]


def test_fallback_values_fill_qvd_paths() -> None:
    [load] = parse_script(
        "Orders: LOAD id FROM [lib://$(vConn)/$(vYear)/orders.qvd] (qvd);",
        {"vConn": "QVD Data", "vYear": "2026"},
    ).qvd_loads
    assert load.file == QvdReference(connection="QVD Data", path="2026/orders.qvd")


def test_app_values_take_precedence_over_recipe_values() -> None:
    source = _source({"vMonth": "2026-10", "vConn": "Other"})
    app = _app(
        "Orders: LOAD id FROM [lib://$(vConn)/$(vMonth)/orders.qvd] (qvd);",
        {"vConn": "QVD Data"},
    )

    [load] = source._parsed_script(app).qvd_loads
    assert load.file == QvdReference(connection="QVD Data", path="2026-10/orders.qvd")


@pytest.mark.parametrize("name", ["$(vSchema)", " vSchema", ""])
def test_variable_name_with_reference_syntax_is_rejected(name: str) -> None:
    with pytest.raises(ValidationError):
        QlikSourceConfig.model_validate(
            {**ONPREM_CONFIG, "script_variables": {name: "sales"}}
        )


def test_changing_recipe_values_re_reads_unchanged_apps() -> None:
    fingerprints = []
    for value in ("2026-10", "2026-11"):
        source = _source({"vMonth": value})
        source._data_connections = {}
        source._init_fingerprint_salt()
        fingerprints.append(source._salted_fingerprint("app-fingerprint"))
    assert fingerprints[0] != fingerprints[1]


def test_recipe_value_stands_in_for_an_expression_variable() -> None:
    source = _source({"vSchema": "sales"})
    app = _app(
        "LIB CONNECT TO 'db';\nT: SQL SELECT id FROM $(vSchema).orders;",
        {"vSchema": "=Lower(vSource)"},
    )
    assert source._app_variables(app) == {"vSchema": "sales"}


def test_empty_app_value_leaves_the_recipe_value() -> None:
    source = _source({"vDir": "2026-10", "vPrefix": "x"})
    app = _app("", {"vDir": "", "vOther": ""})
    # An empty value the recipe doesn't cover is kept, as for a prefix.
    assert source._app_variables(app) == {
        "vDir": "2026-10",
        "vPrefix": "x",
        "vOther": "",
    }


def test_nested_fallback_values_are_expanded_in_sql() -> None:
    assert _sql(
        "LIB CONNECT TO 'db';\nT: SQL SELECT id FROM $(vTable);",
        {"vTable": "$(vSchema).orders", "vSchema": "sales"},
    ) == ["SELECT id FROM sales.orders"]
