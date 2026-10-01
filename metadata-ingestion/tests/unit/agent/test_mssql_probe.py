from typing import Dict

import pytest

from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.recipe import validate_recipe
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig

_BASE: Dict[str, object] = {"host_port": "h:1433", "username": "u", "password": "p"}
_ODBC: Dict[str, object] = {
    **_BASE,
    "uri_args": {"driver": "ODBC Driver 18 for SQL Server"},
}


def test_an_odbc_recipe_validates_as_ingestion_validates_it() -> None:
    recipe = {"source": {"type": "mssql-odbc", "config": _ODBC}}
    assert validate_recipe(recipe)["errors"] == []


def test_uri_args_are_still_refused_on_the_pytds_source_type() -> None:
    recipe = {"source": {"type": "mssql", "config": _ODBC}}
    assert validate_recipe(recipe)["valid"] is False


def test_probe_filter_accepts_an_odbc_recipe() -> None:
    result = check_filters(
        source_type="mssql-odbc",
        config_dict=_ODBC,
        kind="Schema",
        parent_path=[],
        names=["dbo"],
    )
    assert result.results[0].included


@pytest.mark.parametrize(
    "source_type, config, scheme",
    [("mssql", _BASE, "mssql+pytds"), ("mssql-odbc", _ODBC, "mssql+pyodbc")],
)
def test_the_probe_dials_the_driver_ingestion_dials(
    source_type: str, config: Dict[str, object], scheme: str
) -> None:
    validated = validate_source_config(SQLServerConfig, source_type, config)
    assert validated.probe_sql_alchemy_url().startswith(f"{scheme}://")
