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


@pytest.mark.parametrize(
    "extra, single, pinned",
    [
        ({}, False, ""),
        ({"database": "DemoData"}, True, "DemoData"),
        (
            {
                "sqlalchemy_uri": "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7Bx%7D%3BDATABASE%3DNewData%3B"
            },
            True,
            "NewData",
        ),
        ({"sqlalchemy_uri": "mssql+pytds://u:p@h:1433"}, True, ""),
    ],
)
def test_single_database_detection_matches_get_inspectors(
    extra: Dict[str, object], single: bool, pinned: str
) -> None:
    config = SQLServerConfig.model_validate({**_BASE, **extra})
    assert config.is_single_database_recipe() is single
    if single:
        assert config.pinned_database_name() == pinned


def test_database_pattern_is_declared_not_guessed() -> None:
    result = check_filters(
        source_type="mssql",
        config_dict={**_BASE, "database_pattern": {"deny": ["^NewData$"]}},
        kind="Database",
        parent_path=[],
        names=["NewData", "DemoData"],
    )
    assert result.pattern_field == "database_pattern"
    assert [v.included for v in result.results] == [False, True]
    assert result.warnings == []


def test_an_odbc_engine_learns_to_read_sql_variant() -> None:
    """_add_output_converters, applied per connection: the probe never runs
    SQLServerSource.__init__, where ingestion installs it."""
    from datahub.ingestion.source.sql.mssql.source import add_sql_variant_converter

    added: Dict[int, object] = {}

    class _DbapiConnection:
        def add_output_converter(self, sql_type: int, func: object) -> None:
            added[sql_type] = func

    add_sql_variant_converter(_DbapiConnection())
    assert list(added) == [-150]
    # pytds connections have no such method; that must not fail the connection.
    add_sql_variant_converter(object())
