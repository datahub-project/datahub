"""Fivetran's probe: it reads what ingestion reads, reports what the patterns
would drop instead of hiding it, and never returns user identity."""

from typing import Any, Dict, Iterator, List
from unittest import mock
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.source.fivetran.config import FivetranSourceConfig
from datahub.ingestion.source.fivetran.fivetran_probe import FivetranMetadataProbe

_CONNECTOR_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": "conn_a1",
        "connecting_user_id": "user_x",
        "connector_type_id": "postgres",
        "connection_name": "sales_pg",
        "paused": False,
        "sync_frequency": 1440,
        "destination_id": "dest_a",
    },
    {
        "connection_id": "conn_b1",
        "connecting_user_id": "user_x",
        "connector_type_id": "mysql",
        "connection_name": "hr_pg",
        "paused": True,
        "sync_frequency": 360,
        "destination_id": "dest_b",
    },
    {
        "connection_id": "conn_a2",
        "connecting_user_id": "user_x",
        "connector_type_id": "google_sheets",
        "connection_name": "sheets",
        "paused": False,
        "sync_frequency": 360,
        "destination_id": "dest_a",
    },
]


def _route(query: str) -> List[Dict[str, object]]:
    # Order matters: the column query joins source_table, the sync query
    # reads the log table, and only the connectors query names connection_name.
    if "ranked_syncs" in query:
        return []
    if "column_lineage" in query:
        return []
    if "table_lineage" in query:
        return []
    if "connection_name" in query:
        return _CONNECTOR_ROWS
    return []


def _db_recipe(**overrides: object) -> Dict[str, object]:
    recipe: Dict[str, object] = {
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "snowflake_destination_config": {
                "account_id": "acct",
                "username": "u",
                "password": "p",
                "warehouse": "wh",
                "database": "log_db",
                "log_schema": "log_schema",
            },
        },
    }
    recipe.update(overrides)
    return recipe


def _execute(clause: Any, *args: Any, **kwargs: Any) -> MagicMock:
    # FivetranLogDbReader._query runs `conn.execute(text(q))` (SQLAlchemy 2.0)
    # and reads each row through `row._mapping`.
    query = clause.text if hasattr(clause, "text") else str(clause)
    result = MagicMock()
    result.__iter__.return_value = iter([MagicMock(_mapping=row) for row in _route(query)])
    return result


@pytest.fixture
def engine() -> Iterator[MagicMock]:
    # event.listens_for is patched too: the Snowflake reader registers a
    # connect listener, which SQLAlchemy refuses on a MagicMock engine.
    with (
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
        ) as create_engine,
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.event.listens_for",
            lambda *args, **kwargs: lambda fn: fn,
        ),
    ):
        conn = create_engine.return_value.connect.return_value.__enter__.return_value
        conn.execute.side_effect = _execute
        yield create_engine


def _probe(recipe: Dict[str, object]) -> FivetranMetadataProbe:
    return FivetranMetadataProbe.for_config(FivetranSourceConfig.model_validate(recipe))


@pytest.mark.xfail(strict=True, reason="commands land in later tasks")
def test_methods_declare_the_kinds_probe_filter_judges() -> None:
    declared = {spec.command: spec.kind for spec in list_probe_methods("fivetran")}
    assert declared == {
        "destinations": "Destination",
        "connectors": "Connector",
        "connector_tables": None,
        "sync_history": None,
    }


def test_building_the_probe_opens_no_connection(engine: MagicMock) -> None:
    probe = _probe(_db_recipe())
    engine.assert_not_called()
    with probe:
        probe.destinations()
        probe.connectors()
    # One engine for the whole provider, not one per command.
    assert engine.call_count == 1
    engine.return_value.dispose.assert_called_once()


def test_log_database_connectors_include_ones_the_recipe_would_drop(
    engine: MagicMock,
) -> None:
    with _probe(_db_recipe(connector_patterns={"deny": [".*"]})) as probe:
        records = probe.connectors()
    assert [r["name"] for r in records] == ["sales_pg", "hr_pg", "sheets"]
    assert records[0]["connector_id"] == "conn_a1"
    assert records[0]["destination_id"] == "dest_a"
    # Metadata only: the connecting user is fetched by the query and withheld.
    assert not any("user" in key for record in records for key in record)


def test_destinations_are_the_ids_destination_patterns_matches(
    engine: MagicMock,
) -> None:
    with _probe(_db_recipe()) as probe:
        assert [d["name"] for d in probe.destinations()] == ["dest_a", "dest_b"]


def test_connectors_under_one_destination_report_it_as_their_parent(
    engine: MagicMock,
) -> None:
    result = run_probe_method(
        "fivetran", _db_recipe(), "connectors", {"destination": "dest_a"}
    )
    assert result.kind == "Connector"
    assert result.parent_path == ["dest_a"]
    records = result.result
    assert isinstance(records, list)
    assert [r["name"] for r in records] == ["sales_pg", "sheets"]


_API = {"api_key": "k", "api_secret": "s"}


def test_log_database_mode_judges_a_connector_on_its_name_alone() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict=_db_recipe(connector_patterns={"allow": ["^sales_.*"]}),
        kind="Connector",
        parent_path=[],
        names=["sales_pg", "hr_pg"],
    )
    assert {r.name: (r.included, r.excluded_by) for r in result.results} == {
        "sales_pg": (True, None),
        "hr_pg": (False, "connector_patterns"),
    }
    assert result.warnings == []


def test_a_connector_on_a_denied_destination_is_excluded_by_that_destination() -> (
    None
):
    result = check_filters(
        source_type="fivetran",
        config_dict=_db_recipe(destination_patterns={"deny": ["^dest_b$"]}),
        kind="Connector",
        parent_path=["dest_b"],
        names=["hr_pg"],
    )
    verdict = result.results[0]
    assert (verdict.included, verdict.excluded_by) == (False, "destination_patterns")


def test_destinations_are_judged_on_their_id() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict=_db_recipe(destination_patterns={"allow": ["^dest_a$"]}),
        kind="Destination",
        parent_path=[],
        names=["dest_a", "dest_b"],
    )
    assert [r.included for r in result.results] == [True, False]


def test_rest_mode_warns_that_the_id_is_matched_too() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict={
            "api_config": _API,
            "connector_patterns": {"deny": ["^sales_pg$"]},
        },
        kind="Connector",
        parent_path=[],
        names=["sales_pg"],
    )
    # The name alone is denied...
    assert result.results[0].included is False
    # ...but REST ingestion would still keep it via its id, and the caller
    # must be told the verdict covers only half of the rule.
    assert any("connector_id" in w for w in result.warnings)


def test_rest_mode_keeps_a_connector_whose_id_is_allowed() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict={
            "api_config": _API,
            "connector_patterns": {"deny": ["^sales_pg$"]},
        },
        kind="Connector",
        parent_path=[],
        names=["sales_pg"],
        attributes=[{"connector_id": "conn_a1"}],
    )
    assert result.results[0].included is True
    assert result.warnings == []


def test_rest_mode_drops_a_connector_when_neither_id_nor_name_is_allowed() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict={
            "api_config": _API,
            "connector_patterns": {"allow": ["^other$"]},
        },
        kind="Connector",
        parent_path=[],
        names=["sales_pg"],
        attributes=[{"connector_id": "conn_a1"}],
    )
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "connector_patterns",
    )
