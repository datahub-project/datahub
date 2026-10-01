"""Fivetran's probe: it reads what ingestion reads, reports what the patterns
would drop instead of hiding it, and never returns user identity."""

import datetime
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeReadFailed
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


_TABLE_LINEAGE_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": connection_id,
        "source_table_id": f"{connection_id}_st",
        "source_table_name": "orders",
        "source_schema_name": "public",
        "destination_table_id": f"{connection_id}_dt",
        "destination_table_name": "orders",
        "destination_schema_name": "sales",
        "created_at": datetime.datetime(2026, 1, 1),
    }
    for connection_id in ("conn_a1", "conn_a2")
]

_COLUMN_LINEAGE_ROWS: List[Dict[str, object]] = [
    {
        "source_table_id": f"{connection_id}_st",
        "destination_table_id": f"{connection_id}_dt",
        "source_column_name": "id",
        "destination_column_name": "id",
    }
    for connection_id in ("conn_a1", "conn_a2")
]

_SYNC_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": "conn_a1",
        "sync_id": "sync_1",
        "start_time": datetime.datetime(2026, 1, 1, 10, 0),
        "end_time": datetime.datetime(2026, 1, 1, 10, 5),
        "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
    }
]


def _route(query: str) -> List[Dict[str, object]]:
    # Order matters: the column query joins source_table, the sync query
    # reads the log table, and only the connectors query names connection_name.
    if "ranked_syncs" in query:
        return _SYNC_ROWS
    if "column_lineage" in query:
        return _COLUMN_LINEAGE_ROWS
    if "table_lineage" in query:
        return _TABLE_LINEAGE_ROWS
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


_BASE = "https://api.fivetran.com/v1"


def _response(payload: object = None, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    if status >= 400:
        error = requests.HTTPError(f"HTTP {status}")
        error.response = resp
        resp.raise_for_status.side_effect = error
    return resp


def _ok(data: object) -> MagicMock:
    return _response({"code": "Success", "data": data})


def _listed(connector_id: str, name: str, group: str) -> Dict[str, object]:
    return {
        "id": connector_id,
        "schema": name,
        "service": "postgres",
        "paused": False,
        "sync_frequency": 360,
        "group_id": group,
        "connected_by": "user_x",
    }


def _page(*items: Dict[str, object]) -> MagicMock:
    return _ok({"items": list(items), "next_cursor": None})


_GROUPS = _page(
    {"id": "dest_a", "name": "Warehouse A"}, {"id": "dest_b", "name": "Warehouse B"}
)


@contextmanager
def _rest_api(routes: Dict[str, MagicMock]) -> Iterator[None]:
    def get(url: str, **kwargs: Any) -> MagicMock:
        return routes[url.removeprefix(_BASE)]

    # Patched on the class: MagicMock is not a descriptor, so `self` is not
    # passed and `get` sees the URL first.
    with mock.patch.object(requests.Session, "get", side_effect=get):
        yield


def test_rest_connectors_survive_one_destination_disappearing() -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _response(status=404),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        records = probe.connectors()
        warnings = list(probe.warnings)
    assert [r["connector_id"] for r in records] == ["conn_a1"]
    assert any("dest_b" in w for w in warnings)
    assert not any("user" in key for record in records for key in record)


def test_rest_connectors_raise_on_an_auth_failure() -> None:
    with (
        _rest_api({"/groups": _response(status=401)}),
        _probe({"api_config": _API}) as probe,
        pytest.raises(requests.HTTPError),
    ):
        probe.connectors()


def test_a_non_success_reply_is_a_read_failure_not_bad_input() -> None:
    bad = _response({"code": "NotFound_Account", "message": "no such account"})
    with (
        _rest_api({"/groups": bad}),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ProbeReadFailed),
    ):
        probe.destinations()


def test_rest_destinations_are_group_ids_with_their_names() -> None:
    with _rest_api({"/groups": _GROUPS}), _probe({"api_config": _API}) as probe:
        records = probe.destinations()
    assert [(d["name"], d["group_name"]) for d in records] == [
        ("dest_a", "Warehouse A"),
        ("dest_b", "Warehouse B"),
    ]


def test_connector_tables_resolve_a_connector_by_name_or_id(
    engine: MagicMock,
) -> None:
    with _probe(_db_recipe()) as probe:
        by_name = probe.connector_tables("sales_pg")
        by_id = probe.connector_tables("conn_a1")
    assert (
        by_name
        == by_id
        == [
            {
                "source_table": "public.orders",
                "destination_table": "sales.orders",
                "column_count": 1,
                "lineage_source": "log_database",
            }
        ]
    )


def test_google_sheets_connectors_get_no_column_lineage_as_in_ingestion(
    engine: MagicMock,
) -> None:
    with _probe(_db_recipe()) as probe:
        tables = probe.connector_tables("sheets", include_columns=True)
    assert tables[0]["column_count"] == 0
    assert tables[0]["columns"] == []


def test_an_unknown_connector_is_bad_input(engine: MagicMock) -> None:
    with _probe(_db_recipe()) as probe, pytest.raises(ValueError, match="nope"):
        probe.connector_tables("nope")


def test_an_ambiguous_connector_name_is_refused() -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _page(_listed("conn_b1", "sales_pg", "dest_b")),
    }
    with (
        _rest_api(routes),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ValueError, match="conn_b1"),
    ):
        probe.connector_tables("sales_pg")


def test_hybrid_mode_prefers_the_log_and_falls_back_to_rest_schemas(
    engine: MagicMock,
) -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _page(_listed("conn_b9", "crm", "dest_b")),
        # conn_b9 has no rows in the log, so ingestion reads the REST schemas.
        "/connections/conn_b9/schemas": _ok(
            {
                "schemas": {
                    "crm_src": {
                        "name_in_destination": "crm",
                        "tables": {"accounts": {"name_in_destination": "accounts"}},
                    }
                }
            }
        ),
        "/connections/conn_b9/schemas/crm_src/tables/accounts/columns": _ok(
            {"columns": {"id": {"name_in_destination": "id"}}}
        ),
    }
    recipe = _db_recipe(log_source="rest_api", api_config=_API)
    with _rest_api(routes), _probe(recipe) as probe:
        from_log = probe.connector_tables("conn_a1")
        from_rest = probe.connector_tables("conn_b9")
    assert [t["lineage_source"] for t in from_log] == ["log_database"]
    assert from_rest == [
        {
            "source_table": "crm_src.accounts",
            "destination_table": "crm.accounts",
            "column_count": 1,
            "lineage_source": "rest_schemas",
        }
    ]


def test_rest_only_lineage_degrades_on_a_missing_schemas_endpoint() -> None:
    routes = {
        "/groups": _page({"id": "dest_a", "name": "Warehouse A"}),
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/connections/conn_a1/schemas": _response(status=404),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        assert probe.connector_tables("conn_a1") == []
        assert any("conn_a1" in w and "404" in w for w in probe.warnings)


def test_sync_history_reports_run_status_without_message_text(
    engine: MagicMock,
) -> None:
    with _probe(_db_recipe()) as probe:
        runs = probe.sync_history("sales_pg")
    assert runs == [
        {
            "sync_id": "sync_1",
            "start_time": int(datetime.datetime(2026, 1, 1, 10, 0).timestamp()),
            "end_time": int(datetime.datetime(2026, 1, 1, 10, 5).timestamp()),
            "status": "SUCCESSFUL",
        }
    ]


def test_a_rest_only_recipe_has_no_sync_history_and_says_why() -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _page(),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        assert probe.sync_history("conn_a1") == []
        assert any("fivetran_log_config" in w for w in probe.warnings)


def test_a_rest_listing_judged_from_run_applies_the_id_or_name_rule() -> None:
    # The round trip the REST note tells the caller to make: the listing's
    # connector_id reaches the verdict, so a connector whose name is denied but
    # whose id is allowed reads as kept, as ingestion keeps it.
    routes = {
        "/groups": _page({"id": "dest_a", "name": "Warehouse A"}),
        "/groups/dest_a/connections": _page(
            _listed("conn_a1", "sales_pg", "dest_a"),
            _listed("conn_a2", "hr_pg", "dest_a"),
        ),
    }
    recipe: Dict[str, object] = {
        "api_config": _API,
        "connector_patterns": {"allow": ["^conn_a1$", "^nothing$"]},
    }
    with _rest_api(routes):
        run = run_probe_method("fivetran", recipe, "connectors", {})
    listing = listing_from_run(run.to_dict())
    result = check_filters(
        source_type="fivetran",
        config_dict=recipe,
        kind="Connector",
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )
    assert {r.name: r.included for r in result.results} == {
        "sales_pg": True,
        "hr_pg": False,
    }
    assert result.warnings == []
