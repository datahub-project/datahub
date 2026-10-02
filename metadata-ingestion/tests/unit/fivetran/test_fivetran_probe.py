"""Fivetran's probe: it reads what ingestion reads, reports what the patterns
would drop instead of hiding it, and never returns user identity."""

import datetime
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeReadFailed
from datahub.ingestion.source.fivetran.config import FivetranSourceConfig
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import FivetranLogDbReader
from datahub.ingestion.source.fivetran.fivetran_probe import FivetranMetadataProbe
from tests.unit.fivetran.fivetran_probe_fixtures import db_recipe, mocked_log_db


@pytest.fixture
def engine() -> Iterator[MagicMock]:
    with mocked_log_db() as create_engine:
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
    probe = _probe(db_recipe())
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
    with _probe(db_recipe(connector_patterns={"deny": [".*"]})) as probe:
        records = probe.connectors()
    assert [r["name"] for r in records] == ["sales_pg", "hr_pg", "sheets"]
    assert records[0]["connector_id"] == "conn_a1"
    assert records[0]["destination_id"] == "dest_a"
    # Metadata only: the connecting user is fetched by the query and withheld.
    assert not any("user" in key for record in records for key in record)
    assert "user_x" not in str(records)


def test_destinations_are_the_ids_destination_patterns_matches(
    engine: MagicMock,
) -> None:
    with _probe(db_recipe()) as probe:
        assert [d["name"] for d in probe.destinations()] == ["dest_a", "dest_b"]


def test_connectors_under_one_destination_report_it_as_their_parent(
    engine: MagicMock,
) -> None:
    result = run_probe_method(
        "fivetran", db_recipe(), "connectors", {"destination": "dest_a"}
    )
    assert result.kind == "Connector"
    assert result.parent_path == ["dest_a"]
    records = result.result
    assert isinstance(records, list)
    assert [r["name"] for r in records] == ["sales_pg", "sheets"]


_API = {"api_key": "k", "api_secret": "s"}


def test_destinations_are_judged_on_their_id() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict=db_recipe(destination_patterns={"allow": ["^dest_a$"]}),
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


def test_rest_mode_reports_the_id_as_the_target_when_the_id_decides() -> None:
    # The parity test proves the verdict; it cannot see which string matched.
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
    assert (result.results[0].included, result.results[0].target) == (True, "conn_a1")


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
    # The REST field is connected_by, which a key-name check would not catch.
    assert "user_x" not in str(records)


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
    with _probe(db_recipe()) as probe:
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
    with _probe(db_recipe()) as probe:
        tables = probe.connector_tables("sheets", include_columns=True)
    assert tables[0]["column_count"] == 0
    assert tables[0]["columns"] == []


def test_an_unknown_connector_is_bad_input(engine: MagicMock) -> None:
    with _probe(db_recipe()) as probe, pytest.raises(ValueError, match="nope"):
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
    recipe = db_recipe(log_source="rest_api", api_config=_API)
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


@pytest.mark.parametrize(
    "failing",
    [
        pytest.param(_response(status=403), id="forbidden"),
        pytest.param(_response(status=503), id="unavailable"),
        pytest.param(_response({"code": "Error", "message": "nope"}), id="bad-reply"),
    ],
)
def test_rest_only_lineage_degrades_where_ingestion_does(failing: MagicMock) -> None:
    # Ingestion emits the connector without lineage on any recoverable REST
    # failure of /schemas, so failing the command would describe another run.
    routes = {
        "/groups": _page({"id": "dest_a", "name": "Warehouse A"}),
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/connections/conn_a1/schemas": failing,
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        assert probe.connector_tables("conn_a1") == []
        assert any("conn_a1" in w and "without" in w for w in probe.warnings)


def test_a_connector_on_an_unreadable_destination_is_not_reported_missing() -> None:
    # The connector may be on dest_b, which could not be listed: "no such
    # connector" (exit 2) would send the caller to fix a name that is right.
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _response(status=403),
    }
    with (
        _rest_api(routes),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ProbeReadFailed, match="dest_b"),
    ):
        probe.connector_tables("conn_b1")


def test_connector_tables_show_no_columns_when_the_recipe_emits_none(
    engine: MagicMock,
) -> None:
    with _probe(db_recipe(include_column_lineage=False)) as probe:
        tables = probe.connector_tables("sales_pg", include_columns=True)
        warnings = list(probe.warnings)
    assert tables[0]["column_count"] == 0
    assert tables[0]["columns"] == []
    assert any("include_column_lineage" in w for w in warnings)


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
    with _probe(db_recipe()) as probe:
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


_DENY_DEST_B: Dict[str, object] = {"deny": ["^dest_b$"]}


def test_a_destination_pattern_that_could_not_be_applied_is_reported() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict=db_recipe(destination_patterns=_DENY_DEST_B),
        kind="Connector",
        parent_path=[],
        names=["hr_pg"],
    )
    assert result.results[0].included is True
    assert any("destination_patterns" in w for w in result.warnings)


def test_rest_mode_says_nothing_when_the_name_alone_is_kept() -> None:
    result = check_filters(
        source_type="fivetran",
        config_dict={"api_config": _API},
        kind="Connector",
        parent_path=[],
        names=["sales_pg"],
    )
    assert result.results[0].included is True
    assert result.warnings == []


@pytest.mark.parametrize(
    "failing",
    [
        pytest.param(_response(status=403), id="forbidden"),
        pytest.param(_response({"code": "Error", "message": "nope"}), id="bad-reply"),
    ],
)
def test_rest_connectors_skip_a_group_ingestion_would_skip(failing: MagicMock) -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": failing,
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        records = probe.connectors()
        warnings = list(probe.warnings)
    assert [r["connector_id"] for r in records] == ["conn_a1"]
    assert any("dest_b" in w for w in warnings)


def test_a_malformed_reply_does_not_echo_payload_values() -> None:
    # The group lacks its required id, so pydantic rejects it -- and its own
    # message would quote the input dict.
    bad = _page({"name": "private_value_x"})
    with (
        _rest_api({"/groups": bad}),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ProbeReadFailed) as raised,
    ):
        probe.destinations()
    assert "private_value_x" not in str(raised.value)
    assert "missing" in str(raised.value)


def test_a_malformed_reply_logs_no_payload_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    bad = _page({"name": "private_value_x"})
    with (
        _rest_api({"/groups": bad}),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ProbeReadFailed),
    ):
        probe.destinations()
    assert "private_value_x" not in caplog.text


_PLANTED = "planted_secret_x"


def _error_reply() -> MagicMock:
    return _response({"code": "Error", "message": _PLANTED})


def test_a_skipped_destination_warning_carries_no_reply_text() -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _error_reply(),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        probe.connectors()
        warnings = list(probe.warnings)
    assert any("dest_b" in w for w in warnings)
    assert _PLANTED not in " ".join(warnings)


def test_a_skipped_destination_warning_keeps_the_http_status() -> None:
    routes = {
        "/groups": _GROUPS,
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/groups/dest_b/connections": _response(status=403),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        probe.connectors()
        warnings = list(probe.warnings)
    assert any("dest_b" in w and "403" in w for w in warnings)


def test_a_failed_schemas_read_warning_carries_no_reply_text() -> None:
    routes = {
        "/groups": _page({"id": "dest_a", "name": "Warehouse A"}),
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/connections/conn_a1/schemas": _error_reply(),
    }
    with _rest_api(routes), _probe({"api_config": _API}) as probe:
        probe.connector_tables("conn_a1")
        warnings = list(probe.warnings)
    assert any("conn_a1" in w for w in warnings)
    assert _PLANTED not in " ".join(warnings)


def test_an_unusable_reply_failure_carries_no_reply_text() -> None:
    with (
        _rest_api({"/groups": _error_reply()}),
        _probe({"api_config": _API}) as probe,
        pytest.raises(ProbeReadFailed) as raised,
    ):
        probe.destinations()
    assert _PLANTED not in str(raised.value)


def test_a_failed_log_warehouse_lineage_read_warning_carries_no_driver_text(
    engine: MagicMock,
) -> None:
    routes = {
        "/groups": _page({"id": "dest_a", "name": "Warehouse A"}),
        "/groups/dest_a/connections": _page(_listed("conn_a1", "sales_pg", "dest_a")),
        "/connections/conn_a1/schemas": _ok({"schemas": {}}),
    }
    recipe = db_recipe(log_source="rest_api", api_config=_API)
    with (
        _rest_api(routes),
        _probe(recipe) as probe,
        mock.patch.object(
            FivetranLogDbReader,
            "fetch_lineage_for_connectors",
            side_effect=ValueError(_PLANTED),
        ),
    ):
        probe.connector_tables("conn_a1")
        warnings = list(probe.warnings)
    assert any("lineage tables" in w for w in warnings)
    assert _PLANTED not in " ".join(warnings)


def test_a_failing_log_warehouse_open_names_the_class_not_the_text() -> None:
    with (
        _probe(db_recipe()) as probe,
        mock.patch.object(
            FivetranLogDbReader, "__init__", side_effect=ValueError(_PLANTED)
        ),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        probe.connectors()
    assert _PLANTED not in str(raised.value)
    assert "ValueError" in str(raised.value)
