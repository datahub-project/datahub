import json
import logging
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    PermissionDenied,
    Unauthenticated,
)
from databricks.sdk.service import catalog as sdk_catalog
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    GetMetastoreSummaryResponse,
    SchemaInfo,
    TableInfo,
    TableType,
)
from databricks.sdk.service.workspace import ObjectInfo, ObjectType
from databricks.sql.exc import RequestError, ServerOperationError

from datahub.ingestion.agent.error_policy import foreign_label
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeConnectionError
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import TableInfoWithGeneration
from datahub.ingestion.source.unity.unity_probe import UnityCatalogMetadataProbe

BASE: Dict[str, Any] = {
    "workspace_url": "https://example.cloud.databricks.com",
    "token": "t",
}


def _config(**extra: Any) -> UnityCatalogSourceConfig:
    return UnityCatalogSourceConfig.model_validate({**BASE, **extra})


def _fake_ws() -> MagicMock:
    ws = MagicMock()
    ws.config.warehouse_id = None
    return ws


def _probe(ws: MagicMock, **extra: Any) -> UnityCatalogMetadataProbe:
    return UnityCatalogMetadataProbe(ws, _config(**extra))


def test_probe_methods_advertise_the_unity_commands_not_the_sqlalchemy_ones() -> None:
    commands = {s.command for s in list_probe_methods("unity-catalog")}
    assert {"catalogs", "sql"} <= commands
    # The inherited SQLAlchemy getters could not authenticate against Databricks.
    assert "containers" not in commands
    assert "foreign_keys" not in commands


def test_the_databricks_alias_reaches_the_same_provider() -> None:
    assert {s.command for s in list_probe_methods("databricks")} == {
        s.command for s in list_probe_methods("unity-catalog")
    }


def test_catalogs_lists_every_catalog_including_ones_the_pattern_denies() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [
        CatalogInfo(name="main"),
        CatalogInfo(name="my cat"),
    ]
    probe = _probe(ws, catalog_pattern={"deny": ["^main$"]})
    assert probe.catalogs(limit=10) == ["main", "my cat"]


def test_catalogs_honours_a_pinned_list_and_reports_a_missing_entry() -> None:
    ws = _fake_ws()

    def get(name: str, include_browse: bool) -> CatalogInfo:
        if name == "typo":
            raise NotFound("catalog not found")
        return CatalogInfo(name=name)

    ws.catalogs.get.side_effect = get
    probe = _probe(ws, catalogs=["main", "typo"])
    assert probe.catalogs(limit=10) == ["main"]
    ws.catalogs.list.assert_not_called()
    # _get_catalogs has no try around catalogs.get, so the run itself fails.
    assert any("typo" in w and "fail" in w for w in probe.warnings)


def test_catalogs_adds_hive_metastore_when_ingestion_would_read_it() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [
        CatalogInfo(name="main"),
        CatalogInfo(name="hive_metastore"),
    ]
    probe = _probe(ws, warehouse_id="w1", include_hive_metastore=True)
    assert probe.catalogs(limit=10) == ["hive_metastore", "main"]


def test_catalogs_names_the_metastore_the_patterns_are_prefixed_with() -> None:
    # With include_metastore, catalog_pattern and schema_pattern match
    # `metastore.catalog[.schema]`; an agent needs the metastore name to pass
    # it as --parent, and nothing else in the probe returns it.
    ws = _fake_ws()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse(
        name="my metastore"
    )
    probe = _probe(ws, include_metastore=True)
    assert probe.catalogs(limit=10) == ["main"]
    assert any("'my metastore'" in w and "--parent" in w for w in probe.warnings)


def test_catalogs_reads_no_metastore_when_it_is_not_part_of_the_id() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    probe = _probe(ws)
    assert probe.catalogs(limit=10) == ["main"]
    ws.metastores.summary.assert_not_called()
    assert probe.warnings == []


def test_catalogs_still_lists_when_the_metastore_may_not_be_read() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.metastores.summary.side_effect = PermissionDenied("secret-detail")
    probe = _probe(ws, include_metastore=True)
    assert probe.catalogs(limit=10) == ["main"]
    assert any("metastore" in w for w in probe.warnings)
    assert not any("secret-detail" in w for w in probe.warnings)


def test_catalogs_warns_when_no_metastore_is_assigned() -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse()
    probe = _probe(ws, include_metastore=True)
    assert probe.catalogs(limit=10) == ["main"]
    assert any("no assigned metastore" in w for w in probe.warnings)


def test_catalogs_stops_paging_at_the_limit() -> None:
    ws = _fake_ws()
    pulled: List[str] = []

    def gen() -> Iterator[CatalogInfo]:
        for i in range(1000):
            pulled.append(str(i))
            yield CatalogInfo(name=f"c{i}")

    ws.catalogs.list.return_value = gen()
    assert len(_probe(ws).catalogs(limit=3)) == 3
    assert len(pulled) <= 4


def _serve(monkeypatch: pytest.MonkeyPatch, ws: MagicMock) -> None:
    monkeypatch.setattr(
        UnityCatalogMetadataProbe,
        "for_config",
        classmethod(lambda cls, config: cls(ws, config)),
    )


def test_schemas_lists_raw_names_and_carries_the_catalog_as_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="my cat")
    ws.schemas.list.return_value = [
        SchemaInfo(name="analytics"),
        SchemaInfo(name="information_schema"),
    ]
    _serve(monkeypatch, ws)
    result = run_probe_method("unity-catalog", BASE, "schemas", {"catalog": "my cat"})
    assert result.result == ["analytics", "information_schema"]
    assert result.parent_path == ["my cat"]
    assert result.kind == "Schema"
    assert ws.schemas.list.call_args.kwargs["catalog_name"] == "my cat"


def test_schemas_of_an_unknown_catalog_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.catalogs.get.side_effect = NotFound("no such catalog")
    with pytest.raises(ValueError, match="no catalog 'typo'"):
        _probe(ws).schemas(catalog="typo")


def test_schemas_the_credential_cannot_browse_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = PermissionDenied("no USE CATALOG")
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    # Empty WITH a reason, never a silent empty.
    assert any("main" in w and "403" in w for w in probe.warnings)


def _hive_probe(ws: MagicMock) -> UnityCatalogMetadataProbe:
    # The flag needs a warehouse_id, or the config switches it back off.
    return _probe(ws, warehouse_id="w1", include_hive_metastore=True)


def test_hive_metastore_schemas_are_not_probed_and_say_so() -> None:
    ws = _fake_ws()
    probe = _hive_probe(ws)
    assert probe.schemas(catalog="hive_metastore") == []
    ws.catalogs.get.assert_not_called()
    assert any("hive_metastore" in w for w in probe.warnings)


def test_sdk_error_text_never_reaches_the_caller() -> None:
    # An unparseable response embeds the whole request log in the message.
    leaky = "unable to parse response. Request log: GET /api/2.1 Authorization: Bearer s3cr3t"
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = Unauthenticated(leaky)
    with pytest.raises(ProbeConnectionError) as raised:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(raised.value)
    assert "Unauthenticated" in str(raised.value) and "401" in str(raised.value)

    ws.schemas.list.side_effect = PermissionDenied(leaky)
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    assert probe.warnings and not any("s3cr3t" in w for w in probe.warnings)

    ws.catalogs.get.side_effect = NotFound(leaky)
    with pytest.raises(ValueError) as missing:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(missing.value)


@pytest.mark.parametrize(
    "command, arguments, shown",
    [
        ("schemas", {"catalog": "typo"}, "no catalog 'typo' visible"),
        ("tables", {"catalog": "main", "schema": "s"}, "refused as a bad request"),
        (
            "sql",
            {"query": "SELECT table_name FROM main.information_schema.tables"},
            "set warehouse_id in the recipe",
        ),
    ],
)
def test_a_caller_mistake_reaches_the_caller_with_its_message(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    arguments: Dict[str, Any],
    shown: str,
) -> None:
    # Through the framework, which shows only a trusted type's message: a
    # plain ValueError would arrive as "'schemas' failed (ValueError)".
    ws = _fake_ws()

    def get(name: str, include_browse: bool) -> CatalogInfo:
        if name == "typo":
            raise NotFound("no such catalog")
        return CatalogInfo(name=name)

    ws.catalogs.get.side_effect = get
    ws.tables.list.side_effect = BadRequest("INVALID_PARAMETER_VALUE")
    _serve(monkeypatch, ws)
    with pytest.raises(ProbeArgumentError, match=shown):
        run_probe_method("unity-catalog", BASE, command, arguments)


class _OtherLibraryError(Exception):
    error_code = "PERMISSION_DENIED"


def test_an_escaping_sdk_error_is_labelled_with_its_code_never_its_text() -> None:
    exc = PermissionDenied("Bearer s3cr3t", error_code="PERMISSION_DENIED")
    assert UnityCatalogMetadataProbe.probe_error_code(exc) == "PERMISSION_DENIED"
    assert foreign_label(exc, UnityCatalogMetadataProbe) == (
        "PermissionDenied; PERMISSION_DENIED"
    )
    # Text in the code slot is no code, and another library's attribute is
    # not the SDK's.
    assert (
        UnityCatalogMetadataProbe.probe_error_code(
            PermissionDenied("x", error_code="see s3cr3t")
        )
        is None
    )
    assert UnityCatalogMetadataProbe.probe_error_code(_OtherLibraryError()) is None


def test_a_client_that_cannot_be_built_is_reported_without_the_sdk_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(config: UnityCatalogSourceConfig) -> MagicMock:
        raise ValueError(b'{"error":"invalid_client","secret":"s3cr3t"}')

    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.create_workspace_client", broken
    )
    with pytest.raises(ProbeConnectionError) as raised:
        UnityCatalogMetadataProbe.for_config(_config())
    assert "s3cr3t" not in str(raised.value)


_HAS_METRIC_VIEW = getattr(TableType, "METRIC_VIEW", None) is not None


def _tables_ws() -> MagicMock:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    rows = [
        TableInfoWithGeneration(name="orders", table_type=TableType.MANAGED),
        TableInfoWithGeneration(name="v_orders", table_type=TableType.VIEW),
        TableInfoWithGeneration(
            name="mv_orders", table_type=TableType.MATERIALIZED_VIEW
        ),
        TableInfoWithGeneration(name="returns", table_type=TableType.EXTERNAL),
    ]
    if _HAS_METRIC_VIEW:
        rows.append(
            TableInfoWithGeneration(name="kpi", table_type=TableType.METRIC_VIEW)
        )
    ws.tables.list.return_value = rows
    return ws


def test_tables_views_and_metric_views_split_the_way_process_tables_does() -> None:
    probe = _probe(_tables_ws())
    assert probe.tables(catalog="main", schema="analytics") == ["orders", "returns"]
    assert probe.views(catalog="main", schema="analytics") == ["v_orders", "mv_orders"]
    if _HAS_METRIC_VIEW:
        assert probe.metric_views(catalog="main", schema="analytics") == ["kpi"]


def test_stopping_at_the_limit_restores_the_sdk_class_ingestion_patches() -> None:
    # proxy.tables swaps TableInfo for TableInfoWithGeneration around its
    # loop; abandoning the generator mid-loop must not leave the swap behind.
    original = sdk_catalog.TableInfo
    assert _probe(_tables_ws()).tables(catalog="main", schema="analytics", limit=1) == [
        "orders"
    ]
    assert sdk_catalog.TableInfo is original


def test_a_listed_table_round_trips_into_the_verdict_ingestion_makes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _tables_ws()
    _serve(monkeypatch, ws)
    config = {**BASE, "table_pattern": {"allow": [r"^main\.analytics\.orders$"]}}
    listed = run_probe_method(
        "unity-catalog", config, "tables", {"catalog": "main", "schema": "analytics"}
    )
    assert listed.kind is not None and isinstance(listed.result, list)
    verdict = check_filters(
        source_type="unity-catalog",
        config_dict=config,
        kind=listed.kind,
        parent_path=listed.parent_path,
        names=listed.result,
    )
    assert [(r.target, r.included) for r in verdict.results] == [
        ("main.analytics.orders", True),
        ("main.analytics.returns", False),
    ]
    assert verdict.warnings == []


def test_hive_metastore_tables_are_not_probed_and_say_so() -> None:
    ws = _fake_ws()
    probe = _hive_probe(ws)
    assert probe.tables(catalog="hive_metastore", schema="default") == []
    assert probe.columns(catalog="hive_metastore", schema="default", table="t") == []
    ws.tables.list.assert_not_called()
    ws.tables.get.assert_not_called()
    assert any("hive_metastore" in w for w in probe.warnings)


def test_hive_metastore_is_listed_over_rest_while_include_hive_metastore_is_off() -> (
    None
):
    # Without the flag ingestion builds no HiveMetastoreProxy, so a UC-listed
    # hive_metastore goes through the same REST listings as any catalog.
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="hive_metastore")
    ws.schemas.list.return_value = [SchemaInfo(name="default")]
    ws.tables.list.return_value = [
        TableInfoWithGeneration(name="t", table_type=TableType.MANAGED)
    ]
    probe = _probe(ws)
    assert probe.schemas(catalog="hive_metastore") == ["default"]
    assert probe.tables(catalog="hive_metastore", schema="default") == ["t"]
    assert probe.warnings == []


@pytest.mark.parametrize("command", ["tables", "views", "metric_views"])
def test_a_mistyped_schema_is_a_caller_error(command: str) -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.tables.list.side_effect = NotFound("SCHEMA_DOES_NOT_EXIST")
    with pytest.raises(ValueError, match="main.typo"):
        getattr(_probe(ws), command)(catalog="main", schema="typo")


def test_tables_of_a_schema_the_credential_cannot_read_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.tables.list.side_effect = PermissionDenied("no USE SCHEMA")
    probe = _probe(ws)
    assert probe.views(catalog="main", schema="analytics") == []
    assert any("main.analytics" in w for w in probe.warnings)


def test_columns_are_structural_metadata_only() -> None:
    ws = _fake_ws()
    ws.tables.get.return_value = TableInfo(
        name="orders",
        columns=[
            ColumnInfo(name="id", type_text="bigint", nullable=False, comment="pk")
        ],
    )
    cols = _probe(ws).columns(catalog="main", schema="analytics", table="orders")
    assert cols == [
        {
            "name": "id",
            "type": "bigint",
            "nullable": False,
            "comment": "pk",
            "partition_index": None,
        }
    ]
    assert ws.tables.get.call_args.kwargs["full_name"] == "main.analytics.orders"


def test_columns_of_an_unknown_table_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.tables.get.side_effect = NotFound("no such table")
    with pytest.raises(ValueError, match="main.analytics.typo"):
        _probe(ws).columns(catalog="main", schema="analytics", table="typo")


def _notebook(object_id: int, path: str) -> ObjectInfo:
    return ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=object_id, path=path)


# Every spelling a personal path takes, each naming a placeholder principal.
_PERSONAL = {
    "/Users/person.one@example.com/pipeline": "person.one",
    "/users/lower@example.com/n": "lower@",
    "/Repos/repouser@example.com/r/nb": "repouser",
    "/Workspace/Users/wsuser@example.com/n": "wsuser",
    "//Users/dbl@example.com/n": "dbl@",
    "/Shared/../Users/dotdot@example.com/n": "dotdot",
}
_SHARED = ["/Shared/x", "/Workspace/Shared/x", "//Shared//x", "/shared/lower"]


def _notebook_ws(paths: List[str]) -> MagicMock:
    ws = _fake_ws()
    ws.workspace.list.return_value = [
        _notebook(i, path) for i, path in enumerate(paths, start=1)
    ]
    return ws


@pytest.mark.parametrize("path", _SHARED)
def test_shared_notebooks_are_listed_raw_even_while_include_notebooks_is_off(
    path: str,
) -> None:
    probe = _probe(_notebook_ws([path]))
    # The raw path, not the normalised form: it is what notebook_pattern sees.
    assert probe.notebooks() == [path]
    assert probe.warnings == []


@pytest.mark.parametrize("path", [*_PERSONAL, "/Repos/team/report"])
def test_a_path_outside_shared_is_withheld_unless_ingestion_reads_it(
    path: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    _serve(monkeypatch, _notebook_ws(["/Shared/etl", path]))
    result = run_probe_method("unity-catalog", BASE, "notebooks", {})
    assert result.result == ["/Shared/etl"]
    assert any(w.startswith("1 notebook outside") for w in result.warnings)
    principal = _PERSONAL.get(path, "team")
    assert principal not in json.dumps(result.to_dict())
    assert principal not in caplog.text


def test_a_personal_notebook_the_recipe_ingests_is_listed() -> None:
    paths = list(_PERSONAL)
    probe = _probe(
        _notebook_ws(["/Shared/etl", *paths]),
        include_notebooks=True,
        notebook_pattern={"allow": ["^/Shared/.*", "^/Repos/repouser@example.com/.*"]},
    )
    # notebook_pattern allows /Shared/../Users/... too: ingestion would read
    # it, so it is no secret from this recipe.
    assert probe.notebooks() == [
        "/Shared/etl",
        "/Repos/repouser@example.com/r/nb",
        "/Shared/../Users/dotdot@example.com/n",
    ]
    assert any(w.startswith(f"{len(paths) - 2} notebooks") for w in probe.warnings)


def test_a_walk_cut_at_the_limit_reports_the_withheld_count_as_a_lower_bound() -> None:
    probe = _probe(
        _notebook_ws(["/Users/a@example.com/n", "/Shared/a", "/Shared/b", "/Shared/c"])
    )
    assert probe.notebooks(limit=2) == ["/Shared/a", "/Shared/b"]
    assert any(w.startswith("at least 1 notebook") for w in probe.warnings)


def test_notebooks_the_credential_cannot_list_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.workspace.list.side_effect = PermissionDenied("no workspace access")
    probe = _probe(ws)
    assert probe.notebooks() == []
    assert probe.warnings


def _sql_ws() -> MagicMock:
    ws = _fake_ws()
    ws.config.warehouse_id = "w1"
    ws.config.host = "https://example.cloud.databricks.com"
    return ws


def _fake_connection(rows: List[Any]) -> MagicMock:
    cursor = MagicMock()
    cursor.description = [("table_name",)]
    cursor.fetchmany.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn


def test_sql_without_a_warehouse_is_a_recipe_error() -> None:
    with pytest.raises(ValueError, match="warehouse_id"):
        _probe(_fake_ws()).execute_catalog_query(
            "SELECT 1 FROM system.information_schema.tables", 2
        )


def test_sql_opens_one_connection_with_ingestions_params_and_a_server_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: List[Dict[str, Any]] = []
    conn = _fake_connection([("orders",)])

    def connect(**kwargs: Any) -> MagicMock:
        opened.append(kwargs)
        return conn

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    probe = _probe(_sql_ws(), warehouse_id="w1")
    query = "SELECT table_name FROM main.information_schema.tables"
    rows = probe.execute_catalog_query(query, 3)
    probe.execute_catalog_query(query, 3)
    assert list(rows.columns) == ["table_name"]
    assert [list(r) for r in rows.rows] == [["orders"]]
    assert len(opened) == 1
    assert opened[0]["http_path"] == "/sql/1.0/warehouses/w1"
    assert opened[0]["server_hostname"] == "example.cloud.databricks.com"
    assert opened[0]["session_configuration"] == {"STATEMENT_TIMEOUT": "30"}
    conn.cursor.return_value.__enter__.return_value.fetchmany.assert_called_with(3)
    probe.__exit__(None, None, None)
    conn.close.assert_called_once()


def test_sql_through_the_framework_is_scope_checked_before_the_warehouse_is_touched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: List[Dict[str, Any]] = []

    def connect(**kwargs: Any) -> MagicMock:
        opened.append(kwargs)
        return _fake_connection([("orders",)])

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    _serve(monkeypatch, _sql_ws())
    config = {**BASE, "warehouse_id": "w1"}
    with pytest.raises(ValueError):
        run_probe_method(
            "unity-catalog",
            config,
            "sql",
            {"query": "SELECT statement_text FROM system.query.history"},
        )
    assert opened == []
    result = run_probe_method(
        "unity-catalog",
        config,
        "sql",
        {"query": "SELECT table_name FROM main.information_schema.tables"},
    )
    assert isinstance(result.result, dict) and result.result["rows"] == [["orders"]]


def test_the_sql_help_says_it_needs_and_may_start_a_warehouse() -> None:
    (spec,) = [s for s in list_probe_methods("unity-catalog") if s.command == "sql"]
    help_text = " ".join(spec.description.split())
    assert "warehouse_id" in help_text
    assert "start" in help_text and "stopped" in help_text


@pytest.mark.parametrize(
    "error, expected",
    [
        (
            ServerOperationError(
                "[CAST_INVALID_INPUT] The value 's3cr3t-row-value' cannot be cast"
            ),
            ValueError,
        ),
        (
            RequestError("Error during request to server: Bearer s3cr3t"),
            ProbeConnectionError,
        ),
    ],
)
def test_warehouse_error_text_never_reaches_the_caller(
    monkeypatch: pytest.MonkeyPatch, error: Exception, expected: type
) -> None:
    conn = _fake_connection([])
    conn.cursor.return_value.__enter__.return_value.execute.side_effect = error
    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.connect",
        lambda **kwargs: conn,
    )
    probe = _probe(_sql_ws(), warehouse_id="w1")
    with pytest.raises(expected) as raised:
        probe.execute_catalog_query(
            "SELECT table_name FROM main.information_schema.tables", 2
        )
    assert "s3cr3t" not in str(raised.value)
    assert type(error).__name__ in str(raised.value)
