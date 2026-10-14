import json
import logging
from typing import Any, Dict, Iterator, List, Optional
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
from databricks.sdk.service.iam import ServicePrincipal as SdkServicePrincipal
from databricks.sdk.service.workspace import ObjectInfo, ObjectType
from databricks.sql.exc import RequestError, ServerOperationError

from datahub.ingestion.agent.error_policy import foreign_label
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.provider_helpers import PersonalWithholding
from datahub.ingestion.agent.sql_passthrough import QueryBudget
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeSoftError,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import TableInfoWithGeneration
from datahub.ingestion.source.unity.unity_probe import (
    UnityCatalogMetadataProbe,
    _is_shared_path,
)

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


def _denied(code: Optional[str], message: str = "denied") -> PermissionDenied:
    return PermissionDenied(message, error_code=code)


def _not_found(code: Optional[str], message: str = "not found") -> NotFound:
    return NotFound(message, error_code=code)


def test_probe_methods_advertise_the_unity_commands_not_the_sqlalchemy_ones() -> None:
    commands = {s.command for s in list_probe_methods("unity-catalog")}
    assert {"catalogs", "sql"} <= commands
    # The inherited SQLAlchemy getters could not authenticate against Databricks.
    assert "containers" not in commands
    assert "foreign_keys" not in commands


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
            raise _not_found("CATALOG_DOES_NOT_EXIST")
        return CatalogInfo(name=name)

    ws.catalogs.get.side_effect = get
    probe = _probe(ws, catalogs=["main", "typo"])
    assert probe.catalogs(limit=10) == ["main"]
    ws.catalogs.list.assert_not_called()
    assert any("'typo'" in w for w in probe.warnings)


def test_pinned_catalogs_are_listed_once_up_to_the_limit_without_hive_metastore() -> (
    None
):
    ws = _fake_ws()
    ws.catalogs.get.side_effect = lambda name, include_browse: CatalogInfo(name=name)
    probe = _probe(
        ws,
        catalogs=["main", "main", "sales", "ops"],
        warehouse_id="w1",
        include_hive_metastore=True,
    )
    # _get_catalogs reads only the pinned names: no hive_metastore proxy
    # catalog on this path.
    assert probe.catalogs(limit=2) == ["main", "sales"]
    assert [c.args[0] for c in ws.catalogs.get.call_args_list] == [
        "main",
        "main",
        "sales",
    ]


def test_a_refused_catalog_listing_is_the_sessions_failure_not_an_empty_answer() -> (
    None
):
    # The listing leaves out what the principal cannot see, so its 403 can
    # only refuse the session.
    ws = _fake_ws()
    ws.catalogs.list.side_effect = _denied("PERMISSION_DENIED")
    with pytest.raises(ProbeConnectionError):
        _probe(ws).catalogs(limit=10)


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


@pytest.mark.parametrize("command", ["catalogs", "schemas"])
def test_an_unassigned_metastore_warns_instead_of_failing_the_listing(
    command: str,
) -> None:
    ws = _fake_ws()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.return_value = [SchemaInfo(name="analytics")]
    ws.metastores.summary.side_effect = _not_found("METASTORE_DOES_NOT_EXIST")
    probe = _probe(ws, include_metastore=True)
    kwargs = {"limit": 10} if command == "catalogs" else {"catalog": "main"}
    assert getattr(probe, command)(**kwargs)
    assert any("include_metastore: false" in w for w in probe.warnings)


def test_table_listings_name_the_metastore_for_the_parent_path() -> None:
    ws = _tables_ws()
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse(
        name="my metastore"
    )
    probe = _probe(ws, include_metastore=True)
    assert probe.tables(catalog="main", schema="analytics")
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
    ws.metastores.summary.side_effect = _denied("PERMISSION_DENIED", "secret-detail")
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
    ws.catalogs.get.side_effect = _not_found("CATALOG_DOES_NOT_EXIST")
    with pytest.raises(ProbeArgumentError, match="'typo'"):
        _probe(ws).schemas(catalog="typo")


@pytest.mark.parametrize(
    "error",
    [
        # The workspace has no metastore assigned.
        _not_found("METASTORE_DOES_NOT_EXIST"),
        # An HTML 404 from a workspace_url naming the wrong host.
        _not_found("NOT_FOUND"),
        _not_found(None),
    ],
)
def test_a_404_that_names_no_requested_object_is_the_sources_failure(
    error: NotFound,
) -> None:
    ws = _fake_ws()
    ws.catalogs.get.side_effect = error
    with pytest.raises(ProbeConnectionError):
        _probe(ws).schemas(catalog="main")


def test_schemas_the_credential_cannot_browse_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = _denied("PERMISSION_DENIED", "no USE CATALOG")
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    # Empty WITH a reason, never a silent empty.
    assert any("main" in w and "PERMISSION_DENIED" in w for w in probe.warnings)


@pytest.mark.parametrize(
    "code",
    [
        # What an invalid or revoked token's body carries.
        "403",
        # An IP access list block: an HTML page, so the SDK takes the reason.
        "FORBIDDEN",
        None,
    ],
)
def test_a_403_refusing_the_session_is_a_connection_failure(
    code: Optional[str],
) -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.schemas.list.side_effect = _denied(code, "Invalid access token")
    with pytest.raises(ProbeConnectionError):
        _probe(ws).schemas(catalog="main")


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

    ws.schemas.list.side_effect = _denied("PERMISSION_DENIED", leaky)
    probe = _probe(ws)
    assert probe.schemas(catalog="main") == []
    assert probe.warnings and not any("s3cr3t" in w for w in probe.warnings)

    ws.catalogs.get.side_effect = _not_found("CATALOG_DOES_NOT_EXIST", leaky)
    with pytest.raises(ProbeArgumentError) as missing:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(missing.value)


def test_the_token_endpoints_body_in_a_value_error_is_never_shown() -> None:
    # The SDK's OAuth exchange raises ValueError(resp.content) on the first
    # request; it is the source refusing the session, not a bad argument.
    ws = _fake_ws()
    ws.catalogs.get.side_effect = ValueError(
        b'{"error":"invalid_client","client_secret":"s3cr3t"}'
    )
    with pytest.raises(ProbeConnectionError) as raised:
        _probe(ws).schemas(catalog="main")
    assert "s3cr3t" not in str(raised.value)


@pytest.mark.parametrize("raised", [ProbeArgumentError("x"), ProbeSoftError("x")])
def test_the_probes_own_errors_keep_their_type_through_the_sdk_mapping(
    raised: ValueError,
) -> None:
    # Both are ValueErrors, which the mapping otherwise reads as the SDK's
    # auth failure (exit 3).
    with pytest.raises(type(raised)):
        with _probe(_fake_ws())._calling("reading"):
            raise raised


@pytest.mark.parametrize(
    "command, arguments, shown",
    [
        ("schemas", {"catalog": "typo"}, "typo"),
        ("tables", {"catalog": "main", "schema": "s"}, "main.s"),
        (
            "sql",
            {"query": "SELECT table_name FROM main.information_schema.tables"},
            "warehouse_id",
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
            raise _not_found("CATALOG_DOES_NOT_EXIST")
        return CatalogInfo(name=name)

    ws.catalogs.get.side_effect = get
    ws.tables.list.side_effect = BadRequest("bad", error_code="INVALID_PARAMETER_VALUE")
    _serve(monkeypatch, ws)
    with pytest.raises(ProbeArgumentError, match=shown):
        run_probe_method("unity-catalog", BASE, command, arguments)


class _OtherLibraryError(Exception):
    error_code = "PERMISSION_DENIED"


def test_an_escaping_sdk_error_is_labelled_with_its_code_never_its_text() -> None:
    label = foreign_label(
        _denied("PERMISSION_DENIED", "Bearer s3cr3t"), UnityCatalogMetadataProbe
    )
    assert "PERMISSION_DENIED" in label and "s3cr3t" not in label
    # Text in the code slot is no code: the HTTP status names it instead. And
    # another library's attribute is not the SDK's.
    assert (
        UnityCatalogMetadataProbe.probe_error_code(_denied("see s3cr3t")) == "HTTP 403"
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


def test_a_defect_building_the_client_is_not_reported_as_bad_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(config: UnityCatalogSourceConfig) -> MagicMock:
        raise AttributeError("no such attribute")

    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.create_workspace_client", broken
    )
    with pytest.raises(AttributeError):
        UnityCatalogMetadataProbe.for_config(_config())


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


def test_a_listed_table_is_judged_on_its_catalog_schema_table_name(
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


def test_a_mistyped_schema_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.tables.list.side_effect = _not_found("SCHEMA_DOES_NOT_EXIST")
    with pytest.raises(ProbeArgumentError, match="main.typo"):
        _probe(ws).tables(catalog="main", schema="typo")


def test_metric_views_on_an_sdk_without_the_type_warn_instead_of_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.metric_view_supported",
        lambda: False,
    )
    ws = _tables_ws()
    probe = _probe(ws)
    assert probe.metric_views(catalog="main", schema="analytics") == []
    ws.tables.list.assert_not_called()
    assert any("METRIC_VIEW" in w for w in probe.warnings)


def test_tables_of_a_schema_the_credential_cannot_read_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.catalogs.get.return_value = CatalogInfo(name="main")
    ws.tables.list.side_effect = _denied("PERMISSION_DENIED", "no USE SCHEMA")
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
    assert ws.tables.get.call_args.kwargs == {
        "full_name": "main.analytics.orders",
        # As every proxy listing passes it: a browse-only table is listed by
        # `tables`, so its columns must be readable too.
        "include_browse": True,
    }


def test_columns_say_so_when_the_api_returns_no_column_list() -> None:
    ws = _fake_ws()
    ws.tables.get.return_value = TableInfo(name="orders", columns=None)
    probe = _probe(ws)
    assert probe.columns(catalog="main", schema="analytics", table="orders") == []
    assert any("main.analytics.orders" in w for w in probe.warnings)


def test_columns_of_an_unknown_table_is_a_caller_error() -> None:
    ws = _fake_ws()
    ws.tables.get.side_effect = _not_found("TABLE_DOES_NOT_EXIST")
    with pytest.raises(ProbeArgumentError, match="main.analytics.typo"):
        _probe(ws).columns(catalog="main", schema="analytics", table="typo")


def _withheld(count: int, *, stopped_early: bool = False) -> str:
    # The framework's wording for the count, so only the count is pinned here.
    withholding = PersonalWithholding[str](
        is_personal=lambda _: True, would_ingest=lambda _: False, withheld=count
    )
    return withholding.count_text(stopped_early=stopped_early)


@pytest.mark.parametrize(
    "path, shared",
    [
        ("/Shared/x", True),
        ("/Workspace/Shared/x", True),
        ("//Shared//x", True),
        ("/shared/lower", True),
        ("/Users/person.one@example.com/pipeline", False),
        ("/users/lower@example.com/n", False),
        ("/Repos/repouser@example.com/r/nb", False),
        ("/Workspace/Users/wsuser@example.com/n", False),
        ("//Users/dbl@example.com/n", False),
        ("/Shared/../Users/dotdot@example.com/n", False),
        ("/Shared/./x", False),
        ("/Repos/team/report", False),
    ],
)
def test_only_paths_under_shared_count_as_shared_however_spelled(
    path: str, shared: bool
) -> None:
    assert _is_shared_path(path) is shared


def _notebook(object_id: int, path: str) -> ObjectInfo:
    return ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=object_id, path=path)


def _notebook_ws(paths: List[str]) -> MagicMock:
    ws = _fake_ws()
    ws.workspace.list.return_value = [
        _notebook(i, path) for i, path in enumerate(paths, start=1)
    ]
    return ws


def test_shared_notebooks_are_listed_raw_even_while_include_notebooks_is_off() -> None:
    probe = _probe(_notebook_ws(["//Shared//x"]))
    # The raw path, not the normalised form: it is what notebook_pattern sees.
    assert probe.notebooks() == ["//Shared//x"]
    assert probe.warnings == []


def test_a_path_outside_shared_is_withheld_and_never_reaches_the_output_or_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _serve(
        monkeypatch,
        _notebook_ws(["/Shared/etl", "/Users/person.one@example.com/pipeline"]),
    )
    result = run_probe_method("unity-catalog", BASE, "notebooks", {})
    assert result.result == ["/Shared/etl"]
    assert any(w.startswith(_withheld(1)) for w in result.warnings)
    assert "person.one" not in json.dumps(result.to_dict())
    assert "person.one" not in caplog.text


def test_a_personal_notebook_the_recipe_ingests_is_listed() -> None:
    paths = [
        "/Users/person.one@example.com/pipeline",
        "/Repos/repouser@example.com/r/nb",
        "/Shared/../Users/dotdot@example.com/n",
    ]
    probe = _probe(
        _notebook_ws(["/Shared/etl", *paths]),
        include_notebooks=True,
        notebook_pattern={"allow": ["^/Shared/.*", "^/Repos/repouser@example.com/.*"]},
    )
    # notebook_pattern allows /Shared/../Users/... too: ingestion would read
    # it, so it is no secret from this recipe.
    assert probe.notebooks() == ["/Shared/etl", *paths[1:]]
    assert any(w.startswith(_withheld(1)) for w in probe.warnings)


def test_a_walk_cut_at_the_limit_reports_the_withheld_count_as_a_lower_bound() -> None:
    probe = _probe(
        _notebook_ws(["/Users/a@example.com/n", "/Shared/a", "/Shared/b", "/Shared/c"])
    )
    assert probe.notebooks(limit=2) == ["/Shared/a", "/Shared/b"]
    assert any(w.startswith(_withheld(1, stopped_early=True)) for w in probe.warnings)


def test_notebooks_the_credential_cannot_list_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.workspace.list.side_effect = _denied("PERMISSION_DENIED", "no workspace access")
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


def test_sql_opens_one_connection_with_a_server_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: List[Dict[str, Any]] = []
    conn = _fake_connection([("orders",)])

    def connect(**kwargs: Any) -> MagicMock:
        opened.append(kwargs)
        return conn

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    query = "SELECT table_name FROM main.information_schema.tables"
    with _probe(_sql_ws(), warehouse_id="w1") as probe:
        rows = probe.execute_catalog_query(query, 3)
        probe.execute_catalog_query(query, 3)
    assert list(rows.columns) == ["table_name"]
    assert [list(r) for r in rows.rows] == [["orders"]]
    assert len(opened) == 1
    assert opened[0]["session_configuration"] == {"STATEMENT_TIMEOUT": "30"}
    conn.cursor.return_value.__enter__.return_value.fetchmany.assert_called_with(3)
    conn.close.assert_called_once()


def test_sql_without_a_timeout_budget_sets_no_statement_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: List[Dict[str, Any]] = []

    def connect(**kwargs: Any) -> MagicMock:
        opened.append(kwargs)
        return _fake_connection([])

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    probe = _probe(_sql_ws(), warehouse_id="w1")
    monkeypatch.setattr(probe, "query_budget", QueryBudget(timeout_seconds=None))
    probe.execute_catalog_query("SELECT 1 FROM system.information_schema.tables", 1)
    assert "session_configuration" not in opened[0]


def test_sql_retries_the_open_after_a_failed_connect_and_closes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _fake_connection([("orders",)])
    attempts: List[int] = []

    def connect(**kwargs: Any) -> MagicMock:
        attempts.append(1)
        if len(attempts) == 1:
            raise RequestError("Error during request to server")
        return conn

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    query = "SELECT table_name FROM main.information_schema.tables"
    with _probe(_sql_ws(), warehouse_id="w1") as probe:
        with pytest.raises(ProbeConnectionError):
            probe.execute_catalog_query(query, 1)
        assert probe.execute_catalog_query(query, 1).rows == [["orders"]]
    assert len(attempts) == 2
    conn.close.assert_called_once()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT statement_text FROM system.query.history",
        "SELECT * FROM main.analytics.orders",
        "SELECT * FROM system.information_schema.tables t "
        "JOIN main.analytics.orders o ON 1=1",
    ],
)
def test_sql_outside_information_schema_is_refused_before_the_warehouse_is_touched(
    monkeypatch: pytest.MonkeyPatch, query: str
) -> None:
    opened: List[Dict[str, Any]] = []

    def connect(**kwargs: Any) -> MagicMock:
        opened.append(kwargs)
        return _fake_connection([])

    monkeypatch.setattr("datahub.ingestion.source.unity.unity_probe.connect", connect)
    _serve(monkeypatch, _sql_ws())
    with pytest.raises(ValueError):
        run_probe_method(
            "unity-catalog", {**BASE, "warehouse_id": "w1"}, "sql", {"query": query}
        )
    assert opened == []


@pytest.mark.parametrize(
    "query",
    [
        "SELECT table_name FROM system.information_schema.tables",
        "SELECT tag_name FROM main.information_schema.table_tags",
    ],
)
def test_sql_over_either_information_schema_reaches_the_warehouse(
    monkeypatch: pytest.MonkeyPatch, query: str
) -> None:
    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.connect",
        lambda **kwargs: _fake_connection([("orders",)]),
    )
    _serve(monkeypatch, _sql_ws())
    result = run_probe_method(
        "unity-catalog", {**BASE, "warehouse_id": "w1"}, "sql", {"query": query}
    )
    assert isinstance(result.result, dict) and result.result["rows"] == [["orders"]]


@pytest.mark.parametrize(
    "error, expected",
    [
        (
            ServerOperationError(
                "[CAST_INVALID_INPUT] The value 's3cr3t-row-value' cannot be cast"
            ),
            ProbeArgumentError,
        ),
        (
            ServerOperationError(
                "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `s3cr3t` cannot be "
                "resolved"
            ),
            ProbeArgumentError,
        ),
        # The probe's own STATEMENT_TIMEOUT, and the warehouse's failures, are
        # no reason to rewrite the query.
        (
            ServerOperationError("[STATEMENT_TIMEOUT] s3cr3t ran too long"),
            ProbeConnectionError,
        ),
        (
            ServerOperationError("[INTERNAL_ERROR] s3cr3t"),
            ProbeConnectionError,
        ),
        # A missing grant: rewriting the query won't help, so not exit 2.
        (
            ServerOperationError(
                "[INSUFFICIENT_PERMISSIONS] User s3cr3t does not have SELECT"
            ),
            ProbeConnectionError,
        ),
        (ServerOperationError("s3cr3t without a class"), ProbeConnectionError),
        (
            RequestError("Error during request to server: Bearer s3cr3t"),
            ProbeConnectionError,
        ),
        (_denied("PERMISSION_DENIED", "Bearer s3cr3t"), ProbeConnectionError),
    ],
)
def test_warehouse_failures_are_labelled_and_their_text_never_shown(
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
    assert type(raised.value) is expected
    assert "s3cr3t" not in str(raised.value)
    assert type(error).__name__ in str(raised.value)


def test_a_missing_grant_on_sql_names_the_privileges_to_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _fake_connection([])
    conn.cursor.return_value.__enter__.return_value.execute.side_effect = (
        ServerOperationError("[INSUFFICIENT_PERMISSIONS] User lacks SELECT")
    )
    monkeypatch.setattr(
        "datahub.ingestion.source.unity.unity_probe.connect",
        lambda **kwargs: conn,
    )
    probe = _probe(_sql_ws(), warehouse_id="w1")
    with pytest.raises(ProbeConnectionError) as raised:
        probe.execute_catalog_query(
            "SELECT table_name FROM main.information_schema.tables", 2
        )
    assert "SELECT" in str(raised.value) and "CAN USE" in str(raised.value)


def _principal(name: str, app_id: str) -> SdkServicePrincipal:
    return SdkServicePrincipal(
        id=f"id-{app_id}", display_name=name, application_id=app_id
    )


def test_service_principals_lists_display_names_and_never_application_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = _fake_ws()
    ws.service_principals.list.return_value = iter(
        [_principal("etl-bot", "app-1"), _principal("bi-bot", "app-2")]
    )
    _serve(monkeypatch, ws)
    result = run_probe_method("unity-catalog", BASE, "service_principals", {})
    assert result.result == ["etl-bot", "bi-bot"]
    assert result.truncated is False
    payload = json.dumps(result.to_dict())
    assert "app-1" not in payload and "app-2" not in payload


def test_service_principals_the_credential_cannot_list_degrade_with_a_warning() -> None:
    ws = _fake_ws()
    ws.service_principals.list.side_effect = _denied("PERMISSION_DENIED", "s3cr3t")
    probe = _probe(ws)
    assert probe.service_principals() == []
    assert any("service principals" in w for w in probe.warnings)
    assert not any("s3cr3t" in w for w in probe.warnings)
