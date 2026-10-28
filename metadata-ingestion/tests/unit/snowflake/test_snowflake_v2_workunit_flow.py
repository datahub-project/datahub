"""End-to-end orchestration of SnowflakeV2Source.get_workunits_internal.

The schema generator is replaced by a stub that reports a fixed set of discovered
objects; everything downstream of it (queries v2 vs legacy lineage/usage, semantic
view usage gating, assertions, marketplace, stages/tasks/pipes, session probes,
test_connection) runs against a dispatching fake connection.
"""

import os
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.common.semantic_model_gate import ResolvedEmitDecision
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeSchema,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from tests.unit.snowflake.test_snowflake_v2_orchestration import (  # type: ignore[import-untyped]
    QueryResult,
    _Cursor,
    _make_source,
)

CREATED = datetime(2024, 1, 1, tzinfo=timezone.utc)
DB = "TEST_DB"
SCHEMA = "PUBLIC"
SCHEMA_GEN = "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeSchemaGenerator"


def _databases(
    tables: List[str], semantic_views: Optional[List[str]] = None
) -> List[SnowflakeDatabase]:
    schema = SnowflakeSchema(
        name=SCHEMA, created=CREATED, last_altered=None, comment=None
    )
    schema.tables = tables
    schema.semantic_views = semantic_views or []
    db = SnowflakeDatabase(name=DB, created=CREATED, comment=None)
    db.schemas = [schema]
    return [db]


def _run(source: SnowflakeV2Source, databases: List[SnowflakeDatabase]) -> List:
    with (
        patch.object(source, "_snowflake_clear_ocsp_cache"),
        patch(SCHEMA_GEN) as schema_gen,
    ):
        schema_gen.return_value.get_workunits_internal.return_value = iter([])
        schema_gen.return_value.databases = databases
        schema_gen.return_value.dynamic_table_identifiers = set()
        return list(source.get_workunits_internal())


def test_queries_v2_flow_passes_discovered_tables_and_cleans_up_extractor() -> None:
    source = _make_source(use_queries_v2=True, include_table_lineage=True)
    queries_extractor = MagicMock()
    queries_extractor.get_workunits_internal.return_value = iter([])
    queries_extractor.report.audit_log_path = "/tmp/audit.sqlite"
    queries_extractor.close.side_effect = OSError("disk full")

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeQueriesExtractor",
        return_value=queries_extractor,
    ) as extractor_cls:
        _run(source, _databases(["T1"]))

    assert source.discovered_datasets == [f"{DB}.{SCHEMA}.T1".lower()]
    kwargs = extractor_cls.call_args.kwargs
    assert kwargs["discovered_tables"] == source.discovered_datasets
    assert kwargs["config"].include_lineage
    assert source.report.queries_extractor is queries_extractor.report
    # A failed cleanup is downgraded to a warning rather than masking the stage.
    assert [w.title for w in source.report.warnings] == [
        "Failed to clean up after query extraction"
    ]
    assert source.report.failures == []


def test_legacy_flow_runs_lineage_before_usage_and_updates_state() -> None:
    source = _make_source(
        include_table_lineage=True,
        include_usage_stats=True,
        include_assertion_results=True,
    )
    assert source.lineage_extractor is not None
    assert source.usage_extractor is not None
    calls: List[str] = []

    def record(name: str) -> MagicMock:
        def side_effect(*args: object, **kwargs: object) -> Iterator:
            calls.append(name)
            return iter([])

        return MagicMock(side_effect=side_effect)

    with (
        patch.object(
            source.lineage_extractor,
            "add_time_based_lineage_to_aggregator",
            record("lineage"),
        ),
        patch.object(source.lineage_extractor, "update_state", record("state")),
        patch.object(source.usage_extractor, "get_usage_workunits", record("usage")),
        patch(
            "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeAssertionsHandler"
        ) as assertions,
    ):
        assertions.return_value.get_assertion_workunits.return_value = iter([])
        _run(source, _databases(["T1"]))

    assert calls == ["lineage", "state", "usage"]
    assertions.return_value.get_assertion_workunits.assert_called_once_with(
        source.discovered_datasets
    )


def test_semantic_view_usage_needs_technical_schema() -> None:
    source = _make_source(
        include_technical_schema=False,
        semantic_views={"enabled": True, "include_usage": True},
    )
    assert source.semantic_view_usage_extractor is not None

    with patch.object(
        source.semantic_view_usage_extractor, "get_semantic_view_usage_workunits"
    ) as usage:
        _run(source, _databases([], semantic_views=["SV"]))

    usage.assert_not_called()
    assert [w.title for w in source.report.warnings] == [
        "Semantic view queries/usage skipped without technical schema"
    ]


def test_legacy_semantic_views_get_usage_and_queries_and_count_as_datasets() -> None:
    source = _make_source(semantic_views={"enabled": True, "include_usage": True})
    extractor = source.semantic_view_usage_extractor
    assert extractor is not None

    with (
        patch.object(
            extractor, "get_semantic_view_usage_workunits", return_value=iter([])
        ) as usage,
        patch.object(
            extractor, "get_semantic_view_query_workunits", return_value=iter([])
        ) as queries,
    ):
        _run(source, _databases(["T1"], semantic_views=["SV"]))

    expected = {f"{DB}.{SCHEMA}.SV".lower()}
    usage.assert_called_once_with(expected)
    queries.assert_called_once_with(expected)
    assert source.discovered_datasets is not None
    assert f"{DB}.{SCHEMA}.SV".lower() in source.discovered_datasets


def test_semantic_model_mode_emits_only_queries_and_excludes_views_from_datasets() -> (
    None
):
    source = _make_source(
        semantic_views={
            "enabled": True,
            "include_queries": True,
            "emit_semantic_model_entities": True,
        }
    )
    extractor = source.semantic_view_usage_extractor
    assert extractor is not None

    with (
        patch.object(extractor, "get_semantic_view_usage_workunits") as usage,
        patch.object(
            extractor, "get_semantic_view_query_workunits", return_value=iter([])
        ) as queries,
    ):
        _run(source, _databases(["T1"], semantic_views=["SV"]))

    usage.assert_not_called()
    queries.assert_called_once()
    assert source.discovered_datasets == [f"{DB}.{SCHEMA}.T1".lower()]
    assert source.report.semantic_model_emission_effective


def test_marketplace_stages_tasks_and_pipes_run_after_schema_extraction() -> None:
    show_rows: Dict[str, QueryResult] = {
        SnowflakeQuery.show_stages_for_schema(SCHEMA, DB): [
            {"name": "STG", "created_on": CREATED, "type": "INTERNAL"}
        ],
        SnowflakeQuery.show_tasks_for_schema(SCHEMA, DB): [
            {
                "name": "TASK1",
                "created_on": CREATED,
                "definition": "select 1",
                "state": "started",
            }
        ],
        SnowflakeQuery.show_pipes_for_schema(SCHEMA, DB): [
            {
                "name": "PIPE1",
                "created_on": CREATED,
                "definition": "copy into t from @stg",
            }
        ],
    }
    source = _make_source(
        show_rows,
        include_stages=True,
        include_tasks=True,
        include_pipes=True,
        marketplace={"enabled": True},
    )

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeMarketplaceHandler"
    ) as marketplace:
        marketplace.return_value.get_marketplace_workunits.return_value = iter([])
        workunits = _run(source, _databases(["T1"]))

    marketplace.return_value.get_marketplace_workunits.assert_called_once()
    urns = {wu.get_urn() for wu in workunits}
    assert any(u.startswith("urn:li:container:") for u in urns)  # stage
    assert any(u.startswith("urn:li:dataJob:") for u in urns)  # task and pipe
    assert source.report.stages_scanned == 1
    assert source.report.failures == []


def test_create_queries_extractor_mirrors_recipe_window_and_flags() -> None:
    source = _make_source(
        use_queries_v2=True,
        include_queries=False,
        include_query_usage_statistics=False,
        temporary_tables_pattern=["^tmp_.*"],
    )
    schema_extractor = MagicMock()
    schema_extractor.dynamic_table_identifiers = {"db.s.dt"}
    source.discovered_datasets = ["db.s.t"]

    extractor = source._create_queries_extractor(
        source.aggregator._schema_resolver, None, schema_extractor
    )

    assert extractor.config.window.start_time == source.config.start_time
    assert extractor.config.window.end_time == source.config.end_time
    assert extractor.config.temporary_tables_pattern == ["^tmp_.*"]
    assert not extractor.config.include_queries
    assert extractor.discovered_tables == {"db.s.t"}
    assert extractor.dynamic_table_identifiers == {"db.s.dt"}


# --- Semantic model emission resolution ---------------------------------------


def _decision(**overrides: object) -> ResolvedEmitDecision:
    fields = dict(
        enabled=False,
        reason="test",
        is_saas=True,
        version="1.0.0",
        metrics_enabled=None,
        entity_types_capable=False,
    )
    fields.update(overrides)
    return ResolvedEmitDecision(**fields)  # type: ignore[arg-type]


def test_server_veto_of_requested_semantic_model_emission_warns_and_falls_back() -> (
    None
):
    source = _make_source(
        semantic_views={"enabled": True, "emit_semantic_model_entities": True}
    )
    decision = _decision(version_unparseable=True, metrics_probe_failed=True)

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.resolve_emit_semantic_model_entities",
        return_value=decision,
    ):
        source._resolve_semantic_model_emission()

    assert source.config.semantic_views.emit_semantic_model_entities is False
    assert not source.report.semantic_model_emission_effective
    assert [w.title for w in source.report.warnings] == [
        "Could not parse DataHub server version",
        "Could not verify Metrics kill-switch",
        "semantic_views.emit_semantic_model_entities forced off",
    ]


def test_auto_enabled_semantic_model_emission_warns_about_ignored_settings() -> None:
    source = _make_source(
        include_technical_schema=False,
        semantic_views={"enabled": True, "include_usage": True},
    )

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.resolve_emit_semantic_model_entities",
        return_value=_decision(enabled=True, entity_types_capable=True),
    ):
        source._resolve_semantic_model_emission()

    assert source.config.semantic_views.emit_semantic_model_entities is True
    assert [w.title for w in source.report.warnings] == [
        "semantic_views.include_usage ignored",
        "semantic_views.emit_semantic_model_entities requires include_technical_schema",
    ]


def test_semantic_model_resolution_is_skipped_when_semantic_views_are_off() -> None:
    source = _make_source()

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_v2.resolve_emit_semantic_model_entities"
    ) as resolve:
        source._resolve_semantic_model_emission()

    resolve.assert_not_called()


# --- Session probes and connection testing ------------------------------------


def test_each_session_probe_failure_is_reported_separately() -> None:
    source = _make_source(
        {
            SnowflakeQuery.current_version(): RuntimeError("version"),
            SnowflakeQuery.current_role(): RuntimeError("role"),
            SnowflakeQuery.current_warehouse(): RuntimeError("warehouse"),
            SnowflakeQuery.show_tags(): RuntimeError("tags"),
        }
    )

    source.inspect_session_metadata(source.connection)

    assert len(source.report.failures) == 3
    assert source.report.default_warehouse is None
    assert source.report.edition is None


def _grants_connection(
    grants: Dict[str, List[dict]], warehouse: Optional[str] = "WH"
) -> MagicMock:
    def execute(query: str) -> _Cursor:
        if query == "select current_role()":
            return _Cursor([{"CURRENT_ROLE()": "MAIN"}])
        if query == "select current_secondary_roles()":
            return _Cursor([{"CURRENT_SECONDARY_ROLES()": '{"roles": ""}'}])
        if query == "select current_warehouse()":
            return _Cursor([{"CURRENT_WAREHOUSE()": warehouse}])
        if query.startswith("show grants to role "):
            role = query.split('"')[1]
            return _Cursor(grants.get(role, []))
        raise AssertionError(f"Unexpected query: {query}")

    native = MagicMock()
    native.cursor.return_value.execute.side_effect = execute
    return native


CONNECTION_CONFIG = {
    "account_id": "test_account",
    "username": "user",
    "password": "pass",
    "warehouse": "WH",
}


def test_test_connection_derives_capabilities_from_the_role_hierarchy() -> None:
    grants = {
        "MAIN": [
            {"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB"},
            {"privilege": "USAGE", "granted_on": "ROLE", "name": "CHILD"},
        ],
        "CHILD": [
            {
                "privilege": "SELECT",
                "granted_on": "VIEW",
                "name": "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY",
            }
        ],
    }

    with patch("snowflake.connector.connect", return_value=_grants_connection(grants)):
        report = SnowflakeV2Source.test_connection(CONNECTION_CONFIG)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    capabilities = report.capability_report
    assert capabilities is not None
    assert capabilities[SourceCapability.CONTAINERS].capable
    assert capabilities[SourceCapability.SCHEMA_METADATA].capable
    assert capabilities[SourceCapability.LINEAGE_COARSE].capable
    assert capabilities[SourceCapability.USAGE_STATS].capable


def test_test_connection_without_warehouse_marks_warehouse_bound_capabilities() -> None:
    grants = {"MAIN": [{"privilege": "USAGE", "granted_on": "DATABASE", "name": "DB"}]}

    with patch(
        "snowflake.connector.connect",
        return_value=_grants_connection(grants, warehouse=None),
    ):
        report = SnowflakeV2Source.test_connection(CONNECTION_CONFIG)

    capabilities = report.capability_report
    assert capabilities is not None
    assert not capabilities[SourceCapability.SCHEMA_METADATA].capable
    assert "warehouse" in (
        capabilities[SourceCapability.SCHEMA_METADATA].failure_reason or ""
    )
    # CONTAINERS was granted but is not warehouse-bound, so it stays capable.
    assert capabilities[SourceCapability.CONTAINERS].capable
    assert not capabilities[SourceCapability.DESCRIPTIONS].capable


def test_test_connection_reports_connect_failure_as_no_connectivity() -> None:
    with patch("snowflake.connector.connect", side_effect=RuntimeError("refused")):
        report = SnowflakeV2Source.test_connection(CONNECTION_CONFIG)

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.capability_report is None


# --- Construction side effects ------------------------------------------------


def test_password_auth_is_flagged_as_deprecated_in_the_report() -> None:
    source = _make_source(authentication_type="DEFAULT_AUTHENTICATOR", password="pw")

    assert any("password" in w.title.lower() for w in source.report.warnings)


def test_domain_config_creates_a_registry() -> None:
    source = _make_source(domain={"urn:li:domain:sales": {"allow": [".*"]}})

    assert source.domain_registry is not None


def test_close_closes_the_native_connection() -> None:
    source = _make_source()
    native = source.connection.native_connection()

    source.close()

    native.close.assert_called_once()


@pytest.mark.parametrize("system", ["Darwin", "Windows", "Linux"])
def test_ocsp_cache_file_is_removed_when_present(system: str) -> None:
    source = _make_source()

    with (
        patch("platform.system", return_value=system),
        patch("os.path.exists", return_value=True),
        patch("os.remove") as remove,
    ):
        source._snowflake_clear_ocsp_cache()

    removed_path = remove.call_args.args[0]
    assert os.path.basename(removed_path) == "ocsp_response_validation_cache"
    assert "~" not in removed_path


def test_ocsp_cache_removal_failure_is_ignored() -> None:
    source = _make_source()

    with (
        patch("os.path.exists", return_value=True),
        patch("os.remove", side_effect=PermissionError("locked")),
    ):
        source._snowflake_clear_ocsp_cache()  # must not raise
