"""End-to-end orchestration of SnowflakeV2Source.get_workunits_internal.

The schema generator is replaced by a stub that reports a fixed set of discovered
objects; everything downstream of it (queries v2 vs legacy lineage/usage, semantic
view usage gating, assertions, marketplace, stages/tasks/pipes, session probes)
runs against the dispatching fake connection built by ``make_source``.
``test_connection`` is covered by test_snowflake_source.py.
"""

import os
from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import create_autospec, patch

import pytest

from datahub.ingestion.source.common.semantic_model_gate import ResolvedEmitDecision
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesExtractorReport,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeDatabase
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from tests.unit.snowflake.conftest import (
    CREATED,
    DB,
    SCHEMA,
    QueryResult,
    as_mock,
    database,
    log_keys,
    make_source,
    schema,
)

V2 = "datahub.ingestion.source.snowflake.snowflake_v2"


def _databases(
    tables: List[str], semantic_views: Optional[List[str]] = None
) -> List[SnowflakeDatabase]:
    snowflake_schema = schema()
    snowflake_schema.tables = tables
    snowflake_schema.semantic_views = semantic_views or []
    db = database()
    db.schemas = [snowflake_schema]
    return [db]


def _run(source: SnowflakeV2Source, databases: List[SnowflakeDatabase]) -> List:
    with (
        patch.object(source, "_snowflake_clear_ocsp_cache"),
        patch(f"{V2}.SnowflakeSchemaGenerator", autospec=True) as schema_gen,
    ):
        schema_gen.return_value.get_workunits_internal.return_value = iter([])
        schema_gen.return_value.databases = databases
        schema_gen.return_value.dynamic_table_identifiers = set()
        return list(source.get_workunits_internal())


def test_queries_v2_flow_passes_discovered_tables_and_cleans_up_extractor() -> None:
    source = make_source(use_queries_v2=True, include_table_lineage=True)
    queries_extractor = create_autospec(SnowflakeQueriesExtractor, instance=True)
    queries_extractor.get_workunits_internal.return_value = iter([])
    queries_extractor.report = SnowflakeQueriesExtractorReport()
    queries_extractor.report.audit_log_path = "/tmp/audit.sqlite"
    queries_extractor.close.side_effect = OSError("disk full")

    with patch(
        f"{V2}.SnowflakeQueriesExtractor",
        autospec=True,
        return_value=queries_extractor,
    ) as extractor_cls:
        _run(source, _databases(["T1"]))

    assert source.discovered_datasets == [f"{DB}.{SCHEMA}.T1".lower()]
    kwargs = extractor_cls.call_args.kwargs
    assert kwargs["discovered_tables"] == source.discovered_datasets
    assert kwargs["config"].include_lineage
    assert source.report.queries_extractor is queries_extractor.report
    # A failed cleanup is downgraded to a warning rather than masking the stage.
    assert "Failed to clean up after query extraction" in log_keys(
        source.report.warnings
    )
    assert source.report.failures == []


def test_legacy_flow_runs_lineage_before_usage_and_updates_state() -> None:
    """Lineage state is checkpointed before usage runs, so a usage failure
    cannot roll the lineage window back."""
    source = make_source(
        include_table_lineage=True,
        include_usage_stats=True,
        include_assertion_results=True,
    )
    assert source.lineage_extractor is not None
    assert source.usage_extractor is not None
    calls: List[str] = []

    def record(name: str) -> Callable[..., Iterator]:
        def side_effect(*args: object, **kwargs: object) -> Iterator:
            calls.append(name)
            return iter([])

        return side_effect

    with (
        patch.object(
            source.lineage_extractor,
            "add_time_based_lineage_to_aggregator",
            autospec=True,
            side_effect=record("lineage"),
        ),
        patch.object(
            source.lineage_extractor,
            "update_state",
            autospec=True,
            side_effect=record("state"),
        ),
        patch.object(
            source.usage_extractor,
            "get_usage_workunits",
            autospec=True,
            side_effect=record("usage"),
        ),
        patch(f"{V2}.SnowflakeAssertionsHandler", autospec=True) as assertions,
    ):
        assertions.return_value.get_assertion_workunits.return_value = iter([])
        _run(source, _databases(["T1"]))

    assert calls == ["lineage", "state", "usage"]
    assertions.return_value.get_assertion_workunits.assert_called_once_with(
        source.discovered_datasets
    )


def test_semantic_view_usage_needs_technical_schema() -> None:
    source = make_source(
        include_technical_schema=False,
        semantic_views={"enabled": True, "include_usage": True},
    )
    assert source.semantic_view_usage_extractor is not None

    with patch.object(
        source.semantic_view_usage_extractor,
        "get_semantic_view_usage_workunits",
        autospec=True,
    ) as usage:
        _run(source, _databases([], semantic_views=["SV"]))

    usage.assert_not_called()
    assert "Semantic view queries/usage skipped without technical schema" in log_keys(
        source.report.warnings
    )


def test_legacy_semantic_views_get_usage_and_queries_and_count_as_datasets() -> None:
    source = make_source(semantic_views={"enabled": True, "include_usage": True})
    extractor = source.semantic_view_usage_extractor
    assert extractor is not None

    with (
        patch.object(
            extractor,
            "get_semantic_view_usage_workunits",
            autospec=True,
            return_value=iter([]),
        ) as usage,
        patch.object(
            extractor,
            "get_semantic_view_query_workunits",
            autospec=True,
            return_value=iter([]),
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
    source = make_source(
        semantic_views={
            "enabled": True,
            "include_queries": True,
            "emit_semantic_model_entities": True,
        }
    )
    extractor = source.semantic_view_usage_extractor
    assert extractor is not None

    with (
        patch.object(
            extractor, "get_semantic_view_usage_workunits", autospec=True
        ) as usage,
        patch.object(
            extractor,
            "get_semantic_view_query_workunits",
            autospec=True,
            return_value=iter([]),
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
    source = make_source(
        show_rows,
        include_stages=True,
        include_tasks=True,
        include_pipes=True,
        marketplace={"enabled": True},
    )

    with patch(f"{V2}.SnowflakeMarketplaceHandler", autospec=True) as marketplace:
        marketplace.return_value.get_marketplace_workunits.return_value = iter([])
        workunits = _run(source, _databases(["T1"]))

    marketplace.return_value.get_marketplace_workunits.assert_called_once()
    urns = {wu.get_urn() for wu in workunits}
    assert any(u.startswith("urn:li:container:") for u in urns)  # stage
    assert any(u.startswith("urn:li:dataJob:") for u in urns)  # task and pipe
    assert source.report.stages_scanned == 1
    assert source.report.failures == []


def test_create_queries_extractor_mirrors_recipe_window_and_flags() -> None:
    source = make_source(
        use_queries_v2=True,
        include_queries=False,
        include_query_usage_statistics=False,
        temporary_tables_pattern=["^tmp_.*"],
    )
    schema_extractor = create_autospec(SnowflakeSchemaGenerator, instance=True)
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
    """Every reason the server could not confirm support is surfaced, and the
    recipe's request is forced off rather than emitting entities GMS may reject."""
    source = make_source(
        semantic_views={"enabled": True, "emit_semantic_model_entities": True}
    )
    decision = _decision(version_unparseable=True, metrics_probe_failed=True)

    with patch(
        f"{V2}.resolve_emit_semantic_model_entities",
        autospec=True,
        return_value=decision,
    ):
        source._resolve_semantic_model_emission()

    assert source.config.semantic_views.emit_semantic_model_entities is False
    assert not source.report.semantic_model_emission_effective
    assert {
        "Could not parse DataHub server version",
        "Could not verify Metrics kill-switch",
        "semantic_views.emit_semantic_model_entities forced off",
    } <= log_keys(source.report.warnings)


def test_auto_enabled_semantic_model_emission_warns_about_ignored_settings() -> None:
    source = make_source(
        include_technical_schema=False,
        semantic_views={"enabled": True, "include_usage": True},
    )

    with patch(
        f"{V2}.resolve_emit_semantic_model_entities",
        autospec=True,
        return_value=_decision(enabled=True, entity_types_capable=True),
    ):
        source._resolve_semantic_model_emission()

    assert source.config.semantic_views.emit_semantic_model_entities is True
    assert {
        "semantic_views.include_usage ignored",
        "semantic_views.emit_semantic_model_entities requires include_technical_schema",
    } <= log_keys(source.report.warnings)


def test_semantic_model_resolution_is_skipped_when_semantic_views_are_off() -> None:
    source = make_source()

    with patch(f"{V2}.resolve_emit_semantic_model_entities", autospec=True) as resolve:
        source._resolve_semantic_model_emission()

    resolve.assert_not_called()


# --- Session probes -----------------------------------------------------------


def test_each_session_probe_failure_is_reported_separately() -> None:
    source = make_source(
        {
            SnowflakeQuery.current_version(): RuntimeError("version"),
            SnowflakeQuery.current_role(): RuntimeError("role"),
            SnowflakeQuery.current_warehouse(): RuntimeError("warehouse"),
            SnowflakeQuery.show_tags(): RuntimeError("tags"),
        }
    )

    source.inspect_session_metadata(source.connection)

    assert {
        "Could not determine the current Snowflake version",
        "Could not determine the current Snowflake role",
        "Could not determine the current Snowflake warehouse",
    } <= log_keys(source.report.failures)
    assert source.report.default_warehouse is None
    assert source.report.edition is None


# --- Construction side effects ------------------------------------------------


def test_password_auth_is_flagged_as_deprecated_in_the_report() -> None:
    source = make_source(authentication_type="DEFAULT_AUTHENTICATOR", password="pw")

    assert any("password" in key.lower() for key in log_keys(source.report.warnings))


def test_domain_config_creates_a_registry() -> None:
    source = make_source(domain={"urn:li:domain:sales": {"allow": [".*"]}})

    assert source.domain_registry is not None


def test_close_closes_the_native_connection() -> None:
    source = make_source()
    native = as_mock(source.connection.native_connection())

    source.close()

    native.close.assert_called_once()


@pytest.mark.parametrize("system", ["Darwin", "Windows", "Linux"])
def test_ocsp_cache_file_is_removed_when_present(system: str) -> None:
    source = make_source()

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
    source = make_source()

    with (
        patch("os.path.exists", return_value=True),
        patch("os.remove", side_effect=PermissionError("locked")),
    ):
        source._snowflake_clear_ocsp_cache()  # must not raise
