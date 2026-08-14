from typing import Type
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource
from datahub.ingestion.source.sql.mysql import (
    MySQLConfig,
    MySQLProfilingConfig,
    MySQLSource,
)
from datahub.ingestion.source.sql.tidb import TiDBConfig, TiDBSource


def _source() -> MySQLSource:
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True},
    )
    return MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))


def _inspector_returning(rows: list) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = rows
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    return inspector


@pytest.mark.parametrize(
    "source_cls,config_cls,host_port",
    [
        (MySQLSource, MySQLConfig, "localhost:3306"),
        # Doris inherits add_profile_metadata, so an override there has to keep
        # reading positionally too.
        (DorisSource, DorisConfig, "localhost:9030"),
    ],
)
def test_add_profile_metadata_reads_storage_bytes_positionally(
    source_cls: Type[MySQLSource],
    config_cls: Type[MySQLConfig],
    host_port: str,
) -> None:
    # Tuple rows (no named attributes) prove access is positional, not by the
    # label whose case differs across MySQL/MariaDB/Doris/TiDB.
    source = source_cls(
        config_cls(host_port=host_port, profiling={"enabled": True}),
        PipelineContext(run_id="mysql-family-profiling-test"),
    )
    inspector = _inspector_returning(
        [
            ("my_db", "orders", 4096),
            ("my_db", "customers", 8192),
        ]
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "my_db.orders": 4096,
        "my_db.customers": 8192,
    }


def test_generate_profile_candidates_returns_get_identifier_strings() -> None:
    # Whatever generate_profile_candidates returns must match the dataset_name produced by
    # get_identifier character-for-character, or the membership test in
    # is_dataset_eligible_for_profiling silently no-ops. Building candidates via the SAME
    # get_identifier call guarantees that; this test pins the invariant.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = _inspector_returning(
        [
            ("orders", 100, 1024),
            ("customers", 200, 2048),
            ("Mixed_Case", 50, 512),
        ]
    )

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    expected = [
        source.get_identifier(schema="my_db", entity="orders", inspector=inspector),
        source.get_identifier(schema="my_db", entity="customers", inspector=inspector),
        source.get_identifier(schema="my_db", entity="Mixed_Case", inspector=inspector),
    ]
    assert candidates == expected
    # And the concrete shape for two-tier MySQL:
    assert candidates == ["my_db.orders", "my_db.customers", "my_db.Mixed_Case"]


def test_generate_profile_candidates_wiring() -> None:
    # Row/size limits are applied in Python (not in SQL), so a mock inspector can verify filtered
    # results. With limits set, tables over the limits are dropped and the rest are returned as
    # candidate identifiers; with both limits at their None default, the method short-circuits
    # and runs no query at all (restoring the pre-guardrail behaviour of returning no filter).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 150,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    conn = MagicMock()
    # small: under both limits. big_rows: over row limit. big_size: over size limit (1 GB).
    conn.execute.return_value = [
        ("small", 100, 1024),
        ("big_rows", 200, 1024),
        ("big_size", 100, 2 * 1024**3),
    ]
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    args, _kwargs = conn.execute.call_args
    params = args[1]
    assert params == {"schema": "my_db"}
    assert candidates == ["my_db.small"]

    # Default config: both limits None -> no query, returns None.
    default_source = _source()
    default_conn = MagicMock()
    default_inspector = MagicMock()
    default_inspector.engine.connect.return_value.__enter__.return_value = default_conn

    result = default_source.generate_profile_candidates(
        default_inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    default_conn.execute.assert_not_called()


def test_generate_profile_candidates_retains_null_stats_tables() -> None:
    # A table whose table_rows or total_size is NULL must still be profiled — NULL stats
    # must not silently drop a table. This covers the Python-side NULL path (the SQL
    # no longer carries IS NULL OR clauses).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    conn = MagicMock()
    conn.execute.return_value = [
        ("null_rows", None, 1024),
        ("null_size", 100, None),
        ("small", 100, 1024),
    ]
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert candidates == ["my_db.null_rows", "my_db.null_size", "my_db.small"]
    """Pin the MySQLProfilingConfig override set so a new override forces an update here."""
    ge = {name: fi.default for name, fi in GEProfilingConfig.model_fields.items()}
    mysql = {name: fi.default for name, fi in MySQLProfilingConfig.model_fields.items()}
    mysql_overrides = {name for name in mysql if name in ge and mysql[name] != ge[name]}

    assert mysql_overrides == {
        "profile_table_row_limit",
        "profile_table_size_limit",
    }, (
        "MySQLProfilingConfig override set changed — update this test. "
        f"New override set: {sorted(mysql_overrides)}"
    )

    # MariaDB uses MySQLConfig directly, so its profiling IS MySQLProfilingConfig.
    assert MySQLConfig.model_fields["profiling"].annotation is MySQLProfilingConfig


def test_generate_profile_candidates_fails_open_with_warning() -> None:
    # When the information_schema query raises (restricted grants, a proxy, a dialect
    # difference), the method must degrade to no candidate filter (None) and emit a
    # structured warning, rather than aborting profiling for the whole schema. This
    # fail-open is MySQL-scoped (lives here, not in sql_common.loop_profiler_requests)
    # so Oracle and Teradata keep their pre-PR behaviour. Only SQLAlchemyError is caught
    # so programming bugs (ValueError, TypeError) still surface.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    source.report = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = SQLAlchemyError("information_schema denied")
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    assert source.report.warning.called
    _args, kwargs = source.report.warning.call_args
    assert kwargs["title"] == "Failed to generate profile candidates"
    assert "Schema: my_db" in kwargs["context"]
    assert kwargs["exc"] is not None


def test_profile_freshness_warning_fires_lazily_and_once() -> None:
    # The profile_if_updated_since_days warning must fire from generate_profile_candidates
    # (not __init__), and only when the setting is set. The once-ness is not enforced in
    # the source — report_log dedupes on title-message, so repeated calls collapse into a
    # single entry. Assert on the real report's warning entries (not a mock) so the
    # dedupe layer is actually exercised.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_if_updated_since_days": 7,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = MagicMock()

    # Both limits None -> method returns None after the lazy warning, no query needed.
    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )
    assert result is None
    # A second call with a different schema must not create a second entry — same
    # title-message key collapses into the existing one.
    source.generate_profile_candidates(
        inspector, threshold_time=None, schema="other_db"
    )
    entries = list(source.report.warnings)
    assert len(entries) == 1
    assert (
        entries[0].title == "Profiling does not support profile_if_updated_since_days"
    )


def test_profile_freshness_warning_absent_when_setting_unset() -> None:
    # When profile_if_updated_since_days is not set, no warning entry is produced.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = MagicMock()

    source.generate_profile_candidates(inspector, threshold_time=None, schema="my_db")

    assert len(list(source.report.warnings)) == 0


def test_profile_freshness_warning_reaches_doris_replaced_report() -> None:
    # DorisSource.__init__ calls super().__init__ then replaces self.report with a fresh
    # DorisSourceReport. A warning emitted from __init__ would land in the discarded
    # report; the lazy warning from generate_profile_candidates must land in the live
    # DorisSourceReport. Spy on the live report's warning to prove it.
    config = DorisConfig(
        host_port="localhost:9030",
        profiling={
            "enabled": True,
            "profile_if_updated_since_days": 7,
        },
    )
    source = DorisSource(config, PipelineContext(run_id="doris-freshness-test"))
    # source.report is the DorisSourceReport that survived the reassignment.
    source.report.warning = MagicMock()  # type: ignore[method-assign]
    inspector = MagicMock()

    source.generate_profile_candidates(inspector, threshold_time=None, schema="my_db")

    assert source.report.warning.called
    _args, kwargs = source.report.warning.call_args
    assert kwargs["title"] == "Profiling does not support profile_if_updated_since_days"


def test_empty_candidates_emits_info_when_schema_has_tables() -> None:
    # An empty candidate list drops every profile in the schema (the list is additive).
    # When the schema actually has tables, emit at info level so the operator knows
    # profiles are being dropped — either every table genuinely exceeds the limits,
    # or information_schema is not returning them.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    source.report = MagicMock()
    conn = MagicMock()
    conn.execute.return_value = []  # query succeeds but returns nothing
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    inspector.get_table_names.return_value = ["orders", "customers"]

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result == []
    info_calls = source.report.info.call_args_list
    titles = [c.kwargs["title"] for c in info_calls]
    assert "No tables passed the row/size guardrail" in titles
    no_tables_call = next(c for c in info_calls if "No tables" in c.kwargs["title"])
    assert "Schema: my_db" in no_tables_call.kwargs["context"]
    # A warning on this path would break --strict-warnings runs.
    assert not source.report.warning.called


def test_empty_candidates_no_info_when_schema_has_no_tables() -> None:
    # If the schema genuinely has no tables, an empty candidate list is correct — no
    # info entry. Avoids noise on an empty schema.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    source.report = MagicMock()
    conn = MagicMock()
    conn.execute.return_value = []
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    inspector.get_table_names.return_value = []

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result == []
    titles = [c.kwargs["title"] for c in source.report.info.call_args_list]
    assert "No tables passed the row/size guardrail" not in titles


def _new_source_with_mocked_report() -> MySQLSource:
    source = _source()
    source.state_provider = MagicMock()  # type: ignore[assignment]
    source.report = MagicMock()  # type: ignore[assignment]
    return source


def _assert_parent_close_reached(source: MySQLSource) -> None:
    assert source.state_provider.prepare_for_commit.called  # type: ignore[attr-defined]


def test_close_reaches_parent_when_no_timings() -> None:
    # A run with no profiling (or no timings recorded) must still commit the
    # stateful-ingestion checkpoint via super().close().
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs = {}  # type: ignore[assignment]

    source.close()

    _assert_parent_close_reached(source)
    titles = [c.kwargs["title"] for c in source.report.info.call_args_list]  # type: ignore[attr-defined]
    assert "Profiling: expensive tables" not in titles


def test_close_reaches_parent_when_only_fast_timings() -> None:
    # Timings exist but none cross the slow threshold — the advice must not fire,
    # but super().close() still must.
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs = {  # type: ignore[assignment]
        "orders": 1.5,
        "users": 2.0,
    }

    source.close()

    _assert_parent_close_reached(source)
    titles = [c.kwargs["title"] for c in source.report.info.call_args_list]  # type: ignore[attr-defined]
    assert "Profiling: expensive tables" not in titles


def test_close_emits_advice_for_slow_mysql_table() -> None:
    # Sanity check that the advice still fires on MySQL when a table crosses the
    # threshold, and that super().close() runs after the advice.
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs = {"orders": 60.0}  # type: ignore[assignment]

    source.close()

    _assert_parent_close_reached(source)
    titles = [c.kwargs["title"] for c in source.report.info.call_args_list]  # type: ignore[attr-defined]
    assert "Profiling: expensive tables" in titles


@pytest.mark.parametrize(
    "source_cls,config_cls,host_port",
    [
        (DorisSource, DorisConfig, "localhost:9030"),
        (TiDBSource, TiDBConfig, "localhost:4000"),
    ],
)
def test_close_does_not_emit_advice_for_doris_or_tidb(
    source_cls: Type[MySQLSource],
    config_cls: Type[MySQLConfig],
    host_port: str,
) -> None:
    # Doris and TiDB inherit MySQLSource.close() but must not receive the
    # MySQL-worded expensive-tables advice. super().close() still runs.
    config = config_cls(host_port=host_port, profiling={"enabled": True})
    source = source_cls(config, PipelineContext(run_id="platform-scope-test"))
    source.state_provider = MagicMock()  # type: ignore[assignment]
    source.report = MagicMock()  # type: ignore[assignment]
    source.report.profiling_time_taken_per_table_secs = {"orders": 120.0}  # type: ignore[assignment]

    source.close()

    assert source.state_provider.prepare_for_commit.called  # type: ignore[attr-defined]
    titles = [c.kwargs["title"] for c in source.report.info.call_args_list]  # type: ignore[attr-defined]
    assert "Profiling: expensive tables" not in titles
