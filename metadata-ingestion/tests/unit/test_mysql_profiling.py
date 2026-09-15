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


def _populate_profile_cache(source: MySQLSource, schema: str, rows: list) -> None:
    # rows: (table_name, table_rows, data_length).
    for table_name, table_rows, data_length in rows:
        key = f"{schema}.{table_name}"
        # dataset_name_to_storage_bytes is typed Dict[str, int] but the source stores
        # None too (row values are Any there) — views yield NULL data_length.
        source.profile_metadata_info.dataset_name_to_storage_bytes[key] = data_length  # type: ignore[assignment]
        source._table_rows_cache[key] = table_rows
    source._profile_sweep_ran = True
    source._table_rows_available = True


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
    # label whose case differs across MySQL/MariaDB/Doris/TiDB. The sweep also
    # caches table_rows for the guardrail.
    source = source_cls(
        config_cls(host_port=host_port, profiling={"enabled": True}),
        PipelineContext(run_id="mysql-family-profiling-test"),
    )
    inspector = _inspector_returning(
        [
            ("my_db", "orders", 4096, 100),
            ("my_db", "customers", 8192, 200),
            ("my_db", "v_orders", None, None),
        ]
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "my_db.orders": 4096,
        "my_db.customers": 8192,
        "my_db.v_orders": None,
    }
    assert source._table_rows_cache == {
        "my_db.orders": 100,
        "my_db.customers": 200,
        "my_db.v_orders": None,
    }
    assert source._profile_sweep_ran is True
    assert source._table_rows_available is True


def test_add_profile_metadata_raising_sweep_leaves_flags_false() -> None:
    # A sweep that raises (a rewriting proxy, a dropped connection) must leave
    # both flags False so generate_profile_candidates fails open to no guardrail.
    # get_profiling_internal wraps add_profile_metadata in try/except that only
    # warns. The mock raises on every execute, so primary and three-column
    # fallback both fail and the error propagates.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True, "profile_table_row_limit": 1_000_000},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = SQLAlchemyError("information_schema unavailable")
    inspector.engine.connect.return_value.__enter__.return_value = conn

    with pytest.raises(SQLAlchemyError):
        source.add_profile_metadata(inspector)

    assert source._profile_sweep_ran is False
    assert source._table_rows_available is False

    # generate_profile_candidates must fail open (return None) since the sweep never ran.
    # profile_table_row_limit is set so this reaches the _profile_sweep_ran check
    # rather than the both-limits-None short-circuit.
    cand_inspector = MagicMock()
    cand_inspector.get_table_names.return_value = ["orders"]
    result = source.generate_profile_candidates(
        cand_inspector, threshold_time=None, schema="my_db"
    )
    assert result is None


def _inspector_with_failing_primary_sweep() -> MagicMock:
    inspector = MagicMock()
    primary_conn = MagicMock()
    primary_conn.execute.side_effect = SQLAlchemyError("Unknown column 'table_rows'")
    fallback_conn = MagicMock()
    fallback_conn.execute.return_value = [
        ("my_db", "orders", 4096),
        ("my_db", "customers", 8192),
    ]
    inspector.engine.connect.return_value.__enter__.side_effect = [
        primary_conn,
        fallback_conn,
    ]
    return inspector


def test_add_profile_metadata_fallback_populates_storage_bytes_only() -> None:
    # Primary four-column SELECT fails (e.g. missing table_rows); three-column
    # fallback succeeds. sizeInBytes cache is filled; the row cache is not. The
    # fallback sets _profile_sweep_ran (storage bytes populated, so the size
    # limit can run) but leaves _table_rows_available False (row limit
    # unenforceable). No limit is configured here, so the degradation is
    # reported at info: the guardrail was inert anyway, and a warning would
    # fail --strict-warnings runs.
    source = _source()
    inspector = _inspector_with_failing_primary_sweep()

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "my_db.orders": 4096,
        "my_db.customers": 8192,
    }
    assert source._table_rows_cache == {}
    assert source._profile_sweep_ran is True
    assert source._table_rows_available is False
    assert len(list(source.report.warnings)) == 0
    infos = [
        e
        for e in source.report.infos
        if e.title == "Profiling row/size guardrail disabled"
    ]
    assert len(infos) == 1
    assert "sizeInBytes is unaffected" in infos[0].message


def test_add_profile_metadata_fallback_warns_when_a_limit_is_set() -> None:
    # With profile_table_row_limit configured the fallback loses filtering the
    # user asked for (the row limit is unenforceable without table_rows), so it
    # is a warning rather than an info. The size limit is unaffected, but the
    # row limit being the unenforceable one is what makes this a warning.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True, "profile_table_row_limit": 1_000_000},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = _inspector_with_failing_primary_sweep()

    source.add_profile_metadata(inspector)

    assert source._profile_sweep_ran is True
    assert source._table_rows_available is False
    warnings = list(source.report.warnings)
    assert len(warnings) == 1
    assert warnings[0].title == "Profiling row/size guardrail disabled"
    assert any("mysql" in ctx for ctx in warnings[0].context)


def test_add_profile_metadata_fallback_info_when_only_size_limit_set() -> None:
    # Mirror of the warning test: with only profile_table_size_limit set, the
    # fallback loses nothing the user asked for (data_length was fetched, so
    # the size limit still enforces). The degradation is info, not warning —
    # otherwise --strict-warnings fails over a degradation that did not happen.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True, "profile_table_size_limit": 1},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = _inspector_with_failing_primary_sweep()

    source.add_profile_metadata(inspector)

    assert source._profile_sweep_ran is True
    assert source._table_rows_available is False
    assert len(list(source.report.warnings)) == 0
    infos = [
        e
        for e in source.report.infos
        if e.title == "Profiling row/size guardrail disabled"
    ]
    assert len(infos) == 1
    assert "sizeInBytes is unaffected" in infos[0].message


def _populate_fallback_cache(source: MySQLSource, schema: str, rows: list) -> None:
    # rows: (table_name, data_length). Simulates the three-column fallback path:
    # storage bytes populated, row cache empty, _table_rows_available False.
    for table_name, data_length in rows:
        key = f"{schema}.{table_name}"
        source.profile_metadata_info.dataset_name_to_storage_bytes[key] = data_length  # type: ignore[assignment]
    source._profile_sweep_ran = True
    source._table_rows_available = False


def test_size_limit_enforces_on_fallback_path() -> None:
    # The behaviour the split buys: on the fallback path the size limit still
    # filters. With profile_table_size_limit set, one table over and one under
    # the limit, only the under-limit table is a candidate and the over-limit
    # one is attributed to _guardrail_skip[...] == "size".
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True, "profile_table_size_limit": 1},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_fallback_cache(
        source,
        "my_db",
        [
            ("small", 512 * 1024**2),
            ("big", 2 * 1024**3),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["small", "big"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert candidates == ["my_db.small"]
    assert source._guardrail_skip == {"my_db.big": "size"}


def test_row_limit_only_returns_none_on_fallback_path() -> None:
    # Only profile_table_row_limit set, and the fallback path left
    # _table_rows_available False. The row limit is the unenforceable one and
    # nothing else is configured, so generate_profile_candidates short-circuits
    # to None rather than building a full candidate list that retains every
    # table and then no-oping the row check per table.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True, "profile_table_row_limit": 1_000_000},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_fallback_cache(
        source,
        "my_db",
        [("orders", 1024), ("customers", 2048)],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["orders", "customers"]

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    inspector.get_table_names.assert_not_called()
    assert source._guardrail_skip == {}


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
    _populate_profile_cache(
        source,
        "my_db",
        [
            ("orders", 100, 1024),
            ("customers", 200, 2048),
            ("Mixed_Case", 50, 512),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["orders", "customers", "Mixed_Case"]

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
    # Row/size limits are applied in Python against the cache populated by add_profile_metadata
    # (no per-schema query), so a mock inspector with a populated cache verifies filtered results.
    # With limits set, tables over the limits are dropped and the rest are returned as candidate
    # identifiers; with both limits at their None default, the method short-circuits and reads no
    # cache at all (restoring the pre-guardrail behaviour of returning no filter).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 150,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    # small: under both limits. big_rows: over row limit. big_size: over size limit (1 GB).
    _populate_profile_cache(
        source,
        "my_db",
        [
            ("small", 100, 1024),
            ("big_rows", 200, 1024),
            ("big_size", 100, 2 * 1024**3),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["small", "big_rows", "big_size"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert candidates == ["my_db.small"]
    # big_rows attributed to row limit, big_size to size limit.
    assert source._guardrail_skip == {
        "my_db.big_rows": "row",
        "my_db.big_size": "size",
    }

    # Default config: both limits None -> no query, returns None.
    default_source = _source()
    default_inspector = MagicMock()

    result = default_source.generate_profile_candidates(
        default_inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    default_inspector.get_table_names.assert_not_called()


def test_generate_profile_candidates_retains_null_stats_tables() -> None:
    # A table whose table_rows or total_size is NULL must still be profiled — NULL stats
    # must not silently drop a table. This covers the Python-side NULL path (the cache
    # carries NULLs straight through).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_profile_cache(
        source,
        "my_db",
        [
            ("null_rows", None, 1024),
            ("null_size", 100, None),
            ("small", 100, 1024),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["null_rows", "null_size", "small"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert candidates == ["my_db.null_rows", "my_db.null_size", "my_db.small"]


def test_generate_profile_candidates_retains_table_absent_from_cache() -> None:
    # A table returned by get_table_names but absent from the information_schema
    # cache (a partial miss — restricted grants, a rewriting proxy, or a catalog
    # mismatch) must still be a candidate. Missing cache entries yield None stats,
    # and NULL/missing stats are retained rather than treated as over-limit.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    # Cache has "orders" but not "mystery" — a partial miss.
    _populate_profile_cache(
        source,
        "my_db",
        [("orders", 100, 1024)],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["orders", "mystery"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert candidates == ["my_db.orders", "my_db.mystery"]


def test_mysql_profiling_config_override_set_pinned() -> None:
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


def test_profile_table_row_limit_rejects_non_positive() -> None:
    # A value of 0 (or any negative) would make every table exceed the limit,
    # silently excluding the whole instance from profiling. Reject it so the
    # misconfiguration fails fast; null disables the filter.
    with pytest.raises(ValueError, match="profile_table_row_limit"):
        MySQLConfig(
            host_port="localhost:3306",
            profiling={"enabled": True, "profile_table_row_limit": 0},
        )
    with pytest.raises(ValueError, match="profile_table_row_limit"):
        MySQLConfig(
            host_port="localhost:3306",
            profiling={"enabled": True, "profile_table_row_limit": -5},
        )


def test_profile_table_size_limit_rejects_non_positive() -> None:
    with pytest.raises(ValueError, match="profile_table_size_limit"):
        MySQLConfig(
            host_port="localhost:3306",
            profiling={"enabled": True, "profile_table_size_limit": 0},
        )
    with pytest.raises(ValueError, match="profile_table_size_limit"):
        MySQLConfig(
            host_port="localhost:3306",
            profiling={"enabled": True, "profile_table_size_limit": -1},
        )


def test_profile_limits_accept_null() -> None:
    # null is the documented way to disable either filter and must remain valid.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": None,
            "profile_table_size_limit": None,
        },
    )
    assert config.profiling.profile_table_row_limit is None
    assert config.profiling.profile_table_size_limit is None


def test_generate_profile_candidates_fails_open_on_empty_cache() -> None:
    # add_profile_metadata is called inside a try/except Exception in
    # get_profiling_internal that only warns, so the sweep can fail and leave
    # _profile_sweep_ran False. A sweep that did not run must mean "no guardrail —
    # return None", never "no table qualifies" (an empty candidate list is additive
    # and would drop every profile in the run). No warning here: add_profile_metadata
    # is the layer that warned for the sweep failure.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    # _profile_sweep_ran left False (sweep failed upstream).
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["orders", "customers"]

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    # No candidate list built, so no skip attribution and no info/warning.
    inspector.get_table_names.assert_not_called()
    assert not list(source.report.warnings)
    assert not list(source.report.infos)


def test_profile_freshness_info_fires_lazily_and_once() -> None:
    # The profile_if_updated_since_days info must fire from generate_profile_candidates
    # (not __init__), and only when the setting is set. The once-ness is not enforced in
    # the source — report_log dedupes on title-message, so repeated calls collapse into a
    # single entry. Assert on the real report's info entries (not a mock) so the
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

    # Both limits None -> method returns None after the lazy info, no query needed.
    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )
    assert result is None
    # A second call with a different schema must not create a second entry — same
    # title-message key collapses into the existing one.
    source.generate_profile_candidates(
        inspector, threshold_time=None, schema="other_db"
    )
    entries = list(source.report.infos)
    assert len(entries) == 1
    assert (
        entries[0].title == "Profiling does not support profile_if_updated_since_days"
    )
    # No warnings on this path: pre-PR the method raised NotImplementedError, so
    # emitting a warning here would newly break --strict-warnings runs.
    assert len(list(source.report.warnings)) == 0


def test_profile_freshness_info_absent_when_setting_unset() -> None:
    # When profile_if_updated_since_days is not set, no info entry is produced.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True},
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = MagicMock()

    source.generate_profile_candidates(inspector, threshold_time=None, schema="my_db")

    assert len(list(source.report.infos)) == 0
    assert len(list(source.report.warnings)) == 0


def test_profile_freshness_info_reaches_doris_replaced_report() -> None:
    # DorisSource.__init__ calls super().__init__ then replaces self.report with a fresh
    # DorisSourceReport. An info emitted from __init__ would land in the discarded
    # report; the lazy info from generate_profile_candidates must land in the live
    # DorisSourceReport. Spy on the live report's info to prove it.
    config = DorisConfig(
        host_port="localhost:9030",
        profiling={
            "enabled": True,
            "profile_if_updated_since_days": 7,
        },
    )
    source = DorisSource(config, PipelineContext(run_id="doris-freshness-test"))
    # source.report is the DorisSourceReport that survived the reassignment.
    source.report.info = MagicMock()  # type: ignore[method-assign]
    inspector = MagicMock()

    source.generate_profile_candidates(inspector, threshold_time=None, schema="my_db")

    assert source.report.info.called
    _args, kwargs = source.report.info.call_args
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
    # Every table over the row limit -> empty candidate list.
    _populate_profile_cache(
        source,
        "my_db",
        [
            ("orders", 2_000_000, 1024),
            ("customers", 3_000_000, 1024),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["orders", "customers"]

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result == []
    info_entries = list(source.report.infos)
    titles = [e.title for e in info_entries]
    assert "No tables passed the row/size guardrail" in titles
    no_tables_entry = next(
        e for e in info_entries if e.title and "No tables" in e.title
    )
    assert "Schema: my_db" in no_tables_entry.context
    # A warning on this path would break --strict-warnings runs.
    assert len(list(source.report.warnings)) == 0


def test_empty_candidates_no_info_when_schema_has_no_tables() -> None:
    # If the schema genuinely has no tables, an empty candidate list is correct — no
    # info entry. Avoids noise on an empty schema. The cache is non-empty (sweep
    # succeeded for another schema) so the empty-cache fail-open does not trigger.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_profile_cache(source, "other_db", [("orders", 100, 1024)])
    inspector = MagicMock()
    inspector.get_table_names.return_value = []

    result = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    assert result == []
    titles = [e.title for e in source.report.infos]
    assert "No tables passed the row/size guardrail" not in titles


def _new_source_with_mocked_report() -> MySQLSource:
    # state_provider must be mocked: super().close() reaches
    # StatefulIngestionSourceBase.close(), which calls prepare_for_commit on it,
    # and a real state provider is not wired in a unit test. The report stays real
    # so the advice path and report counters are exercised end-to-end.
    source = _source()
    source.state_provider = MagicMock()  # type: ignore[assignment]
    return source


def _assert_parent_close_reached(source: MySQLSource) -> None:
    assert source.state_provider.prepare_for_commit.called  # type: ignore[attr-defined]


def test_close_reaches_parent_when_no_timings() -> None:
    # A run with no profiling (or no timings recorded) must still commit the
    # stateful-ingestion checkpoint via super().close().
    source = _new_source_with_mocked_report()

    source.close()

    _assert_parent_close_reached(source)
    titles = [e.title for e in source.report.infos]
    assert "Profiling: expensive tables" not in titles


def test_close_reaches_parent_when_only_fast_timings() -> None:
    # Timings exist but none cross the slow threshold — the advice must not fire,
    # but super().close() still must.
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs["orders"] = 1.5
    source.report.profiling_time_taken_per_table_secs["users"] = 2.0

    source.close()

    _assert_parent_close_reached(source)
    titles = [e.title for e in source.report.infos]
    assert "Profiling: expensive tables" not in titles


def test_close_emits_advice_for_slow_mysql_table() -> None:
    # Sanity check that the advice still fires on MySQL when a table crosses the
    # threshold, and that super().close() runs after the advice.
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs["orders"] = 60.0

    source.close()

    _assert_parent_close_reached(source)
    titles = [e.title for e in source.report.infos]
    assert "Profiling: expensive tables" in titles


def test_close_advice_names_only_slow_tables_with_real_times() -> None:
    # The advice must name only tables that actually crossed the slow threshold,
    # with their real elapsed times — not fast tables dragged in by the top-N
    # sort, and never a fabricated time. Seed one slow table and several fast
    # ones, then assert the slow table's real time appears and no fast table's
    # name appears in the emitted context.
    source = _new_source_with_mocked_report()
    source.report.profiling_time_taken_per_table_secs["slow_table"] = 60.0
    source.report.profiling_time_taken_per_table_secs["fast_a"] = 0.4
    source.report.profiling_time_taken_per_table_secs["fast_b"] = 0.3
    source.report.profiling_time_taken_per_table_secs["fast_c"] = 0.2
    source.report.profiling_time_taken_per_table_secs["fast_d"] = 0.1

    source.close()

    advice = next(
        e for e in source.report.infos if e.title == "Profiling: expensive tables"
    )
    context = ", ".join(advice.context)
    assert "slow_table (60.0s)" in context
    for fast in ("fast_a", "fast_b", "fast_c", "fast_d"):
        assert fast not in context


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
    source.report.profiling_time_taken_per_table_secs["orders"] = 120.0

    source.close()

    assert source.state_provider.prepare_for_commit.called  # type: ignore[attr-defined]
    titles = [e.title for e in source.report.infos]
    assert "Profiling: expensive tables" not in titles


def test_guardrail_skips_attributed_without_double_count() -> None:
    # Guardrailed tables must land in profiling_skipped_row_limit / profiling_skipped_size_limit,
    # not profiling_skipped_other. The override on is_dataset_eligible_for_profiling passes
    # profile_candidates=None to super so the base does NOT also increment
    # profiling_skipped_other for the same table (which would double-count).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 150,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_profile_cache(
        source,
        "my_db",
        [
            ("small", 100, 1024),
            ("big_rows", 200, 1024),
            ("big_size", 100, 2 * 1024**3),
        ],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["small", "big_rows", "big_size"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )
    assert candidates == ["my_db.small"]

    # is_dataset_eligible_for_profiling is called by loop_profiler_requests for every
    # table; simulate that here to assert attribution + no double-count.
    for table in ["small", "big_rows", "big_size"]:
        dataset_name = f"my_db.{table}"
        source.is_dataset_eligible_for_profiling(
            dataset_name, "my_db", inspector, candidates
        )

    assert source.report.profiling_skipped_row_limit.get("my_db", 0) == 1
    assert source.report.profiling_skipped_size_limit.get("my_db", 0) == 1
    assert "my_db" not in source.report.profiling_skipped_other


def test_guardrail_row_limit_takes_precedence_when_both_exclude() -> None:
    # A table excluded by both limits is counted once, in the row-limit bucket — row
    # limit takes precedence (cheaper-to-fix reason reported first). This precedence
    # is set in generate_profile_candidates and documented there.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 150,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    _populate_profile_cache(
        source,
        "my_db",
        [("both", 200, 2 * 1024**3)],
    )
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["both"]

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )
    assert candidates == []

    source.is_dataset_eligible_for_profiling(
        "my_db.both", "my_db", inspector, candidates
    )

    assert source.report.profiling_skipped_row_limit.get("my_db", 0) == 1
    assert source.report.profiling_skipped_size_limit.get("my_db", 0) == 0
    assert "my_db" not in source.report.profiling_skipped_other
