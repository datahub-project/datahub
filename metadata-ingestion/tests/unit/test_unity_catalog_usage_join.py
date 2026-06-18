from datetime import datetime, timezone
from types import SimpleNamespace
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor

_TS = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _row(**kw):
    # Databricks Row supports attribute access; SimpleNamespace replicates that.
    return SimpleNamespace(**kw)


def _make_proxy(rows):
    proxy = UnityCatalogApiProxy.__new__(UnityCatalogApiProxy)
    proxy.warehouse_id = "wh1"
    proxy.report = UnityCatalogReport()
    # Both usage methods wrap iteration in contextlib.closing(), which calls .close()
    # on the returned object when the with-block exits. list_iterator has no .close(),
    # so we use a generator expression to get a closeable iterator each call.
    _streaming_mock = MagicMock(side_effect=lambda *a, **kw: (r for r in rows))
    proxy._execute_sql_query_streaming = _streaming_mock  # type: ignore[method-assign]
    return proxy


# ---------------------------------------------------------------------------
# Proxy tests: get_query_usage_via_system_tables
# ---------------------------------------------------------------------------


def test_groups_read_write_and_external_rows_by_statement():
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        # statement s1: read of cat.sch.src (target NULL), write to cat.sch.tgt (source NULL)
        _row(
            statement_id="s1",
            statement_text="MERGE ...",
            statement_type="MERGE",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name="cat.sch.src",
            source_type="TABLE",
            source_path=None,
            target_table_full_name=None,
            target_type=None,
        ),
        _row(
            statement_id="s1",
            statement_text="MERGE ...",
            statement_type="MERGE",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name=None,
            source_type=None,
            source_path=None,
            target_table_full_name="cat.sch.tgt",
            target_type="TABLE",
        ),
        # statement s2: read via external path — contributes no source_table (no-op)
        _row(
            statement_id="s2",
            statement_text="SELECT ...",
            statement_type="SELECT",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name=None,
            source_type="PATH",
            source_path="s3://b/p",
            target_table_full_name=None,
            target_type=None,
        ),
    ]
    proxy = _make_proxy(rows)
    out = list(proxy.get_query_usage_via_system_tables(ts, ts))

    assert [q.query.query_id for q in out] == ["s1", "s2"]
    s1 = out[0]
    assert s1.source_tables == ["cat.sch.src"]
    assert s1.target_tables == ["cat.sch.tgt"]
    assert s1.query.statement_type is not None
    s2 = out[1]
    assert s2.source_tables == []
    assert s2.target_tables == []


def test_empty_result_yields_nothing():
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    proxy = _make_proxy([])
    assert list(proxy.get_query_usage_via_system_tables(ts, ts)) == []


def test_invalid_statement_type_yields_row_with_none_type():
    """An unrecognized statement_type string must not abort the generator.  The bad row
    should still be yielded (statement_type=None) and the valid row must also appear.
    Also asserts the warning was recorded on the proxy report."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        _row(
            statement_id="bad1",
            statement_text="FROBNICATE ...",
            statement_type="WEIRD_TYPE",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name="cat.sch.src",
            source_type="TABLE",
            source_path=None,
            target_table_full_name=None,
            target_type=None,
        ),
        _row(
            statement_id="ok1",
            statement_text="SELECT ...",
            statement_type="SELECT",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name="cat.sch.src",
            source_type="TABLE",
            source_path=None,
            target_table_full_name=None,
            target_type=None,
        ),
    ]
    proxy = _make_proxy(rows)
    out = list(proxy.get_query_usage_via_system_tables(ts, ts))

    assert [q.query.query_id for q in out] == ["bad1", "ok1"], (
        "both statements must be yielded even when one has an unknown statement_type"
    )
    bad = next(q for q in out if q.query.query_id == "bad1")
    ok = next(q for q in out if q.query.query_id == "ok1")
    assert bad.query.statement_type is None, (
        f"expected None for unknown type, got {bad.query.statement_type!r}"
    )
    assert ok.query.statement_type is not None, (
        "valid SELECT statement_type should be set"
    )
    # Item 9: unknown statement type must also be recorded as a warning
    warning_messages = [str(w.message) for w in proxy.report.warnings]
    assert any("unknown-statement-type" in m for m in warning_messages), (
        f"expected 'unknown-statement-type' warning, got: {warning_messages}"
    )


def test_sql_contains_required_filters():
    """Regression guard: the SQL built by get_query_usage_via_system_tables must include
    the direct_access, statement_id IS NOT NULL, and NULL-timestamp filters."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    proxy = _make_proxy([])
    list(proxy.get_query_usage_via_system_tables(ts, ts))

    assert proxy._execute_sql_query_streaming.called
    sql_arg: str = proxy._execute_sql_query_streaming.call_args[0][0]
    assert "direct_access = true" in sql_arg, "missing direct_access filter"
    assert "statement_id IS NOT NULL" in sql_arg, (
        "missing statement_id IS NOT NULL filter"
    )
    assert "start_time IS NOT NULL" in sql_arg, "missing NULL timestamp filter"
    assert "end_time IS NOT NULL" in sql_arg, "missing NULL timestamp filter"


# ---------------------------------------------------------------------------
# Row-parse error recovery: get_query_usage_via_system_tables
# ---------------------------------------------------------------------------


class _BrokenRow:
    """A row whose statement_text attribute raises to simulate a construction failure.

    statement_id and statement_type are readable (needed before construction),
    but statement_text raises so QueryStatementInfo cannot be built.
    """

    def __init__(self, sid: str, ts: datetime) -> None:
        self.statement_id = sid
        self.statement_type = "SELECT"
        self.executed_by = "u@x.io"
        self.executed_by_user_id = 1
        self.executed_as = None
        self.executed_as_user_id = None
        self.end_time = ts
        self.source_table_full_name = None
        self.source_type = None
        self.target_table_full_name = None
        self.target_type = None

    @property
    def start_time(self) -> datetime:
        raise RuntimeError("injected failure during row construction")

    @property
    def statement_text(self) -> str:
        raise RuntimeError("injected failure during row construction")


def test_row_parse_error_skips_failed_row_and_yields_valid_rows() -> None:
    """When one row raises during QueryStatementInfo construction the generator must:
    (a) not raise, (b) still yield all other valid statements, (c) record a warning.

    Additionally verifies the stale-info fix: a row that fails as a NEW statement_id
    must not cause subsequent rows with a *different* valid statement_id to be
    mis-attributed to the previous (stale) statement.
    """
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)

    # ok1: valid first statement — contributes a source table
    ok1_row = _row(
        statement_id="ok1",
        statement_text="SELECT 1",
        statement_type="SELECT",
        executed_by="u@x.io",
        executed_by_user_id=1,
        executed_as=None,
        executed_as_user_id=None,
        start_time=ts,
        end_time=ts,
        source_table_full_name="cat.sch.src",
        source_type="TABLE",
        target_table_full_name=None,
        target_type=None,
    )

    # bad: a NEW statement_id whose construction raises — neither ok1 nor ok2 should
    # absorb its source_table
    bad_row = _BrokenRow("bad", ts)

    # ok2: another valid statement after the bad one
    ok2_row = _row(
        statement_id="ok2",
        statement_text="INSERT INTO cat.sch.tgt SELECT 1",
        statement_type="INSERT",
        executed_by="u@x.io",
        executed_by_user_id=1,
        executed_as=None,
        executed_as_user_id=None,
        start_time=ts,
        end_time=ts,
        source_table_full_name=None,
        source_type=None,
        target_table_full_name="cat.sch.tgt",
        target_type="TABLE",
    )

    proxy = _make_proxy([ok1_row, bad_row, ok2_row])
    out = list(proxy.get_query_usage_via_system_tables(ts, ts))

    # (a) generator must not raise — confirmed by reaching this line.
    # Exact sequence (not membership): guards the stale-info fix. The pre-fix code
    # advanced current_id/yielded BEFORE construction, so the failed `bad` row flushed
    # `ok1` a second time, producing ['ok1', 'ok1', 'ok2']. Membership asserts would
    # tolerate that duplicate; an exact-sequence assert catches it.
    ids = [q.query.query_id for q in out]
    assert ids == ["ok1", "ok2"], f"unexpected statement sequence: {ids}"

    # (b) valid rows are yielded with correct tables
    ok1_out = next(q for q in out if q.query.query_id == "ok1")
    ok2_out = next(q for q in out if q.query.query_id == "ok2")

    assert ok1_out.source_tables == ["cat.sch.src"], (
        f"ok1 source_tables wrong: {ok1_out.source_tables}"
    )
    assert ok1_out.target_tables == [], (
        f"ok1 target_tables wrong: {ok1_out.target_tables}"
    )

    # Stale-info check: ok2's target must NOT be attributed to ok1
    assert ok2_out.target_tables == ["cat.sch.tgt"], (
        f"ok2 target_tables wrong: {ok2_out.target_tables}"
    )
    assert ok2_out.source_tables == [], (
        f"ok2 source_tables wrong: {ok2_out.source_tables}"
    )

    # (c) a warning must have been recorded for the bad row
    assert len(proxy.report.warnings) >= 1, (
        "expected at least one warning for the failed row"
    )
    warning_messages = [str(w.message) for w in proxy.report.warnings]
    assert any("usage-row-parse" in m for m in warning_messages), (
        f"expected 'usage-row-parse' warning, got: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# _execute_sql_query_streaming tests
# ---------------------------------------------------------------------------


def _make_streaming_proxy() -> UnityCatalogApiProxy:
    """Build a bare proxy with warehouse_id set; _workspace_client is mocked."""
    proxy = UnityCatalogApiProxy.__new__(UnityCatalogApiProxy)
    proxy.warehouse_id = "wh1"
    proxy.report = UnityCatalogReport()
    proxy._workspace_client = MagicMock()
    return proxy


def _make_mock_cursor(batches: List[List]) -> MagicMock:
    """Return a mock cursor whose fetchmany() returns successive batches then []."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    batch_iter = iter(batches + [[]])
    cursor.fetchmany = MagicMock(side_effect=lambda n: next(batch_iter))
    return cursor


def _make_mock_connection(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor = MagicMock(return_value=cursor)
    return conn


def test_streaming_yields_all_rows_across_batches() -> None:
    """_execute_sql_query_streaming must yield all rows across multiple fetchmany batches."""
    all_rows = [_row(v=i) for i in range(7)]
    batches = [all_rows[:3], all_rows[3:6], all_rows[6:]]
    cursor = _make_mock_cursor(batches)
    conn = _make_mock_connection(cursor)

    proxy = _make_streaming_proxy()
    with (
        patch(
            "datahub.ingestion.source.unity.proxy.get_sql_connection_params",
            return_value={},
        ),
        patch(
            "datahub.ingestion.source.unity.proxy.connect",
            return_value=conn,
        ),
    ):
        result = list(proxy._execute_sql_query_streaming("SELECT 1"))

    assert result == all_rows, f"expected {all_rows}, got {result}"
    # Connection and cursor must be closed (context managers exited)
    conn.__exit__.assert_called_once()
    cursor.__exit__.assert_called_once()


def test_streaming_reports_warning_on_error_and_does_not_raise() -> None:
    """_execute_sql_query_streaming must report a warning and yield nothing on DB error."""
    proxy = _make_streaming_proxy()
    with (
        patch(
            "datahub.ingestion.source.unity.proxy.get_sql_connection_params",
            return_value={},
        ),
        patch(
            "datahub.ingestion.source.unity.proxy.connect",
            side_effect=RuntimeError("simulated DB failure"),
        ),
    ):
        result = list(proxy._execute_sql_query_streaming("SELECT 1"))

    assert result == [], "expected no rows on error"
    warning_messages = [str(w.message) for w in proxy.report.warnings]
    assert any("sql-query-failed" in m for m in warning_messages), (
        f"expected 'sql-query-failed' warning, got: {warning_messages}"
    )


def test_streaming_mid_stream_error_reports_warning_and_closes_connection() -> None:
    """fetchmany succeeds on the first batch then raises on the second.

    The consumer (list()) must not raise, 'sql-query-failed' must be reported,
    and the connection/cursor context managers must still be exited (no leak).
    """
    first_batch = [_row(v=i) for i in range(3)]

    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    # First call returns a batch; second call raises mid-stream
    cursor.fetchmany = MagicMock(
        side_effect=[first_batch, RuntimeError("mid-stream DB error")]
    )

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor = MagicMock(return_value=cursor)

    proxy = _make_streaming_proxy()
    with (
        patch(
            "datahub.ingestion.source.unity.proxy.get_sql_connection_params",
            return_value={},
        ),
        patch(
            "datahub.ingestion.source.unity.proxy.connect",
            return_value=conn,
        ),
    ):
        result = list(proxy._execute_sql_query_streaming("SELECT 1"))

    # Only the first batch rows should be yielded; the error is swallowed
    assert result == first_batch, f"expected first-batch rows, got {result}"
    # Warning must be reported
    warning_messages = [str(w.message) for w in proxy.report.warnings]
    assert any("sql-query-failed" in m for m in warning_messages), (
        f"expected 'sql-query-failed' warning, got: {warning_messages}"
    )
    # Connection/cursor must have been closed despite the mid-stream error
    conn.__exit__.assert_called_once()
    cursor.__exit__.assert_called_once()


def test_streaming_no_warehouse_id_yields_nothing_and_warns() -> None:
    """When warehouse_id is None the streaming method must yield nothing and report a warning."""
    proxy = _make_streaming_proxy()
    proxy.warehouse_id = None  # type: ignore[assignment]

    result = list(proxy._execute_sql_query_streaming("SELECT 1"))

    assert result == [], "expected no rows when warehouse_id is absent"
    # The no-warehouse path uses message "Cannot execute SQL query" (same as the
    # non-streaming sibling _execute_sql_query).
    warning_messages = [str(w.message) for w in proxy.report.warnings]
    assert any("Cannot execute SQL query" in m for m in warning_messages), (
        f"expected 'Cannot execute SQL query' warning, got: {warning_messages}"
    )


def test_closing_wrapper_ensures_connection_closed_on_early_abort() -> None:
    """Consumer breaks after the first row; closing() must still close the connection.

    This proves the fix in get_query_usage_via_system_tables and
    get_query_history_via_system_tables: wrapping iteration in closing() causes
    the inner generator's close() to be called when the outer with-block exits,
    which triggers the streaming method's 'with connect(...)' __exit__ and
    releases the DB connection deterministically.
    """
    all_rows = [_row(v=i) for i in range(5)]
    batches = [all_rows[:3], all_rows[3:]]
    cursor = _make_mock_cursor(batches)
    conn = _make_mock_connection(cursor)

    proxy = _make_streaming_proxy()
    with (
        patch(
            "datahub.ingestion.source.unity.proxy.get_sql_connection_params",
            return_value={},
        ),
        patch(
            "datahub.ingestion.source.unity.proxy.connect",
            return_value=conn,
        ),
    ):
        from contextlib import closing as _closing

        # Simulate a consumer that abandons iteration after the first row
        gen = proxy._execute_sql_query_streaming("SELECT 1")
        with _closing(gen) as rows:
            for _row_val in rows:
                break  # early abort after one row

    # Even though the consumer stopped early, the connection must be closed
    conn.__exit__.assert_called_once()
    cursor.__exit__.assert_called_once()


# ---------------------------------------------------------------------------
# Proxy edge-case tests
# ---------------------------------------------------------------------------


def test_single_row_with_source_and_target() -> None:
    """A row where both source_type and target_type are non-null → both lists populated."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    # A single row contributes to both source and target only if source_type and
    # target_type are both non-None. The proxy appends source_table when
    # target_type is None, and target_table when source_type is None. So for
    # source_type=non-null + target_type=non-null neither condition triggers.
    # The test verifies that behavior explicitly for a combined row.
    rows = [
        _row(
            statement_id="s1",
            statement_text="MERGE ...",
            statement_type="MERGE",
            executed_by="u@x.io",
            executed_by_user_id=1,
            executed_as=None,
            executed_as_user_id=None,
            start_time=ts,
            end_time=ts,
            source_table_full_name="cat.sch.src",
            source_type="TABLE",
            source_path=None,
            target_table_full_name="cat.sch.tgt",
            target_type="TABLE",
        ),
    ]
    proxy = _make_proxy(rows)
    out = list(proxy.get_query_usage_via_system_tables(ts, ts))

    assert len(out) == 1
    s1 = out[0]
    # When both source_type and target_type are non-null, neither append condition
    # fires — the row contributes nothing to either list.
    assert s1.source_tables == []
    assert s1.target_tables == []


# ---------------------------------------------------------------------------
# Usage extractor tests: aggregator-backed emission
# ---------------------------------------------------------------------------


def _query(text: str, qid: str = "s1") -> Query:
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    return Query(
        query_id=qid,
        query_text=text,
        statement_type=None,
        start_time=ts,
        end_time=ts,
        user_id=1,
        user_name="u@x.io",
        executed_as_user_id=None,
        executed_as_user_name=None,
    )


def _extractor(config: MagicMock, proxy: MagicMock) -> UnityCatalogUsageExtractor:
    ex = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    ex.config = config
    ex.report = MagicMock()
    ex.proxy = proxy
    ex.table_urn_builder = lambda ref: (
        f"urn:li:dataset:(urn:li:dataPlatform:databricks,{ref.qualified_table_name},PROD)"
    )
    ex.user_urn_builder = lambda u: f"urn:li:corpuser:{u}"
    ex.platform = "databricks"
    return ex


def test_builds_aggregator_and_feeds_observed_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import datahub.ingestion.source.unity.usage as usage_mod

    captured: dict = {}
    agg_instance: list = []  # single-element list so the closure can capture it

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            captured["kwargs"] = kw
            self.observed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.emit_queries = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT * FROM main.s.t", "s1"),
        _query("SELECT * FROM main.s.t2", "s2"),
    ]
    ex = _extractor(config, proxy)

    list(ex.get_usage_workunits(set()))

    assert captured["kwargs"]["generate_lineage"] is False
    assert captured["kwargs"]["generate_usage_statistics"] is True
    assert captured["kwargs"]["generate_operations"] is True
    assert captured["kwargs"]["generate_queries"] is True
    # generate_query_usage_statistics must be True so queries emit even when
    # generate_lineage=False (it's a separate flag in SqlParsingAggregator)
    assert captured["kwargs"]["generate_query_usage_statistics"] is True
    assert captured["closed"] is True

    # Both queries must have been fed to the aggregator
    agg = agg_instance[0]
    assert len(agg.observed) == 2, (
        f"expected 2 observed queries, got {len(agg.observed)}"
    )
    observed_texts = [o.query for o in agg.observed]
    assert "SELECT * FROM main.s.t" in observed_texts
    assert "SELECT * FROM main.s.t2" in observed_texts


def test_use_system_tables_join_delegates_to_config() -> None:
    """_use_system_tables_join() must delegate to config.usage_uses_system_tables()."""
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    config = MagicMock()
    config.usage_uses_system_tables.return_value = True
    ex = _extractor(config, proxy)
    assert ex._use_system_tables_join() is True

    config.usage_uses_system_tables.return_value = False
    assert ex._use_system_tables_join() is False
