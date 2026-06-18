from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor


def _row(**kw):
    # Databricks Row supports attribute access; SimpleNamespace replicates that.
    return SimpleNamespace(**kw)


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

    Wrapping iteration in closing() causes the inner generator's close() to be called
    when the outer with-block exits, which triggers the streaming method's
    'with connect(...)' __exit__ and releases the DB connection deterministically.
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


def test_observed_query_timestamps_normalized_to_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timestamps with non-UTC-singleton tzinfo must be normalized to timezone.utc.

    SqlParsingAggregator asserts `timestamp.tzinfo in {None, timezone.utc}`.
    Databricks returns tz-aware datetimes with a fixed-offset tzinfo that is NOT
    the timezone.utc singleton (even when the offset is +00:00), triggering an
    AssertionError that silently drops all usage. This test proves the fix.
    """
    import datahub.ingestion.source.unity.usage as usage_mod

    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    def _query_with_ts(ts: datetime, qid: str) -> Query:
        return Query(
            query_id=qid,
            query_text=f"SELECT {qid}",
            statement_type=None,
            start_time=ts,
            end_time=ts,
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
        )

    # +02:00 fixed-offset — clearly not timezone.utc singleton
    ts_plus2 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    # +00:00 fixed-offset — same wall-clock as UTC but NOT the timezone.utc singleton
    ts_fixed_utc = datetime(2026, 6, 1, 10, 0, tzinfo=timezone(timedelta(0)))

    config = MagicMock()
    config.emit_queries = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query_with_ts(ts_plus2, "q1"),
        _query_with_ts(ts_fixed_utc, "q2"),
    ]
    ex = _extractor(config, proxy)

    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 2, f"expected 2 queries, got {len(agg.observed)}"

    for obs in agg.observed:
        ts = obs.timestamp
        assert ts is not None, "timestamp must not be None"
        # Must satisfy the SqlParsingAggregator assertion: tzinfo in {None, timezone.utc}
        assert ts.tzinfo is timezone.utc, (
            f"expected timezone.utc singleton, got {ts.tzinfo!r} for timestamp {ts}"
        )


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


def test_is_allowed_table_predicate_scopes_to_ingested_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """is_allowed_table passed to SqlParsingAggregator must accept ingested tables only.

    The predicate must return True for any table name built from the ingested
    table_refs and False for tables outside the ingested set (e.g. system tables
    that appear in query history but were never ingested by this recipe).
    """
    import datahub.ingestion.source.unity.usage as usage_mod
    from datahub.ingestion.source.unity.proxy_types import TableReference

    captured: dict = {}

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            captured["kwargs"] = kw

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.emit_queries = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)

    ref_a = TableReference(
        metastore=None, catalog="main", schema="sales", table="orders"
    )
    ref_b = TableReference(
        metastore=None, catalog="main", schema="sales", table="customers"
    )
    table_refs = {ref_a, ref_b}

    list(ex.get_usage_workunits(table_refs))

    predicate = captured["kwargs"].get("is_allowed_table")
    assert predicate is not None, (
        "is_allowed_table predicate must be passed to the aggregator"
    )

    # Ingested table names (catalog.schema.table format as produced by DatasetUrn.name)
    assert predicate("main.sales.orders") is True, (
        "predicate must return True for ingested table main.sales.orders"
    )
    assert predicate("main.sales.customers") is True, (
        "predicate must return True for ingested table main.sales.customers"
    )
    # Case-insensitive: Unity Catalog names may differ in case
    assert predicate("MAIN.SALES.ORDERS") is True, "predicate must be case-insensitive"

    # Non-ingested tables (system tables observed in query history but not in this recipe)
    assert predicate("system.access.table_lineage") is False, (
        "predicate must return False for system.access.table_lineage (not ingested)"
    )
    assert predicate("other_catalog.schema.table") is False, (
        "predicate must return False for tables from catalogs not in this recipe"
    )


def test_is_allowed_table_predicate_platform_instance_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """is_allowed_table must work correctly when a platform_instance is configured.

    The aggregator's _name_from_urn strips the platform_instance prefix from the
    dataset URN, yielding a bare "catalog.schema.table" string that it passes to
    the predicate.  When a platform_instance (e.g. "core_finance") is present,
    DatasetUrn.name returns "core_finance.main.lineagedemo.price" — WITH the prefix —
    which never matches the bare name the aggregator passes.

    The fix builds allowed_names from TableReference.qualified_table_name, which is
    always the 3-part "catalog.schema.table" with NO platform_instance prefix.
    This test proves the predicate accepts the bare name the aggregator actually passes.
    """
    import datahub.ingestion.source.unity.usage as usage_mod
    from datahub.ingestion.source.unity.proxy_types import TableReference

    captured: dict = {}

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            captured["kwargs"] = kw

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.emit_queries = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    # Build an extractor whose table_urn_builder includes a platform_instance prefix
    # in the URN — exactly what the connector does when platform_instance="core_finance".
    ex = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    ex.config = config
    ex.report = MagicMock()
    ex.proxy = proxy
    ex.table_urn_builder = lambda ref: (
        f"urn:li:dataset:(urn:li:dataPlatform:databricks,"
        f"core_finance.{ref.qualified_table_name},PROD)"
    )
    ex.user_urn_builder = lambda u: f"urn:li:corpuser:{u}"
    ex.platform = "databricks"

    ref = TableReference(
        metastore=None, catalog="main", schema="lineagedemo", table="price"
    )
    # qualified_table_name for this ref is "main.lineagedemo.price" — 3-part, no prefix.
    assert ref.qualified_table_name == "main.lineagedemo.price"

    list(ex.get_usage_workunits({ref}))

    predicate = captured["kwargs"].get("is_allowed_table")
    assert predicate is not None, (
        "is_allowed_table predicate must be passed to the aggregator"
    )

    # The aggregator calls the predicate with the bare name (platform_instance stripped).
    # The previous (broken) implementation would have built allowed_names from
    # DatasetUrn.name = "core_finance.main.lineagedemo.price", causing a mismatch.
    assert predicate("main.lineagedemo.price") is True, (
        "predicate must accept bare catalog.schema.table name (platform_instance stripped)"
    )
    assert predicate("MAIN.LINEAGEDEMO.PRICE") is True, (
        "predicate must be case-insensitive"
    )
    assert predicate("system.access.table_lineage") is False, (
        "predicate must reject tables not in the ingested set"
    )
