from contextlib import closing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Iterator, List
from unittest.mock import MagicMock, patch

import pytest

import datahub.ingestion.source.unity.usage as usage_mod
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass
from datahub.sql_parsing.schema_resolver import SchemaResolver


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
    # Connection and cursor must be closed (closing() calls .close())
    conn.close.assert_called_once()
    cursor.close.assert_called_once()


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
    warning_titles = [str(w.title) for w in proxy.report.warnings]
    assert any("Failed to run SQL query" in t for t in warning_titles), (
        f"expected 'Failed to run SQL query' warning, got: {warning_titles}"
    )


def test_streaming_mid_stream_error_reports_warning_and_closes_connection() -> None:
    """fetchmany succeeds on the first batch then raises on the second.

    The consumer (list()) must not raise, 'Failed to run SQL query' must be reported,
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
    warning_titles = [str(w.title) for w in proxy.report.warnings]
    assert any("Failed to run SQL query" in t for t in warning_titles), (
        f"expected 'Failed to run SQL query' warning, got: {warning_titles}"
    )
    # Connection/cursor must have been closed despite the mid-stream error (closing() calls .close())
    conn.close.assert_called_once()
    cursor.close.assert_called_once()


def test_streaming_no_warehouse_id_yields_nothing_and_warns() -> None:
    """When warehouse_id is None the streaming method must yield nothing and report a warning."""
    proxy = _make_streaming_proxy()
    proxy.warehouse_id = None  # type: ignore[assignment]

    result = list(proxy._execute_sql_query_streaming("SELECT 1"))

    assert result == [], "expected no rows when warehouse_id is absent"
    # The no-warehouse path reports title="Cannot execute SQL query" (same as the
    # non-streaming sibling _execute_sql_query).
    warning_titles = [str(w.title) for w in proxy.report.warnings]
    assert any("Cannot execute SQL query" in t for t in warning_titles), (
        f"expected 'Cannot execute SQL query' warning title, got: {warning_titles}"
    )


def test_closing_wrapper_ensures_connection_closed_on_early_abort() -> None:
    """Consumer breaks after the first row; closing() must still close the connection.

    Wrapping iteration in closing() causes the inner generator's close() to be called
    when the outer with-block exits, which triggers the streaming method's
    closing(connection).__exit__ → connection.close() and releases the DB connection.
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
        # Simulate a consumer that abandons iteration after the first row
        gen = proxy._execute_sql_query_streaming("SELECT 1")
        with closing(gen) as rows:
            for _row_val in rows:
                break  # early abort after one row

    # Even though the consumer stopped early, the connection and cursor must be closed.
    # The new implementation uses closing() which calls .close() rather than __exit__.
    conn.close.assert_called_once()
    cursor.close.assert_called_once()


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
    ex.report = UnityCatalogReport()
    ex.proxy = proxy
    ex.table_urn_builder = lambda ref: (
        f"urn:li:dataset:(urn:li:dataPlatform:databricks,{ref.qualified_table_name},PROD)"
    )
    ex.user_urn_builder = lambda u: f"urn:li:corpuser:{u}"
    ex.platform = "databricks"
    ex.schema_resolver = SchemaResolver(
        platform="databricks",
        platform_instance=None,
        env="PROD",
    )
    return ex


def test_builds_aggregator_and_feeds_observed_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    config.include_queries = True
    config.include_query_usage_statistics = True
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
    # naive datetime (no tzinfo) — must be treated as UTC via replace(tzinfo=utc)
    ts_naive = datetime(2026, 6, 1, 8, 30)

    config = MagicMock()
    config.include_queries = True
    config.include_query_usage_statistics = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query_with_ts(ts_plus2, "q1"),
        _query_with_ts(ts_fixed_utc, "q2"),
        _query_with_ts(ts_naive, "q3"),
        # None start_time: must pass through as-is without crashing.
        _query_with_ts(None, "q4"),  # type: ignore[arg-type]
    ]
    ex = _extractor(config, proxy)

    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 4, f"expected 4 queries, got {len(agg.observed)}"

    for obs in agg.observed:
        ts = obs.timestamp
        if ts is None:
            # None start_time passes through unchanged — checked separately below.
            continue
        # Must satisfy the SqlParsingAggregator assertion: tzinfo in {None, timezone.utc}
        assert ts.tzinfo is timezone.utc, (
            f"expected timezone.utc singleton, got {ts.tzinfo!r} for timestamp {ts}"
        )

    # The naive timestamp (q3) must be treated as UTC via replace — wall-clock unchanged.
    obs_naive = next(o for o in agg.observed if o.query == "SELECT q3")
    assert obs_naive.timestamp == datetime(2026, 6, 1, 8, 30, tzinfo=timezone.utc), (
        f"naive ts must be UTC-stamped without wall-clock adjustment, got {obs_naive.timestamp}"
    )

    # None start_time must be passed through as-is — no crash, no coercion.
    obs_none = next(o for o in agg.observed if o.query == "SELECT q4")
    assert obs_none.timestamp is None, (
        f"None start_time must yield timestamp=None, got {obs_none.timestamp!r}"
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
    config.include_queries = False
    config.include_query_usage_statistics = False
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
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    # Build an extractor whose table_urn_builder includes a platform_instance prefix
    # in the URN — exactly what the connector does when platform_instance="core_finance".
    ex = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    ex.config = config
    ex.report = UnityCatalogReport()
    ex.proxy = proxy
    ex.table_urn_builder = lambda ref: (
        f"urn:li:dataset:(urn:li:dataPlatform:databricks,"
        f"core_finance.{ref.qualified_table_name},PROD)"
    )
    ex.user_urn_builder = lambda u: f"urn:li:corpuser:{u}"
    ex.platform = "databricks"
    ex.schema_resolver = SchemaResolver(
        platform="databricks",
        platform_instance="core_finance",
        env="PROD",
    )

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


def test_schema_resolver_passed_to_aggregator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When schema_resolver is set on the extractor, _build_aggregator must pass
    THAT instance to SqlParsingAggregator rather than creating a fresh empty one.
    """
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
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.platform_instance = None
    config.env = "PROD"
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    resolver = SchemaResolver(
        platform="databricks",
        platform_instance=None,
        env="PROD",
    )
    ex = _extractor(config, proxy)
    ex.schema_resolver = resolver

    list(ex.get_usage_workunits(set()))

    assert captured["kwargs"]["schema_resolver"] is resolver, (
        "_build_aggregator must pass the extractor's schema_resolver to SqlParsingAggregator"
    )


def test_default_db_set_to_single_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When all table_refs share one catalog, every ObservedQuery.default_db must equal it."""
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

    config = MagicMock()
    config.include_queries = True
    config.include_query_usage_statistics = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT * FROM lineagedemo.dinner", "q1"),
        _query("SELECT * FROM lineagedemo.lunch", "q2"),
    ]
    ex = _extractor(config, proxy)

    refs = {
        TableReference(
            metastore=None, catalog="main", schema="lineagedemo", table="dinner"
        ),
        TableReference(
            metastore=None, catalog="main", schema="lineagedemo", table="lunch"
        ),
    }
    list(ex.get_usage_workunits(refs))

    agg = agg_instance[0]
    assert len(agg.observed) == 2
    for obs in agg.observed:
        assert obs.default_db == "main", (
            f"expected default_db='main', got {obs.default_db!r}"
        )
        assert obs.default_schema is None


def test_default_db_none_for_multi_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When table_refs span two catalogs, every ObservedQuery.default_db must be None."""
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

    config = MagicMock()
    config.include_queries = True
    config.include_query_usage_statistics = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT * FROM main.s.t", "q1"),
    ]
    ex = _extractor(config, proxy)

    refs = {
        TableReference(metastore=None, catalog="main", schema="s", table="t"),
        TableReference(metastore=None, catalog="other", schema="s", table="t2"),
    }
    list(ex.get_usage_workunits(refs))

    agg = agg_instance[0]
    assert len(agg.observed) == 1
    assert agg.observed[0].default_db is None, (
        f"expected default_db=None for multi-catalog, got {agg.observed[0].default_db!r}"
    )


def test_default_db_none_for_empty_table_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When table_refs is empty, default_db must be None and no crash must occur."""
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

    config = MagicMock()
    config.include_queries = True
    config.include_query_usage_statistics = True
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT 1", "q1"),
    ]
    ex = _extractor(config, proxy)

    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 1
    assert agg.observed[0].default_db is None, (
        f"expected default_db=None for empty refs, got {agg.observed[0].default_db!r}"
    )


def test_schema_resolver_explicit_empty_resolver_passed_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When an explicit empty SchemaResolver is provided, _build_aggregator must pass
    THAT instance to SqlParsingAggregator without modification.
    """
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
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.platform_instance = None
    config.env = "PROD"
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    resolver = SchemaResolver(
        platform="databricks",
        platform_instance=None,
        env="PROD",
    )
    ex = _extractor(config, proxy)
    ex.schema_resolver = resolver

    list(ex.get_usage_workunits(set()))

    assert captured["kwargs"]["schema_resolver"] is resolver, (
        "_build_aggregator must pass the provided SchemaResolver instance to SqlParsingAggregator"
    )


def test_auto_empty_usage_emitted_for_unqueried_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ingested table with no observed queries must still get a zero-usage aspect.

    auto_empty_dataset_usage_statistics wraps the aggregator stream and emits empty
    usage workunits for every URN in dataset_urns that had no usage in the window.
    The test feeds ONE query for a *different* table so num_queries > 0 (auto_empty
    runs), then checks that the *unqueried* table still receives a zero-usage workunit.
    """

    class FakeAgg:
        """Produces no metadata — simulates a window where no queries touched the table."""

        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 2, tzinfo=timezone.utc)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    # Provide real time-window values so auto_empty_dataset_usage_statistics can
    # compute bucket timestamps via config.majority_buckets().
    config.start_time = start
    config.end_time = end
    config.bucket_duration = BucketDuration.DAY
    config.majority_buckets.return_value = [start]

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    # Feed ONE query for a different table so num_queries > 0 — this ensures
    # auto_empty_dataset_usage_statistics is reached and can reset unqueried tables.
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT * FROM main.sales.products", "q1"),
    ]

    ex = _extractor(config, proxy)
    # Use a real report so num_queries reflects actual query count.
    ex.report = UnityCatalogReport()

    # ref_unqueried is the table that had no matching query in this window.
    ref_unqueried = TableReference(
        metastore=None, catalog="main", schema="sales", table="orders"
    )
    expected_urn = ex.table_urn_builder(ref_unqueried)

    workunits = list(ex.get_usage_workunits({ref_unqueried}))

    # At least one workunit must carry a DatasetUsageStatistics aspect for the
    # unqueried table's URN — the zero-usage reset workunit.
    usage_wus = [
        wu
        for wu in workunits
        if wu.get_urn() == expected_urn
        and wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
    ]
    assert usage_wus, (
        f"Expected a zero-usage DatasetUsageStatistics workunit for {expected_urn!r}, "
        f"but none was emitted. Workunits: {[wu.id for wu in workunits]}"
    )
    usage_aspect = usage_wus[0].get_aspect_of_type(DatasetUsageStatisticsClass)
    assert usage_aspect is not None
    assert usage_aspect.totalSqlQueries == 0, (
        f"Zero-usage aspect must have totalSqlQueries=0, got {usage_aspect.totalSqlQueries}"
    )
    assert usage_aspect.uniqueUserCount == 0, (
        f"Zero-usage aspect must have uniqueUserCount=0, got {usage_aspect.uniqueUserCount}"
    )


def test_zero_queries_emits_no_usage_workunits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the query history is empty (num_queries == 0), NO datasetUsageStatistics
    workunit must be emitted, and the no-queries-found warning must fire.

    Emitting zero-usage aspects on a query-less run would wrongly wipe existing
    usage stats in DataHub (e.g. when the warehouse has no recent activity or the
    connector lacks SELECT privilege on query history tables).
    """

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    # Use a real report so num_queries starts at 0 and the comparison works correctly.
    ex.report = UnityCatalogReport()

    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    workunits = list(ex.get_usage_workunits({ref}))

    usage_wus = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
    ]
    assert not usage_wus, (
        f"Expected NO datasetUsageStatistics workunits when num_queries == 0, "
        f"but got {len(usage_wus)}: {[wu.id for wu in usage_wus]}"
    )

    # The "No queries found for usage" warning must be reported.
    warning_titles = [w.title for w in ex.report.warnings]
    assert any(t == "No queries found for usage" for t in warning_titles), (
        f"Expected 'No queries found for usage' warning, got: {warning_titles}"
    )


def test_fetch_failure_reports_failure_and_no_usage_workunits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When num_queries == 0 AND a fetch failure was recorded during the run,
    get_usage_workunits must call report_failure with 'Failed to fetch query history' and
    return without emitting any datasetUsageStatistics workunits or the benign
    'No queries found for usage' warning.

    This guards the C1 fix: a swallowed streaming fetch failure must not look like
    a normal empty-history run.
    """

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True

    report = UnityCatalogReport()

    # Simulate a proxy whose _fetch_queries (via get_query_history_via_system_tables)
    # swallows a streaming DB error: it yields no rows AND bumps the fetch-failure
    # counter — exactly what _execute_sql_query_streaming does on exception.
    def _failing_fetch(*_args: object, **_kw: object) -> Iterator[Query]:
        report.num_usage_query_fetch_failures += 1
        return iter([])

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.side_effect = _failing_fetch

    ex = _extractor(config, proxy)
    ex.report = report

    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    workunits = list(ex.get_usage_workunits({ref}))

    # (a) A 'Failed to fetch query history' failure must be recorded.
    failure_keys = [getattr(f, "title", None) or f.message for f in report.failures]
    assert any("Failed to fetch query history" in k for k in failure_keys), (
        f"expected 'Failed to fetch query history' failure, got: {failure_keys}"
    )

    # (b) The benign 'No queries found for usage' warning must NOT be emitted.
    warning_titles = [w.title for w in report.warnings]
    assert not any(t == "No queries found for usage" for t in warning_titles), (
        f"'No queries found for usage' must not fire when a fetch failure occurred, "
        f"got warnings: {warning_titles}"
    )

    # (c) No datasetUsageStatistics workunits must be emitted.
    usage_wus = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
    ]
    assert not usage_wus, (
        f"Expected NO datasetUsageStatistics workunits on fetch failure, "
        f"but got {len(usage_wus)}: {[wu.id for wu in usage_wus]}"
    )


# ---------------------------------------------------------------------------
# Error-path tests
# ---------------------------------------------------------------------------


def test_gen_metadata_raises_propagates_to_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If gen_metadata() raises during iteration, the exception propagates to the caller.

    After the B2 restructure, yield from is outside any except block, so errors
    raised during iteration (including from gen_metadata) propagate to the consumer.
    The finally block still closes the aggregator cleanly.
    """

    closed_calls: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            raise RuntimeError("gen_metadata boom")
            yield  # make it a generator

        def close(self) -> None:
            closed_calls.append(True)

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    # Supply one query so num_queries > 0 and gen_metadata() is actually reached.
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT 1", "q1"),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    # gen_metadata errors now propagate to the caller (yield is outside any except).
    with pytest.raises(RuntimeError, match="gen_metadata boom"):
        list(ex.get_usage_workunits(set()))

    # The aggregator must still have been closed cleanly via the finally block.
    assert closed_calls, "aggregator.close() must be called even on gen_metadata error"


def test_aggregator_close_raises_does_not_propagate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If aggregator.close() raises in the finally block, the exception must be swallowed.

    The finally guard must log a warning but not allow the close() error to propagate,
    even when the main try-block completed normally.
    """

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            raise RuntimeError("close boom")

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    # Must not raise (the close() error is swallowed by the finally guard).
    list(ex.get_usage_workunits(set()))


def test_single_bad_query_skipped_others_still_fed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single add_observed_query failure must skip that query (incrementing
    num_queries_dropped) and continue feeding the remaining queries.

    The overall run must not raise, and the other queries must still appear in the
    aggregator's observed list.
    """
    observed: list = []
    call_count = [0]  # mutable cell so the closure can increment it

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, obs: object, **kw: object) -> None:
            call_count[0] += 1
            # Raise on the first call only; subsequent calls succeed.
            if call_count[0] == 1:
                raise ValueError("bad query")
            observed.append(obs)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query("SELECT bad", "q_bad"),  # will raise in add_observed_query
        _query("SELECT ok1", "q_ok1"),  # must still be fed
        _query("SELECT ok2", "q_ok2"),  # must still be fed
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    # Must not raise.
    list(ex.get_usage_workunits(set()))

    # The two good queries must have been observed.
    observed_texts = [o.query for o in observed]
    assert "SELECT ok1" in observed_texts, (
        f"expected 'SELECT ok1' in observed, got: {observed_texts}"
    )
    assert "SELECT ok2" in observed_texts, (
        f"expected 'SELECT ok2' in observed, got: {observed_texts}"
    )

    # The dropped query must be counted.
    assert ex.report.num_queries_dropped == 1, (
        f"expected num_queries_dropped=1, got {ex.report.num_queries_dropped}"
    )

    # A "Skipped query during usage extraction" warning must have been recorded.
    warning_titles = [w.title for w in ex.report.warnings]
    assert any(t == "Skipped query during usage extraction" for t in warning_titles), (
        f"expected 'Skipped query during usage extraction' warning, got: {warning_titles}"
    )


# ---------------------------------------------------------------------------
# New tests: API path routing, mid-stream fetch failure, flag rename/split
# ---------------------------------------------------------------------------


def test_fetch_queries_routes_through_api_when_system_tables_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When usage_uses_system_tables returns False, _fetch_queries must use
    proxy.query_history and NOT proxy.get_query_history_via_system_tables.
    """
    observed: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, obs: object, **kw: object) -> None:
            observed.append(obs)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = True
    config.include_query_usage_statistics = True
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = False  # force API path

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.query_history.return_value = [
        _query("SELECT * FROM api.s.t", "api_q1"),
    ]
    # System tables path must NOT be called
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    list(ex.get_usage_workunits(set()))

    # Must have used the REST API path
    proxy.query_history.assert_called_once()
    proxy.get_query_history_via_system_tables.assert_not_called()

    # The API query must have been fed to the aggregator
    observed_texts = [o.query for o in observed]
    assert "SELECT * FROM api.s.t" in observed_texts, (
        f"expected API query in observed, got: {observed_texts}"
    )


def test_midstream_fetch_failure_with_some_queries_reports_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a fetch failure occurs after >=1 queries are already processed
    (num_queries > 0 AND fetch_failed), get_usage_workunits must still record
    a 'Failed to fetch query history' failure — not just the benign warning.
    """

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True

    report = UnityCatalogReport()

    def _partial_failing_fetch(*_args: object, **_kw: object) -> "Iterator[Query]":
        # Yield one real query, then simulate a mid-stream DB error by
        # incrementing the failure counter (as _execute_sql_query_streaming does).
        yield _query("SELECT * FROM main.s.t", "q1")
        report.num_usage_query_fetch_failures += 1

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.side_effect = _partial_failing_fetch

    ex = _extractor(config, proxy)
    ex.report = report

    ref = TableReference(metastore=None, catalog="main", schema="s", table="t")
    list(ex.get_usage_workunits({ref}))

    # num_queries > 0 (one query was processed) AND fetch_failed → must report_failure
    assert report.num_queries >= 1, (
        f"expected >=1 query processed, got {report.num_queries}"
    )
    failure_keys = [getattr(f, "title", None) or f.message for f in report.failures]
    assert any("Failed to fetch query history" in k for k in failure_keys), (
        f"expected 'Failed to fetch query history' failure when mid-stream failure occurs "
        f"with num_queries>0, got: {failure_keys}"
    )


def _run_flag_case(
    monkeypatch: pytest.MonkeyPatch,
    include_q: bool,
    include_qu_stats: bool,
) -> dict:
    """Helper: run get_usage_workunits with the given flag values; return aggregator kwargs."""
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
    config.include_queries = include_q
    config.include_query_usage_statistics = include_qu_stats
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    list(ex.get_usage_workunits(set()))
    return captured["kwargs"]


def test_include_queries_flag_controls_generate_queries_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """include_queries (True/False) must map directly to generate_queries kwarg,
    and include_query_usage_statistics must map to generate_query_usage_statistics.
    """
    kwargs = _run_flag_case(monkeypatch, include_q=True, include_qu_stats=True)
    assert kwargs["generate_queries"] is True
    assert kwargs["generate_query_usage_statistics"] is True

    kwargs = _run_flag_case(monkeypatch, include_q=False, include_qu_stats=False)
    assert kwargs["generate_queries"] is False
    assert kwargs["generate_query_usage_statistics"] is False

    kwargs = _run_flag_case(monkeypatch, include_q=True, include_qu_stats=False)
    assert kwargs["generate_queries"] is True
    assert kwargs["generate_query_usage_statistics"] is False
