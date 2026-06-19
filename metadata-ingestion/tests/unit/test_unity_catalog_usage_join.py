import pathlib
from contextlib import closing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.sql import QueryStatementType

import datahub.ingestion.source.unity.usage as usage_mod
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedList


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
    # MagicMock attributes are truthy; set explicit defaults for newer bool flags.
    if isinstance(config.skip_sqlglot_when_system_table_lineage_missing, MagicMock):
        config.skip_sqlglot_when_system_table_lineage_missing = False
    if isinstance(config.push_down_database_pattern_access_history, MagicMock):
        config.push_down_database_pattern_access_history = False
    if isinstance(config.include_column_usage_stats, MagicMock):
        config.include_column_usage_stats = False
    if isinstance(config.catalog_pattern, MagicMock):
        config.catalog_pattern = None

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


def _register_tables(ex: UnityCatalogUsageExtractor, full_names: List[str]) -> None:
    """Seed the schema resolver so preparsed resolution succeeds for these tables.

    resolve_table_parts signals resolution via SchemaInfo, which is only present for
    tables the recipe ingested, so preparsed-path tests must register the tables they
    expect to resolve to dataset URNs.
    """
    for name in full_names:
        catalog, schema, table = name.split(".")
        urn = ex.schema_resolver.resolve_table_parts(
            database=catalog, db_schema=schema, table=table
        )[0]
        ex.schema_resolver.add_raw_schema_info(urn, {"col": "int"})


def test_builds_aggregator_and_feeds_observed_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}
    agg_instance: list = []  # single-element list so the closure can capture it

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            captured["kwargs"] = kw
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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

    # Queries without system-table lineage fall back to sqlglot (ObservedQuery).
    agg = agg_instance[0]
    assert len(agg.observed) == 2, (
        f"expected 2 observed queries, got {len(agg.observed)}"
    )
    assert len(agg.preparsed) == 0
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

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

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

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

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

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

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

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

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


def test_zero_queries_emits_empty_usage_for_idle_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the query history is genuinely empty (num_queries == 0 and no fetch
    failure), the ingested tables must still receive a zero-usage aspect for the
    window, and the no-queries-found warning must fire.

    A genuinely idle window should record a current-bucket zero datapoint (via
    auto_empty_dataset_usage_statistics) rather than leaving stale usage stats —
    fetch *failures* are handled separately and short-circuit before this point
    (see test_fetch_failure_reports_failure_and_no_usage_workunits).
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

    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 2, tzinfo=timezone.utc)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    # Real time-window values so auto_empty_dataset_usage_statistics can compute
    # bucket timestamps via config.majority_buckets().
    config.start_time = start
    config.end_time = end
    config.bucket_duration = BucketDuration.DAY
    config.majority_buckets.return_value = [start]

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    # Use a real report so num_queries starts at 0 and the comparison works correctly.
    ex.report = UnityCatalogReport()

    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    expected_urn = ex.table_urn_builder(ref)
    workunits = list(ex.get_usage_workunits({ref}))

    # The idle window must still stamp a zero-usage aspect for the ingested table.
    usage_wus = [
        wu
        for wu in workunits
        if wu.get_urn() == expected_urn
        and wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
    ]
    assert usage_wus, (
        f"Expected a zero-usage DatasetUsageStatistics workunit for {expected_urn!r} "
        f"on an idle window, but none was emitted. Workunits: {[wu.id for wu in workunits]}"
    )
    usage_aspect = usage_wus[0].get_aspect_of_type(DatasetUsageStatisticsClass)
    assert usage_aspect is not None
    assert usage_aspect.totalSqlQueries == 0
    assert usage_aspect.uniqueUserCount == 0

    # Zeros must be scoped to the ingested table set, not over-stamped onto
    # tables this recipe didn't ingest.
    other_urn = ex.table_urn_builder(
        TableReference(
            metastore=None, catalog="main", schema="sales", table="not_ingested"
        )
    )
    assert not any(
        wu.get_urn() == other_urn
        and wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
        for wu in workunits
    ), "idle window must not stamp usage for non-ingested tables"

    # The "No queries found for usage" warning must still be reported.
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
    workunits = list(ex.get_usage_workunits({ref}))

    # num_queries > 0 (one query was processed) AND fetch_failed → must report_failure
    assert report.num_queries >= 1, (
        f"expected >=1 query processed, got {report.num_queries}"
    )
    failure_keys = [getattr(f, "title", None) or f.message for f in report.failures]
    assert any("Failed to fetch query history" in k for k in failure_keys), (
        f"expected 'Failed to fetch query history' failure when mid-stream failure occurs "
        f"with num_queries>0, got: {failure_keys}"
    )

    usage_wus = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(DatasetUsageStatisticsClass) is not None
    ]
    assert not usage_wus, (
        "fetch failure must return before emitting usage workunits (W2)"
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


def _query_with_lineage(
    text: str,
    qid: str,
    *,
    sources: List[str],
    targets: Optional[List[str]] = None,
) -> Query:
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
        source_table_full_names=sources,
        target_table_full_names=targets or [],
    )


def test_preparsed_query_used_when_system_table_lineage_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
        _query_with_lineage(
            "SELECT * FROM main.sales.orders",
            "s1",
            sources=["main.sales.orders"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.sales.orders"])
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.preparsed) == 1
    assert len(agg.observed) == 0
    assert ex.report.num_queries_preparsed_from_lineage == 1
    assert ex.report.num_queries_observed_sqlglot == 0
    assert ex.report.num_queries_without_system_table_lineage == 0
    assert agg.preparsed[0].upstreams, "preparsed query must carry upstream urns"


def test_fallback_to_observed_when_lineage_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
    # Malformed table name cannot be resolved to a URN -> fall back to sqlglot.
    proxy.get_query_history_via_system_tables.return_value = [
        _query_with_lineage("SELECT 1", "s1", sources=["not_a_valid_name"]),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 1
    assert len(agg.preparsed) == 0
    assert ex.report.num_queries_observed_sqlglot == 1
    assert ex.report.num_queries_preparsed_fallback_to_sqlglot == 1
    assert ex.report.num_queries_without_system_table_lineage == 0
    warning_titles = [str(w.title) for w in ex.report.warnings]
    assert any("fell back to SQL parsing" in t for t in warning_titles)


def test_sqlglot_when_no_system_table_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    proxy.get_query_history_via_system_tables.return_value = [
        Query(
            query_id="s-no-lineage",
            query_text="SELECT COUNT(*) FROM fivetran.smoke.base_table",
            statement_type=QueryStatementType.SELECT,
            start_time=ts,
            end_time=ts,
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
            source_table_full_names=[],
            target_table_full_names=[],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 1
    assert len(agg.preparsed) == 0
    assert ex.report.num_queries_without_system_table_lineage == 1
    assert ex.report.num_queries_observed_sqlglot == 1
    assert ex.report.num_queries_preparsed_from_lineage == 0
    warning_titles = [str(w.title) for w in ex.report.warnings]
    assert any("Queries missing system-table lineage" in t for t in warning_titles)


def test_get_query_history_via_system_tables_groups_lineage_rows() -> None:
    """Joined rows for the same statement_id must be grouped into one Query."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        _row(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t",
            target_table_full_name=None,
        ),
        _row(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t2",
            target_table_full_name=None,
        ),
        _row(
            statement_id="s2",
            statement_text="SELECT 1",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name=None,
            target_table_full_name=None,
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert len(result) == 2
    assert result[0].query_id == "s1"
    assert sorted(result[0].source_table_full_names) == ["main.s.t", "main.s.t2"]
    assert result[1].query_id == "s2"
    assert result[1].source_table_full_names == []


def test_get_query_history_row_parse_error_does_not_corrupt_lineage() -> None:
    """A bad row must not leave lineage sets that attach to a later statement."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        _row(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t1",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t1",
            target_table_full_name=None,
        ),
        _row(
            statement_id="s2",
            statement_text="SELECT bad",
            statement_type="NOT_A_REAL_TYPE",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.orphan",
            target_table_full_name=None,
        ),
        _row(
            statement_id="s3",
            statement_text="SELECT * FROM main.s.t3",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t3",
            target_table_full_name=None,
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert len(result) == 2
    assert result[0].query_id == "s1"
    assert result[0].source_table_full_names == ["main.s.t1"]
    assert result[1].query_id == "s3"
    assert result[1].source_table_full_names == ["main.s.t3"]
    assert proxy.report.num_queries_missing_info == 1
    warning_titles = [str(w.title) for w in proxy.report.warnings]
    assert warning_titles.count("Failed to parse queries from system tables") == 1


# ---------------------------------------------------------------------------
# Catalog pushdown tests (Snowflake database_pattern parity)
# ---------------------------------------------------------------------------


def test_get_query_history_without_catalog_pushdown_has_no_semi_join() -> None:
    """When catalog_pattern is None, SQL must not include table_lineage semi-join."""
    captured: dict = {}

    def _capture_streaming(query: str, params: tuple) -> Iterator[object]:
        captured["query"] = query
        captured["params"] = params
        yield from []

    proxy = _make_streaming_proxy()
    with patch.object(
        proxy, "_execute_sql_query_streaming", side_effect=_capture_streaming
    ):
        list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert "tl2.statement_id" not in captured["query"]
    assert len(captured["params"]) == 4


def test_get_query_history_with_catalog_pushdown_adds_semi_join() -> None:
    """When catalog_pattern is set, SQL must semi-join table_lineage with RLIKE filter."""
    from datahub.configuration.common import AllowDenyPattern

    captured: dict = {}

    def _capture_streaming(query: str, params: tuple) -> Iterator[object]:
        captured["query"] = query
        captured["params"] = params
        yield from []

    catalog_pattern = AllowDenyPattern(allow=["^main$"], deny=[])

    proxy = _make_streaming_proxy()
    with patch.object(
        proxy, "_execute_sql_query_streaming", side_effect=_capture_streaming
    ):
        list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
                catalog_pattern=catalog_pattern,
            )
        )

    query_sql = captured["query"]
    assert "tl2.statement_id" in query_sql
    assert "system.access.table_lineage tl2" in query_sql
    assert "UPPER(tl2.source_table_catalog) RLIKE %s" in query_sql
    assert "UPPER(tl2.target_table_catalog) RLIKE %s" in query_sql
    assert "^MAIN$" not in query_sql
    assert len(captured["params"]) == 8
    assert captured["params"][6:] == ("^MAIN$", "^MAIN$")


def test_get_query_history_catalog_pushdown_deny_pattern() -> None:
    """Deny patterns must appear as NOT RLIKE in the semi-join filter."""
    from datahub.configuration.common import AllowDenyPattern

    captured: dict = {}

    def _capture_streaming(query: str, params: tuple) -> Iterator[object]:
        captured["query"] = query
        captured["params"] = params
        return
        yield  # pragma: no cover - makes this a generator for closing()

    catalog_pattern = AllowDenyPattern(allow=[".*"], deny=["^system$"])

    proxy = _make_streaming_proxy()
    with patch.object(
        proxy, "_execute_sql_query_streaming", side_effect=_capture_streaming
    ):
        list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
                catalog_pattern=catalog_pattern,
            )
        )

    query_sql = captured["query"]
    assert "NOT RLIKE %s" in query_sql
    assert "^SYSTEM$" not in query_sql
    assert captured["params"][6:] == ("^SYSTEM$", "^SYSTEM$")


def test_fetch_queries_passes_catalog_pattern_when_pushdown_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """usage._fetch_queries must pass catalog_pattern when pushdown flag is on."""

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    from datahub.configuration.common import AllowDenyPattern

    catalog_pattern = AllowDenyPattern(allow=["^prod$"], deny=[])

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    config.push_down_database_pattern_access_history = True
    config.catalog_pattern = catalog_pattern
    config.start_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
    config.end_time = datetime(2026, 6, 2, tzinfo=timezone.utc)

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    list(ex.get_usage_workunits(set()))

    proxy.get_query_history_via_system_tables.assert_called_once_with(
        config.start_time,
        config.end_time,
        catalog_pattern=catalog_pattern,
        include_operational_stats=False,
    )


def test_fetch_queries_omits_catalog_pattern_when_pushdown_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """usage._fetch_queries must not pass catalog_pattern when pushdown flag is off."""

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
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
    config.push_down_database_pattern_access_history = False
    config.start_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
    config.end_time = datetime(2026, 6, 2, tzinfo=timezone.utc)

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    list(ex.get_usage_workunits(set()))

    proxy.get_query_history_via_system_tables.assert_called_once_with(
        config.start_time,
        config.end_time,
        catalog_pattern=None,
        include_operational_stats=False,
    )


def test_get_query_history_mid_statement_error_preserves_partial_lineage() -> None:
    """A bad continuation row must flush accumulated lineage for that statement."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)

    class BrokenLineageRow:
        def __init__(self, **fields: object) -> None:
            self._fail_lineage = bool(fields.pop("_fail_lineage", False))
            self._fields = fields

        def __getattr__(self, name: str) -> object:
            if name in self._fields:
                if name == "source_table_full_name" and self._fail_lineage:
                    raise ValueError("broken lineage field")
                return self._fields[name]
            raise AttributeError(name)

    rows = [
        BrokenLineageRow(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t1 JOIN main.s.t2",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t1",
            target_table_full_name=None,
        ),
        BrokenLineageRow(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t1 JOIN main.s.t2",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            _fail_lineage=True,
            target_table_full_name=None,
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert len(result) == 1
    assert result[0].query_id == "s1"
    assert result[0].source_table_full_names == ["main.s.t1"]
    assert proxy.report.num_queries_missing_info == 0
    assert proxy.report.num_lineage_row_field_read_errors == 1


def test_get_query_history_mid_statement_statement_id_error_skips_row() -> None:
    """A row whose statement_id can't be read is skipped without splitting the
    in-progress statement: it stays one Query (no duplicate emission), carries the
    lineage from readable rows, and the bad row counts as a field-read error."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)

    class BrokenStatementIdRow:
        def __init__(self, **fields: object) -> None:
            self._fail_statement_id = bool(fields.pop("_fail_statement_id", False))
            self._fields = fields

        def __getattr__(self, name: str) -> object:
            if name == "statement_id" and self._fail_statement_id:
                raise ValueError("broken statement_id field")
            if name in self._fields:
                return self._fields[name]
            raise AttributeError(name)

    rows = [
        BrokenStatementIdRow(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t1",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.t1",
            target_table_full_name=None,
        ),
        BrokenStatementIdRow(
            statement_id="s1",
            statement_text="SELECT * FROM main.s.t1",
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            _fail_statement_id=True,
            source_table_full_name="main.s.t2",
            target_table_full_name=None,
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert len(result) == 1
    assert result[0].query_id == "s1"
    assert result[0].source_table_full_names == ["main.s.t1"]
    # The bad row is a field-read error, not a query-parse failure, and the
    # statement is emitted exactly once (not split into duplicates).
    assert proxy.report.num_queries_missing_info == 0
    assert proxy.report.num_lineage_row_field_read_errors == 1


def test_preparsed_query_multi_target_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
        _query_with_lineage(
            "INSERT INTO main.s.t1 SELECT * FROM main.s.src",
            "s1",
            sources=["main.s.src"],
            targets=["main.s.t1", "main.s.t2"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.s.src", "main.s.t1", "main.s.t2"])
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.preparsed) == 2
    assert sum(p.query_count for p in agg.preparsed) == 1
    assert len({p.downstream for p in agg.preparsed}) == 2


def test_preparsed_query_target_only_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
        _query_with_lineage(
            "TRUNCATE TABLE main.s.t1",
            "s1",
            sources=[],
            targets=["main.s.t1"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.s.t1"])
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.preparsed) == 1
    assert len(agg.observed) == 0
    assert agg.preparsed[0].downstream is not None
    assert not agg.preparsed[0].upstreams


def test_preparsed_query_partial_urn_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
        _query_with_lineage(
            "SELECT * FROM main.s.t1 JOIN bad_name",
            "s1",
            sources=["main.s.t1", "not_a_valid_name"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.s.t1"])
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.preparsed) == 1
    assert len(agg.observed) == 0
    assert len(agg.preparsed[0].upstreams) == 1
    assert ex.report.num_lineage_tables_unresolvable == 1
    warning_titles = [str(w.title) for w in ex.report.warnings]
    assert any("Unresolvable lineage table names" in t for t in warning_titles)


def test_build_catalog_column_filter_binds_patterns_with_special_chars() -> None:
    from datahub.configuration.common import AllowDenyPattern
    from datahub.ingestion.source.unity.proxy import _build_catalog_column_filter

    sql, params = _build_catalog_column_filter(
        "tl2.source_table_catalog",
        AllowDenyPattern(allow=["^main'catalog$"], deny=[], ignoreCase=False),
    )
    assert "RLIKE %s" in sql
    assert "^main'catalog$" not in sql
    assert params == ["^main'catalog$"]


def test_get_query_history_catalog_pushdown_binds_pattern_params() -> None:
    """Catalog regex patterns must be bound as parameters, not interpolated into SQL."""
    from datahub.configuration.common import AllowDenyPattern

    captured: dict = {}

    def _capture_streaming(query: str, params: tuple) -> Iterator[object]:
        captured["query"] = query
        captured["params"] = params
        yield from []

    malicious_pattern = "' OR 1=1 --"
    catalog_pattern = AllowDenyPattern(allow=[malicious_pattern], deny=[])

    proxy = _make_streaming_proxy()
    with patch.object(
        proxy, "_execute_sql_query_streaming", side_effect=_capture_streaming
    ):
        list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
                catalog_pattern=catalog_pattern,
            )
        )

    assert malicious_pattern not in captured["query"]
    assert "RLIKE %s" in captured["query"]
    assert malicious_pattern.upper() in captured["params"]


def test_usage_statement_types_select_only_when_ops_disabled() -> None:
    from databricks.sdk.service.sql import QueryStatementType

    from datahub.ingestion.source.unity.proxy_types import usage_statement_types

    read_types = usage_statement_types(include_operational_stats=False)
    all_types = usage_statement_types(include_operational_stats=True)

    assert read_types == frozenset({QueryStatementType.SELECT})
    assert QueryStatementType.INSERT in all_types
    assert QueryStatementType.SELECT in all_types


def test_skip_sqlglot_when_system_table_lineage_missing_skips_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.skip_sqlglot_when_system_table_lineage_missing = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        Query(
            query_id="s-no-lineage",
            query_text="SELECT COUNT(*) FROM fivetran.smoke.base_table",
            statement_type=QueryStatementType.SELECT,
            start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
            source_table_full_names=[],
            target_table_full_names=[],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 0
    assert len(agg.preparsed) == 0
    assert ex.report.num_queries_skipped_without_system_table_lineage == 1
    assert ex.report.num_queries_without_system_table_lineage == 0
    assert ex.report.num_queries_observed_sqlglot == 0
    warning_titles = [str(w.title) for w in ex.report.warnings]
    assert any(
        "Queries skipped without system-table lineage" in t for t in warning_titles
    )


def test_api_fetch_passes_include_operational_stats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    config.include_operational_stats = True
    config.usage_uses_system_tables.return_value = False

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.query_history.return_value = [_query("SELECT 1", "q1")]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    proxy.query_history.assert_called_once()
    _, kwargs = proxy.query_history.call_args
    assert kwargs.get("include_operational_stats") is True


def test_mixed_routing_counters(monkeypatch: pytest.MonkeyPatch) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

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
        _query_with_lineage(
            "SELECT * FROM main.sales.orders",
            "preparsed",
            sources=["main.sales.orders"],
        ),
        Query(
            query_id="no-lineage",
            query_text="SELECT 1",
            statement_type=QueryStatementType.SELECT,
            start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
            source_table_full_names=[],
            target_table_full_names=[],
        ),
        _query_with_lineage(
            "SELECT * FROM unknown.cat.table",
            "urn-fallback",
            sources=["not_a_valid_name"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.sales.orders"])
    list(ex.get_usage_workunits(set()))

    assert ex.report.num_queries == 3
    assert ex.report.num_queries_preparsed_from_lineage == 1
    assert ex.report.num_queries_without_system_table_lineage == 1
    assert ex.report.num_queries_preparsed_fallback_to_sqlglot == 1
    assert ex.report.num_queries_observed_sqlglot == 2
    assert (
        ex.report.num_queries
        == ex.report.num_queries_preparsed_from_lineage
        + ex.report.num_queries_observed_sqlglot
        + ex.report.num_queries_skipped_without_system_table_lineage
    )


def test_groups_target_lineage_rows() -> None:
    """Two joined rows with the same statement_id but different targets group correctly."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        _row(
            statement_id="s1",
            statement_text="INSERT INTO main.s.t1 SELECT * FROM main.s.src",
            statement_type="INSERT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.src",
            target_table_full_name="main.s.t1",
        ),
        _row(
            statement_id="s1",
            statement_text="INSERT INTO main.s.t1 SELECT * FROM main.s.src",
            statement_type="INSERT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name="main.s.src",
            target_table_full_name="main.s.t2",
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
                include_operational_stats=True,
            )
        )

    assert len(result) == 1
    assert sorted(result[0].target_table_full_names) == ["main.s.t1", "main.s.t2"]


def test_build_catalog_column_filter_respects_ignore_case_false() -> None:
    from datahub.configuration.common import AllowDenyPattern
    from datahub.ingestion.source.unity.proxy import _build_catalog_column_filter

    sql, params = _build_catalog_column_filter(
        "tl2.source_table_catalog",
        AllowDenyPattern(allow=["^Main$"], deny=[], ignoreCase=False),
    )
    assert "UPPER" not in sql
    assert "RLIKE %s" in sql
    assert params == ["^Main$"]


def test_get_query_history_catalog_pushdown_ignore_case_true() -> None:
    from datahub.configuration.common import AllowDenyPattern

    captured: dict = {}

    def _capture_streaming(query: str, params: tuple) -> Iterator[object]:
        captured["query"] = query
        captured["params"] = params
        yield from []

    catalog_pattern = AllowDenyPattern(allow=["^main$"], deny=[])

    proxy = _make_streaming_proxy()
    with patch.object(
        proxy, "_execute_sql_query_streaming", side_effect=_capture_streaming
    ):
        list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
                catalog_pattern=catalog_pattern,
            )
        )

    query_sql = captured["query"]
    assert "UPPER(tl2.source_table_catalog) RLIKE %s" in query_sql
    assert "UPPER(tl2.target_table_catalog) RLIKE %s" in query_sql
    assert captured["params"][6:] == ("^MAIN$", "^MAIN$")


def test_aggregate_parse_failure_warning() -> None:
    """All unparseable rows must produce one summary warning, not one per row."""
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        _row(
            statement_id="s1",
            statement_text=None,
            statement_type="SELECT",
            start_time=ts,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name=None,
            target_table_full_name=None,
        ),
        _row(
            statement_id="s2",
            statement_text="SELECT 1",
            statement_type="SELECT",
            start_time=None,
            end_time=ts,
            executed_by="u@x.io",
            executed_as="u@x.io",
            executed_by_user_id=1,
            executed_as_user_id=1,
            source_table_full_name=None,
            target_table_full_name=None,
        ),
    ]

    cursor = _make_mock_cursor([rows])
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
        result = list(
            proxy.get_query_history_via_system_tables(
                datetime(2026, 6, 1, tzinfo=timezone.utc),
                datetime(2026, 6, 2, tzinfo=timezone.utc),
            )
        )

    assert result == []
    assert proxy.report.num_queries_missing_info == 2
    warning_titles = [str(w.title) for w in proxy.report.warnings]
    assert warning_titles.count("Failed to parse queries from system tables") == 1


def test_full_name_to_urn_quoted_identifier() -> None:
    config = MagicMock()
    proxy = MagicMock()
    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    quoted = "main.`schema.with.dots`.orders"

    # A well-formed name that is not in the schema resolver (i.e. not ingested by
    # this recipe) is unresolvable: resolution is signalled by SchemaInfo, which is
    # only present for known tables.
    assert ex._full_name_to_urn(quoted) is None
    assert ex.report.num_lineage_tables_unresolvable == 1

    # Once registered, the quoted identifier still parses to the right URN and
    # resolves without recounting it as unresolvable.
    synthesized = ex.schema_resolver.resolve_table_parts(
        database="main", db_schema="schema.with.dots", table="orders"
    )[0]
    ex.schema_resolver.add_raw_schema_info(synthesized, {"id": "int"})

    urn = ex._full_name_to_urn(quoted)
    assert urn == synthesized
    assert "main.schema.with.dots.orders" in urn
    assert ex.report.num_lineage_tables_unresolvable == 1


def test_preparsed_fingerprint_falls_back_to_statement_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fingerprinting error must not drop a query that already has resolved
    lineage; the query id falls back to a deterministic statement_id-based id."""

    def _boom(*args: object, **kwargs: object) -> str:
        raise ValueError("sqlglot tokenization failed")

    monkeypatch.setattr(usage_mod, "get_query_fingerprint", _boom)

    config = MagicMock()
    proxy = MagicMock()
    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.sales.orders"])

    query = _query_with_lineage(
        "SELECT * FROM main.sales.orders",
        "stmt-123",
        sources=["main.sales.orders"],
    )
    preparsed = ex._to_preparsed_queries(query)

    assert len(preparsed) == 1
    assert preparsed[0].query_id == "unity-stmt-stmt-123"
    assert preparsed[0].upstreams, "lineage must still be emitted despite fp error"
    assert ex.report.num_queries_preparsed_fingerprint_fallback == 1


def _no_op_aggregator(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)


def test_no_queries_hint_mentions_overrestrictive_pushdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """System-tables path + catalog pushdown + zero queries: the 'No queries found'
    hint must point operators at a possibly over-restrictive catalog_pattern."""
    from datahub.configuration.common import AllowDenyPattern

    _no_op_aggregator(monkeypatch)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    config.push_down_database_pattern_access_history = True
    config.catalog_pattern = AllowDenyPattern(allow=["^nonexistent$"])

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    no_query_warnings = [
        w for w in ex.report.warnings if w.title == "No queries found for usage"
    ]
    assert no_query_warnings, "expected 'No queries found for usage' warning"
    context = " ".join(no_query_warnings[0].context)
    assert "over-restrictive" in context, (
        f"expected over-restrictive pushdown hint, got context: {context!r}"
    )


def test_all_unparseable_history_warns_and_suppresses_no_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When every history row fails to parse (num_queries == 0 but missing_info > 0),
    the parse-failure warning fires and the benign 'No queries found' warning does not."""
    _no_op_aggregator(monkeypatch)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    config.push_down_database_pattern_access_history = False
    config.catalog_pattern = None

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    # Mirror production, where proxy and extractor share one report: the proxy
    # records per-row parse failures while yielding no usable queries.
    def _fetch_all_unparseable(*args: object, **kwargs: object) -> Iterator[Query]:
        ex.report.num_queries_missing_info += 2
        return iter([])

    proxy.get_query_history_via_system_tables.side_effect = _fetch_all_unparseable

    list(ex.get_usage_workunits(set()))

    warning_titles = [w.title for w in ex.report.warnings]
    assert "Query history rows could not be parsed" in warning_titles
    assert "No queries found for usage" not in warning_titles


def test_include_column_usage_stats_forces_sqlglot_with_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.include_column_usage_stats = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        _query_with_lineage(
            "SELECT * FROM main.sales.orders",
            "s1",
            sources=["main.sales.orders"],
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    _register_tables(ex, ["main.sales.orders"])
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.preparsed) == 0
    assert len(agg.observed) == 1
    assert ex.report.num_queries_preparsed_from_lineage == 0
    assert ex.report.num_queries_observed_sqlglot == 1


def test_include_column_usage_stats_overrides_skip_sqlglot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg_instance: list = []

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []
            self.preparsed: list = []
            agg_instance.append(self)

        def add_observed_query(self, observed: object, **kw: object) -> None:
            self.observed.append(observed)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            self.preparsed.append(parsed)

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.include_column_usage_stats = True
    config.skip_sqlglot_when_system_table_lineage_missing = True
    config.usage_uses_system_tables.return_value = True
    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = [
        Query(
            query_id="s-no-lineage",
            query_text="SELECT COUNT(*) FROM fivetran.smoke.base_table",
            statement_type=QueryStatementType.SELECT,
            start_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
        ),
    ]

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    list(ex.get_usage_workunits(set()))

    agg = agg_instance[0]
    assert len(agg.observed) == 1
    assert ex.report.num_queries_skipped_without_system_table_lineage == 0
    assert ex.report.num_queries_observed_sqlglot == 1


def test_fetch_queries_omits_catalog_pattern_when_column_usage_stats_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            pass

        def add_observed_query(self, observed: object, **kw: object) -> None:
            pass

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            pass

        def gen_metadata(self):  # type: ignore[return]
            return iter([])

        def close(self) -> None:
            pass

    monkeypatch.setattr(usage_mod, "SqlParsingAggregator", FakeAgg)

    from datahub.configuration.common import AllowDenyPattern

    catalog_pattern = AllowDenyPattern(allow=["^prod$"], deny=[])

    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.include_column_usage_stats = True
    config.usage_uses_system_tables.return_value = True
    config.push_down_database_pattern_access_history = True
    config.catalog_pattern = catalog_pattern
    config.start_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
    config.end_time = datetime(2026, 6, 2, tzinfo=timezone.utc)

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.return_value = []

    ex = _extractor(config, proxy)
    list(ex.get_usage_workunits(set()))

    proxy.get_query_history_via_system_tables.assert_called_once_with(
        config.start_time,
        config.end_time,
        catalog_pattern=None,
        include_operational_stats=False,
    )


def test_queries_drained_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch generator must be fully consumed before the first _add_query_to_aggregator
    call (drain-before-parse invariant).

    Holding the Databricks warehouse cursor open across slow per-query sqlglot parsing
    causes the server-side operation handle to be evicted with a non-retryable
    RESOURCE_DOES_NOT_EXIST error. The fix drains all rows into a FileBackedList first
    (releasing the cursor/connection in seconds), then routes/parses from the buffer.
    Ref: https://github.com/databricks/databricks-sql-python/pull/785

    This test FAILS on the old interleaved code (first route call happens mid-fetch,
    before the generator is exhausted, so fetch_exhausted would be False).
    """
    fetch_exhausted = [False]
    fetch_exhausted_at_first_route: list = []

    def _fake_fetch_queries() -> Iterator[Query]:
        ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
        for i in range(3):
            yield Query(
                query_id=f"q{i}",
                query_text=f"SELECT {i}",
                statement_type=None,
                start_time=ts,
                end_time=ts,
                user_id=1,
                user_name="u@x.io",
                executed_as_user_id=None,
                executed_as_user_name=None,
            )
        # Only set after the last item is yielded (generator fully consumed).
        fetch_exhausted[0] = True

    class FakeAgg:
        def __init__(self, **kw: object) -> None:
            self.observed: list = []

        def add_observed_query(self, obs: object, **kw: object) -> None:
            # Record the drain state at the moment of the very first routing call.
            if not fetch_exhausted_at_first_route:
                fetch_exhausted_at_first_route.append(fetch_exhausted[0])
            self.observed.append(obs)

        def add_preparsed_query(self, parsed: object, **kw: object) -> None:
            if not fetch_exhausted_at_first_route:
                fetch_exhausted_at_first_route.append(fetch_exhausted[0])

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

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()
    monkeypatch.setattr(ex, "_fetch_queries", _fake_fetch_queries)

    list(ex.get_usage_workunits(set()))

    # All three queries must have been routed.
    assert len(fetch_exhausted_at_first_route) == 1, (
        "expected exactly one first-route observation"
    )
    # Key assertion: fetch was fully exhausted BEFORE the first route call.
    assert fetch_exhausted_at_first_route[0] is True, (
        "fetch generator was not fully drained before the first _add_query_to_aggregator call; "
        "the old interleaved code would cause this to be False"
    )
    # All three queries must still have been routed despite the drain-first change.
    assert ex.report.num_queries == 3, (
        f"expected 3 queries counted, got {ex.report.num_queries}"
    )


def _audit_log_config(tmp_path: pathlib.Path) -> MagicMock:
    """A MagicMock config wired for the system-tables usage path with a real
    local_temp_path (so the named/window-keyed audit-log cache is exercised)."""
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 2, tzinfo=timezone.utc)
    config = MagicMock()
    config.include_queries = False
    config.include_query_usage_statistics = False
    config.include_operational_stats = False
    config.usage_uses_system_tables.return_value = True
    config.local_temp_path = tmp_path
    config.start_time = start
    config.end_time = end
    config.bucket_duration = BucketDuration.DAY
    config.majority_buckets.return_value = [start]
    return config


def test_audit_log_persists_and_second_run_reloads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """With local_temp_path set, the audit log persists after a run, and a second run
    over the same window reloads it and skips the fetch (the reload-from-file route)."""
    _no_op_aggregator(monkeypatch)
    config = _audit_log_config(tmp_path)

    fetch_calls: List[int] = []

    def _fetch(*_a: object, **_k: object) -> Iterator[Query]:
        fetch_calls.append(1)
        return iter([_query("SELECT * FROM main.sales.orders", "q1")])

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"
    proxy.get_query_history_via_system_tables.side_effect = _fetch
    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")

    # First run: cache miss -> fetches, and leaves the audit log behind.
    ex1 = _extractor(config, proxy)
    ex1.report = UnityCatalogReport()
    list(ex1.get_usage_workunits({ref}))
    assert len(fetch_calls) == 1, "first run must fetch"
    assert ex1.report.num_queries == 1
    assert len(list(tmp_path.glob("unity_usage_audit_log_*.sqlite"))) == 1, (
        "audit log must persist after a successful run so a re-run can reload it"
    )

    # Second run, same window: cache hit -> reloads from file, no new fetch.
    ex2 = _extractor(config, proxy)
    ex2.report = UnityCatalogReport()
    list(ex2.get_usage_workunits({ref}))
    assert len(fetch_calls) == 1, "second run must reload from file, not re-fetch"
    assert ex2.report.num_queries == 1, "the cached query must be read back"


def test_audit_log_cache_hit_skips_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """When a window-keyed audit log already exists, the run reads it and skips the
    fetch entirely; the cached file is left in place for re-use."""
    _no_op_aggregator(monkeypatch)
    config = _audit_log_config(tmp_path)

    proxy = MagicMock()
    proxy.warehouse_id = "wh1"

    ex = _extractor(config, proxy)
    ex.report = UnityCatalogReport()

    # Pre-populate the exact window-keyed file the extractor will look for.
    audit_file = tmp_path / ex._audit_log_filename()
    conn = ConnectionWrapper(audit_file)
    cached: FileBackedList[Query] = FileBackedList(conn)
    cached.append(_query("SELECT * FROM main.sales.orders", "cached1"))
    cached.close()
    conn.close()
    assert audit_file.exists()

    fetch_calls: List[int] = []

    def _fetch(*_a: object, **_k: object) -> Iterator[Query]:
        fetch_calls.append(1)
        return iter([])

    proxy.get_query_history_via_system_tables.side_effect = _fetch

    ref = TableReference(metastore=None, catalog="main", schema="sales", table="orders")
    list(ex.get_usage_workunits({ref}))

    assert not fetch_calls, "fetch must be skipped on a cache hit"
    assert ex.report.num_queries == 1, "the one cached query must be read from the file"
    # A cached run leaves the file in place (only non-cached runs remove it).
    assert audit_file.exists()
