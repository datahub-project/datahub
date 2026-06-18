from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Iterable, List
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.sql import QueryStatementType

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import (
    UnityCatalogSourceConfig,
    UsageDataSource,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    Query,
    QueryStatementInfo,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import (
    QueryTableInfo,
    TableMap,
    UnityCatalogUsageExtractor,
)
from datahub.metadata.schema_classes import QueryPropertiesClass, QuerySubjectsClass

_TS = datetime(2026, 6, 1, tzinfo=timezone.utc)
REDACTED = "<Redacted>"


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
    should still be yielded (statement_type=None) and the valid row must also appear."""
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


def _make_query(query_id: str, text: str = "SELECT 1") -> Query:
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    return Query(
        query_id=query_id,
        query_text=text,
        statement_type=QueryStatementType.SELECT,
        start_time=ts,
        end_time=ts,
        user_id=1,
        user_name="user@example.com",
        executed_as_user_id=None,
        executed_as_user_name=None,
    )


def _make_usage_extractor(fake_proxy: MagicMock) -> UnityCatalogUsageExtractor:
    """Build a UnityCatalogUsageExtractor with a fake proxy, bypassing real init."""
    config = UnityCatalogSourceConfig.model_validate(
        {
            "token": "fake",
            "workspace_url": "https://fake.azuredatabricks.net",
            "usage_data_source": UsageDataSource.SYSTEM_TABLES.value,
            "parse_unmatched_queries": True,
            "include_operational_stats": False,
            "warehouse_id": "wh1",
        }
    )

    report = UnityCatalogReport()

    def table_urn_builder(ref: TableReference) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:databricks,{ref.qualified_table_name},PROD)"

    def user_urn_builder(user: str) -> str:
        return f"urn:li:corpuser:{user}"

    extractor = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    extractor.config = config
    extractor.report = report
    extractor.proxy = fake_proxy
    extractor.table_urn_builder = table_urn_builder
    extractor.user_urn_builder = user_urn_builder
    extractor.platform = "databricks"
    extractor.__post_init__()
    return extractor


def test_system_tables_path_aggregates_and_parse_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """s1 is lineage-matched; s2 is history-only and triggers parse-fallback."""
    # Known table reference that lives in "table_refs"
    known_ref = TableReference(
        metastore=None,
        catalog="cat",
        schema="sch",
        table="src",
        last_updated=None,
    )

    # s1: lineage-matched — source table is cat.sch.src
    q1 = _make_query("s1", "SELECT * FROM cat.sch.src")
    info_s1 = QueryStatementInfo(
        query=q1,
        source_tables=["cat.sch.src"],
        target_tables=[],
    )

    # s2: history-only — no lineage rows, will go through parse fallback
    q2 = _make_query("s2", "SELECT * FROM cat.sch.src")

    fake_proxy = MagicMock()
    fake_proxy.warehouse_id = "wh1"

    def _usage_via_system_tables(
        start: datetime, end: datetime
    ) -> Iterable[QueryStatementInfo]:
        yield info_s1

    def _history_via_system_tables(start: datetime, end: datetime) -> Iterable[Query]:
        yield q1
        yield q2

    fake_proxy.get_query_usage_via_system_tables.side_effect = _usage_via_system_tables
    fake_proxy.get_query_history_via_system_tables.side_effect = (
        _history_via_system_tables
    )

    extractor = _make_usage_extractor(fake_proxy)

    # Stub aggregate_event so it doesn't need a real SQLite aggregator
    extractor.usage_aggregator.aggregate_event = MagicMock()  # type: ignore[method-assign]
    # Also stub generate_workunits to return nothing
    extractor.usage_aggregator.generate_workunits = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
    extractor.usage_aggregator.close = MagicMock()  # type: ignore[method-assign]

    # Spy on _parse_query: for s2 return a QueryTableInfo with the known ref
    parse_spy = MagicMock(
        return_value=QueryTableInfo(
            source_tables=[known_ref],
            target_tables=[],
        )
    )
    monkeypatch.setattr(extractor, "_parse_query", parse_spy)

    # Run the extractor (drain the generator to trigger all side effects)
    list(extractor.get_usage_workunits({known_ref}))

    # s1 was resolved via lineage
    assert extractor.report.num_queries_resolved_via_lineage == 1
    # s2 triggered the parse fallback (and parse succeeded)
    assert extractor.report.num_queries_resolved_via_parse_fallback == 1
    # s2 was counted as missing lineage before the parse fallback ran
    assert extractor.report.num_queries_missing_lineage == 1
    # _parse_query called exactly once (for s2 only, not s1)
    parse_spy.assert_called_once()
    called_query = parse_spy.call_args[0][0]
    assert called_query.query_id == "s2"


def test_parse_unmatched_queries_disabled_skips_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When parse_unmatched_queries=False, the history API is never called and the
    parse-fallback counter stays at zero."""
    known_ref = TableReference(
        metastore=None,
        catalog="cat",
        schema="sch",
        table="src",
        last_updated=None,
    )

    q1 = _make_query("s1", "SELECT * FROM cat.sch.src")
    info_s1 = QueryStatementInfo(
        query=q1,
        source_tables=["cat.sch.src"],
        target_tables=[],
    )

    fake_proxy = MagicMock()
    fake_proxy.warehouse_id = "wh1"
    fake_proxy.get_query_usage_via_system_tables.return_value = iter([info_s1])

    config = UnityCatalogSourceConfig.model_validate(
        {
            "token": "fake",
            "workspace_url": "https://fake.azuredatabricks.net",
            "usage_data_source": UsageDataSource.SYSTEM_TABLES.value,
            "parse_unmatched_queries": False,
            "include_operational_stats": False,
            "warehouse_id": "wh1",
        }
    )

    report = UnityCatalogReport()

    def table_urn_builder(ref: TableReference) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:databricks,{ref.qualified_table_name},PROD)"

    def user_urn_builder(user: str) -> str:
        return f"urn:li:corpuser:{user}"

    extractor = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    extractor.config = config
    extractor.report = report
    extractor.proxy = fake_proxy
    extractor.table_urn_builder = table_urn_builder
    extractor.user_urn_builder = user_urn_builder
    extractor.platform = "databricks"
    extractor.__post_init__()

    extractor.usage_aggregator.aggregate_event = MagicMock()  # type: ignore[method-assign]
    extractor.usage_aggregator.generate_workunits = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
    extractor.usage_aggregator.close = MagicMock()  # type: ignore[method-assign]

    list(extractor.get_usage_workunits({known_ref}))

    # The history endpoint must NOT be called when parse_unmatched_queries=False
    fake_proxy.get_query_history_via_system_tables.assert_not_called()
    # No queries went through the parse fallback
    assert extractor.report.num_queries_resolved_via_parse_fallback == 0


# ---------------------------------------------------------------------------
# Task 4: _emit_query_entity tests
# ---------------------------------------------------------------------------


def _make_extractor() -> UnityCatalogUsageExtractor:
    """Build an extractor without a proxy (unit-testing _emit_query_entity directly)."""
    config = UnityCatalogSourceConfig.model_validate(
        {
            "token": "fake",
            "workspace_url": "https://fake.azuredatabricks.net",
            "usage_data_source": UsageDataSource.SYSTEM_TABLES.value,
            "warehouse_id": "wh1",
            "emit_queries": True,
        }
    )
    report = UnityCatalogReport()

    def table_urn_builder(ref: TableReference) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:databricks,{ref.qualified_table_name},PROD)"

    def user_urn_builder(user: str) -> str:
        return f"urn:li:corpuser:{user}"

    extractor = UnityCatalogUsageExtractor.__new__(UnityCatalogUsageExtractor)
    extractor.config = config
    extractor.report = report
    extractor.proxy = MagicMock()
    extractor.table_urn_builder = table_urn_builder
    extractor.user_urn_builder = user_urn_builder
    extractor.platform = "databricks"
    extractor.__post_init__()
    return extractor


def _make_info(
    query_id: str, text: str, source_tables: list, target_tables: list
) -> QueryStatementInfo:
    return QueryStatementInfo(
        query=Query(
            query_id=query_id,
            query_text=text,
            statement_type=None,
            start_time=_TS,
            end_time=_TS,
            user_id=1,
            user_name="u@x.io",
            executed_as_user_id=None,
            executed_as_user_name=None,
        ),
        source_tables=source_tables,
        target_tables=target_tables,
    )


def _get_mcp_aspect_names(wus: List[MetadataWorkUnit]) -> List[str]:
    """Extract aspectNames from MetadataChangeProposalWrapper workunits."""
    return [
        wu.metadata.aspectName
        for wu in wus
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName is not None
    ]


def _get_mcp_by_aspect(
    wus: List[MetadataWorkUnit], aspect_name: str
) -> MetadataChangeProposalWrapper:
    """Return the first MetadataChangeProposalWrapper whose aspectName matches."""
    return next(
        wu.metadata  # type: ignore[misc]
        for wu in wus
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == aspect_name
    )


def test_emit_query_entity_skips_text_when_redacted() -> None:
    """Redacted query text: emits queryProperties with empty statement, increments counter."""
    extractor = _make_extractor()
    info = _make_info(query_id="s9", text=REDACTED, source_tables=[], target_tables=[])
    table_map: TableMap = {}

    wus = list(extractor._emit_query_entity(info, table_map))

    assert "queryProperties" in _get_mcp_aspect_names(wus), (
        "expected a queryProperties aspect"
    )
    mcp = _get_mcp_by_aspect(wus, "queryProperties")
    assert isinstance(mcp.aspect, QueryPropertiesClass)
    assert mcp.aspect.statement.value == "", (
        f"expected empty statement value for redacted query, got {mcp.aspect.statement.value!r}"
    )
    assert extractor.report.num_queries_text_redacted == 1
    assert extractor.report.num_query_entities_emitted == 1


def test_emit_query_entity_skips_when_no_id_and_redacted() -> None:
    """No query_id AND redacted text: must not emit anything and counter stays 0."""
    extractor = _make_extractor()
    info = _make_info(query_id=None, text=REDACTED, source_tables=[], target_tables=[])  # type: ignore[arg-type]
    table_map: TableMap = {}

    wus = list(extractor._emit_query_entity(info, table_map))

    assert wus == [], f"expected no workunits, got {wus}"
    assert extractor.report.num_query_entities_emitted == 0


def test_emit_query_entity_real_text_with_tables() -> None:
    """Real query text + resolved tables: emits queryProperties + querySubjects, increments counter."""
    extractor = _make_extractor()

    known_ref = TableReference(
        metastore=None, catalog="cat", schema="sch", table="src", last_updated=None
    )
    target_ref = TableReference(
        metastore=None, catalog="cat", schema="sch", table="tgt", last_updated=None
    )

    # Build a table_map the same way _get_workunits_internal does
    table_map: TableMap = {}
    for ref in [known_ref, target_ref]:
        table_map[ref.table] = [ref]
        table_map[f"{ref.schema}.{ref.table}"] = [ref]
        table_map[ref.qualified_table_name] = [ref]

    info = _make_info(
        query_id="s10",
        text="SELECT * FROM cat.sch.src",
        source_tables=["cat.sch.src"],
        target_tables=["cat.sch.tgt"],
    )

    wus = list(extractor._emit_query_entity(info, table_map))

    aspect_names = _get_mcp_aspect_names(wus)
    assert "queryProperties" in aspect_names, "expected queryProperties"
    assert "querySubjects" in aspect_names, "expected querySubjects"

    props_mcp = _get_mcp_by_aspect(wus, "queryProperties")
    assert isinstance(props_mcp.aspect, QueryPropertiesClass)
    assert props_mcp.aspect.statement.value == "SELECT * FROM cat.sch.src"

    subjects_mcp = _get_mcp_by_aspect(wus, "querySubjects")
    assert isinstance(subjects_mcp.aspect, QuerySubjectsClass)
    assert len(subjects_mcp.aspect.subjects) == 2, (
        f"expected 2 subjects, got {len(subjects_mcp.aspect.subjects)}"
    )

    assert extractor.report.num_queries_text_redacted == 0
    assert extractor.report.num_query_entities_emitted == 1


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
