from datetime import datetime, timezone
from typing import Iterable
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, QueryStatementInfo
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor


def _row(**kw):
    # databricks Row supports __getitem__ by column name; dict is a sufficient stand-in
    return kw


def _make_proxy(rows):
    proxy = UnityCatalogApiProxy.__new__(UnityCatalogApiProxy)
    proxy.warehouse_id = "wh1"
    proxy._execute_sql_query = MagicMock(return_value=rows)  # type: ignore[method-assign]
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
        # statement s2: read via external path
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
    assert s2.external_source_paths == ["s3://b/p"]


def test_empty_result_yields_nothing():
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    proxy = _make_proxy([])
    assert list(proxy.get_query_usage_via_system_tables(ts, ts)) == []


def _make_query(query_id: str, text: str = "SELECT 1") -> Query:
    from databricks.sdk.service.sql import QueryStatementType

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
    from datahub.ingestion.source.unity.config import (
        UnityCatalogSourceConfig,
        UsageDataSource,
    )
    from datahub.ingestion.source.unity.proxy_types import TableReference

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
    from datahub.ingestion.source.unity.proxy_types import TableReference
    from datahub.ingestion.source.unity.usage import QueryTableInfo

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
        external_source_paths=[],
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
    # _parse_query called exactly once (for s2 only, not s1)
    parse_spy.assert_called_once()
    called_query = parse_spy.call_args[0][0]
    assert called_query.query_id == "s2"
