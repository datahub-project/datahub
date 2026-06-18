from datetime import datetime, timezone
from unittest.mock import MagicMock

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy


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
