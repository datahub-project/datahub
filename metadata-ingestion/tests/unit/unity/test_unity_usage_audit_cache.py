"""The query-history audit-log cache (local_temp_path): reuse across runs,
recovery from an unreadable file, and cleanup when fetching fails."""

import pathlib
from datetime import timedelta
from typing import List, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.unit.unity.conftest import WAREHOUSE_ID, unity_config

ORDERS = TableReference(metastore=None, catalog="main", schema="sales", table="orders")


@pytest.fixture
def config(tmp_path: pathlib.Path) -> UnityCatalogSourceConfig:
    return unity_config(
        warehouse_id=WAREHOUSE_ID,
        usage_data_source="API",
        local_temp_path=str(tmp_path),
    )


@pytest.fixture
def proxy() -> MagicMock:
    proxy = MagicMock(spec=UnityCatalogApiProxy)
    # Set in UnityCatalogApiProxy.__init__, so the class spec does not carry it.
    proxy.warehouse_id = WAREHOUSE_ID
    return proxy


def _query(config: UnityCatalogSourceConfig, qid: str) -> Query:
    ts = config.start_time + timedelta(hours=1)
    return Query(
        query_id=qid,
        query_text="SELECT id FROM main.sales.orders",
        statement_type=None,
        start_time=ts,
        end_time=ts,
        user_id=1,
        user_name="analyst@example.com",
        executed_as_user_id=None,
        executed_as_user_name=None,
    )


def _run(
    config: UnityCatalogSourceConfig, proxy: MagicMock
) -> Tuple[UnityCatalogUsageExtractor, List[MetadataWorkUnit]]:
    extractor = UnityCatalogUsageExtractor(
        config=config,
        report=UnityCatalogReport(),
        proxy=proxy,
        table_urn_builder=lambda ref: (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,"
            f"{ref.qualified_table_name},PROD)"
        ),
        user_urn_builder=lambda user: f"urn:li:corpuser:{user}",
        schema_resolver=SchemaResolver(platform="databricks", env="PROD"),
    )
    return extractor, list(extractor.get_usage_workunits({ORDERS}))


def _cache_files(tmp_path: pathlib.Path) -> List[pathlib.Path]:
    return sorted(tmp_path.glob("unity_usage_audit_log_*.sqlite"))


def test_second_run_in_the_same_window_reads_the_cached_audit_log(
    config: UnityCatalogSourceConfig, proxy: MagicMock, tmp_path: pathlib.Path
) -> None:
    """The cache file is keyed by the time window, so a rerun over the same
    window must not call query history again and still produce usage."""
    proxy.query_history.return_value = iter([_query(config, "q1")])

    first, _ = _run(config, proxy)
    assert first.report.num_queries == 1
    assert len(_cache_files(tmp_path)) == 1

    proxy.query_history.reset_mock()
    second, workunits = _run(config, proxy)

    proxy.query_history.assert_not_called()
    assert second.report.num_queries == 1
    assert workunits, "cached queries still produce usage"


def test_unreadable_cached_audit_log_is_discarded_and_history_refetched(
    config: UnityCatalogSourceConfig, proxy: MagicMock, tmp_path: pathlib.Path
) -> None:
    proxy.query_history.return_value = iter([_query(config, "q1")])
    _run(config, proxy)
    (cache_file,) = _cache_files(tmp_path)
    cache_file.write_bytes(b"this is not a sqlite database")

    proxy.query_history.return_value = iter([_query(config, "q2")])
    extractor, _ = _run(config, proxy)

    assert extractor.report.num_queries == 1
    assert "Discarded unreadable cached audit log" in {
        w.title for w in extractor.report.warnings
    }
    # The rebuilt cache is a valid sqlite file again.
    assert cache_file.read_bytes().startswith(b"SQLite format 3")


def test_fetch_error_fails_the_run_and_leaves_no_partial_cache(
    config: UnityCatalogSourceConfig, proxy: MagicMock, tmp_path: pathlib.Path
) -> None:
    """A partially written cache would be reused as complete on the next run,
    so a fetch failure must delete it as well as failing the run."""
    proxy.query_history.side_effect = RuntimeError("query history API unavailable")

    extractor, workunits = _run(config, proxy)

    assert workunits == []
    assert "Usage extraction failed" in {f.title for f in extractor.report.failures}
    assert _cache_files(tmp_path) == []
