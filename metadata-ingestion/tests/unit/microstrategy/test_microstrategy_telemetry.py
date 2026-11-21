import logging
from typing import Any, Iterator, List
from unittest import mock

import pytest
from pytest import MonkeyPatch

from datahub.ingestion.source.microstrategy.client import MicroStrategyClient
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    MicroStrategyLineageExtractor,
    WarehouseLineageContext,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.telemetry import (
    RunProgress,
    SqlglotWarningFilter,
    dashboard_seconds_bucket,
    endpoint_template,
    timed_generator_phase,
    timed_iter,
    timed_phase,
)
from datahub.utilities.file_backed_collections import FileBackedDict

_OBJECT_ID = "975C6B213B4DBCA9E59CAB80ABBA98FE"
_INSTANCE_ID = "5EF5353155407668866F609421112F2D"
_CONTEXT = WarehouseLineageContext(
    platform="snowflake", env="PROD", database="SALES_DB", schema="SALES"
)
_MULTIPASS_SQL = (
    "CREATE VOLATILE TABLE T0SP000 AS\n"
    "select region_number as region, net_sales_amt as revenue "
    "from SALES.fact_sales\n"
    "\n"
    "select region, revenue from T0SP000\n"
)
_FACT_SALES = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _config(**overrides: Any) -> MicroStrategyConfig:
    return MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary", **overrides}
    )


def _extractor(**overrides: Any) -> MicroStrategyLineageExtractor:
    return MicroStrategyLineageExtractor(_config(**overrides), MicroStrategyReport())


def test_endpoint_template_replaces_object_and_instance_ids() -> None:
    assert (
        endpoint_template(
            "get",
            f"/api/v2/reports/{_OBJECT_ID}/instances/{_INSTANCE_ID}/sqlView?x=1",
        )
        == "GET /api/v2/reports/{id}/instances/{id}/sqlView"
    )
    assert (
        endpoint_template("POST", "/api/model/x/12345678-1234-1234-1234-123456789abc")
        == "POST /api/model/x/{id}"
    )
    # Short path segments that merely look hex are left alone.
    assert endpoint_template("GET", "/api/v2/cafe") == "GET /api/v2/cafe"


def test_dashboard_seconds_bucket_edges() -> None:
    assert dashboard_seconds_bucket(0.5) == "0-10s"
    assert dashboard_seconds_bucket(10) == "10-30s"
    assert dashboard_seconds_bucket(299.9) == "120-300s"
    assert dashboard_seconds_bucket(5000) == ">=1800s"


def test_api_calls_are_charged_to_the_innermost_phase() -> None:
    report = MicroStrategyReport()
    with report.phase("outer"):
        report.report_api_call("GET", f"/api/objects/{_OBJECT_ID}", 200, 1.5)
        with report.phase("inner"):
            report.report_api_call("GET", f"/api/objects/{_OBJECT_ID}", 404, 0.5)
    report.report_api_call("GET", "/api/projects", None, 2.0)

    assert report.api_calls == 3
    assert report.api_call_seconds == pytest.approx(4.0)
    assert report.api_calls_by_endpoint["GET /api/objects/{id}"] == 2
    assert report.api_max_seconds_by_endpoint["GET /api/objects/{id}"] == 1.5
    assert report.api_responses_by_endpoint_status == {
        "GET /api/objects/{id} 200": 1,
        "GET /api/objects/{id} 404": 1,
        "GET /api/projects error": 1,
    }
    assert report.api_seconds_by_phase == {
        "outer": 1.5,
        "inner": 0.5,
        "(no phase)": 2.0,
    }
    assert report.phase_calls == {"outer": 1, "inner": 1}


def test_timed_iter_excludes_consumer_time(monkeypatch: MonkeyPatch) -> None:
    clock = _FakeClock()
    monkeypatch.setattr("time.perf_counter", clock)
    report = MicroStrategyReport()

    def produce() -> Iterator[int]:
        clock.now += 2.0  # producing the first item
        yield 1
        clock.now += 3.0  # producing the second item
        yield 2

    for _ in timed_iter(report, "work", produce()):
        clock.now += 100.0  # the pipeline writing the item to the sink

    assert report.phase_seconds["work"] == pytest.approx(5.0)
    assert report.phase_calls["work"] == 1
    assert report._phase_stack == []


def test_timed_iter_closes_the_wrapped_generator_when_abandoned() -> None:
    report = MicroStrategyReport()
    closed: List[bool] = []

    def produce() -> Iterator[int]:
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    wrapped = timed_iter(report, "work", produce())
    assert next(wrapped) == 1
    wrapped.close()  # type: ignore[attr-defined]

    assert closed == [True]
    assert report._phase_stack == []


class _Owner:
    def __init__(self) -> None:
        self.report = MicroStrategyReport()

    @timed_phase("plain")
    def plain(self, value: int) -> int:
        return value * 2

    @timed_generator_phase("gen")
    def gen(self) -> Iterator[int]:
        yield 1
        yield 2


def test_phase_decorators_time_methods_and_generators() -> None:
    owner = _Owner()

    assert owner.plain(21) == 42
    assert list(owner.gen()) == [1, 2]

    assert owner.report.phase_calls == {"plain": 1, "gen": 1}
    assert set(owner.report.phase_seconds) == {"plain", "gen"}


def test_timed_phase_rejects_generators() -> None:
    with pytest.raises(TypeError, match="timed_generator_phase"):

        @timed_phase("oops")
        def generator(self: Any) -> Iterator[int]:
            yield 1


def test_sqlglot_filter_drops_and_counts_unsupported_property_warnings() -> None:
    report = MicroStrategyReport()
    log_filter = SqlglotWarningFilter(report)

    def record(message: str) -> logging.LogRecord:
        return logging.LogRecord("sqlglot", logging.WARNING, "x", 1, message, (), None)

    assert not log_filter.filter(record("Unsupported property volatileproperty"))
    assert not log_filter.filter(record("Unsupported property volatileproperty"))
    assert log_filter.filter(record("Something else"))
    assert report.sqlglot_warnings_suppressed == {
        "Unsupported property volatileproperty": 2
    }


def test_client_records_every_attempt_and_retry_backoff(
    monkeypatch: MonkeyPatch,
) -> None:
    report = MicroStrategyReport()
    client = MicroStrategyClient(_config(), report)

    class _Response:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code
            self.headers: dict = {}
            self.content = b"{}"

        def raise_for_status(self) -> None:
            return None

    responses = [_Response(503), _Response(200)]
    monkeypatch.setattr(client.session, "request", lambda **kwargs: responses.pop(0))

    with mock.patch("time.sleep"):
        client._request("GET", f"/api/objects/{_OBJECT_ID}")

    assert report.api_calls == 2
    assert report.api_responses_by_endpoint_status == {
        "GET /api/objects/{id} 503": 1,
        "GET /api/objects/{id} 200": 1,
    }
    assert report.api_retries == 1
    assert report.api_retry_sleep_seconds == 1.0


def test_identical_sql_is_parsed_once_and_matched_per_dataset() -> None:
    extractor = _extractor()

    first = extractor.warehouse_lineage_from_sql(
        _MULTIPASS_SQL, _CONTEXT, ["Region", "Revenue"], label="rep-1"
    )
    # Another document's dataset over the same report: different field casing
    # and only one of the columns.
    second = extractor.warehouse_lineage_from_sql(
        _MULTIPASS_SQL, _CONTEXT, ["REVENUE"], label="rep-1"
    )

    assert first.upstream_urns == second.upstream_urns == [_FACT_SALES]
    assert first.field_upstreams == {
        "Region": [f"urn:li:schemaField:({_FACT_SALES},region_number)"],
        "Revenue": [f"urn:li:schemaField:({_FACT_SALES},net_sales_amt)"],
    }
    assert second.field_upstreams == {
        "REVENUE": [f"urn:li:schemaField:({_FACT_SALES},net_sales_amt)"]
    }
    report = extractor.report
    assert report.sql_parses_executed == 1
    assert report.sql_parse_cache_hits == 1
    # The per-dataset counter keeps its meaning.
    assert report.sql_views_parsed == 2
    assert list(report.slowest_sql_parses) == [
        f"rep-1 (2 statements, {len(_MULTIPASS_SQL)} chars)"
    ]


def test_column_lineage_request_is_part_of_the_cache_key() -> None:
    extractor = _extractor()

    table_only = extractor.warehouse_lineage_from_sql(_MULTIPASS_SQL, _CONTEXT, [])
    with_fields = extractor.warehouse_lineage_from_sql(
        _MULTIPASS_SQL, _CONTEXT, ["Region"]
    )

    assert table_only.field_upstreams == {}
    assert with_fields.field_upstreams == {
        "Region": [f"urn:li:schemaField:({_FACT_SALES},region_number)"]
    }
    assert extractor.report.sql_parses_executed == 2
    assert extractor.report.sql_parse_cache_hits == 0


def test_failed_parse_is_reported_once_and_served_from_the_cache() -> None:
    extractor = _extractor()

    with mock.patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_from_sql_statements",
        side_effect=RuntimeError("parser exploded"),
    ) as parse:
        for _ in range(3):
            assert (
                extractor.warehouse_lineage_from_sql(
                    "select * from fact_sales", _CONTEXT, []
                ).upstream_urns
                == []
            )

    assert parse.call_count == 1
    report = extractor.report
    assert report.sql_parse_failure_count == 1
    assert len(report.warnings) == 1
    assert report.sql_parse_failure_cache_hits == 2
    assert report.sql_views_parsed == 3


def test_parse_cache_spills_past_its_memory_size_without_losing_entries() -> None:
    extractor = _extractor(cache_memory_entries=2)
    sqls = [f"select * from fact_{index}" for index in range(6)]

    for sql in sqls:
        extractor.warehouse_lineage_from_sql(sql, _CONTEXT, [])
    for sql in sqls:
        extractor.warehouse_lineage_from_sql(sql, _CONTEXT, [])

    # Nothing evicted: every second read is a cache hit, served from disk.
    assert extractor.report.sql_parses_executed == 6
    assert extractor.report.sql_parse_cache_hits == 6


def test_project_reset_drops_the_parse_cache_and_records_its_size() -> None:
    extractor = _extractor()
    extractor.warehouse_lineage_from_sql(_MULTIPASS_SQL, _CONTEXT, [])

    extractor.reset_project_caches()
    extractor.warehouse_lineage_from_sql(_MULTIPASS_SQL, _CONTEXT, [])

    assert extractor.report.sql_parses_executed == 2
    assert extractor.report.cache_entries_peak == {"sql_parse": 1}


def test_project_reset_counts_a_spilled_cache_without_calling_len() -> None:
    # FileBackedDict.__len__ binds one SQL variable per in-memory key, which
    # can exceed SQLite's limit with a large cache_memory_entries; the reset
    # must report the cache size without it.
    extractor = _extractor(cache_memory_entries=2)
    for index in range(5):
        extractor.warehouse_lineage_from_sql(
            f"select * from fact_{index}", _CONTEXT, []
        )

    with mock.patch.object(
        FileBackedDict, "__len__", side_effect=AssertionError("len() called")
    ):
        extractor.reset_project_caches()

    assert extractor.report.cache_entries_peak == {"sql_parse": 5}


def test_one_schema_resolver_is_shared_across_parses() -> None:
    extractor = _extractor()
    graph = mock.MagicMock()
    graph.get_entities.return_value = {}
    resolvers: List[Any] = []

    def fake_parse(**kwargs: Any) -> Any:
        resolvers.append(kwargs["schema_resolver"])
        raise RuntimeError("stop after capturing the resolver")

    with mock.patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_from_sql_statements",
        side_effect=fake_parse,
    ):
        extractor.warehouse_lineage_from_sql(
            "select * from a", _CONTEXT, [], graph=graph
        )
        extractor.warehouse_lineage_from_sql(
            "select * from b", _CONTEXT, [], graph=graph
        )

    assert len(resolvers) == 2
    assert resolvers[0] is resolvers[1]
    extractor.close()


def test_schema_resolution_is_timed_through_the_shared_resolver() -> None:
    extractor = _extractor()
    graph = mock.MagicMock()
    graph.get_entities.return_value = {}

    extractor.warehouse_lineage_from_sql(
        "select region from fact_sales", _CONTEXT, [], graph=graph
    )

    report = extractor.report
    assert report.schema_resolution_calls >= 1
    assert report.schema_resolver.num_schema_cache_misses >= 1
    extractor.close()


def test_run_progress_logs_each_dashboard_and_periodic_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    report = MicroStrategyReport()
    progress = RunProgress(report, every_dashboards=2, every_seconds=3600)

    for index in range(2):
        before = progress.snapshot()
        report.phase_seconds["dashboard"] += 12.0
        report.report_api_call("GET", "/api/projects", 200, 4.0)
        report.datasets_scanned += 3
        with caplog.at_level(logging.INFO):
            progress.dashboard_done("Adhoc_RMA", f"Doc {index}", f"id-{index}", before)

    assert report.dashboards_timed == 2
    assert report.dashboard_seconds_histogram == {"10-30s": 2}
    assert report.datasets_by_slow_dashboard == {
        "Adhoc_RMA/Doc 0 (id-0)": 3,
        "Adhoc_RMA/Doc 1 (id-1)": 3,
    }
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Dashboard 'Doc 1' (id-1) took 12.0s: 1 API calls 4.0s" in m for m in messages
    )
    assert any(m.startswith("MicroStrategy progress: 2 dashboards") for m in messages)


def test_slowest_tables_keep_only_the_top_rows() -> None:
    report = MicroStrategyReport()
    for index in range(40):
        report.report_dashboard_timing(f"doc-{index}", float(index), 1)

    assert len(report.slowest_dashboards) == 25
    assert min(report.slowest_dashboards.values()) == 15.0
    assert set(report.datasets_by_slow_dashboard) == set(report.slowest_dashboards)


def test_report_serializes_telemetry_rounded() -> None:
    report = MicroStrategyReport()
    with report.phase("x"):
        report.report_api_call("GET", "/api/projects", 200, 0.123456789)

    serialized = report.as_obj()

    assert serialized["api_call_seconds"] == 0.123
    assert serialized["api_seconds_by_phase"] == {"x": 0.123}
    assert "_phase_stack" not in serialized


def test_endpoint_template_replaces_dossier_chapter_and_visualization_keys() -> None:
    # Visualization and chapter keys are not 32-hex object ids, and left in
    # place they made one endpoint row per visualization.
    base = f"/api/v2/dossiers/{_OBJECT_ID}/instances/{_INSTANCE_ID}"
    expected = (
        "GET /api/v2/dossiers/{id}/instances/{id}/chapters/{key}/visualizations/{key}"
    )
    assert (
        endpoint_template("GET", f"{base}/chapters/K36/visualizations/K52") == expected
    )
    assert (
        endpoint_template(
            "GET",
            f"{base}/chapters/W305FE5BF92C64F2CA6D26C8BE05DABA2/visualizations/"
            "K83D395FF5640E145258BC2B1E1CA12FC",
        )
        == expected
    )
    assert (
        endpoint_template("GET", f"{base}/chapters")
        == "GET /api/v2/dossiers/{id}/instances/{id}/chapters"
    )
