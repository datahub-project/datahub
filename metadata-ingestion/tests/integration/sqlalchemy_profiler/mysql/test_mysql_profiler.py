"""Integration tests for the SQLAlchemy profiler with MySQL.

The decisive test runs the same table with query_combiner_flatten_enabled off
and on and asserts identical profile output (flattening changes SQL text, not
results), AND asserts the flat path actually executed via the combiner report
counters. Mirrors the Postgres sqlalchemy_profiler suite. No default flip: the
flag stays False; the flip is a separate follow-up PR.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLSource
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)
from tests.test_helpers.docker_helpers import is_mysql_up, wait_for_port

FROZEN_TIME = "2024-01-01 12:00:00"
MYSQL_PORT = 53310


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/sqlalchemy_profiler/mysql"


@pytest.fixture(scope="module")
def mysql_runner(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mysql"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testmysql_profiler",
            3306,
            timeout=120,
            checker=lambda: is_mysql_up("testmysql_profiler", 3306),
        )
        yield docker_services


def _make_source(flatten_enabled: bool) -> MySQLSource:
    from datahub.ingestion.source.sql.mysql import MySQLConfig

    config_dict = {
        "username": "root",
        "password": "example",
        "host_port": f"localhost:{MYSQL_PORT}",
        "database": "testdb",
        "profiling": {
            "enabled": True,
            "method": "sqlalchemy",
            "include_field_null_count": True,
            "include_field_distinct_count": True,
            "include_field_min_value": True,
            "include_field_max_value": True,
            "include_field_mean_value": True,
            "include_field_median_value": True,
            "include_field_stddev_value": True,
            "include_field_quantiles": True,
            "include_field_histogram": True,
            "include_field_distinct_value_frequencies": True,
            "include_field_sample_values": True,
            "query_combiner_flatten_enabled": flatten_enabled,
        },
    }
    config = MySQLConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-mysql-profiler")
    return MySQLSource(config, ctx)


@pytest.fixture
def mysql_source(request):
    # Yield-teardown fixture (deliberate deviation from the Postgres suite, whose
    # postgres_source fixture returns the source with no close() -- leaking the
    # engine). Closing here makes a failing assert exception-safe. This does NOT
    # fully solve the leak: get_profile_for_table calls list(source.get_inspectors()),
    # and get_inspectors creates a fresh engine per database that source.close() does
    # not dispose. That is shared source behavior (the Postgres suite has the same
    # shape); don't try to fix it here.
    #
    # request.param (set via indirect parametrize) is the flatten_enabled bool.
    source = _make_source(request.param)
    yield source
    source.close()


@dataclass
class ProfilerRequest:
    """Simple request class for the profiler (mirrors the Postgres suite)."""

    pretty_name: str
    batch_kwargs: dict


def get_profile_for_table(
    source: MySQLSource,
    schema: str,
    table: str,
    max_workers: int = 1,
) -> Optional[DatasetProfileClass]:
    """Profile a single table (max_workers clamps to 1 for a 1-element list)."""
    inspectors = list(source.get_inspectors())
    if not inspectors:
        return None
    inspector = inspectors[0]
    profiler = source.get_profiler_instance(inspector)
    request = ProfilerRequest(
        pretty_name=f"{schema}.{table}",
        batch_kwargs={"schema": schema, "table": table},
    )
    profiles = list(profiler.generate_profiles([request], max_workers=max_workers))  # type: ignore[arg-type,list-item]
    if profiles:
        return profiles[0][1]
    return None


def get_profiles_for_tables(
    source: MySQLSource,
    schema: str,
    tables: list,
    max_workers: int,
) -> Dict[str, Optional[DatasetProfileClass]]:
    """Fan-out: one ProfilerRequest per table in a single generate_profiles() call.

    sqlalchemy_profiler clamps max_workers to min(max_workers, len(requests)), so
    only a multi-table call exercises real concurrency -- this is the first
    integration coverage of the combiner's per-thread/per-greenlet bookkeeping.
    Returns a dict keyed by table name.
    """
    inspectors = list(source.get_inspectors())
    assert inspectors
    inspector = inspectors[0]
    profiler = source.get_profiler_instance(inspector)
    requests = [
        ProfilerRequest(
            pretty_name=f"{schema}.{t}",
            batch_kwargs={"schema": schema, "table": t},
        )
        for t in tables
    ]
    profiles = list(profiler.generate_profiles(requests, max_workers=max_workers))  # type: ignore[arg-type,list-item]
    return {req.pretty_name.split(".", 1)[1]: profile for req, profile in profiles}


def _field_profile(
    profile: DatasetProfileClass, field_path: str
) -> Optional[DatasetFieldProfileClass]:
    assert profile.fieldProfiles is not None
    return next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == field_path), None
    )


def _profile_deterministic_dict(profile: DatasetProfileClass) -> Dict[str, Any]:
    """Project a DatasetProfileClass onto a deterministic, comparable dict.

    Built on to_obj() so new fields added to DatasetProfile are covered by
    default rather than silently dropped by an allowlist. timestampMillis is the
    only intentionally-dropped field (set from wall-clock time at aspect
    construction). sampleValues order is not guaranteed across execution paths,
    so sort it. partitionSpec is deterministic (FULL_TABLE_SNAPSHOT).
    """
    obj: Dict[str, Any] = dict(profile.to_obj())
    obj.pop("timestampMillis", None)
    for fp in obj.get("fieldProfiles") or []:
        if fp.get("sampleValues") is not None:
            fp["sampleValues"] = sorted(fp["sampleValues"])
    return obj


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize("mysql_source", [False, True], indirect=True)
def test_basic_statistics_exact_values(mysql_runner, mysql_source):
    """Sanity: correct values under both flag states (the 'stays green' guard)."""
    profile = get_profile_for_table(mysql_source, "testdb", "test_exact_numeric")
    assert profile is not None
    assert profile.rowCount == 7
    value_col = _field_profile(profile, "value_col")
    assert value_col is not None
    assert value_col.nullCount == 2
    assert value_col.min == "1"
    assert value_col.max == "5"
    assert value_col.mean is not None
    assert float(value_col.mean) == pytest.approx(3.0, rel=1e-6)
    # MySQL's bare STDDEV() is population stddev, but adapter.get_column_stdev
    # emits stddev_samp, so ~1.5811 holds.
    assert value_col.stdev is not None
    assert float(value_col.stdev) == pytest.approx(1.5811, rel=1e-3)
    assert value_col.uniqueCount == 5


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize("mysql_source", [False, True], indirect=True)
def test_edge_case_empty_table(mysql_runner, mysql_source):
    profile = get_profile_for_table(mysql_source, "testdb", "test_empty")
    assert profile is not None
    assert profile.rowCount == 0


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize("mysql_source", [False, True], indirect=True)
def test_edge_case_single_row(mysql_runner, mysql_source):
    profile = get_profile_for_table(mysql_source, "testdb", "test_single_row")
    assert profile is not None
    assert profile.rowCount == 1
    value_col = _field_profile(profile, "value_col")
    assert value_col is not None
    assert value_col.min == "42"
    assert value_col.max == "42"


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize("mysql_source", [False, True], indirect=True)
def test_edge_case_all_nulls(mysql_runner, mysql_source):
    profile = get_profile_for_table(mysql_source, "testdb", "test_all_nulls")
    assert profile is not None
    assert profile.rowCount == 3
    value_col = _field_profile(profile, "value_col")
    assert value_col is not None
    assert value_col.nullCount == 3
    assert value_col.min is None
    assert value_col.max is None


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize(
    "table,numeric_col",
    [("test_distinct_heavy", "col_a"), ("test_mixed_types", "int_col")],
)
def test_flatten_off_vs_on_identical_profile(mysql_runner, table, numeric_col):
    """Decisive: identical profile output with the flatten flag off vs on.

    test_distinct_heavy has 8 columns (id + col_a..col_g), so 8 COUNT(DISTINCT)
    aggregates; at cap=5 that splits into 2 distinct-chunk flat statements.
    test_mixed_types covers FLOAT/DECIMAL/DATE/TEXT/DATETIME/BOOL -- the types
    where a flat SELECT's coercion could plausibly differ from the CTE path.
    """
    off_source = _make_source(False)
    on_source = _make_source(True)
    try:
        off = get_profile_for_table(off_source, "testdb", table)
        on = get_profile_for_table(on_source, "testdb", table)
        assert off is not None and on is not None

        # T2: full to_obj() comparison, normalized for nondeterminism.
        assert _profile_deterministic_dict(off) == _profile_deterministic_dict(on)

        # T3: "identical AND non-trivial" -- the fields MySQL actually populates
        # must be populated, so the equality above is not just None == None.
        # median IS populated on MySQL: adapters/mysql.py:47 returns None from
        # get_median_expr, but base_adapter.get_column_median falls back to a
        # Python OFFSET/LIMIT query (base_adapter.py:498-511).
        numeric = _field_profile(on, numeric_col)
        assert numeric is not None
        assert numeric.uniqueCount is not None
        assert numeric.min is not None and numeric.max is not None
        assert numeric.mean is not None and numeric.stdev is not None
        assert numeric.median is not None

        # T3: document the real capability gap. quantiles are unsupported
        # (base_adapter.get_quantiles_expr defaults to None; mysql.py does not
        # override; no fallback). Asserting quantiles IS None makes the test
        # trip if native quantile support is ever added.
        assert numeric.quantiles is None

        # distinctValueFrequencies is None for a different reason than "high
        # cardinality": convert_to_cardinality (profiling/common.py:40-41) tests
        # pct_unique == 1.0 -> Cardinality.UNIQUE BEFORE the cardinality buckets
        # that gate frequency computation. Both fixture columns have every
        # non-null value distinct (col_a: 10 distinct / 10 non-null; int_col:
        # 3 distinct / 3 non-null), so they resolve to Cardinality.UNIQUE,
        # which is outside the allowed set {ONE, TWO, VERY_FEW, FEW} for which
        # frequencies are computed.
        # NOTE: this depends on every non-null value in the column being
        # distinct. Add a duplicate to either fixture column and pct_unique
        # drops below 1.0; with unique_count < 20 the column lands in VERY_FEW,
        # frequencies ARE computed, and this assertion fails with no obvious
        # link to the setup.sql edit.
        assert numeric.distinctValueFrequencies is None

        # B2: assert the flat path actually executed. Without this the equality
        # holds trivially if the flag failed to plumb through or _is_flattenable
        # rejected every query. Counters are wired end-to-end: SQLSourceReport.
        # query_combiner is the same report object the SQLAlchemyProfiler uses.
        off_report = off_source.report.query_combiner
        on_report = on_source.report.query_combiner
        assert off_report is not None, "combiner report missing -- plumbing regression"
        assert on_report is not None, "combiner report missing -- plumbing regression"
        assert off_report.flat_queries_issued == 0
        assert on_report.flat_queries_issued >= 2
        assert on_report.scans_avoided > 0
    finally:
        off_source.close()
        on_source.close()


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize("mysql_source", [True], indirect=True)
def test_concurrent_profiles_complete(mysql_runner, mysql_source):
    """Concurrency correctness: profile all six tables in one generate_profiles()
    call with max_workers=6 so the combiner's per-thread/per-greenlet bookkeeping
    gets real integration coverage (every existing profiler suite runs
    single-threaded). flatten_enabled=True so the flat path runs under
    concurrency. Asserts every profile comes back (no None) with correct rowCount.
    """
    tables = [
        "test_exact_numeric",
        "test_mixed_types",
        "test_empty",
        "test_single_row",
        "test_all_nulls",
        "test_distinct_heavy",
    ]
    expected = {
        "test_exact_numeric": 7,
        "test_mixed_types": 4,
        "test_empty": 0,
        "test_single_row": 1,
        "test_all_nulls": 3,
        "test_distinct_heavy": 10,
    }
    profiles = get_profiles_for_tables(mysql_source, "testdb", tables, max_workers=6)
    for table in tables:
        profile = profiles[table]
        assert profile is not None, f"missing profile for {table}"
        assert profile.rowCount == expected[table], table
