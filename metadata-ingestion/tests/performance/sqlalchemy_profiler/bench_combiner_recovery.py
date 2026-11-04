"""Count the statements a profile issues when a combined statement fails.

Usage: python -m tests.performance.sqlalchemy_profiler.bench_combiner_recovery [--latency-ms 20]

Profiles a SQLite table through SQLAlchemyProfiler.generate_profiles and counts
the statements that reach the table, for a few failure shapes. Statement counts
are deterministic. Wall time on a local SQLite file mostly measures Python, so
--latency-ms adds a fixed sleep per statement to stand in for the round trip to
a remote warehouse; it is the median of several runs.
"""

import argparse
import statistics
import tempfile
import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import sqlalchemy as sa

from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)


class Scenario(NamedTuple):
    name: str
    columns: int
    flatten: bool = False
    stddev: bool = False
    deny: Optional[str] = None
    max_queries_to_combine: int = 40


SCENARIOS = [
    Scenario("12 cols, one column denied", 12, deny="c3"),
    Scenario("40 cols, one column denied", 40, deny="c17"),
    Scenario("12 cols, one column denied, flatten", 12, flatten=True, deny="c3"),
    Scenario("40 cols, one column denied, flatten", 40, flatten=True, deny="c17"),
    Scenario("12 cols, stddev_samp missing on SQLite", 12, stddev=True),
    Scenario("20 cols, max_queries_to_combine=100", 20, max_queries_to_combine=100),
    Scenario("12 cols, nothing fails", 12),
]


def _db(path: str, columns: int) -> Tuple[sa.engine.Engine, sa.Connection]:
    engine = sa.create_engine(f"sqlite:///{path}")
    meta = sa.MetaData()
    table = sa.Table(
        "probe", meta, *[sa.Column(f"c{i}", sa.Integer) for i in range(columns)]
    )
    conn = engine.connect()
    meta.create_all(conn)
    conn.execute(
        table.insert(), [{f"c{i}": n % 7 for i in range(columns)} for n in range(200)]
    )
    conn.commit()
    return engine, conn


def run(s: Scenario, latency: float) -> Tuple[int, float, Dict[str, int], List[str]]:
    with tempfile.TemporaryDirectory() as tmp:
        engine, conn = _db(f"{tmp}/probe.db", s.columns)
        issued: List[str] = []

        def on_statement(c: Any, cur: Any, statement: str, *_: Any) -> None:
            text = " ".join(statement.split()).replace('"', "")
            if "probe" not in text or not text.upper().startswith(("SELECT", "WITH")):
                return
            issued.append(text)
            if latency:
                time.sleep(latency)
            # Stands in for a column-level grant: any statement touching the
            # column fails.
            if s.deny and (f"{s.deny})" in text or f"{s.deny} " in text):
                raise sa.exc.OperationalError("denied", {}, Exception("denied"))

        sa.event.listen(engine, "before_cursor_execute", on_statement)
        config = ProfilingConfig(
            enabled=True,
            query_combiner_enabled=True,
            query_combiner_flatten_enabled=s.flatten,
            max_queries_to_combine=s.max_queries_to_combine,
            include_field_sample_values=False,
            include_field_median_value=False,
            include_field_stddev_value=s.stddev,
        )
        profiler = SQLAlchemyProfiler(
            conn=conn,
            report=SQLSourceReport(),
            config=config,
            platform="sqlite",
            env="TEST",
        )
        request = ProfilerRequest(
            pretty_name="main.probe", batch_kwargs={"table": "probe"}
        )
        start = time.perf_counter()
        results = list(
            profiler.generate_profiles([request], max_workers=1, platform="sqlite")
        )
        elapsed = time.perf_counter() - start
        profile = results[0][1]
        assert profile is not None
        fields = sorted(
            repr(sorted(f.to_obj().items())) for f in profile.fieldProfiles or []
        )
        report = profiler.report.query_combiner
        assert report is not None
        counters = {
            "combined": report.combined_queries_issued,
            "alone": report.uncombined_queries_issued,
        }
        conn.close()
        engine.dispose()
        return len(issued), elapsed, counters, fields


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    print(f"{'scenario':42} {'statements':>10} {'median ms':>10}  profile digest")
    for s in SCENARIOS:
        times = []
        for _ in range(args.runs):
            n, elapsed, counters, fields = run(s, args.latency_ms / 1000)
            times.append(elapsed)
        # Same digest before and after a change means the same field profiles.
        digest = sum(len(f) * (i + 1) for i, f in enumerate(fields)) % 10**6
        print(
            f"{s.name:42} {n:>10} {statistics.median(times) * 1000:>10.1f}"
            f"  {digest:06d} {counters}"
        )


if __name__ == "__main__":
    main()
