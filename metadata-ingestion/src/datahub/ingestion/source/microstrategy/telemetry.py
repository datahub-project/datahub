"""Run telemetry for the MicroStrategy source: where a run's time goes.

A run is a long serial sequence of MicroStrategy REST calls, SQL parses and
DataHub schema lookups, and the ingestion stage report sees all of it as one
"Metadata Extraction" stage. This module splits that time into named phases,
attributes every API call to the phase that made it, and logs per-dashboard
and periodic progress lines so a run in flight can be diagnosed from its log.

Phases are timed only while the source itself runs. A generator phase is
timed one step at a time, so the time the pipeline spends writing a yielded
work unit to the sink is never charged to the phase that yielded it. Phases
nest and their seconds are inclusive: ``sql_parse`` time is also counted in
the ``document_warehouse_lineage`` phase that triggered it.
"""

import functools
import inspect
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    TypeVar,
    cast,
)

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

try:
    import resource
except ImportError:  # pragma: no cover - not available on Windows
    resource = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

# Phase names. Kept here so the report, the progress log and the decorators
# agree on spelling.
PHASE_DASHBOARD = "dashboard"
PHASE_DASHBOARD_SEARCH = "dashboard_search"
PHASE_SQL_PARSE = "sql_parse"
PHASE_WAREHOUSE_FIELD_LOOKUP = "warehouse_field_lookup"

# MicroStrategy object ids (32 hex characters) and instance ids are path
# segments; replacing them gives one row per endpoint instead of one per object.
_OBJECT_ID_SEGMENT_RE = re.compile(r"/[0-9A-Fa-f]{32}(?=/|$)")
_UUID_SEGMENT_RE = re.compile(
    r"/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}(?=/|$)"
)

# Per-dashboard wall-time buckets, upper bounds in seconds.
_DASHBOARD_SECONDS_BUCKETS = (10, 30, 60, 120, 300, 600, 1800)

# sqlglot warns once per generated statement for every dialect property it
# cannot render (MicroStrategy multi-pass SQL creates VOLATILE tables, so a
# single report can log dozens). The warnings change nothing in the lineage
# and push every useful line out of the executor's bounded log tail.
_SQLGLOT_SUPPRESSED_PREFIXES = ("Unsupported property",)


class _HasReport(Protocol):
    report: "MicroStrategyReport"


def endpoint_template(method: str, path: str) -> str:
    """``GET /api/v2/reports/{id}/instances/{id}/sqlView`` for any report."""
    path = path.split("?", 1)[0]
    path = _OBJECT_ID_SEGMENT_RE.sub("/{id}", path)
    path = _UUID_SEGMENT_RE.sub("/{id}", path)
    return f"{method.upper()} {path}"


def peak_rss_mb() -> Optional[float]:
    """Peak resident set size of this process, or None where unavailable."""
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and kilobytes on Linux.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(peak / divisor, 1)


def dashboard_seconds_bucket(seconds: float) -> str:
    lower = 0
    for upper in _DASHBOARD_SECONDS_BUCKETS:
        if seconds < upper:
            return f"{lower}-{upper}s"
        lower = upper
    return f">={lower}s"


def timed_phase(name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Time a method of an object with a ``report`` as phase ``name``.

    Generator methods must use ``timed_generator_phase`` instead: wrapping a
    generator here would time only its creation.
    """

    def decorate(method: Callable[P, R]) -> Callable[P, R]:
        if inspect.isgeneratorfunction(method):
            raise TypeError(
                f"{method.__qualname__} is a generator; use timed_generator_phase"
            )

        @functools.wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            owner = cast(_HasReport, args[0])
            with owner.report.phase(name):
                return method(*args, **kwargs)

        return wrapper

    return decorate


def timed_generator_phase(
    name: str,
) -> Callable[[Callable[P, Iterable[T]]], Callable[P, Iterator[T]]]:
    """Time a generator method of an object with a ``report``, step by step."""

    def decorate(method: Callable[P, Iterable[T]]) -> Callable[P, Iterator[T]]:
        @functools.wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Iterator[T]:
            owner = cast(_HasReport, args[0])
            return timed_iter(owner.report, name, method(*args, **kwargs))

        return wrapper

    return decorate


def timed_iter(
    report: "MicroStrategyReport", name: str, iterable: Iterable[T]
) -> Iterator[T]:
    """Yield from ``iterable``, charging only the time spent producing items."""
    iterator = iter(iterable)
    first = True
    try:
        while True:
            with report.phase(name, count=first):
                first = False
                try:
                    item = next(iterator)
                except StopIteration:
                    return
            yield item
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


class SqlglotWarningFilter(logging.Filter):
    """Drop sqlglot's per-statement "Unsupported property" warnings, counting them."""

    def __init__(self, report: "MicroStrategyReport") -> None:
        super().__init__()
        self._report = report

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for prefix in _SQLGLOT_SUPPRESSED_PREFIXES:
            if message.startswith(prefix):
                self._report.sqlglot_warnings_suppressed[message] += 1
                return False
        return True


@dataclass(frozen=True)
class TelemetrySnapshot:
    """Counters read before a dashboard so its own share can be logged after."""

    seconds: float
    api_calls: int
    api_seconds: float
    sql_parses: int
    sql_parse_seconds: float
    sql_parse_cache_hits: int
    datasets: int


class RunProgress:
    """Per-dashboard timing lines plus a periodic whole-run progress line."""

    def __init__(
        self,
        report: "MicroStrategyReport",
        every_dashboards: int,
        every_seconds: float,
    ) -> None:
        self._report = report
        self._every_dashboards = every_dashboards
        self._every_seconds = every_seconds
        self._started = time.perf_counter()
        self._last_logged = self._started
        self._since_logged = 0

    def snapshot(self) -> TelemetrySnapshot:
        report = self._report
        return TelemetrySnapshot(
            seconds=report.phase_seconds.get(PHASE_DASHBOARD, 0.0),
            api_calls=report.api_calls,
            api_seconds=report.api_call_seconds,
            sql_parses=report.sql_parses_executed,
            sql_parse_seconds=report.sql_parse_seconds,
            sql_parse_cache_hits=report.sql_parse_cache_hits,
            datasets=report.datasets_scanned,
        )

    def dashboard_done(
        self,
        project_name: str,
        dashboard_name: str,
        dashboard_id: str,
        before: TelemetrySnapshot,
    ) -> None:
        after = self.snapshot()
        seconds = after.seconds - before.seconds
        datasets = after.datasets - before.datasets
        self._report.report_dashboard_timing(
            f"{project_name}/{dashboard_name} ({dashboard_id})", seconds, datasets
        )
        logger.info(
            "Dashboard %r (%s) took %.1fs: %d API calls %.1fs, %d SQL parses "
            "%.1fs, %d parse cache hits, %d datasets",
            dashboard_name,
            dashboard_id,
            seconds,
            after.api_calls - before.api_calls,
            after.api_seconds - before.api_seconds,
            after.sql_parses - before.sql_parses,
            after.sql_parse_seconds - before.sql_parse_seconds,
            after.sql_parse_cache_hits - before.sql_parse_cache_hits,
            datasets,
        )
        self._since_logged += 1
        now = time.perf_counter()
        if (
            self._since_logged >= self._every_dashboards
            or now - self._last_logged >= self._every_seconds
        ):
            self.log_progress()

    def log_progress(self) -> None:
        report = self._report
        self._last_logged = time.perf_counter()
        self._since_logged = 0
        report.peak_rss_mb = peak_rss_mb()
        elapsed = self._last_logged - self._started
        processed = report.dashboards_timed
        top_phases = ", ".join(
            f"{name} {seconds:.0f}s"
            for name, seconds in sorted(
                report.phase_seconds.items(), key=lambda item: item[1], reverse=True
            )[:6]
        )
        logger.info(
            "MicroStrategy progress: %d dashboards in %.0fs (%.1fs each, %d "
            "datasets); API %d calls %.0fs (%d retries, %.0fs backoff); SQL "
            "parse %d runs %.0fs, %d cache hits; schema lookups %.0fs; peak "
            "RSS %s MB; phases: %s",
            processed,
            elapsed,
            report.dashboard_seconds_total / processed if processed else 0.0,
            report.datasets_scanned,
            report.api_calls,
            report.api_call_seconds,
            report.api_retries,
            report.api_retry_sleep_seconds,
            report.sql_parses_executed,
            report.sql_parse_seconds,
            report.sql_parse_cache_hits,
            report.schema_resolution_seconds,
            report.peak_rss_mb,
            top_phases,
        )
