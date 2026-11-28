import functools
import inspect
import logging
import re
import sys
import threading
import time
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

from pydantic import BaseModel, ConfigDict
from typing_extensions import ParamSpec

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS,
    MSTR_ENDPOINT_ID_PLACEHOLDER,
    MSTR_ENDPOINT_KEY_REPLACEMENT,
    MSTR_SQLGLOT_SUPPRESSED_PREFIXES,
    MicroStrategyPhase,
)

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

# 32-hex object ids and UUIDs, so one endpoint is one row.
_OBJECT_ID_SEGMENT_RE = re.compile(r"/[0-9A-Fa-f]{32}(?=/|$)")
_UUID_SEGMENT_RE = re.compile(
    r"/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}(?=/|$)"
)
# Not 32-hex object ids, so a literal match would make one row per visualization.
_DOSSIER_KEY_SEGMENT_RE = re.compile(r"/(chapters|pages|visualizations)/[^/]+")


class _HasReport(Protocol):
    report: "MicroStrategyReport"


def endpoint_template(method: str, path: str) -> str:
    path = path.split("?", 1)[0]
    path = _OBJECT_ID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = _UUID_SEGMENT_RE.sub(MSTR_ENDPOINT_ID_PLACEHOLDER, path)
    path = _DOSSIER_KEY_SEGMENT_RE.sub(MSTR_ENDPOINT_KEY_REPLACEMENT, path)
    return f"{method.upper()} {path}"


def peak_rss_mb() -> Optional[float]:
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and kilobytes on Linux.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(peak / divisor, 1)


def dashboard_seconds_bucket(seconds: float) -> str:
    lower = 0
    for upper in MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS:
        if seconds < upper:
            return f"{lower}-{upper}s"
        lower = upper
    return f">={lower}s"


def timed_phase(name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
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
    # Step by step, so sink writes between yields are not charged to the phase.
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
    def __init__(self, report: "MicroStrategyReport") -> None:
        super().__init__()
        self._report = report

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for prefix in MSTR_SQLGLOT_SUPPRESSED_PREFIXES:
            if message.startswith(prefix):
                self._report.report_sqlglot_warning_suppressed(message)
                return False
        return True


class TelemetrySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    seconds: float
    api_calls: int
    api_seconds: float
    sql_parses: int
    sql_parse_seconds: float
    sql_parse_cache_hits: int
    datasets: int


class RunProgress:
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
        self._lock = threading.RLock()

    def snapshot(self) -> TelemetrySnapshot:
        report = self._report
        with report._telemetry_lock:
            return TelemetrySnapshot(
                seconds=report.phase_seconds.get(MicroStrategyPhase.DASHBOARD, 0.0),
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
        *,
        override_seconds: Optional[float] = None,
        override_api_calls: Optional[int] = None,
        override_api_seconds: Optional[float] = None,
    ) -> None:
        with self._lock:
            after = self.snapshot()
            seconds = (
                override_seconds
                if override_seconds is not None
                else after.seconds - before.seconds
            )
            api_calls = (
                override_api_calls
                if override_api_calls is not None
                else after.api_calls - before.api_calls
            )
            api_seconds = (
                override_api_seconds
                if override_api_seconds is not None
                else after.api_seconds - before.api_seconds
            )
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
                api_calls,
                api_seconds,
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
        with self._lock:
            self._last_logged = time.perf_counter()
            self._since_logged = 0
            rss = peak_rss_mb()
            with report._telemetry_lock:
                report.peak_rss_mb = rss
                elapsed = self._last_logged - self._started
                processed = report.dashboards_timed
                dashboard_seconds_total = report.dashboard_seconds_total
                datasets_scanned = report.datasets_scanned
                api_calls = report.api_calls
                api_call_seconds = report.api_call_seconds
                api_retries = report.api_retries
                api_retry_sleep_seconds = report.api_retry_sleep_seconds
                sql_parses_executed = report.sql_parses_executed
                sql_parse_seconds = report.sql_parse_seconds
                sql_parse_cache_hits = report.sql_parse_cache_hits
                schema_resolution_seconds = report.schema_resolution_seconds
                phase_seconds = dict(report.phase_seconds)
            top_phases = ", ".join(
                f"{name} {phase_seconds[name]:.0f}s"
                for name in sorted(
                    phase_seconds, key=lambda key: phase_seconds[key], reverse=True
                )[:6]
            )
            logger.info(
                "MicroStrategy progress: %d dashboards in %.0fs (%.1fs each, %d "
                "datasets); API %d calls %.0fs (%d retries, %.0fs backoff); SQL "
                "parse %d runs %.0fs, %d cache hits; schema lookups %.0fs; peak "
                "RSS %s MB; phases: %s",
                processed,
                elapsed,
                dashboard_seconds_total / processed if processed else 0.0,
                datasets_scanned,
                api_calls,
                api_call_seconds,
                api_retries,
                api_retry_sleep_seconds,
                sql_parses_executed,
                sql_parse_seconds,
                sql_parse_cache_hits,
                schema_resolution_seconds,
                rss,
                top_phases,
            )
