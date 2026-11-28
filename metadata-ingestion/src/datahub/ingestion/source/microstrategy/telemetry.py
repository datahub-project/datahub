import functools
import inspect
import logging
import time
from typing import Callable, Iterable, Iterator, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict
from typing_extensions import Concatenate, ParamSpec

from datahub.ingestion.source.microstrategy.constants import (
    MSTR_SQLGLOT_SUPPRESSED_PREFIXES,
    MicroStrategyPhase,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.telemetry_helpers import IntervalGate

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

_PROGRESS_TOP_PHASES = 6


class _HasReport(Protocol):
    @property
    def report(self) -> MicroStrategyReport: ...


S = TypeVar("S", bound=_HasReport)


def timed_phase(
    name: MicroStrategyPhase,
) -> Callable[[Callable[Concatenate[S, P], R]], Callable[Concatenate[S, P], R]]:
    def decorate(
        method: Callable[Concatenate[S, P], R],
    ) -> Callable[Concatenate[S, P], R]:
        if inspect.isgeneratorfunction(method):
            raise TypeError(
                f"{method.__qualname__} is a generator; use timed_generator_phase"
            )

        @functools.wraps(method)
        def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
            with self.report.phase(name):
                return method(self, *args, **kwargs)

        return wrapper

    return decorate


def timed_generator_phase(
    name: MicroStrategyPhase,
) -> Callable[
    [Callable[Concatenate[S, P], Iterable[T]]], Callable[Concatenate[S, P], Iterator[T]]
]:
    def decorate(
        method: Callable[Concatenate[S, P], Iterable[T]],
    ) -> Callable[Concatenate[S, P], Iterator[T]]:
        @functools.wraps(method)
        def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> Iterator[T]:
            return timed_iter(self.report, name, method(self, *args, **kwargs))

        return wrapper

    return decorate


def timed_iter(
    report: MicroStrategyReport, name: MicroStrategyPhase, iterable: Iterable[T]
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
    def __init__(self, report: MicroStrategyReport) -> None:
        super().__init__()
        self._report = report

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for prefix in MSTR_SQLGLOT_SUPPRESSED_PREFIXES:
            if message.startswith(prefix):
                self._report.sqlglot_warnings_suppressed[message] += 1
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
        report: MicroStrategyReport,
        every_dashboards: int,
        every_seconds: float,
    ) -> None:
        self._report = report
        self._gate = IntervalGate(every_dashboards, every_seconds)
        self._started = time.perf_counter()

    def snapshot(self) -> TelemetrySnapshot:
        report = self._report
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
        if self._gate.due(self._report.dashboards_timed):
            self.log_progress()

    def log_progress(self) -> None:
        report = self._report
        processed = report.dashboards_timed
        self._gate.mark(processed)
        report.update_peak_rss()
        elapsed = time.perf_counter() - self._started
        top_phases = ", ".join(
            f"{name} {seconds:.0f}s"
            for name, seconds in sorted(
                report.phase_seconds.items(), key=lambda item: item[1], reverse=True
            )[:_PROGRESS_TOP_PHASES]
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
