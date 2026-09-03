import logging
from typing import Callable, Dict, Iterable, List, Protocol, TypeVar

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.montecarlo.assertion import (
    PLATFORM,
    MonteCarloAssertionBuilder,
)
from datahub.ingestion.source.montecarlo.client import (
    MonteCarloAuthError,
    MonteCarloClient,
    MonteCarloMetricPoint,
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.utilities.ratelimiter import DailyCallBudgetExceeded

logger = logging.getLogger(__name__)

# Run-level failures that must abort the whole run, not be demoted to a per-item
# warning or per-phase failure: a distinct exception type is pointless if the
# top-level emit loop swallows it via a broad ``except Exception``.
_FATAL_RUN_ERRORS = (DailyCallBudgetExceeded, MonteCarloAuthError)


class _HasUuid(Protocol):
    uuid: str


_UuidItem = TypeVar("_UuidItem", bound=_HasUuid)


@platform_name("Monte Carlo", id=PLATFORM)
@config_class(MonteCarloSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(
    SourceCapability.PLATFORM_INSTANCE, "Enabled via connection_to_platform_map"
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Monitor/rule descriptions become assertion descriptions",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
class MonteCarloSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Monte Carlo monitors, custom rules and alerts as DataHub assertions.

    Each monitor/rule becomes an ``Assertion`` (CUSTOM) on its monitored dataset, and
    each alert/incident becomes an ``AssertionRunEvent`` failure on that assertion.
    """

    report: MonteCarloSourceReport

    def __init__(self, config: MonteCarloSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = MonteCarloSourceReport()
        self.client = MonteCarloClient(config, report=self.report)
        self.resolver = MconResolver(config, self.client, self.report)
        self.builder = MonteCarloAssertionBuilder(config, self.report, self.resolver)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MonteCarloSource":
        config = MonteCarloSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _emit(
        self,
        kind: str,
        fetch: Callable[[], Iterable[_UuidItem]],
        scan: Callable[[], None],
        build: Callable[[_UuidItem], Iterable[MetadataWorkUnit]],
    ) -> Iterable[MetadataWorkUnit]:
        """Fetch a set of Monte Carlo objects and build workunits from each.

        A failure fetching the set (network error, malformed page) is reported as a
        phase-level failure so the remaining phases still run; a failure building a
        single item is reported as a warning and that item is skipped. Run-level
        failures (``_FATAL_RUN_ERRORS``: exhausted daily budget, rejected
        credentials) propagate to abort the whole run instead.
        """
        try:
            items = fetch()
            for item in items:
                scan()
                try:
                    yield from build(item)
                except _FATAL_RUN_ERRORS:
                    raise
                except Exception as e:
                    self.report.report_build_failure()
                    self.report.warning(
                        title="Failed to build workunits for item",
                        message="Skipping this item due to an unexpected error.",
                        context=f"kind={kind}, uuid={item.uuid}",
                        exc=e,
                    )
        except _FATAL_RUN_ERRORS:
            raise
        except Exception as e:
            self.report.failure(
                title="Failed to fetch items from Monte Carlo",
                message="Could not fetch items; this ingestion phase was skipped.",
                context=f"kind={kind}",
                exc=e,
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.config.include_assertions:
            yield from self._emit(
                "monitor",
                self.client.get_monitors,
                self.report.report_monitor_scanned,
                self.builder.build_assertion,
            )
            yield from self._emit(
                "custom rule",
                self.client.get_custom_rules,
                self.report.report_custom_rule_scanned,
                self.builder.build_assertion,
            )

        # Alerts are emitted after definitions so run events can attach to the
        # assertions ingested above.
        if self.config.include_alerts:
            yield from self._emit(
                "alert",
                self.client.get_alerts,
                self.report.report_alert_scanned,
                self.builder.build_run_event,
            )

        # Partial-failure guard: if any monitor/rule failed to build due to a
        # transient error (exception in getTable, build_assertion, or
        # build_run_event), record a source-level failure so the stale-entity-
        # removal interlock at stale_entity_removal_handler.py:288 trips and
        # skips soft-deletion. Without this, a partial run (e.g. 40/100
        # monitors hit a transient getTable error) clears all three fail-safes
        # — no failures (warnings instead), events_produced > 0, entity delta
        # below fail_safe_threshold — and the handler soft-deletes the 40
        # absent URNs even though those monitors still exist in Monte Carlo.
        # Permanent failures (table genuinely gone, unmapped platform) are
        # excluded: those are legitimate deletions, not transient errors.
        if self.config.include_assertions and self.report.build_failures > 0:
            self.report.failure(
                title="Partial build failures; skipping stale entity removal",
                message=(
                    f"{self.report.build_failures} monitor(s)/rule(s) failed to "
                    "build due to transient errors (network, API, or unexpected "
                    "exceptions). Stale entity removal is skipped to avoid "
                    "soft-deleting assertions that may still exist in Monte Carlo."
                ),
                context=(
                    f"assertions_emitted={self.report.assertions_emitted}, "
                    f"build_failures={self.report.build_failures}, "
                    f"mcons_resolution_failed={self.report.mcons_resolution_failed}"
                ),
            )

        # Run events (measured values) are emitted after definitions and alerts.
        # SUCCESS run events carry the measured metric value on AssertionResult;
        # FAILURE events are left to the alerts path above. Gated by
        # run_events_lookback_days being set (None disables).
        if self.config.run_events_lookback_days:
            yield from self._emit_run_events()

    def _emit_run_events(self) -> Iterable[MetadataWorkUnit]:
        """Fetch monitor run history + measured metrics and emit SUCCESS run
        events. For each ingested monitor: one getJobExecutions call (first=N,
        no pagination), filter for SUCCESS runs, fetch measured values via
        getMetricsV4 (first=1, cached per mcon+metric for the run), and emit
        all SUCCESS runs — the latest carrying the measured value, older ones
        carrying only per-run execution metadata."""
        from datetime import datetime, timedelta, timezone

        # Gated at the call site (run_events_lookback_days is truthy here), but
        # narrow for mypy since the field is Optional[int].
        lookback_days = self.config.run_events_lookback_days
        assert lookback_days is not None
        now = datetime.now(tz=timezone.utc)
        start_time = now - timedelta(days=lookback_days)
        # Cache metric points per (mcon, metric_name) for the duration of this
        # ingestion run, so multiple monitors sharing a table don't refetch.
        metric_cache: Dict[str, List[MonteCarloMetricPoint]] = {}

        for monitor_uuid, ingested in self.builder.iter_ingested_monitors():
            if ingested.mcon is None:
                continue
            metric_names = self.builder.metric_names_for_monitor(ingested.definition)
            try:
                executions = self.client.get_job_executions(
                    monitor_uuid,
                    lookback_days,
                    self.config.run_events_first,
                )
            except _FATAL_RUN_ERRORS:
                raise
            except Exception as e:
                self.report.warning(
                    title="Failed to fetch run events for monitor",
                    message="getJobExecutions failed; skipping this monitor.",
                    context=f"monitor_uuid={monitor_uuid}",
                    exc=e,
                )
                continue

            success_executions = [
                ex for ex in executions if (ex.status or "").upper() == "SUCCESS"
            ]
            if not success_executions:
                continue

            # Fetch measured values (cached per mcon+metric). Attach the latest
            # point(s) to the latest SUCCESS run; older SUCCESS runs get no
            # measured value (jobExecutionUuid is null on metric points, so a
            # per-run join is not possible — best-effort temporal correlation).
            latest_metrics: List[MonteCarloMetricPoint] = []
            for metric_name in metric_names:
                cache_key = f"{ingested.mcon}:{metric_name}"
                if cache_key not in metric_cache:
                    field = self.builder.field_for_metric(
                        ingested.definition, metric_name
                    )
                    metric_cache[cache_key] = self.client.get_metrics_v4(
                        mcon=ingested.mcon,
                        metric_name=metric_name,
                        start_time=start_time,
                        field=field,
                        first=1,
                    )
                    self.report.report_metric_point_fetched(
                        len(metric_cache[cache_key])
                    )
                latest_metrics.extend(metric_cache[cache_key])

            for i, execution in enumerate(success_executions):
                self.report.report_job_execution_scanned()
                # Only the latest SUCCESS run carries the measured value.
                points = latest_metrics if i == 0 else []
                try:
                    yield from self.builder.build_run_events_from_execution(
                        execution, points
                    )
                except _FATAL_RUN_ERRORS:
                    raise
                except Exception as e:
                    self.report.report_build_failure()
                    self.report.warning(
                        title="Failed to build run event",
                        message="Skipping this run event due to an error.",
                        context=f"monitor_uuid={monitor_uuid}, "
                        f"job_execution_uuid={execution.job_execution_uuid}",
                        exc=e,
                    )

    def get_report(self) -> MonteCarloSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = MonteCarloSourceConfig.parse_obj(config_dict)
            client = MonteCarloClient(config)
            # A cheap, paginated call validates auth and connectivity.
            next(iter(client.get_custom_rules()), None)
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report
