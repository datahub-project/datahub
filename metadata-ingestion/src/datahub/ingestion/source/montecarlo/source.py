import logging
from typing import Iterable, List, Optional

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
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.montecarlo.assertion import (
    PLATFORM,
    MonteCarloAssertionBuilder,
)
from datahub.ingestion.source.montecarlo.client import MonteCarloClient
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@platform_name("Monte Carlo", id=PLATFORM)
@config_class(MonteCarloSourceConfig)
@support_status(SupportStatus.TESTING)
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
        self.client = MonteCarloClient(config)
        self.resolver = MconResolver(config, self.client, self.report)
        self.builder = MonteCarloAssertionBuilder(config, self.report, self.resolver)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MonteCarloSource":
        config = MonteCarloSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.config.emit_assertions:
            for monitor in self.client.get_monitors():
                self.report.report_monitor_scanned()
                try:
                    yield from self.builder.build_assertion(monitor)
                except Exception as e:
                    self.report.warning(
                        title="Failed to build assertion for monitor",
                        message="Skipping monitor due to an unexpected error.",
                        context=f"monitor_uuid={monitor.uuid}",
                        exc=e,
                    )
            for rule in self.client.get_custom_rules():
                self.report.report_custom_rule_scanned()
                try:
                    yield from self.builder.build_assertion(rule)
                except Exception as e:
                    self.report.warning(
                        title="Failed to build assertion for custom rule",
                        message="Skipping custom rule due to an unexpected error.",
                        context=f"rule_uuid={rule.uuid}",
                        exc=e,
                    )

        # Alerts are emitted after definitions so run events can attach to the
        # assertions ingested above.
        if self.config.alerts.enabled:
            for alert in self.client.get_alerts():
                self.report.report_alert_scanned()
                try:
                    yield from self.builder.build_run_event(alert)
                except Exception as e:
                    self.report.warning(
                        title="Failed to build run event for alert",
                        message="Skipping alert due to an unexpected error.",
                        context=f"alert_uuid={alert.uuid}",
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
