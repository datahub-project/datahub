import logging
from typing import Iterable, Optional

from pydantic import Field

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)

COMPACT_PATH = "/openapi/operations/analytics/compact"


class DataHubAnalyticsCompactionConfig(ConfigModel):
    max_hours_to_seal: Optional[int] = Field(
        default=None,
        description="Override server default max hours to seal per call",
    )
    max_days_to_compact: Optional[int] = Field(
        default=None,
        description="Override server default max days to compact per call",
    )
    max_months_to_compact: Optional[int] = Field(
        default=None,
        description="Override server default max months to compact per call",
    )
    max_wall_clock_millis: Optional[int] = Field(
        default=None,
        description="Override server default wall-clock budget in milliseconds",
    )


class DataHubAnalyticsCompactionReport(SourceReport):
    skipped_unavailable: bool = False
    lock_not_acquired: bool = False
    more_work_remaining: bool = False
    hours_sealed: int = 0
    days_compacted: int = 0
    months_compacted: int = 0
    implementation: Optional[str] = None


@platform_name("DataHubAnalyticsCompaction", id="datahub-analytics-compaction")
@config_class(DataHubAnalyticsCompactionConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubAnalyticsCompactionSource(Source):
    """Thin SYSTEM source that triggers GMS analytics compaction once per run."""

    def __init__(self, ctx: PipelineContext, config: DataHubAnalyticsCompactionConfig):
        super().__init__(ctx)
        self.config = config
        self.report = DataHubAnalyticsCompactionReport()
        # Side-effect source: posts to GMS compact API and emits no metadata workunits.
        self.report.event_not_produced_warn = False

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DataHubAnalyticsCompactionSource":
        config = DataHubAnalyticsCompactionConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        graph = self.ctx.graph
        if graph is None:
            raise OperationalError(
                "DataHub graph client is required for analytics compaction"
            )

        payload: dict = {}
        if self.config.max_hours_to_seal is not None:
            payload["maxHoursToSeal"] = self.config.max_hours_to_seal
        if self.config.max_days_to_compact is not None:
            payload["maxDaysToCompact"] = self.config.max_days_to_compact
        if self.config.max_months_to_compact is not None:
            payload["maxMonthsToCompact"] = self.config.max_months_to_compact
        if self.config.max_wall_clock_millis is not None:
            payload["maxWallClockMillis"] = self.config.max_wall_clock_millis

        url = f"{graph._gms_server}{COMPACT_PATH}"
        try:
            response = graph._session.post(url, json=payload or {})
        except Exception as exc:
            raise OperationalError(f"Analytics compact request failed: {exc}") from exc

        if response.status_code == 503:
            logger.info(
                "Analytics compaction backend unavailable; soft-skipping this run"
            )
            self.report.skipped_unavailable = True
            return []

        if response.status_code >= 400:
            raise OperationalError(
                f"Analytics compact failed status={response.status_code} body={response.text}"
            )

        body = response.json()
        self.report.lock_not_acquired = bool(body.get("lockNotAcquired"))
        self.report.more_work_remaining = bool(body.get("moreWorkRemaining"))
        self.report.hours_sealed = int(body.get("hoursSealed") or 0)
        self.report.days_compacted = int(body.get("daysCompacted") or 0)
        self.report.months_compacted = int(body.get("monthsCompacted") or 0)
        impl = body.get("implementation")
        self.report.implementation = str(impl) if impl is not None else None
        logger.info(
            "Analytics compact result lockNotAcquired=%s moreWork=%s hours=%s days=%s months=%s impl=%s",
            self.report.lock_not_acquired,
            self.report.more_work_remaining,
            self.report.hours_sealed,
            self.report.days_compacted,
            self.report.months_compacted,
            self.report.implementation,
        )
        return []

    def get_report(self) -> SourceReport:
        return self.report
