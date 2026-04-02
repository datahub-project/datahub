"""Usage statistics extractor for LookerV2Source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Set

from looker_sdk.error import SDKError

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker_v2 import looker_v2_usage as looker_usage

if TYPE_CHECKING:
    from datahub.ingestion.source.looker.looker_common import LookerUserRegistry
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context

logger = logging.getLogger(__name__)


class LookerUsageExtractor:
    """Emits usage statistics for dashboards and looks.

    Reads from ctx: dashboards_for_usage (populated by DashboardProcessor).
    Must run after DashboardProcessor.
    """

    def __init__(
        self, ctx: "LookerV2Context", user_registry: "LookerUserRegistry"
    ) -> None:
        self._ctx = ctx
        self._user_registry = user_registry

    def _make_dashboard_urn(self, dashboard_id: str) -> str:
        platform_instance: Optional[str] = None
        if self._ctx.config.include_platform_instance_in_urns:
            platform_instance = self._ctx.config.platform_instance
        return builder.make_dashboard_urn(
            name=dashboard_id,
            platform=self._ctx.platform,
            platform_instance=platform_instance,
        )

    def _make_chart_urn(self, element_id: str) -> str:
        platform_instance: Optional[str] = None
        if self._ctx.config.include_platform_instance_in_urns:
            platform_instance = self._ctx.config.platform_instance
        return builder.make_chart_urn(
            name=element_id,
            platform=self._ctx.platform,
            platform_instance=platform_instance,
        )

    def process(self) -> Iterable[MetadataWorkUnit]:
        if not self._ctx.dashboards_for_usage:
            return

        stat_generator_config = looker_usage.StatGeneratorConfig(
            looker_api_wrapper=self._ctx.looker_api,
            looker_user_registry=self._user_registry,
            interval=self._ctx.config.extract_usage_history_for_interval,
            strip_user_ids_from_email=self._ctx.config.strip_user_ids_from_email,
            max_threads=self._ctx.config.max_concurrent_requests,
        )

        # Dashboard usage stats
        try:
            dashboard_stat_generator = looker_usage.create_dashboard_stat_generator(
                config=stat_generator_config,
                report=self._ctx.reporter,
                urn_builder=self._make_dashboard_urn,
                looker_dashboards=self._ctx.dashboards_for_usage,
            )
            emitted = 0
            for mcp in dashboard_stat_generator.generate_usage_stat_mcps():
                yield mcp.as_workunit()
                emitted += 1
            if emitted > 0:
                self._ctx.reporter.dashboards_with_usage += 1
        except (SDKError, ValueError, KeyError) as e:
            self._ctx.reporter.report_warning(
                title="Dashboard Usage Stats Failed",
                message="Error generating dashboard usage stats",
                context=str(e),
            )

        # Chart/Look usage stats
        looks: List[looker_usage.LookerChartForUsage] = []
        for dashboard in self._ctx.dashboards_for_usage:
            if hasattr(dashboard, "looks") and dashboard.looks:
                looks.extend(dashboard.looks)

        # Deduplicate looks
        seen_ids: Set[str] = set()
        unique_looks: List[looker_usage.LookerChartForUsage] = []
        for look in looks:
            if look.id and str(look.id) not in seen_ids:
                seen_ids.add(str(look.id))
                unique_looks.append(look)

        if unique_looks:
            try:
                chart_stat_generator = looker_usage.create_chart_stat_generator(
                    config=stat_generator_config,
                    report=self._ctx.reporter,
                    urn_builder=self._make_chart_urn,
                    looker_looks=unique_looks,
                )
                emitted = 0
                for mcp in chart_stat_generator.generate_usage_stat_mcps():
                    yield mcp.as_workunit()
                    emitted += 1
                if emitted > 0:
                    self._ctx.reporter.charts_with_usage += 1
            except (SDKError, ValueError, KeyError) as e:
                self._ctx.reporter.report_warning(
                    title="Chart Usage Stats Failed",
                    message="Error generating chart usage stats",
                    context=str(e),
                )
