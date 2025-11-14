import copy
import time
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataHubActionStatusClass
from datahub.utilities import logging_manager
from datahub_actions.pipeline.pipeline import Pipeline
from loguru import logger

# Import the necessary classes from the open source codebase
from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    ReportingAction,
    StageStatus,
)
from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.notifications.constants import DATAHUB_SYSTEM_ACTOR


class ActionStatsReporter:
    def __init__(self, pipeline: Pipeline, graph: DataHubGraph, stage: Stage):
        from datahub.metadata.schema_classes import DataHubActionStatusClass

        assert DataHubActionStatusClass.ASPECT_NAME
        self.pipeline = pipeline
        self.graph = graph
        self.stage = stage

        assert isinstance(self.pipeline.action, ReportingAction)
        self.action: ReportingAction = self.pipeline.action
        self.action_urn = self.action.action_urn

    def get_server_stats(self) -> Optional[DataHubActionStatusClass]:
        return self.graph.get_aspect(self.action_urn, DataHubActionStatusClass)

    def run_action_stats_reporter(self, report_interval_secs: float) -> None:
        time.sleep(report_interval_secs)

        while True:
            current_client_stats = self._build_client_stats()

            if self.stage == Stage.LIVE:
                if self._action_should_stop():
                    logger.info(f"Server state for {self.action_urn} is STOPPED")
                    logger.info("Stopping the pipeline")
                    try:
                        self.pipeline.stop()
                        if current_client_stats and current_client_stats.live:
                            current_client_stats.live.statusCode = (
                                models.DataHubActionStageStatusCodeClass.STOPPED
                            )
                        logger.info("Pipeline stopped successfully")
                    except Exception as e:
                        logger.error(f"Error stopping pipeline: {e}")
                    return

            self._report(current_client_stats)
            time.sleep(report_interval_secs)

    def report(self) -> None:
        # For on-demand reporting (e.g. at process exit).
        current_client_stats = self._build_client_stats()
        self._report(current_client_stats)

    def _action_should_stop(self) -> bool:
        action_info = self.graph.get_aspect(
            self.action_urn, models.DataHubActionInfoClass
        )
        if not action_info:
            logger.info(
                f"No action info found for {self.action_urn}. Stopping the action."
            )
            return True

        if action_info:
            if (
                action_info.state
                and action_info.state == models.DataHubActionStateClass.INACTIVE
            ):
                return True
        return False

    def _build_client_stats(self) -> Optional[DataHubActionStatusClass]:
        # TODO: Return None if there is no new data to report.

        pipeline_stats = self.pipeline.stats()
        action_stats = self.action.get_report()

        if not hasattr(pipeline_stats, "started_at"):
            start_time = int(time.time() * 1000)
        else:
            start_time = pipeline_stats.started_at

        if not action_stats.start_time:
            action_stats.start_time = start_time

        server_stats = self.get_server_stats()

        return self._merge_stage_stats(self.stage, server_stats, action_stats)

    @staticmethod
    def _get_reporting_auditstamp() -> models.AuditStampClass:
        return models.AuditStampClass(
            time=int(time.time() * 1000), actor=DATAHUB_SYSTEM_ACTOR
        )

    @classmethod
    def _action_stats_mapper(
        cls, action_stats: ActionStageReport
    ) -> models.DataHubActionStageStatusClass:
        reporting_auditstamp = cls._get_reporting_auditstamp()

        updated_status_class = models.DataHubActionStageStatusClass(
            startTime=0,
            statusCode=models.DataHubActionStageStatusCodeClass.RUNNING,
            reportedTime=reporting_auditstamp,
        )

        # Include self-logs in the report.
        updated_status_class.report = logging_manager.get_log_buffer().format_lines()

        updated_status_class.startTime = action_stats.start_time
        updated_status_class.endTime = (
            action_stats.end_time if action_stats.end_time else None
        )
        updated_status_class.statusCode = {
            StageStatus.SUCCESS: models.DataHubActionStageStatusCodeClass.SUCCESS,
            StageStatus.FAILURE: models.DataHubActionStageStatusCodeClass.FAILED,
            StageStatus.RUNNING: models.DataHubActionStageStatusCodeClass.RUNNING,
            StageStatus.STOPPED: models.DataHubActionStageStatusCodeClass.RUNNING,
        }[action_stats.status or StageStatus.RUNNING]

        updated_status_class.reportedTime = reporting_auditstamp

        updated_status_class.structuredReport = models.StructuredExecutionReportClass(
            type="ACTION_REPORT",
            serializedValue=action_stats.model_dump_json(),
            contentType=JSON_CONTENT_TYPE,
        )

        updated_status_class.customProperties = {
            k: str(v) for k, v in action_stats.aggregatable_stats().items()
        }

        return updated_status_class

    @classmethod
    def _merge_stage_stats(
        cls,
        stage: Stage,
        existing_stats: Optional[models.DataHubActionStatusClass],
        new_stats: Optional[ActionStageReport],
    ) -> Optional[models.DataHubActionStatusClass]:
        if not new_stats:
            return existing_stats

        action_stats = cls._action_stats_mapper(new_stats)

        if existing_stats:
            updated_stats = copy.deepcopy(existing_stats)
        else:
            updated_stats = DataHubActionStatusClass()

        if stage == Stage.LIVE:
            # TODO: Aggregate the stats in customProperties with the existing stats.
            # There's some extra trickiness, since we need to "checkpoint" the
            # current stats so we only increment the diff every time we report.
            updated_stats.live = action_stats
        else:  # bootstrap/rollback
            setattr(updated_stats, stage.value.lower(), action_stats)

        return updated_stats

    def _report(self, stats: Optional[models.DataHubActionStatusClass]) -> None:
        """
        Report the stats to the graph.
        """

        if stats:
            # logger.debug(f"Reporting stats for {self.action_urn}: {stats}")
            self.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=self.action_urn,
                    aspect=stats,
                )
            )
