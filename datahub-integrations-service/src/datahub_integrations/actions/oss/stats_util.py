import abc
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional

import pydantic
from datahub.ingestion.api.report import Report
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel


class EventProcessingStats(BaseModel):
    """
    A class to represent the event-oriented processing stats for a pipeline.
    Note: Might be merged into ActionStats in the future.
    """

    num_events_processed: int = pydantic.Field(
        default=0,
        description="The number of events we have processed. Does not include no-ops.",
    )
    last_seen_event_time: Optional[str] = pydantic.Field(
        default=None, description="The event time of the last event we processed"
    )
    last_event_processed_time: Optional[str] = pydantic.Field(
        default=None, description="The time at which we processed the last event"
    )
    last_seen_event_time_success: Optional[str] = pydantic.Field(
        default=None,
        description="The event time of the last event we processed successfully",
    )
    last_event_processed_time_success: Optional[str] = pydantic.Field(
        default=None,
        description="The time at which we processed the last event successfully",
    )
    last_seen_event_time_failure: Optional[str] = pydantic.Field(
        default=None,
        description="The event time of the last event we processed unsuccessfully",
    )
    last_event_processed_time_failure: Optional[str] = pydantic.Field(
        default=None,
        description="The time at which we processed the last event unsuccessfully",
    )

    @classmethod
    def _get_event_time(cls, event: EventEnvelope) -> Optional[str]:
        """
        Get the event time from the event.
        """
        if event.event_type == ENTITY_CHANGE_EVENT_V1_TYPE:
            if isinstance(event.event, EntityChangeEvent):
                return (
                    datetime.fromtimestamp(
                        event.event.auditStamp.time / 1000.0, tz=timezone.utc
                    ).isoformat()
                    if event.event.auditStamp
                    else None
                )
        elif event.event_type == METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            if isinstance(event.event, MetadataChangeLogEvent):
                return (
                    datetime.fromtimestamp(
                        event.event.auditHeader.time / 1000.0, tz=timezone.utc
                    ).isoformat()
                    if event.event.auditHeader
                    else None
                )
        return None

    def start(self, event: EventEnvelope) -> None:
        """
        Update the stats based on the event.
        """
        self.last_event_processed_time = datetime.now(tz=timezone.utc).isoformat()
        self.last_seen_event_time = self._get_event_time(event)

    def end(
        self,
        event: EventEnvelope,
        success: bool,
        action_urn: Optional[str] = None,
        action_type: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> None:
        """
        Update the stats based on the event.

        Args:
            event: The event that was processed
            success: Whether processing succeeded
            action_urn: Optional action URN for OTEL metrics
            action_type: Optional action type for OTEL metrics
            stage: Optional stage for OTEL metrics
        """
        processing_time = datetime.now(timezone.utc)

        if success:
            self.last_seen_event_time_success = (
                self._get_event_time(event) or self.last_seen_event_time_success
            )
            self.last_event_processed_time_success = processing_time.isoformat()
        else:
            self.last_seen_event_time_failure = (
                self._get_event_time(event) or self.last_seen_event_time_failure
            )
            self.last_event_processed_time_failure = processing_time.isoformat()

        # Increment events processed counter
        self.num_events_processed += 1

        # Record OTEL metrics if action context is available
        if action_urn and action_type and stage:
            from datahub_integrations.observability.event_processing_metrics import (
                record_event_lag,
                record_event_processed,
            )

            # Record event processed
            record_event_processed(
                action_urn=action_urn,
                action_type=action_type,
                stage=stage,
                success=success,
            )

            # Record event processing lag
            event_time_str = self._get_event_time(event)
            if event_time_str:
                try:
                    event_time = datetime.fromisoformat(event_time_str)
                    record_event_lag(
                        action_urn=action_urn,
                        action_type=action_type,
                        stage=stage,
                        event_time=event_time,
                        processing_time=processing_time,
                    )
                except (ValueError, TypeError):
                    # If we can't parse the event time, skip lag recording
                    pass

    def __str__(self) -> str:
        return json.dumps(self.model_dump(), indent=2)


class StageStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    RUNNING = "running"
    STOPPED = "stopped"


class ActionStageReport(BaseModel):
    # All stats here are only for the current run of the current stage.

    # Attributes that should be aggregated across runs should be prefixed with "total_".
    # Only ints can be aggregated.

    start_time: int = 0

    end_time: int = 0

    total_assets_to_process: int = -1  # -1 if unknown

    total_assets_processed: int = 0

    total_actions_executed: int = 0

    total_assets_impacted: int = 0

    event_processing_stats: Optional[EventProcessingStats] = None

    status: Optional[StageStatus] = None

    warnings: list[str] = []

    # Action context for OTEL metrics (optional for backwards compatibility)
    action_urn: Optional[str] = None
    action_type: Optional[str] = None
    stage: Optional[str] = None

    def start(self) -> None:
        self.start_time = int(datetime.now().timestamp() * 1000)
        self.status = StageStatus.RUNNING

    def end(self, success: bool) -> None:
        self.end_time = int(datetime.now().timestamp() * 1000)
        self.status = StageStatus.SUCCESS if success else StageStatus.FAILURE

    def increment_assets_processed(self, asset: str) -> None:
        # TODO: If we want to track unique assets, use a counting set.
        # For now, just increment
        self.total_assets_processed += 1

        # Record OTEL metric if action context is available
        if self.action_urn and self.action_type and self.stage:
            from datahub_integrations.observability.event_processing_metrics import (
                record_asset_processed,
            )

            record_asset_processed(
                action_urn=self.action_urn,
                action_type=self.action_type,
                stage=self.stage,
            )

    def increment_assets_impacted(self, asset: str) -> None:
        # TODO: If we want to track unique assets, use a counting set.
        # For now, just increment
        self.total_assets_impacted += 1

        # Record OTEL metric if action context is available
        if self.action_urn and self.action_type and self.stage:
            from datahub_integrations.observability.event_processing_metrics import (
                record_asset_impacted,
            )

            record_asset_impacted(
                action_urn=self.action_urn,
                action_type=self.action_type,
                stage=self.stage,
            )

    def record_event_start(self, event: EventEnvelope) -> None:
        """Helper to start event processing with automatic context passing.

        This is a convenience wrapper around event_processing_stats.start() that
        automatically initializes event_processing_stats if needed.
        """
        if not self.event_processing_stats:
            self.event_processing_stats = EventProcessingStats()
        self.event_processing_stats.start(event)

    def record_event_end(self, event: EventEnvelope, success: bool) -> None:
        """Helper to end event processing with automatic OTEL metrics recording.

        This is a convenience wrapper around event_processing_stats.end() that
        automatically passes action context for OTEL metrics if available.
        """
        if not self.event_processing_stats:
            return

        self.event_processing_stats.end(
            event=event,
            success=success,
            action_urn=self.action_urn,
            action_type=self.action_type,
            stage=self.stage,
        )

    def as_obj(self) -> dict:
        return Report.to_pure_python_obj(self)

    def aggregatable_stats(self) -> Dict[str, int]:
        all_items = self.model_dump()

        stats = {k: v for k, v in all_items.items() if k.startswith("total_")}

        # If total_assets_to_process is unknown, don't include it.
        if self.total_assets_to_process == -1:
            stats.pop("total_assets_to_process")

        # Add a few additional special cases of aggregatable stats.
        if self.event_processing_stats:
            for key, value in self.event_processing_stats.model_dump().items():
                if value is not None:
                    stats[f"event_processing_stats.{key}"] = str(value)

        return stats


class ReportingAction(Action, abc.ABC):
    def __init__(self, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx

        self.action_urn: str
        if "urn:li:dataHubAction:" in ctx.pipeline_name:
            # The pipeline name might get a prefix before the urn:li:... part.
            # We need to remove that prefix to get the urn:li:dataHubAction part.
            action_urn_part = ctx.pipeline_name.split("urn:li:dataHubAction:")[1]
            self.action_urn = f"urn:li:dataHubAction:{action_urn_part}"
        else:
            self.action_urn = f"urn:li:dataHubAction:{ctx.pipeline_name}"

    @abc.abstractmethod
    def get_report(self) -> ActionStageReport:
        pass


assert hasattr(ActionStageReport, "as_obj")
