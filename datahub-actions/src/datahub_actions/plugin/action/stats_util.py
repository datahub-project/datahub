# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
import json
from datetime import datetime, timezone
from typing import Dict, Optional

import pydantic
from pydantic import BaseModel

from datahub.ingestion.api.report import Report, SupportsAsObj
from datahub.utilities.str_enum import StrEnum
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext


class EventProcessingStats(BaseModel):
    """
    A class to represent the event-oriented processing stats for a pipeline.
    Note: Might be merged into ActionStats in the future.
    """

    last_seen_event_time: Optional[str] = pydantic.Field(
        None, description="The event time of the last event we processed"
    )
    last_event_processed_time: Optional[str] = pydantic.Field(
        None, description="The time at which we processed the last event"
    )
    last_seen_event_time_success: Optional[str] = pydantic.Field(
        None, description="The event time of the last event we processed successfully"
    )
    last_event_processed_time_success: Optional[str] = pydantic.Field(
        None, description="The time at which we processed the last event successfully"
    )
    last_seen_event_time_failure: Optional[str] = pydantic.Field(
        None, description="The event time of the last event we processed unsuccessfully"
    )
    last_event_processed_time_failure: Optional[str] = pydantic.Field(
        None, description="The time at which we processed the last event unsuccessfully"
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

    def end(self, event: EventEnvelope, success: bool) -> None:
        """
        Update the stats based on the event.
        """

        if success:
            self.last_seen_event_time_success = (
                self._get_event_time(event) or self.last_seen_event_time_success
            )
            self.last_event_processed_time_success = datetime.now(
                timezone.utc
            ).isoformat()
        else:
            self.last_seen_event_time_failure = (
                self._get_event_time(event) or self.last_seen_event_time_failure
            )
            self.last_event_processed_time_failure = datetime.now(
                timezone.utc
            ).isoformat()

    def __str__(self) -> str:
        return json.dumps(self.dict(), indent=2)


class StageStatus(StrEnum):
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

    def increment_assets_impacted(self, asset: str) -> None:
        # TODO: If we want to track unique assets, use a counting set.
        # For now, just increment
        self.total_assets_impacted += 1

    def as_obj(self) -> dict:
        return Report.to_pure_python_obj(self)

    def aggregatable_stats(self) -> Dict[str, int]:
        all_items = self.dict()

        stats = {k: v for k, v in all_items.items() if k.startswith("total_")}

        # If total_assets_to_process is unknown, don't include it.
        if self.total_assets_to_process == -1:
            stats.pop("total_assets_to_process")

        # Add a few additional special cases of aggregatable stats.
        if self.event_processing_stats:
            for key, value in self.event_processing_stats.dict().items():
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


assert isinstance(ActionStageReport(), SupportsAsObj)
