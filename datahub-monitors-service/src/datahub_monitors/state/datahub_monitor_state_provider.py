import logging
import time
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import MonitorTimeseriesStateClass
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.assertion.types import AssertionState, AssertionStateType
from datahub_monitors.exceptions import InvalidParametersException

from .assertion_state_provider import AssertionStateProvider

logger = logging.getLogger(__name__)


class DataHubMonitorStateProvider(AssertionStateProvider):
    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
    )
    def _get_latest_monitor_time_series_state(
        self,
        entity_urn: str,
    ) -> Optional[AssertionState]:
        latest_state = self.graph.get_latest_timeseries_value(
            entity_urn=entity_urn,
            aspect_type=MonitorTimeseriesStateClass,
            filter_criteria_map={},
        )
        if latest_state is None:
            return None

        return AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=latest_state.timestampMillis,
            properties=latest_state.customProperties,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
    )
    def _save_monitor_time_series_state(
        self, entity_urn: str, state: AssertionState
    ) -> None:
        monitor_state = MonitorTimeseriesStateClass(
            customProperties=state.properties,
            timestampMillis=int(time.time() * 1000),
        )
        save_state_mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=monitor_state,
        )

        # Now emit an MCP
        self.graph.emit_mcp(save_state_mcp)

    def get_state(
        self,
        entity_urn: str,
        assertion_state_type: AssertionStateType,
    ) -> Optional[AssertionState]:
        if assertion_state_type == AssertionStateType.MONITOR_TIMESERIES_STATE:
            return self._get_latest_monitor_time_series_state(entity_urn)

        raise InvalidParametersException(
            message=f"Unsupported assertion state type {assertion_state_type} provided!",
            parameters={"assertion_state_type": assertion_state_type},
        )

    def save_state(self, entity_urn: str, state: AssertionState) -> None:
        if state.type == AssertionStateType.MONITOR_TIMESERIES_STATE:
            self._save_monitor_time_series_state(entity_urn, state)
            return

        raise InvalidParametersException(
            message=f"Unsupported assertion state type {state.type} provided!",
            parameters={"assertion_state_type": state.type},
        )
