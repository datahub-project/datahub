import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_state_provider import IngestionStateProvider, JobId
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ChangeTypeClass,
    DatahubIngestionCheckpointClass,
    DatahubIngestionRunSummaryClass,
    TimeWindowSizeClass,
)

logger = logging.getLogger(__name__)


class DatahubIngestionStateProviderConfig(ConfigModel):
    datahub_api: Optional[DatahubClientConfig] = DatahubClientConfig()


class DatahubIngestionStateProvider(IngestionStateProvider):
    orchestrator_name: str = "datahub"

    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        if not self._is_server_stateful_ingestion_capable():
            raise ConfigurationError(
                "Datahub server is not capable of supporting stateful ingestion."
                " Please consider upgrading to the latest server version to use this feature."
            )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> IngestionStateProvider:
        if ctx.graph:
            return cls(ctx.graph)
        elif config_dict is None:
            raise ConfigurationError("Missing provider configuration")
        else:
            provider_config = DatahubIngestionStateProviderConfig.parse_obj(config_dict)
            if provider_config.datahub_api:
                graph = DataHubGraph(provider_config.datahub_api)
                return cls(graph)
            else:
                raise ConfigurationError(
                    "Missing datahub_api. Provide either a global one or under the state_provider."
                )

    def _is_server_stateful_ingestion_capable(self) -> bool:
        server_config = self.graph.get_config() if self.graph else None
        if server_config and server_config.get("statefulIngestionCapable"):
            return True
        return False

    def get_latest_checkpoint(
        self,
        pipeline_name: str,
        platform_instance_id: str,
        job_name: JobId,
    ) -> Optional[DatahubIngestionCheckpointClass]:

        logger.info(
            f"Querying for the latest ingestion checkpoint for pipelineName:'{pipeline_name}',"
            f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}'"
        )

        data_job_urn = builder.make_data_job_urn(
            self.orchestrator_name, pipeline_name, job_name
        )
        latest_checkpoint: Optional[
            DatahubIngestionCheckpointClass
        ] = self.graph.get_latest_timeseries_value(
            entity_urn=data_job_urn,
            aspect_name="datahubIngestionCheckpoint",
            filter_criteria_map={
                "pipelineName": pipeline_name,
                "platformInstanceId": platform_instance_id,
            },
            aspect_type=DatahubIngestionCheckpointClass,
        )
        if latest_checkpoint:
            logger.info(
                f"The last committed ingestion checkpoint for pipelineName:'{pipeline_name}',"
                f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}' found with start_time:"
                f" {datetime.fromtimestamp(latest_checkpoint.timestampMillis/1000, tz=timezone.utc)} and a"
                f" bucket duration of {latest_checkpoint.eventGranularity}."
            )
            return latest_checkpoint
        else:
            logger.info(
                f"No committed ingestion checkpoint for pipelineName:'{pipeline_name}',"
                f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}' found"
            )

        return None

    def commit_checkpoints(
        self, job_checkpoints: Dict[JobId, DatahubIngestionCheckpointClass]
    ) -> None:
        for job_name, checkpoint in job_checkpoints.items():
            # Emit the ingestion state for each job
            logger.info(
                f"Committing ingestion checkpoint for pipeline:'{checkpoint.pipelineName}',"
                f"instance:'{checkpoint.platformInstanceId}', job:'{job_name}'"
            )

            datajob_urn = builder.make_data_job_urn(
                self.orchestrator_name,
                checkpoint.pipelineName,
                job_name,
            )

            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityType="dataJob",
                    entityUrn=datajob_urn,
                    aspectName="datahubIngestionCheckpoint",
                    aspect=checkpoint,
                    changeType=ChangeTypeClass.UPSERT,
                )
            )

            logger.info(
                f"Committed ingestion checkpoint for pipeline:'{checkpoint.pipelineName}',"
                f"instance:'{checkpoint.platformInstanceId}', job:'{job_name}'"
            )

    @staticmethod
    def get_end_time(ingestion_state: DatahubIngestionRunSummaryClass) -> int:
        start_time_millis = ingestion_state.timestampMillis
        granularity = ingestion_state.eventGranularity
        granularity_millis = (
            DatahubIngestionStateProvider.get_granularity_to_millis(granularity)
            if granularity is not None
            else 0
        )
        return start_time_millis + granularity_millis

    @staticmethod
    def get_time_window_size(interval_str: str) -> TimeWindowSizeClass:
        to_calendar_interval: Dict[str, str] = {
            "s": CalendarIntervalClass.SECOND,
            "m": CalendarIntervalClass.MINUTE,
            "h": CalendarIntervalClass.HOUR,
            "d": CalendarIntervalClass.DAY,
            "W": CalendarIntervalClass.WEEK,
            "M": CalendarIntervalClass.MONTH,
            "Q": CalendarIntervalClass.QUARTER,
            "Y": CalendarIntervalClass.YEAR,
        }
        interval_pattern = re.compile(r"(\d+)([s|m|h|d|W|M|Q|Y])")
        token_search = interval_pattern.search(interval_str)
        if token_search is None:
            raise ValueError("Invalid interval string:", interval_str)
        (multiples_str, unit_str) = (token_search.group(1), token_search.group(2))
        if not multiples_str or not unit_str:
            raise ValueError("Invalid interval string:", interval_str)
        unit = to_calendar_interval.get(unit_str)
        if not unit:
            raise ValueError("Invalid time unit token:", unit_str)
        return TimeWindowSizeClass(unit=unit, multiple=int(multiples_str))

    @staticmethod
    def get_granularity_to_millis(granularity: TimeWindowSizeClass) -> int:
        to_millis_from_interval: Dict[str, int] = {
            CalendarIntervalClass.SECOND: 1000,
            CalendarIntervalClass.MINUTE: 60 * 1000,
            CalendarIntervalClass.HOUR: 60 * 60 * 1000,
            CalendarIntervalClass.DAY: 24 * 60 * 60 * 1000,
            CalendarIntervalClass.WEEK: 7 * 24 * 60 * 60 * 1000,
            CalendarIntervalClass.MONTH: 31 * 7 * 24 * 60 * 60 * 1000,
            CalendarIntervalClass.QUARTER: 90 * 7 * 24 * 60 * 60 * 1000,
            CalendarIntervalClass.YEAR: 365 * 7 * 24 * 60 * 60 * 1000,
        }
        units_to_millis = to_millis_from_interval.get(str(granularity.unit), None)
        if not units_to_millis:
            raise ValueError("Invalid unit", granularity.unit)
        return granularity.multiple * units_to_millis
