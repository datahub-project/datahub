import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    CheckpointJobStatesMap,
    IngestionCheckpointingProviderBase,
    IngestionCheckpointingProviderConfig,
    JobId,
    JobStateFilterType,
    JobStateKey,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatahubIngestionCheckpointClass,
)

logger = logging.getLogger(__name__)


class DatahubIngestionStateProviderConfig(IngestionCheckpointingProviderConfig):
    datahub_api: Optional[DatahubClientConfig] = DatahubClientConfig()


class DatahubIngestionCheckpointingProvider(IngestionCheckpointingProviderBase):
    orchestrator_name: str = "datahub"

    def __init__(self, graph: DataHubGraph, name: str):
        super().__init__(name)
        self.graph = graph
        if not self._is_server_stateful_ingestion_capable():
            raise ConfigurationError(
                "Datahub server is not capable of supporting stateful ingestion."
                " Please consider upgrading to the latest server version to use this feature."
            )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> IngestionCheckpointingProviderBase:
        if ctx.graph:
            # Use the pipeline-level graph if set
            return cls(ctx.graph, name)
        elif config_dict is None:
            raise ConfigurationError("Missing provider configuration.")
        else:
            provider_config = DatahubIngestionStateProviderConfig.parse_obj(config_dict)
            if provider_config.datahub_api:
                graph = DataHubGraph(provider_config.datahub_api)
                return cls(graph, name)
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

        data_job_urn = self.get_data_job_urn(
            self.orchestrator_name, pipeline_name, job_name, platform_instance_id
        )
        latest_checkpoint: Optional[
            DatahubIngestionCheckpointClass
        ] = self.graph.get_latest_timeseries_value(
            entity_urn=data_job_urn,
            aspect_type=DatahubIngestionCheckpointClass,
            filter_criteria_map={
                "pipelineName": pipeline_name,
                "platformInstanceId": platform_instance_id,
            },
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

    def get_previous_states(
        self,
        state_key: JobStateKey,
        last_only: bool = True,
        filter_opt: Optional[JobStateFilterType] = None,
    ) -> List[CheckpointJobStatesMap]:
        if not last_only:
            raise NotImplementedError(
                "Currently supports retrieving only the last commited state."
            )
        if filter_opt is not None:
            raise NotImplementedError(
                "Support for optional filters is not implemented yet."
            )
        checkpoints: List[CheckpointJobStatesMap] = []
        last_job_checkpoint_map: CheckpointJobStatesMap = {}
        for job_name in state_key.job_names:
            last_job_checkpoint = self.get_latest_checkpoint(
                state_key.pipeline_name, state_key.platform_instance_id, job_name
            )
            if last_job_checkpoint is not None:
                last_job_checkpoint_map[job_name] = last_job_checkpoint
        checkpoints.append(last_job_checkpoint_map)
        return checkpoints

    def commit(self) -> None:
        if not self.state_to_commit:
            logger.warning(f"No state available to commit for {self.name}")
            return None

        for job_name, checkpoint in self.state_to_commit.items():
            # Emit the ingestion state for each job
            logger.info(
                f"Committing ingestion checkpoint for pipeline:'{checkpoint.pipelineName}',"
                f"instance:'{checkpoint.platformInstanceId}', job:'{job_name}'"
            )

            self.committed = False

            datajob_urn = self.get_data_job_urn(
                self.orchestrator_name,
                checkpoint.pipelineName,
                job_name,
                checkpoint.platformInstanceId,
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

            self.committed = True

            logger.info(
                f"Committed ingestion checkpoint for pipeline:'{checkpoint.pipelineName}',"
                f"instance:'{checkpoint.platformInstanceId}', job:'{job_name}'"
            )
