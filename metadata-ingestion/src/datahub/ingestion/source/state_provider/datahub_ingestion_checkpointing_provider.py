import logging
from datetime import datetime
from typing import Any, Dict, Optional

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    IngestionCheckpointingProviderConfig,
    JobId,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

logger = logging.getLogger(__name__)


class DatahubIngestionStateProviderConfig(IngestionCheckpointingProviderConfig):
    datahub_api: DatahubClientConfig = DatahubClientConfig()


class DatahubIngestionCheckpointingProvider(IngestionCheckpointingProviderBase):
    orchestrator_name: str = "datahub"

    def __init__(
        self,
        graph: DataHubGraph,
    ):
        super().__init__(self.__class__.__name__)
        self.graph = graph
        if not self._is_server_stateful_ingestion_capable():
            raise ConfigurationError(
                "Datahub server is not capable of supporting stateful ingestion."
                " Please consider upgrading to the latest server version to use this feature."
            )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "DatahubIngestionCheckpointingProvider":
        config = DatahubIngestionStateProviderConfig.parse_obj(config_dict)
        if ctx.graph:
            # Use the pipeline-level graph if set
            return cls(ctx.graph)
        else:
            return cls(DataHubGraph(config.datahub_api))

    def _is_server_stateful_ingestion_capable(self) -> bool:
        server_config = self.graph.get_config() if self.graph else None
        if server_config and server_config.get("statefulIngestionCapable"):
            return True
        return False

    def get_latest_checkpoint(
        self,
        pipeline_name: str,
        job_name: JobId,
    ) -> Optional[DatahubIngestionCheckpointClass]:
        logger.debug(
            f"Querying for the latest ingestion checkpoint for pipelineName:'{pipeline_name}',"
            f" job_name:'{job_name}'"
        )

        data_job_urn = self.get_data_job_urn(
            self.orchestrator_name, pipeline_name, job_name
        )

        latest_checkpoint: Optional[
            DatahubIngestionCheckpointClass
        ] = self.graph.get_latest_timeseries_value(
            entity_urn=data_job_urn,
            aspect_type=DatahubIngestionCheckpointClass,
            filter_criteria_map={
                "pipelineName": pipeline_name,
            },
        )
        if latest_checkpoint:
            logger.debug(
                f"The last committed ingestion checkpoint for pipelineName:'{pipeline_name}',"
                f" job_name:'{job_name}' found with start_time:"
                f" {datetime.utcfromtimestamp(latest_checkpoint.timestampMillis/1000)}"
            )
            return latest_checkpoint
        else:
            logger.debug(
                f"No committed ingestion checkpoint for pipelineName:'{pipeline_name}',"
                f" job_name:'{job_name}' found"
            )

        return None

    def commit(self) -> None:
        if not self.state_to_commit:
            logger.warning(f"No state available to commit for {self.name}")
            return None

        for job_name, checkpoint in self.state_to_commit.items():
            # Emit the ingestion state for each job
            logger.debug(
                f"Committing ingestion checkpoint for pipeline:'{checkpoint.pipelineName}', "
                f"job:'{job_name}'"
            )

            self.committed = False

            datajob_urn = self.get_data_job_urn(
                self.orchestrator_name,
                checkpoint.pipelineName,
                job_name,
            )

            # We don't want the state payloads to show up in search. As such, we emit the
            # dataJob aspects as soft-deleted. This doesn't affect the ability to query
            # them using the timeseries API.
            self.graph.soft_delete_entity(
                urn=datajob_urn,
            )
            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=datajob_urn,
                    aspect=checkpoint,
                )
            )

            self.committed = True

            logger.debug(
                f"Committed ingestion checkpoint for pipeline:'{checkpoint.pipelineName}', "
                f"job:'{job_name}'"
            )
