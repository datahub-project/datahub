import logging
import pathlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    IngestionCheckpointingProviderConfig,
    JobId,
)
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

logger = logging.getLogger(__name__)


class FileIngestionStateProviderConfig(IngestionCheckpointingProviderConfig):
    filename: str


class FileIngestionCheckpointingProvider(IngestionCheckpointingProviderBase):
    orchestrator_name: str = "file"

    def __init__(self, config: FileIngestionStateProviderConfig):
        super().__init__(self.__class__.__name__)
        self.config = config

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "FileIngestionCheckpointingProvider":
        config = FileIngestionStateProviderConfig.parse_obj(config_dict)
        return cls(config)

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
        latest_checkpoint: Optional[DatahubIngestionCheckpointClass] = None
        try:
            for obj in read_metadata_file(pathlib.Path(self.config.filename)):
                if (
                    isinstance(obj, MetadataChangeProposalWrapper)
                    and obj.entityUrn == data_job_urn
                    and obj.aspect
                    and isinstance(obj.aspect, DatahubIngestionCheckpointClass)
                    and obj.aspect.get("pipelineName", "") == pipeline_name
                ):
                    latest_checkpoint = obj.aspect
                    break
        except FileNotFoundError:
            logger.debug(f"File {self.config.filename} not found")

        if latest_checkpoint:
            logger.debug(
                f"The last committed ingestion checkpoint for pipelineName:'{pipeline_name}',"
                f" job_name:'{job_name}' found with start_time:"
                f" {datetime.utcfromtimestamp(latest_checkpoint.timestampMillis / 1000)}"
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

        checkpoint_workunits: List[MetadataChangeProposalWrapper] = []
        for job_name, checkpoint in self.state_to_commit.items():
            # Emit the ingestion state for each job
            logger.debug(
                f"Committing ingestion checkpoint for pipeline:'{checkpoint.pipelineName}', "
                f"job:'{job_name}'"
            )
            datajob_urn = self.get_data_job_urn(
                self.orchestrator_name,
                checkpoint.pipelineName,
                job_name,
            )
            checkpoint_workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=datajob_urn,
                    aspect=checkpoint,
                )
            )
        write_metadata_file(pathlib.Path(self.config.filename), checkpoint_workunits)
        self.committed = True
        logger.debug(
            f"Committed all ingestion checkpoints for pipeline:'{checkpoint.pipelineName}'"
        )
