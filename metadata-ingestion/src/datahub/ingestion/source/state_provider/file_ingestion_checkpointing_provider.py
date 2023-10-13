import logging
import pathlib
from datetime import datetime
from typing import Any, Dict, Optional, cast

from datahub.configuration.common import ConfigurationError
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
    filename: Optional[str] = None


class FileIngestionCheckpointingProvider(IngestionCheckpointingProviderBase):
    orchestrator_name: str = "file"

    def __init__(self, filename: str, name: str):
        super().__init__(name)
        self.filename = filename
        self.committed = False

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> "FileIngestionCheckpointingProvider":
        if config_dict is None:
            raise ConfigurationError("Missing provider configuration.")
        else:
            provider_config = FileIngestionStateProviderConfig.parse_obj_allow_extras(
                config_dict
            )
            if provider_config.filename:
                return cls(provider_config.filename, name)
            else:
                raise ConfigurationError(
                    "Missing filename. Provide filename under the state_provider configuration."
                )

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
        for obj in read_metadata_file(pathlib.Path(self.filename)):
            if isinstance(obj, MetadataChangeProposalWrapper) and obj.aspect:
                if (
                    obj.entityUrn == data_job_urn
                    and obj.aspectName == "datahubIngestionCheckpoint"
                    and obj.aspect.get("pipelineName", "") == pipeline_name
                ):
                    latest_checkpoint = cast(
                        Optional[DatahubIngestionCheckpointClass], obj.aspect
                    )
                    break

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

            datajob_urn = self.get_data_job_urn(
                self.orchestrator_name,
                checkpoint.pipelineName,
                job_name,
            )

            if not self.committed:
                checkpoint_workunit = MetadataChangeProposalWrapper(
                    entityUrn=datajob_urn,
                    aspect=checkpoint,
                )
                write_metadata_file(pathlib.Path(self.filename), [checkpoint_workunit])
                self.committed = True
            else:
                existing_checkpoint_workunits = [
                    obj for obj in read_metadata_file(pathlib.Path(self.filename))
                ]
                existing_checkpoint_workunits.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=datajob_urn,
                        aspect=checkpoint,
                    )
                )
                write_metadata_file(
                    pathlib.Path(self.filename), existing_checkpoint_workunits
                )

            logger.debug(
                f"Committed ingestion checkpoint for pipeline:'{checkpoint.pipelineName}', "
                f"job:'{job_name}'"
            )
