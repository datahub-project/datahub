import logging
from typing import Any, Dict, Optional, Type

import pydantic

from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    DynamicTypedConfig,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_state_provider import IngestionStateProvider, JobId
from datahub.ingestion.api.source import Source
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state_provider.datahub_ingestion_state_provider import (
    DatahubIngestionStateProviderConfig,
)
from datahub.ingestion.source.state_provider.state_provider_registry import (
    ingestion_state_provider_registry,
)
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

logger: logging.Logger = logging.getLogger(__name__)


class StatefulIngestionConfig(ConfigModel):
    """
    Basic Stateful Ingestion Specific Configuration for any source.
    """

    enabled: bool = False
    max_checkpoint_state_size: int = 2 ** 24  # 16MB
    state_provider: Optional[DynamicTypedConfig] = DynamicTypedConfig(
        type="datahub", config=DatahubIngestionStateProviderConfig()
    )
    ignore_old_state: bool = False
    ignore_new_state: bool = False

    @pydantic.root_validator()
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("enabled"):
            if values.get("state_provider") is None:
                raise ConfigurationError(
                    "Must specify state_provider configuration if stateful ingestion is enabled."
                )
        return values


class StatefulIngestionConfigBase(ConfigModel):
    """
    Base configuration class for stateful ingestion for source configs to inherit from.
    """

    stateful_ingestion: Optional[StatefulIngestionConfig] = None


class StatefulIngestionSourceBase(Source):
    """
    Defines the base class for all stateful sources.
    """

    def __init__(
        self, config: StatefulIngestionConfigBase, ctx: PipelineContext
    ) -> None:
        super().__init__(ctx)
        self.stateful_ingestion_config = config.stateful_ingestion
        self.source_config_type = type(config)
        self.last_checkpoints: Dict[JobId, Optional[Checkpoint]] = {}
        self.cur_checkpoints: Dict[JobId, Optional[Checkpoint]] = {}
        self._initialize_state_provider()

    def _initialize_state_provider(self) -> None:
        self.ingestion_state_provider: Optional[IngestionStateProvider] = None
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.state_provider is not None
            and self.stateful_ingestion_config.enabled
        ):
            if self.ctx.pipeline_name is None:
                raise ConfigurationError(
                    "pipeline_name must be provided if stateful ingestion is enabled."
                )
            state_provider_class = ingestion_state_provider_registry.get(
                self.stateful_ingestion_config.state_provider.type
            )
            self.ingestion_state_provider = state_provider_class.create(
                self.stateful_ingestion_config.state_provider.dict().get("config", {}),
                self.ctx,
            )
            if self.stateful_ingestion_config.ignore_old_state:
                logger.warning(
                    "The 'ignore_old_state' config is True. The old checkpoint state will not be provided."
                )
            if self.stateful_ingestion_config.ignore_new_state:
                logger.warning(
                    "The 'ignore_new_state' config is True. The new checkpoint state will not be created."
                )

            logger.debug(
                f"Successfully created {self.stateful_ingestion_config.state_provider.type} state provider."
            )

    def is_stateful_ingestion_configured(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.enabled
            and self.ingestion_state_provider is not None
        ):
            return True
        return False

    # Basic methods that sub-classes must implement
    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        raise NotImplementedError("Sub-classes must implement this method.")

    def get_platform_instance_id(self) -> str:
        raise NotImplementedError("Sub-classes must implement this method.")

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        """
        Sub-classes should override this method to tell if checkpointing is enabled for this run.
        For instance, currently all of the SQL based sources use checkpointing for stale entity removal.
        They would turn it on only if remove_stale_metadata=True. Otherwise, the feature won't work correctly.
        """
        raise NotImplementedError("Sub-classes must implement this method.")

    def _get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[CheckpointStateBase]
    ) -> Optional[Checkpoint]:
        """
        This is a template method implementation for querying the last checkpoint state.
        """
        last_checkpoint: Optional[Checkpoint] = None
        if self.is_stateful_ingestion_configured():
            # Obtain the latest checkpoint from GMS for this job.
            last_checkpoint_aspect = self.ingestion_state_provider.get_latest_checkpoint(  # type: ignore
                pipeline_name=self.ctx.pipeline_name,  # type: ignore
                platform_instance_id=self.get_platform_instance_id(),
                job_name=job_id,
            )
            # Convert it to a first-class Checkpoint object.
            last_checkpoint = Checkpoint.create_from_checkpoint_aspect(
                job_name=job_id,
                checkpoint_aspect=last_checkpoint_aspect,
                config_class=self.source_config_type,
                state_class=checkpoint_state_class,
            )
        return last_checkpoint

    # Base-class implementations for common state management tasks.
    def get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[CheckpointStateBase]
    ) -> Optional[Checkpoint]:
        if not self.is_stateful_ingestion_configured() or (
            self.stateful_ingestion_config
            and self.stateful_ingestion_config.ignore_old_state
        ):
            return None

        if JobId not in self.last_checkpoints:
            self.last_checkpoints[job_id] = self._get_last_checkpoint(
                job_id, checkpoint_state_class
            )
        return self.last_checkpoints[job_id]

    def get_current_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        if not self.is_stateful_ingestion_configured():
            return None

        if job_id not in self.cur_checkpoints:
            self.cur_checkpoints[job_id] = (
                self.create_checkpoint(job_id)
                if self.is_checkpointing_enabled(job_id)
                else None
            )
        return self.cur_checkpoints[job_id]

    def commit_checkpoints(self) -> None:
        if not self.is_stateful_ingestion_configured():
            return None
        if (
            self.stateful_ingestion_config
            and self.stateful_ingestion_config.ignore_new_state
        ):
            logger.info(
                "The `ignore_new_state` config is True. Not committing current checkpoint."
            )
            return None
        if self.ctx.dry_run_mode or self.ctx.preview_mode:
            logger.warning(
                f"Will not be committing checkpoints in dry_run_mode(={self.ctx.dry_run_mode})"
                f" or preview_mode(={self.ctx.preview_mode})."
            )
            return None
        job_checkpoint_aspects: Dict[JobId, DatahubIngestionCheckpointClass] = {}
        for job_name, job_checkpoint in self.cur_checkpoints.items():
            if job_checkpoint is None:
                continue
            try:
                checkpoint_aspect = job_checkpoint.to_checkpoint_aspect(
                    self.stateful_ingestion_config.max_checkpoint_state_size  # type: ignore
                )
            except Exception as e:
                logger.error(
                    f"Failed to convert checkpoint to aspect for job {job_name}. It will not be committed.",
                    e,
                )
            else:
                if checkpoint_aspect is not None:
                    job_checkpoint_aspects[job_name] = checkpoint_aspect

        self.ingestion_state_provider.commit_checkpoints(  # type: ignore
            job_checkpoints=job_checkpoint_aspects
        )
