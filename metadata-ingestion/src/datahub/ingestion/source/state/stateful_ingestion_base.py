import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, Type, TypeVar, cast

import pydantic
from pydantic.fields import Field
from pydantic.generics import GenericModel

from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    DynamicTypedConfig,
)
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    JobId,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.state.checkpoint import Checkpoint, StateType
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.ingestion.source.state_provider.state_provider_registry import (
    ingestion_checkpoint_provider_registry,
)
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    DatahubIngestionRunSummaryClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class DynamicTypedStateProviderConfig(DynamicTypedConfig):
    # Respecifying the base-class just to override field level docs

    type: str = Field(
        description="The type of the state provider to use. For DataHub use `datahub`",
    )
    # This config type is declared Optional[Any] here. The eventual parser for the
    # specified type is responsible for further validation.
    config: Optional[Any] = Field(
        default=None,
        description="The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19).",
    )


class StatefulIngestionConfig(ConfigModel):
    """
    Basic Stateful Ingestion Specific Configuration for any source.
    """

    enabled: bool = Field(
        default=False,
        description="The type of the ingestion state provider registered with datahub.",
    )
    max_checkpoint_state_size: pydantic.PositiveInt = Field(
        default=2**24,  # 16 MB
        description="The maximum size of the checkpoint state in bytes. Default is 16MB",
        hidden_from_schema=True,
    )
    state_provider: Optional[DynamicTypedStateProviderConfig] = Field(
        default=None,
        description="The ingestion state provider configuration.",
        hidden_from_schema=True,
    )
    ignore_old_state: bool = Field(
        default=False,
        description="If set to True, ignores the previous checkpoint state.",
    )
    ignore_new_state: bool = Field(
        default=False,
        description="If set to True, ignores the current checkpoint state.",
    )

    @pydantic.root_validator()
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("enabled"):
            if values.get("state_provider") is None:
                values["state_provider"] = DynamicTypedStateProviderConfig(
                    type="datahub", config=None
                )
        return values


CustomConfig = TypeVar("CustomConfig", bound=StatefulIngestionConfig)


class StatefulIngestionConfigBase(
    DatasetSourceConfigBase, GenericModel, Generic[CustomConfig]
):
    """
    Base configuration class for stateful ingestion for source configs to inherit from.
    """

    stateful_ingestion: Optional[CustomConfig] = Field(
        default=None, description="Stateful Ingestion Config"
    )


@dataclass
class StatefulIngestionReport(SourceReport):
    pass


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
        self.run_summaries_to_report: Dict[JobId, DatahubIngestionRunSummaryClass] = {}
        self.report: StatefulIngestionReport = StatefulIngestionReport()
        self._initialize_checkpointing_state_provider()
        self._usecase_handlers: Dict[JobId, StatefulIngestionUsecaseHandlerBase] = {}

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    #
    # Checkpointing specific support.
    #

    def _initialize_checkpointing_state_provider(self) -> None:
        self.ingestion_checkpointing_state_provider: Optional[
            IngestionCheckpointingProviderBase
        ] = None
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.state_provider is not None
            and self.stateful_ingestion_config.enabled
        ):
            if self.ctx.pipeline_name is None:
                raise ConfigurationError(
                    "pipeline_name must be provided if stateful ingestion is enabled."
                )
            checkpointing_state_provider_class = (
                ingestion_checkpoint_provider_registry.get(
                    self.stateful_ingestion_config.state_provider.type
                )
            )
            if checkpointing_state_provider_class is None:
                raise ConfigurationError(
                    f"Cannot find checkpoint provider class of type={self.stateful_ingestion_config.state_provider.type} "
                    " in the registry! Please check the type of the checkpointing provider in your config."
                )
            config_dict: Dict[str, Any] = cast(
                Dict[str, Any],
                self.stateful_ingestion_config.state_provider.dict().get("config", {}),
            )
            self.ingestion_checkpointing_state_provider = checkpointing_state_provider_class.create(  # type: ignore
                config_dict=config_dict,
                ctx=self.ctx,
                name=checkpointing_state_provider_class.__name__,
            )
            assert self.ingestion_checkpointing_state_provider
            if self.stateful_ingestion_config.ignore_old_state:
                logger.warning(
                    "The 'ignore_old_state' config is True. The old checkpoint state will not be provided."
                )
            if self.stateful_ingestion_config.ignore_new_state:
                logger.warning(
                    "The 'ignore_new_state' config is True. The new checkpoint state will not be created."
                )
            # Add the checkpoint state provide to the platform context.
            self.ctx.register_checkpointer(self.ingestion_checkpointing_state_provider)

            logger.debug(
                f"Successfully created {self.stateful_ingestion_config.state_provider.type} state provider."
            )

    def register_stateful_ingestion_usecase_handler(
        self, usecase_handler: StatefulIngestionUsecaseHandlerBase
    ) -> None:
        """
        Registers a use-case handler with the common-base class.

        NOTE: The use-case handlers must invoke this method from their constructor(__init__) on the source.
        Also, the subclasses should not override this method.
        """
        assert usecase_handler.job_id not in self._usecase_handlers
        self._usecase_handlers[usecase_handler.job_id] = usecase_handler

    def is_stateful_ingestion_configured(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.enabled
            and self.ingestion_checkpointing_state_provider is not None
        ):
            return True
        return False

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Template base-class method that delegates the initial checkpoint state creation to
        the appropriate use-case handler registered for the job_id.
        """
        if job_id not in self._usecase_handlers:
            raise ValueError(f"No use-case handler for job_id{job_id}")
        return self._usecase_handlers[job_id].create_checkpoint()

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        """
        Template base-class method that delegates the check if checkpointing is enabled for the job
        to the appropriate use-case handler registered for the job_id.
        """
        if job_id not in self._usecase_handlers:
            raise ValueError(f"No use-case handler for job_id{job_id}")
        return self._usecase_handlers[job_id].is_checkpointing_enabled()

    # Methods that sub-classes must implement
    @abstractmethod
    def get_platform_instance_id(self) -> str:
        raise NotImplementedError("Sub-classes must implement this method.")

    def _get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[StateType]
    ) -> Optional[Checkpoint]:
        """
        This is a template method implementation for querying the last checkpoint state.
        """
        last_checkpoint: Optional[Checkpoint] = None
        if self.is_stateful_ingestion_configured():
            # Obtain the latest checkpoint from GMS for this job.
            last_checkpoint_aspect = self.ingestion_checkpointing_state_provider.get_latest_checkpoint(  # type: ignore
                pipeline_name=self.ctx.pipeline_name,  # type: ignore
                platform_instance_id=self.get_platform_instance_id(),
                job_name=job_id,
            )
            # Convert it to a first-class Checkpoint object.
            last_checkpoint = Checkpoint[StateType].create_from_checkpoint_aspect(
                job_name=job_id,
                checkpoint_aspect=last_checkpoint_aspect,
                config_class=self.source_config_type,
                state_class=checkpoint_state_class,
            )
        return last_checkpoint

    # Base-class implementations for common state management tasks.
    def get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[StateType]
    ) -> Optional[Checkpoint]:
        if not self.is_stateful_ingestion_configured() or (
            self.stateful_ingestion_config
            and self.stateful_ingestion_config.ignore_old_state
        ):
            return None

        if job_id not in self.last_checkpoints:
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

    def _prepare_checkpoint_states_for_commit(self) -> None:
        # Perform validations
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

        # Prepare the state the checkpointing provider should commit.
        job_checkpoint_aspects: Dict[JobId, DatahubIngestionCheckpointClass] = {}
        for job_name, job_checkpoint in self.cur_checkpoints.items():
            if job_checkpoint is None:
                continue
            job_checkpoint.prepare_for_commit()
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

        # Set the state to commit in the provider.
        assert self.ingestion_checkpointing_state_provider
        self.ingestion_checkpointing_state_provider.state_to_commit.update(
            job_checkpoint_aspects
        )

    def prepare_for_commit(self) -> None:
        """NOTE: Sources should call this method from their close method."""
        self._prepare_checkpoint_states_for_commit()

    def close(self) -> None:
        self.prepare_for_commit()
        super().close()
