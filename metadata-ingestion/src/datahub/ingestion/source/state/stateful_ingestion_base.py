import logging
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, Type, TypeVar

import pydantic
from pydantic import root_validator
from pydantic.fields import Field

from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    DynamicTypedConfig,
)
from datahub.configuration.pydantic_migration_helpers import GenericModel
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import capability
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
    JobId,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.source.state.checkpoint import Checkpoint, StateType
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.ingestion.source.state_provider.state_provider_registry import (
    ingestion_checkpoint_provider_registry,
)
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass

logger: logging.Logger = logging.getLogger(__name__)


class DynamicTypedStateProviderConfig(DynamicTypedConfig):
    # Respecifying the base-class just to override field level docs

    type: str = Field(
        description="The type of the state provider to use. For DataHub use `datahub`",
    )
    config: Dict[str, Any] = Field(
        default={},
        description="The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19).",
    )


class StatefulIngestionConfig(ConfigModel):
    """
    Basic Stateful Ingestion Specific Configuration for any source.
    """

    enabled: bool = Field(
        default=False,
        description="Whether or not to enable stateful ingest. "
        "Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
    )
    max_checkpoint_state_size: pydantic.PositiveInt = Field(
        default=2**24,  # 16 MB
        description="The maximum size of the checkpoint state in bytes. Default is 16MB",
        hidden_from_docs=True,
    )
    state_provider: Optional[DynamicTypedStateProviderConfig] = Field(
        default=None,
        description="The ingestion state provider configuration.",
        hidden_from_docs=True,
    )
    ignore_old_state: bool = Field(
        default=False,
        description="If set to True, ignores the previous checkpoint state.",
        hidden_from_docs=True,
    )
    ignore_new_state: bool = Field(
        default=False,
        description="If set to True, ignores the current checkpoint state.",
        hidden_from_docs=True,
    )

    @pydantic.root_validator(skip_on_failure=True)
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("enabled"):
            if values.get("state_provider") is None:
                values["state_provider"] = DynamicTypedStateProviderConfig(
                    type="datahub", config={}
                )
        return values


CustomConfig = TypeVar("CustomConfig", bound=StatefulIngestionConfig)


class StatefulIngestionConfigBase(GenericModel, Generic[CustomConfig]):
    """
    Base configuration class for stateful ingestion for source configs to inherit from.
    """

    stateful_ingestion: Optional[CustomConfig] = Field(
        default=None, description="Stateful Ingestion Config"
    )


class StatefulLineageConfigMixin(ConfigModel):
    enable_stateful_lineage_ingestion: bool = Field(
        default=True,
        description="Enable stateful lineage ingestion."
        " This will store lineage window timestamps after successful lineage ingestion. "
        "and will not run lineage ingestion for same timestamps in subsequent run. ",
    )

    _store_last_lineage_extraction_timestamp = pydantic_renamed_field(
        "store_last_lineage_extraction_timestamp", "enable_stateful_lineage_ingestion"
    )

    @root_validator(skip_on_failure=True)
    def lineage_stateful_option_validator(cls, values: Dict) -> Dict:
        sti = values.get("stateful_ingestion")
        if not sti or not sti.enabled:
            if values.get("enable_stateful_lineage_ingestion"):
                logger.warning(
                    "Stateful ingestion is disabled, disabling enable_stateful_lineage_ingestion config option as well"
                )
                values["enable_stateful_lineage_ingestion"] = False

        return values


class StatefulProfilingConfigMixin(ConfigModel):
    enable_stateful_profiling: bool = Field(
        default=True,
        description="Enable stateful profiling."
        " This will store profiling timestamps per dataset after successful profiling. "
        "and will not run profiling again in subsequent run if table has not been updated. ",
    )

    _store_last_profiling_timestamps = pydantic_renamed_field(
        "store_last_profiling_timestamps", "enable_stateful_profiling"
    )

    @root_validator(skip_on_failure=True)
    def profiling_stateful_option_validator(cls, values: Dict) -> Dict:
        sti = values.get("stateful_ingestion")
        if not sti or not sti.enabled:
            if values.get("enable_stateful_profiling"):
                logger.warning(
                    "Stateful ingestion is disabled, disabling enable_stateful_profiling config option as well"
                )
                values["enable_stateful_profiling"] = False
        return values


class StatefulUsageConfigMixin(BaseTimeWindowConfig):
    enable_stateful_usage_ingestion: bool = Field(
        default=True,
        description="Enable stateful lineage ingestion."
        " This will store usage window timestamps after successful usage ingestion. "
        "and will not run usage ingestion for same timestamps in subsequent run. ",
    )

    _store_last_usage_extraction_timestamp = pydantic_renamed_field(
        "store_last_usage_extraction_timestamp", "enable_stateful_usage_ingestion"
    )

    @root_validator(skip_on_failure=True)
    def last_usage_extraction_stateful_option_validator(cls, values: Dict) -> Dict:
        sti = values.get("stateful_ingestion")
        if not sti or not sti.enabled:
            if values.get("enable_stateful_usage_ingestion"):
                logger.warning(
                    "Stateful ingestion is disabled, disabling enable_stateful_usage_ingestion config option as well"
                )
                values["enable_stateful_usage_ingestion"] = False
        return values


@dataclass
class StatefulIngestionReport(SourceReport):
    pass


@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class StatefulIngestionSourceBase(Source):
    """
    Defines the base class for all stateful sources.
    """

    def __init__(
        self,
        config: StatefulIngestionConfigBase[StatefulIngestionConfig],
        ctx: PipelineContext,
    ) -> None:
        super().__init__(ctx)
        self.report: StatefulIngestionReport = StatefulIngestionReport()
        self.state_provider = StateProviderWrapper(config.stateful_ingestion, ctx)

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        # TODO: Remove this method.
        self.report.warning(key, reason)

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        # TODO: Remove this method.
        self.report.failure(key, reason)

    def close(self) -> None:
        self.state_provider.prepare_for_commit()
        super().close()


class StateProviderWrapper:
    def __init__(
        self,
        config: Optional[StatefulIngestionConfig],
        ctx: PipelineContext,
    ) -> None:
        self.ctx = ctx
        self.stateful_ingestion_config = config

        self.last_checkpoints: Dict[JobId, Optional[Checkpoint]] = {}
        self.cur_checkpoints: Dict[JobId, Optional[Checkpoint]] = {}
        self.report: StatefulIngestionReport = StatefulIngestionReport()
        self._initialize_checkpointing_state_provider()
        self._usecase_handlers: Dict[JobId, StatefulIngestionUsecaseHandlerBase] = {}

    #
    # Checkpointing specific support.
    #

    def _initialize_checkpointing_state_provider(self) -> None:
        self.ingestion_checkpointing_state_provider: Optional[
            IngestionCheckpointingProviderBase
        ] = None

        if (
            self.stateful_ingestion_config is None
            and self.ctx.graph
            and self.ctx.pipeline_name
        ):
            logger.info(
                "Stateful ingestion will be automatically enabled, as datahub-rest sink is used or `datahub_api` is specified"
            )
            self.stateful_ingestion_config = StatefulIngestionConfig(
                enabled=True,
                state_provider=DynamicTypedStateProviderConfig(type="datahub"),
            )

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
            self.ingestion_checkpointing_state_provider = (
                checkpointing_state_provider_class.create(
                    config_dict=self.stateful_ingestion_config.state_provider.config,
                    ctx=self.ctx,
                )
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

    def _get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[StateType]
    ) -> Optional[Checkpoint]:
        """
        This is a template method implementation for querying the last checkpoint state.
        """
        last_checkpoint: Optional[Checkpoint] = None
        if self.is_stateful_ingestion_configured():
            # Obtain the latest checkpoint from GMS for this job.
            assert self.ctx.pipeline_name
            assert self.ingestion_checkpointing_state_provider
            last_checkpoint_aspect = (
                self.ingestion_checkpointing_state_provider.get_latest_checkpoint(
                    pipeline_name=self.ctx.pipeline_name,
                    job_name=job_id,
                )
            )

            # Convert it to a first-class Checkpoint object.
            last_checkpoint = Checkpoint[StateType].create_from_checkpoint_aspect(
                job_name=job_id,
                checkpoint_aspect=last_checkpoint_aspect,
                state_class=checkpoint_state_class,
            )
        return last_checkpoint

    # Base-class implementations for common state management tasks.
    def get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[StateType]
    ) -> Optional[Checkpoint[StateType]]:
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
        assert self.stateful_ingestion_config

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
                    self.stateful_ingestion_config.max_checkpoint_state_size
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
