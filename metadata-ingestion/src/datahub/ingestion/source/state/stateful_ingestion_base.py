import logging
import platform
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Type, cast

import psutil
import pydantic

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
from datahub.ingestion.api.ingestion_job_reporting_provider_base import (
    IngestionReportingProviderBase,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state_provider.state_provider_registry import (
    ingestion_checkpoint_provider_registry,
)
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    DatahubIngestionRunSummaryClass,
    JobStatusClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class StatefulIngestionConfig(ConfigModel):
    """
    Basic Stateful Ingestion Specific Configuration for any source.
    """

    enabled: bool = False
    # fmt: off
    max_checkpoint_state_size: pydantic.PositiveInt = 2**24  # 16MB
    # fmt: on
    state_provider: Optional[DynamicTypedConfig] = None
    ignore_old_state: bool = False
    ignore_new_state: bool = False

    @pydantic.root_validator()
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("enabled"):
            if values.get("state_provider") is None:
                values["state_provider"] = DynamicTypedConfig(
                    type="datahub", config=None
                )
        return values


class StatefulIngestionConfigBase(DatasetSourceConfigBase):
    """
    Base configuration class for stateful ingestion for source configs to inherit from.
    """

    stateful_ingestion: Optional[StatefulIngestionConfig] = None


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
        self._initialize_checkpointing_state_provider()
        self.report: StatefulIngestionReport = StatefulIngestionReport()

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

    def is_stateful_ingestion_configured(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.enabled
            and self.ingestion_checkpointing_state_provider is not None
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
            last_checkpoint_aspect = self.ingestion_checkpointing_state_provider.get_latest_checkpoint(  # type: ignore
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

    #
    # Reporting specific support.
    #
    def _is_reporting_enabled(self):
        for rc in self.ctx.get_reporters():
            assert rc is not None
            return True
        return False

    def _create_default_job_run_summary(self) -> DatahubIngestionRunSummaryClass:
        assert self.ctx.pipeline_name
        job_run_summary_default = DatahubIngestionRunSummaryClass(
            timestampMillis=int(datetime.utcnow().timestamp() * 1000),
            pipelineName=self.ctx.pipeline_name,
            platformInstanceId=self.get_platform_instance_id(),
            runId=self.ctx.run_id,
            runStatus=JobStatusClass.COMPLETED,
        )
        # Add system specific info
        job_run_summary_default.systemHostName = platform.node()
        job_run_summary_default.operatingSystemName = platform.system()
        job_run_summary_default.numProcessors = psutil.cpu_count(logical=True)
        vmem = psutil.virtual_memory()
        job_run_summary_default.availableMemory = getattr(vmem, "available", None)
        job_run_summary_default.totalMemory = getattr(vmem, "total", None)
        # Sources can add config in config + source report in custom_value.
        # and also populate other source specific metrics.
        return job_run_summary_default

    def get_job_run_summary(
        self, job_id: JobId
    ) -> Optional[DatahubIngestionRunSummaryClass]:
        """
        Get the cached/newly created job run summary for this job if reporting is configured.
        """
        if not self._is_reporting_enabled():
            return None
        if job_id not in self.run_summaries_to_report:
            self.run_summaries_to_report[
                job_id
            ] = self._create_default_job_run_summary()
        return self.run_summaries_to_report[job_id]

    #
    # Commit handoff to provider for both checkpointing and reporting.
    #
    def _prepare_job_run_summaries_for_commit(self) -> None:
        for reporting_committable in self.ctx.get_reporters():
            if isinstance(reporting_committable, IngestionReportingProviderBase):
                reporting_provider = cast(
                    IngestionReportingProviderBase, reporting_committable
                )
                reporting_provider.state_to_commit.update(self.run_summaries_to_report)
                logger.info(
                    f"Successfully handed-off job run summaries to {reporting_provider.name}."
                )

    def prepare_for_commit(self) -> None:
        """NOTE: Sources should call this method from their close method."""
        self._prepare_checkpoint_states_for_commit()
        self._prepare_job_run_summaries_for_commit()
