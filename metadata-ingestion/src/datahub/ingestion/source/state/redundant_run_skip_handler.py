import logging
from typing import Optional, cast

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.ingestion_job_state_provider import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.usage_common_state import BaseUsageCheckpointState
from datahub.utilities.time import get_datetime_from_ts_millis_in_utc

logger: logging.Logger = logging.getLogger(__name__)


class StatefulRedundantRunSkipConfig(StatefulIngestionConfig):
    """
    Base specialized config of Stateful Ingestion to skip redundant runs.
    """

    # Defines the alias 'force_rerun' for ignore_old_state field.
    ignore_old_state = pydantic.Field(False, alias="force_rerun")


class RedundantRunSkipHandler:
    """
    The stateful ingestion helper class that handles skipping redundant runs.
    This contains the generic logic for all sources that need to support skipping redundant runs.
    """

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: Optional[StatefulIngestionConfigBase],
        job_id: JobId,
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.source = source
        self.config = config
        self.stateful_ingestion_config = (
            cast(StatefulRedundantRunSkipConfig, self.config.stateful_ingestion)
            if self.config
            else None
        )
        self.job_id = job_id
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.checkpointing_enabled: bool = source.is_stateful_ingestion_configured()

    def _ignore_old_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_old_state
        ):
            return True
        return False

    def _ignore_new_state(self) -> bool:
        if (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_new_state
        ):
            return True
        return False

    def is_checkpointing_enabled(self) -> bool:
        return self.checkpointing_enabled

    def create_checkpoint(
        self,
        start_time_millis: pydantic.PositiveInt,
        end_time_millis: pydantic.PositiveInt,
    ) -> Optional[Checkpoint]:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return None

        assert self.config is not None
        assert self.pipeline_name is not None
        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            platform_instance_id=self.source.get_platform_instance_id(),
            run_id=self.run_id,
            config=cast(ConfigModel, self.config),
            state=BaseUsageCheckpointState(
                begin_timestamp_millis=start_time_millis,
                end_timestamp_millis=end_time_millis,
            ),
        )

    def init_checkpoints(self) -> None:
        # Triggers checkpoint creation.
        if not self._ignore_new_state():
            self.source.get_current_checkpoint(self.job_id)

    def should_skip_this_run(self, cur_start_time_millis: int) -> bool:
        if not self.is_checkpointing_enabled() or self._ignore_old_state():
            return False
        # Determine from the last check point state
        last_successful_pipeline_run_end_time_millis: Optional[int] = None
        last_checkpoint = self.source.get_last_checkpoint(
            self.job_id, BaseUsageCheckpointState
        )
        if last_checkpoint and last_checkpoint.state:
            state = cast(BaseUsageCheckpointState, last_checkpoint.state)
            last_successful_pipeline_run_end_time_millis = state.end_timestamp_millis

        if (
            last_successful_pipeline_run_end_time_millis is not None
            and cur_start_time_millis <= last_successful_pipeline_run_end_time_millis
        ):
            warn_msg = (
                f"Skippig this run, since the last run's bucket duration end: "
                f"{get_datetime_from_ts_millis_in_utc(last_successful_pipeline_run_end_time_millis)}"
                f" is later than the current start_time: {get_datetime_from_ts_millis_in_utc(cur_start_time_millis)}"
            )
            logger.warning(warn_msg)
            self.source.get_report().report_warning("skip-run", warn_msg)
            return True
        return False
