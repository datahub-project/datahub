import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Dict, Optional, Tuple, cast

import pydantic

from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.utilities.time import (
    TimeWindow,
    datetime_to_ts_millis,
    ts_millis_to_datetime,
)

logger: logging.Logger = logging.getLogger(__name__)


class RedundantRunSkipHandler(
    StatefulIngestionUsecaseHandlerBase[BaseTimeWindowCheckpointState],
    metaclass=ABCMeta,
):
    """
    The stateful ingestion helper class that handles skipping redundant runs.
    This contains the generic logic for all sources that need to support skipping redundant runs.
    """

    INVALID_TIMESTAMP_VALUE: pydantic.PositiveInt = 1

    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase[StatefulIngestionConfig],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        self.source = source
        self.state_provider = source.state_provider
        self.stateful_ingestion_config: Optional[
            StatefulIngestionConfig
        ] = config.stateful_ingestion
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._job_id = self._init_job_id()
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

        # step -> step status
        self.status: Dict[str, bool] = {}

    def _ignore_new_state(self) -> bool:
        return (
            self.stateful_ingestion_config is not None
            and self.stateful_ingestion_config.ignore_new_state
        )

    def _init_job_id(self) -> JobId:
        platform: Optional[str] = None
        source_class = type(self.source)
        if hasattr(source_class, "get_platform_name"):
            platform = source_class.get_platform_name()  # type: ignore

        # Default name for everything else
        job_name_suffix = self.get_job_name_suffix()
        return JobId(
            f"{platform}_skip_redundant_run{job_name_suffix}"
            if platform
            else f"skip_redundant_run{job_name_suffix}"
        )

    @abstractmethod
    def get_job_name_suffix(self):
        raise NotImplementedError("Sub-classes must override this method.")

    @property
    def job_id(self) -> JobId:
        return self._job_id

    def is_checkpointing_enabled(self) -> bool:
        return self.state_provider.is_stateful_ingestion_configured()

    def create_checkpoint(self) -> Optional[Checkpoint[BaseTimeWindowCheckpointState]]:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return None

        assert self.pipeline_name is not None
        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=BaseTimeWindowCheckpointState(
                begin_timestamp_millis=self.INVALID_TIMESTAMP_VALUE,
                end_timestamp_millis=self.INVALID_TIMESTAMP_VALUE,
            ),
        )

    def report_current_run_status(self, step: str, status: bool) -> None:
        """
        A helper to track status of all steps of current run.
        This will be used to decide overall status of the run.
        Checkpoint state will not be updated/committed for current run if there are any failures.
        """
        self.status[step] = status

    def is_current_run_successful(self) -> bool:
        return all(self.status.values())

    def get_current_checkpoint(
        self,
    ) -> Optional[Checkpoint]:
        if (
            not self.is_checkpointing_enabled()
            or self._ignore_new_state()
            or not self.is_current_run_successful()
        ):
            return None
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        return cur_checkpoint

    def should_skip_this_run(
        self, cur_start_time: datetime, cur_end_time: datetime
    ) -> bool:
        skip: bool = False

        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, BaseTimeWindowCheckpointState
        )

        if last_checkpoint:
            last_run_time_window = TimeWindow(
                ts_millis_to_datetime(last_checkpoint.state.begin_timestamp_millis),
                ts_millis_to_datetime(last_checkpoint.state.end_timestamp_millis),
            )

            logger.debug(
                f"{self.job_id} : Last run start, end times:"
                f"({last_run_time_window})"
            )

            # If current run's time window is subset of last run's time window, then skip.
            # Else there is at least some part in current time window that was not covered in past run's time window
            if last_run_time_window.contains(TimeWindow(cur_start_time, cur_end_time)):
                skip = True

        return skip

    def suggest_run_time_window(
        self,
        cur_start_time: datetime,
        cur_end_time: datetime,
        allow_reduce: int = True,
        allow_expand: int = False,
    ) -> Tuple[datetime, datetime]:
        # If required in future, allow_reduce, allow_expand can be accepted as user input
        # as part of stateful ingestion configuration. It is likely that they may cause
        # more confusion than help to most users hence not added to start with.
        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, BaseTimeWindowCheckpointState
        )
        if (last_checkpoint is None) or self.should_skip_this_run(
            cur_start_time, cur_end_time
        ):
            return cur_start_time, cur_end_time

        suggested_start_time, suggested_end_time = cur_start_time, cur_end_time

        last_run = last_checkpoint.state.to_time_interval()
        self.log(f"Last run start, end times:{last_run}")
        cur_run = TimeWindow(cur_start_time, cur_end_time)

        if cur_run.starts_after(last_run):
            # scenario of time gap between past successful run window and current run window - maybe due to failed past run
            # Should we keep some configurable limits here to decide how much increase in time window is fine ?
            if allow_expand:
                suggested_start_time = last_run.end_time
                self.log(
                    f"Expanding time window. Updating start time to {suggested_start_time}."
                )
            else:
                self.log(
                    f"Observed gap in last run end time({last_run.end_time}) and current run start time({cur_start_time})."
                )
        elif allow_reduce and cur_run.left_intersects(last_run):
            # scenario of scheduled ingestions with default start, end times
            suggested_start_time = last_run.end_time
            self.log(
                f"Reducing time window. Updating start time to {suggested_start_time}."
            )
        elif allow_reduce and cur_run.right_intersects(last_run):
            # a manual backdated run
            suggested_end_time = last_run.start_time
            self.log(
                f"Reducing time window. Updating end time to {suggested_end_time}."
            )

        # make sure to consider complete time bucket for usage
        if last_checkpoint.state.bucket_duration:
            suggested_start_time = get_time_bucket(
                suggested_start_time, last_checkpoint.state.bucket_duration
            )

        self.log(
            "Adjusted start, end times: "
            f"({suggested_start_time}, {suggested_end_time})"
        )
        return (suggested_start_time, suggested_end_time)

    def log(self, msg: str) -> None:
        logger.info(f"{self.job_id} : {msg}")


class RedundantLineageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_lineage"

    def update_state(self, start_time: datetime, end_time: datetime) -> None:
        cur_checkpoint = self.get_current_checkpoint()
        if cur_checkpoint:
            cur_state = cast(BaseTimeWindowCheckpointState, cur_checkpoint.state)
            cur_state.begin_timestamp_millis = datetime_to_ts_millis(start_time)
            cur_state.end_timestamp_millis = datetime_to_ts_millis(end_time)


class RedundantUsageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_usage"

    def update_state(
        self, start_time: datetime, end_time: datetime, bucket_duration: BucketDuration
    ) -> None:
        cur_checkpoint = self.get_current_checkpoint()
        if cur_checkpoint:
            cur_state = cast(BaseTimeWindowCheckpointState, cur_checkpoint.state)
            cur_state.begin_timestamp_millis = datetime_to_ts_millis(start_time)
            cur_state.end_timestamp_millis = datetime_to_ts_millis(end_time)
            cur_state.bucket_duration = bucket_duration
