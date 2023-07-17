import logging
from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple, cast

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
    datetime_to_ts_millis,
    get_datetime_from_ts_millis_in_utc,
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
            else job_name_suffix
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

    def update_state(
        self,
        start_time_millis: pydantic.PositiveInt,
        end_time_millis: pydantic.PositiveInt,
        bucket_duration: Optional[BucketDuration] = None,
    ) -> None:
        if not self.is_checkpointing_enabled() or self._ignore_new_state():
            return
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(BaseTimeWindowCheckpointState, cur_checkpoint.state)
        cur_state.begin_timestamp_millis = start_time_millis
        cur_state.end_timestamp_millis = end_time_millis
        cur_state.bucket_duration = bucket_duration

    def should_skip_this_run(
        self, cur_start_time_millis: int, cur_end_time_millis: int
    ) -> Tuple[bool, int, int]:
        skip: bool = False
        suggested_start_time_millis: int = cur_start_time_millis
        suggested_end_time_millis: int = cur_end_time_millis

        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, BaseTimeWindowCheckpointState
        )

        if last_checkpoint and last_checkpoint.state:
            # Determine from the last check point state
            last_run_start_time, last_run_end_time = self.get_last_run_start_end_times(
                last_checkpoint
            )

            logger.debug(
                f"{self.job_id} : Last run start, end times:"
                f"({get_datetime_from_ts_millis_in_utc(last_run_start_time)}, {get_datetime_from_ts_millis_in_utc(last_run_end_time)})"
            )

            # In case of usage, it is assumed that start_time_millis (cur_start_time_millis as well as last_run_start_time) always conincides
            # with floored bucket start time. This should be taken care of outside this scope.

            if cur_start_time_millis >= last_run_start_time:
                if cur_start_time_millis > last_run_end_time:
                    # scenario of time gap between past successful run window and current run window - maybe due to failed past run
                    # Should we keep some configurable limits here to decide how much increase in time window is fine ?
                    suggested_start_time_millis = last_run_end_time
                    logger.info(
                        f"{self.job_id} : Expanding time window. Changing start time to  {get_datetime_from_ts_millis_in_utc(last_run_end_time)}"
                    )
                elif cur_end_time_millis > last_run_end_time:
                    # scenario of scheduled ingestions with default start, end times
                    suggested_start_time_millis = last_run_end_time
                    logger.info(
                        f"{self.job_id} : Reducing time window. Changing start time to  {get_datetime_from_ts_millis_in_utc(last_run_end_time)}"
                    )

                else:
                    # current run's time window is subset of last run's time window
                    skip = True
            else:
                # cur_start_time_millis < last_run_start_time
                # This is most likely a manual backdated run which we should always run.
                # Do we really need below optimisation/reducing time window for manual runs ?
                if last_run_start_time < cur_end_time_millis <= last_run_end_time:
                    suggested_end_time_millis = last_run_start_time
                    logger.info(
                        f"{self.job_id} : Reducing time window. Changing end time to  {get_datetime_from_ts_millis_in_utc(last_run_end_time)}"
                    )
                else:
                    # last run's time window is subset of current run's time window
                    pass  # execute normally

            logger.info(
                f"{self.job_id} : Skip the run ? {skip}. Suggested start, end times: "
                f"({get_datetime_from_ts_millis_in_utc(suggested_start_time_millis)}, {get_datetime_from_ts_millis_in_utc(suggested_end_time_millis)})"
            )
        return skip, suggested_start_time_millis, suggested_end_time_millis

    def get_last_run_start_end_times(
        self, last_checkpoint: Checkpoint[BaseTimeWindowCheckpointState]
    ) -> Tuple[int, int]:
        last_run_start_time: int = last_checkpoint.state.begin_timestamp_millis
        last_run_end_time: int = last_checkpoint.state.end_timestamp_millis

        if last_checkpoint.state.bucket_duration is not None:
            last_run_end_time = datetime_to_ts_millis(
                get_time_bucket(
                    get_datetime_from_ts_millis_in_utc(last_run_end_time),
                    last_checkpoint.state.bucket_duration,
                )
            )

        return last_run_start_time, last_run_end_time


class RedundantLineageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_lineage"


class RedundantUsageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_usage"
