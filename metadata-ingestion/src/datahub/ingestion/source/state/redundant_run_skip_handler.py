import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict
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
from datahub.utilities.time import datetime_to_ts_millis, ts_millis_to_datetime

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
        self.status: Dict[str, bool] = defaultdict(lambda: True)

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

    def is_current_run_succeessful(self) -> bool:
        return all(self.status)

    def update_state(
        self,
        start_time: datetime,
        end_time: datetime,
        bucket_duration: Optional[BucketDuration] = None,
    ) -> None:
        if (
            not self.is_checkpointing_enabled()
            or self._ignore_new_state()
            or not self.is_current_run_succeessful()
        ):
            return
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        assert cur_checkpoint is not None
        cur_state = cast(BaseTimeWindowCheckpointState, cur_checkpoint.state)
        cur_state.begin_timestamp_millis = datetime_to_ts_millis(start_time)
        cur_state.end_timestamp_millis = datetime_to_ts_millis(end_time)
        cur_state.bucket_duration = bucket_duration

    def should_skip_this_run(
        self, cur_start_time: datetime, cur_end_time: datetime
    ) -> bool:
        skip: bool = False

        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, BaseTimeWindowCheckpointState
        )

        if last_checkpoint:
            last_run_start_time: datetime = ts_millis_to_datetime(
                last_checkpoint.state.begin_timestamp_millis
            )
            last_run_end_time: datetime = ts_millis_to_datetime(
                last_checkpoint.state.end_timestamp_millis
            )

            logger.debug(
                f"{self.job_id} : Last run start, end times:"
                f"({last_run_start_time}, {last_run_end_time})"
            )

            # If current run's time window is subset of last run's time window, then skip.
            # Else there is at least some part in current time window that was not covered in past run's time window
            if (
                last_run_start_time <= cur_start_time <= last_run_end_time
                and last_run_start_time <= cur_end_time <= last_run_end_time
            ):
                skip = True

        return skip

    def suggest_run_time_window(
        self,
        cur_start_time: datetime,
        cur_end_time: datetime,
        allow_reduce: int = True,
        allow_expand: int = False,
    ) -> Tuple[datetime, datetime]:
        last_checkpoint = self.state_provider.get_last_checkpoint(
            self.job_id, BaseTimeWindowCheckpointState
        )
        if (last_checkpoint is None) or self.should_skip_this_run(
            cur_start_time, cur_end_time
        ):
            return cur_start_time, cur_end_time

        suggested_start_time: datetime = cur_start_time
        suggested_end_time: datetime = cur_end_time

        last_run_start_time, last_run_end_time = self.get_last_run_time_window(
            last_checkpoint
        )

        # In case of usage, it is assumed that start_time_millis (cur_start_time_millis as well as last_run_start_time) always conincides
        # with floored bucket start time. This should be taken care of outside this scope.

        if cur_start_time >= last_run_start_time:
            if cur_start_time > last_run_end_time:
                if allow_expand:
                    # scenario of time gap between past successful run window and current run window - maybe due to failed past run
                    # Should we keep some configurable limits here to decide how much increase in time window is fine ?
                    suggested_start_time = last_run_end_time
                    logger.info(
                        f"{self.job_id} : Expanding time window. Changing start time to  {last_run_end_time}"
                    )
                else:
                    logger.warn(
                        f"{self.job_id} : Observed gap in last run end time({last_run_end_time}) and current run start time({cur_start_time})."
                    )
            elif cur_end_time > last_run_end_time and allow_reduce:
                # scenario of scheduled ingestions with default start, end times
                suggested_start_time = last_run_end_time
                logger.info(
                    f"{self.job_id} : Reducing time window. Changing start time to  {last_run_end_time}"
                )
        else:
            # cur_start_time_millis < last_run_start_time
            # This is most likely a manual backdated run which we should always run.
            # Do we really need below optimisation/reducing time window for manual runs ?
            if last_run_start_time < cur_end_time <= last_run_end_time and allow_reduce:
                suggested_end_time = last_run_start_time
                logger.info(
                    f"{self.job_id} : Reducing time window. Changing end time to  {last_run_end_time}"
                )

        logger.info(
            f"{self.job_id} : Suggested start, end times: "
            f"({suggested_start_time}, {suggested_end_time})"
        )
        return (suggested_start_time, suggested_end_time)

    def get_last_run_time_window(
        self, last_checkpoint: Checkpoint[BaseTimeWindowCheckpointState]
    ) -> Tuple[datetime, datetime]:
        # Determine from the last check point state
        last_run_start_time: int = last_checkpoint.state.begin_timestamp_millis
        last_run_end_time: int = last_checkpoint.state.end_timestamp_millis

        # For time bucket based aggregations - e.g. usage
        if last_checkpoint.state.bucket_duration is not None:
            last_run_end_time = datetime_to_ts_millis(
                get_time_bucket(
                    ts_millis_to_datetime(last_run_end_time),
                    last_checkpoint.state.bucket_duration,
                )
            )

        logger.debug(
            f"{self.job_id} : Last run start, end times:"
            f"({ts_millis_to_datetime(last_run_start_time)}, {ts_millis_to_datetime(last_run_end_time)})"
        )

        return ts_millis_to_datetime(last_run_start_time), ts_millis_to_datetime(
            last_run_end_time
        )


class RedundantLineageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_lineage"


class RedundantUsageRunSkipHandler(RedundantRunSkipHandler):
    def get_job_name_suffix(self):
        return "_usage"

    def update_state(
        self,
        start_time: datetime,
        end_time: datetime,
        bucket_duration: Optional[BucketDuration] = None,
    ) -> None:
        assert bucket_duration is not None
        return super().update_state(start_time, end_time, bucket_duration)

    def get_last_run_time_window(
        self, last_checkpoint: Checkpoint[BaseTimeWindowCheckpointState]
    ) -> Tuple[datetime, datetime]:
        assert last_checkpoint.state.bucket_duration is not None
        return super().get_last_run_time_window(last_checkpoint)
