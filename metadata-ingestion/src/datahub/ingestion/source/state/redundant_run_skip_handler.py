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

    def is_current_run_succeessful(self) -> bool:
        return all(self.status.values())

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

        last_run = self.get_last_run_time_window(last_checkpoint)
        cur_run = TimeWindow(cur_start_time, cur_end_time)

        # In case of usage, it is assumed that start_time_millis (cur_start_time_millis as well as last_run_start_time) always conincides
        # with floored bucket start time. This should be taken care of outside this scope.

        if cur_run.starts_after(last_run) and cur_run.start_time != last_run.end_time:
            # scenario of time gap between past successful run window and current run window - maybe due to failed past run
            # Should we keep some configurable limits here to decide how much increase in time window is fine ?
            if allow_expand:
                suggested_start_time = last_run.end_time
                self.log("Expanding time window. Updating start time.")
            else:
                self.log(
                    f"Observed gap in last run end time({last_run.end_time}) and current run start time({cur_start_time})."
                )
        elif (
            allow_reduce
            and (cur_run.contains(last_run) or cur_run.intersects(last_run))
            and cur_run.ends_after(last_run)
        ):
            # scenario of scheduled ingestions with default start, end times
            suggested_start_time = last_run.end_time
            self.log("Reducing time window. Updating start time.")
        elif (
            allow_reduce
            and (cur_run.contains(last_run) or cur_run.intersects(last_run))
            and last_run.ends_after(cur_run)
        ):
            # a manual backdated run
            suggested_end_time = last_run.start_time
            self.log("Reducing time window. Updating end time.")

        self.log(
            "Suggested start, end times: "
            f"({suggested_start_time}, {suggested_end_time})"
        )
        return (suggested_start_time, suggested_end_time)

    def log(self, msg: str) -> None:
        logger.info(f"{self.job_id} : {msg}")

    def get_last_run_time_window(
        self, last_checkpoint: Checkpoint[BaseTimeWindowCheckpointState]
    ) -> TimeWindow:
        # Determine from the last check point state
        last_run_start_time: int = last_checkpoint.state.begin_timestamp_millis
        last_run_end_time: int = last_checkpoint.state.end_timestamp_millis

        # For time bucket based aggregations - e.g. usage
        # Case : Ingestion is scheduled to be run on 17:00:00 time everyday with bucket_duration=DAY.
        # Run on 2023-08-16 would ingest usage for 2023-08-15 00:00:00 to 2023-08-16 17:00:00 .
        # Run on 2023-08-17 would ingest usage for 2023-08-16 00:00:00 to 2023-08-17 17:00:00 .
        # The code makes sure that usage is ingested for complete bucket and not partial bucket.
        if last_checkpoint.state.bucket_duration is not None:
            last_run_end_time = datetime_to_ts_millis(
                get_time_bucket(
                    ts_millis_to_datetime(last_run_end_time),
                    last_checkpoint.state.bucket_duration,
                )
            )

        self.log(
            f"Last run start, end times:({ts_millis_to_datetime(last_run_start_time)}, {ts_millis_to_datetime(last_run_end_time)})"
        )

        return TimeWindow(
            ts_millis_to_datetime(last_run_start_time),
            ts_millis_to_datetime(last_run_end_time),
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
    ) -> TimeWindow:
        assert last_checkpoint.state.bucket_duration is not None
        return super().get_last_run_time_window(last_checkpoint)
