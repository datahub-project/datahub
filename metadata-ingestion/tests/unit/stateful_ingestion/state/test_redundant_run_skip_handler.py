from datetime import datetime, timezone
from typing import Iterable
from unittest import mock

import pytest

from datahub.configuration.time_window_config import BucketDuration, get_time_bucket
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.usage_common_state import (
    BaseTimeWindowCheckpointState,
)
from datahub.utilities.time import datetime_to_ts_millis


@pytest.fixture
def stateful_source(mock_datahub_graph: DataHubGraph) -> Iterable[SnowflakeV2Source]:
    pipeline_name = "test_redundant_run_lineage"
    run_id = "test_redundant_run"
    ctx = PipelineContext(
        pipeline_name=pipeline_name,
        run_id=run_id,
        graph=mock_datahub_graph,
    )
    config = SnowflakeV2Config(
        account_id="ABC12345.ap-south-1",
        username="TST_USR",
        password="TST_PWD",
        stateful_ingestion=StatefulStaleMetadataRemovalConfig(
            enabled=True,
            # Uses the graph from the pipeline context.
        ),
    )

    with mock.patch(
        "datahub.sql_parsing.sql_parsing_aggregator.ToolMetaExtractor.create",
    ) as mock_checkpoint, mock.patch("snowflake.connector.connect"):
        mock_checkpoint.return_value = mock.MagicMock()

        yield SnowflakeV2Source(ctx=ctx, config=config)


def test_redundant_run_job_ids(stateful_source: SnowflakeV2Source) -> None:
    assert stateful_source.lineage_extractor is not None
    assert stateful_source.lineage_extractor.redundant_run_skip_handler is not None
    assert (
        stateful_source.lineage_extractor.redundant_run_skip_handler.job_id
        == "Snowflake_skip_redundant_run_lineage"
    )

    assert stateful_source.usage_extractor is not None
    assert stateful_source.usage_extractor.redundant_run_skip_handler is not None
    assert (
        stateful_source.usage_extractor.redundant_run_skip_handler.job_id
        == "Snowflake_skip_redundant_run_usage"
    )


# last run
last_run_start_time = datetime(2023, 7, 2, tzinfo=timezone.utc)
last_run_end_time = datetime(2023, 7, 3, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "start_time,end_time,should_skip,suggested_start_time,suggested_end_time",
    [
        # Case = current run time window is same as of last run time window
        [
            datetime(2023, 7, 2, tzinfo=timezone.utc),
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            True,
            None,
            None,
        ],
        # Case = current run time window is starts at same time as of last run time window but ends later
        [
            datetime(2023, 7, 2, tzinfo=timezone.utc),
            datetime(2023, 7, 3, 18, tzinfo=timezone.utc),
            False,
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            datetime(2023, 7, 3, 18, tzinfo=timezone.utc),
        ],
        # Case = current run time window is subset of last run time window
        [
            datetime(2023, 7, 2, tzinfo=timezone.utc),
            datetime(2023, 7, 3, tzinfo=timezone.utc),
            True,
            None,
            None,
        ],
        # Case = current run time window is after last run time window but has some overlap with last run
        # Scenario for next day's run for scheduled daily ingestions
        [
            datetime(2023, 7, 3, tzinfo=timezone.utc),
            datetime(2023, 7, 4, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            datetime(2023, 7, 4, 12, tzinfo=timezone.utc),
        ],
        # Case = current run time window is after last run time window and has no overlap with last run
        [
            datetime(2023, 7, 5, tzinfo=timezone.utc),
            datetime(2023, 7, 7, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 7, 5, tzinfo=timezone.utc),
            datetime(2023, 7, 7, 12, tzinfo=timezone.utc),
        ],
        # Case = current run time window is before last run time window but has some overlap with last run
        # Scenario for manual run for past dates
        [
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 2, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 2, tzinfo=timezone.utc),
        ],
        # Case = current run time window starts before last run time window and ends exactly on last run end time
        # Scenario for manual run for past dates
        [
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 3, 12, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            datetime(2023, 7, 2, tzinfo=timezone.utc),
        ],
        # Case = current run time window is before last run time window and has no overlap with last run
        # Scenario for manual run for past dates
        [
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 6, 30, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 6, 30, tzinfo=timezone.utc),
        ],
        # Case = current run time window subsumes last run time window and extends on both sides
        # Scenario for manual run
        [
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 7, 20, tzinfo=timezone.utc),
            False,
            datetime(2023, 6, 20, tzinfo=timezone.utc),
            datetime(2023, 7, 20, tzinfo=timezone.utc),
        ],
    ],
)
def test_redundant_run_skip_handler(
    stateful_source: SnowflakeV2Source,
    start_time: datetime,
    end_time: datetime,
    should_skip: bool,
    suggested_start_time: datetime,
    suggested_end_time: datetime,
) -> None:
    # mock_datahub_graph

    # mocked_source = mock.MagicMock()
    # mocked_config = mock.MagicMock()

    with mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.get_last_checkpoint"
    ) as mocked_fn:
        set_mock_last_run_time_window(
            mocked_fn,
            last_run_start_time,
            last_run_end_time,
        )

        # Redundant Lineage Skip Handler
        assert stateful_source.lineage_extractor is not None
        assert stateful_source.lineage_extractor.redundant_run_skip_handler is not None
        assert (
            stateful_source.lineage_extractor.redundant_run_skip_handler.should_skip_this_run(
                start_time, end_time
            )
            == should_skip
        )

        if not should_skip:
            suggested_time_window = stateful_source.lineage_extractor.redundant_run_skip_handler.suggest_run_time_window(
                start_time, end_time
            )
            assert suggested_time_window == (suggested_start_time, suggested_end_time)

        set_mock_last_run_time_window_usage(
            mocked_fn, last_run_start_time, last_run_end_time
        )
        # Redundant Usage Skip Handler
        assert stateful_source.usage_extractor is not None
        assert stateful_source.usage_extractor.redundant_run_skip_handler is not None
        assert (
            stateful_source.usage_extractor.redundant_run_skip_handler.should_skip_this_run(
                start_time, end_time
            )
            == should_skip
        )

        if not should_skip:
            suggested_time_window = stateful_source.usage_extractor.redundant_run_skip_handler.suggest_run_time_window(
                start_time, end_time
            )
            assert suggested_time_window == (
                get_time_bucket(suggested_start_time, BucketDuration.DAY),
                suggested_end_time,
            )


def set_mock_last_run_time_window(mocked_fn, start_time, end_time):
    mock_checkpoint = mock.MagicMock()
    mock_checkpoint.state = BaseTimeWindowCheckpointState(
        begin_timestamp_millis=datetime_to_ts_millis(start_time),
        end_timestamp_millis=datetime_to_ts_millis(end_time),
    )
    mocked_fn.return_value = mock_checkpoint


def set_mock_last_run_time_window_usage(mocked_fn, start_time, end_time):
    mock_checkpoint = mock.MagicMock()
    mock_checkpoint.state = BaseTimeWindowCheckpointState(
        begin_timestamp_millis=datetime_to_ts_millis(start_time),
        end_timestamp_millis=datetime_to_ts_millis(end_time),
        bucket_duration=BucketDuration.DAY,
    )
    mocked_fn.return_value = mock_checkpoint


def test_successful_run_creates_checkpoint(stateful_source: SnowflakeV2Source) -> None:
    assert stateful_source.lineage_extractor is not None
    assert stateful_source.lineage_extractor.redundant_run_skip_handler is not None
    with mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.create_checkpoint"
    ) as mocked_create_checkpoint_fn, mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.get_last_checkpoint"
    ) as mocked_fn:
        set_mock_last_run_time_window(
            mocked_fn,
            last_run_start_time,
            last_run_end_time,
        )
        stateful_source.lineage_extractor.redundant_run_skip_handler.update_state(
            datetime.now(tz=timezone.utc), datetime.now(tz=timezone.utc)
        )
        mocked_create_checkpoint_fn.assert_called_once()


def test_failed_run_does_not_create_checkpoint(
    stateful_source: SnowflakeV2Source,
) -> None:
    assert stateful_source.lineage_extractor is not None
    assert stateful_source.lineage_extractor.redundant_run_skip_handler is not None
    stateful_source.lineage_extractor.redundant_run_skip_handler.report_current_run_status(
        "some_step", False
    )
    with mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.create_checkpoint"
    ) as mocked_create_checkpoint_fn, mock.patch(
        "datahub.ingestion.source.state.stateful_ingestion_base.StateProviderWrapper.get_last_checkpoint"
    ) as mocked_fn:
        set_mock_last_run_time_window(
            mocked_fn,
            last_run_start_time,
            last_run_end_time,
        )
        stateful_source.lineage_extractor.redundant_run_skip_handler.update_state(
            datetime.now(tz=timezone.utc), datetime.now(tz=timezone.utc)
        )
        mocked_create_checkpoint_fn.assert_not_called()
