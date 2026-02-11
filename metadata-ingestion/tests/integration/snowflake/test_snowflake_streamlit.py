from datetime import datetime, timezone
from typing import cast
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import FROZEN_TIME, default_query_results

pytestmark = pytest.mark.integration_batch_2


@freeze_time(FROZEN_TIME)
def test_snowflake_streamlit_ingestion(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """
    Test that Streamlit applications are properly ingested as dashboard entities
    with all the correct aspects and properties.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_streamlit_test_events.json"
    golden_file = test_resources_dir / "snowflake_streamlit_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_streamlits=True,
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        # Check that Streamlit apps are reported
        report = cast(SnowflakeV2Report, pipeline.source.get_report())
        assert report.streamlit_apps_scanned == 2

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_streamlit_filtering(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """
    Test that Streamlit pattern filtering works correctly.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_streamlit_filtered_test_events.json"
    golden_file = test_resources_dir / "snowflake_streamlit_filtered_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_streamlits=True,
                        streamlit_pattern=AllowDenyPattern(
                            allow=[".*STREAMLIT_APP_1$"]
                        ),
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        # Check that only STREAMLIT_APP_1 is scanned and ingested
        report = cast(SnowflakeV2Report, pipeline.source.get_report())
        assert report.streamlit_apps_scanned == 2
        # One should be dropped due to filtering - just verify filtering happened
        assert any("STREAMLIT_APP_2" in item for item in report.filtered)

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
                r"root\[\d+\]\['systemMetadata'\]",
            ],
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_streamlit_disabled(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """
    Test that when include_streamlits is False, no Streamlit apps are ingested.
    """
    output_file = tmp_path / "snowflake_streamlit_disabled_test_events.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake",
                    config=SnowflakeV2Config(
                        account_id="ABC12345.ap-south-1.aws",
                        username="TST_USR",
                        password="TST_PWD",
                        match_fully_qualified_names=True,
                        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                        include_technical_schema=True,
                        include_table_lineage=False,
                        include_column_lineage=False,
                        include_usage_stats=False,
                        include_streamlits=False,
                        start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                        end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(
                            tzinfo=timezone.utc
                        ),
                    ),
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

        # Check that no Streamlit apps are scanned when disabled
        report = cast(SnowflakeV2Report, pipeline.source.get_report())
        assert report.streamlit_apps_scanned == 0

        # Verify output file contains no dashboard entities for Streamlit
        with open(output_file) as f:
            events = [line for line in f.readlines()]
            # Filter for dashboard entities
            dashboard_events = [
                event for event in events if '"entityType": "dashboard"' in event
            ]
            assert len(dashboard_events) == 0
