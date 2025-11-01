from typing import cast
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import (
    FROZEN_TIME,
    default_query_results,
)

pytestmark = pytest.mark.integration_batch_2


@freeze_time(FROZEN_TIME)
def test_snowflake_marketplace(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_marketplace_test_events.json"
    golden_file = test_resources_dir / "snowflake_marketplace_golden.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline_config_dict = {
            "source": {
                "type": "snowflake",
                "config": {
                    "username": "user",
                    "password": "password",
                    "account_id": "ABC12345",
                    "warehouse": "COMPUTE_WH",
                    "role": "datahub_role",
                    "database_pattern": {"allow": ["TEST_DB"]},
                    # Enable marketplace features
                    "include_marketplace_listings": True,
                    "include_marketplace_purchases": True,
                    "include_marketplace_usage": True,
                    # Disable other features for focused testing
                    "include_table_lineage": False,
                    "include_usage_stats": False,
                    "include_operational_stats": False,
                    "include_queries": False,  # Disable query entity generation
                    "use_queries_v2": False,  # Disable queries v2
                    "profiling": {"enabled": False},
                    "stateful_ingestion": {"enabled": False},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }

        pipeline = Pipeline.create(pipeline_config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the report
        report = cast(SnowflakeV2Report, pipeline.source.get_report())

        # Check marketplace-specific report metrics
        assert report.marketplace_listings_scanned == 2
        assert report.marketplace_purchases_scanned == 1
        assert report.marketplace_data_products_created == 2
        assert report.marketplace_enhanced_datasets == 1
        assert report.marketplace_usage_events_processed == 2

        # Verify the output matches expectations
        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=output_file,
            golden_path=golden_file,
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_marketplace_with_filtering(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """Test marketplace with listing pattern filtering"""
    pytestconfig.rootpath / "tests/integration/snowflake"

    output_file = tmp_path / "snowflake_marketplace_filtered_test_events.json"

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        sf_cursor.execute.side_effect = default_query_results

        pipeline_config_dict = {
            "source": {
                "type": "snowflake",
                "config": {
                    "username": "user",
                    "password": "password",
                    "account_id": "ABC12345",
                    "warehouse": "COMPUTE_WH",
                    "role": "datahub_role",
                    "database_pattern": {"allow": ["TEST_DB"]},
                    # Enable marketplace with filtering
                    "include_marketplace_listings": True,
                    "include_marketplace_purchases": True,
                    "include_marketplace_usage": False,  # Disable usage for this test
                    "marketplace_listing_pattern": {
                        "allow": ["ACME_CORP.*"],
                        "deny": [".*WEATHER.*"],
                    },
                    # Disable other features
                    "include_table_lineage": False,
                    "include_usage_stats": False,
                    "include_operational_stats": False,
                    "include_queries": False,  # Disable query entity generation
                    "use_queries_v2": False,  # Disable queries v2
                    "profiling": {"enabled": False},
                    "stateful_ingestion": {"enabled": False},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }

        pipeline = Pipeline.create(pipeline_config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the report
        report = cast(SnowflakeV2Report, pipeline.source.get_report())

        # Only ACME_CORP listing should be ingested (Weather one is denied)
        assert report.marketplace_listings_scanned == 1
        assert report.marketplace_purchases_scanned == 1
        assert report.marketplace_data_products_created == 1
        assert report.marketplace_enhanced_datasets == 1
        # Usage disabled in this test
        assert report.marketplace_usage_events_processed == 0
