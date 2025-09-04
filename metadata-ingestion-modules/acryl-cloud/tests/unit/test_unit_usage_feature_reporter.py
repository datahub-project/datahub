from typing import Any, Dict, List
from unittest.mock import Mock

import polars as pl
import pytest

from acryl_datahub_cloud.datahub_usage_reporting.excluded import EXCLUDED_PATTERNS
from acryl_datahub_cloud.datahub_usage_reporting.usage_feature_reporter import (
    DataHubUsageFeatureReportingSource,
    DataHubUsageFeatureReportingSourceConfig,
)
from datahub.ingestion.api.common import PipelineContext


def create_test_source(use_exp_cdf: bool = True) -> DataHubUsageFeatureReportingSource:
    """Create a DataHubUsageFeatureReportingSource instance for testing."""
    config = DataHubUsageFeatureReportingSourceConfig(
        dashboard_usage_enabled=False,
        chart_usage_enabled=False,
        dataset_usage_enabled=True,
        user_usage_enabled=True,
        stateful_ingestion=None,
        server=None,
        query_timeout=10,
        extract_batch_size=500,
        extract_delay=0.25,
        lookback_days=30,
        use_exp_cdf=use_exp_cdf,
        sibling_usage_enabled=False,
        streaming_mode=False,
        experimental_full_streaming=False,
        use_server_side_aggregation=True,
        disable_write_usage=False,
        set_upstream_table_max_modification_time_for_views=True,
        generate_patch=False,
        excluded_platforms=EXCLUDED_PATTERNS,
    )
    ctx = Mock(spec=PipelineContext)
    ctx.graph = None
    return DataHubUsageFeatureReportingSource(ctx, config)


class TestUserDatasetUsageMap:
    """Test class for _create_user_dataset_usage_map function."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=True)

    @pytest.fixture
    def sample_users_data(self) -> pl.LazyFrame:
        """Create sample user usage data for testing."""
        data = [
            # User 1 with multiple datasets
            {
                "user": "user1@company.com",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 100,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset2",
                "platform": "bigquery",
                "count": 80,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset3",
                "platform": "snowflake",
                "count": 60,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset4",
                "platform": "postgres",
                "count": 40,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset5",
                "platform": "mysql",
                "count": 20,
            },
            # User 2 with different usage patterns
            {
                "user": "user2@domain.org",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 150,
            },
            {
                "user": "user2@domain.org",
                "urn": "dataset6",
                "platform": "bigquery",
                "count": 90,
            },
            {
                "user": "user2@domain.org",
                "urn": "dataset7",
                "platform": "snowflake",
                "count": 30,
            },
            # User 3 with single dataset
            {
                "user": "user3@example.net",
                "urn": "dataset8",
                "platform": "redshift",
                "count": 75,
            },
        ]
        return pl.LazyFrame(data)

    @pytest.fixture
    def large_dataset_users_data(self) -> pl.LazyFrame:
        """Create sample data with more than 25 datasets for a user to test top_n filtering."""
        data = []
        platforms = ["snowflake", "bigquery", "postgres", "mysql", "redshift"]
        # Create 30 datasets for user1 with decreasing usage counts
        for i in range(30):
            data.append(
                {
                    "user": "user1@company.com",
                    "urn": f"dataset{i + 1}",
                    "platform": platforms[i % len(platforms)],
                    "count": 100 - i,
                }
            )
        # Add a few datasets for user2
        for i in range(5):
            data.append(
                {
                    "user": "user2@domain.org",
                    "urn": f"dataset{i + 1}",
                    "platform": platforms[i % len(platforms)],
                    "count": 50 - i * 5,
                }
            )
        return pl.LazyFrame(data)

    def test_basic_functionality(
        self,
        source: DataHubUsageFeatureReportingSource,
        sample_users_data: pl.LazyFrame,
    ) -> None:
        """Test basic functionality of _create_user_dataset_usage_map."""
        result_lf = source._create_user_dataset_usage_map(sample_users_data)
        result_df = result_lf.collect()

        # Check that we have the expected number of users
        assert len(result_df) == 3

        # Check columns exist
        expected_columns = {
            "user",
            "top_datasets_map",
            "userUsageTotalPast30Days",
            "platform_usage_pairs",
        }
        assert set(result_df.columns) == expected_columns

        # Convert to list of dicts for easier testing
        result_list = result_df.to_dicts()

        # Check user1's top datasets (should be ordered by count descending)
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        user1_datasets = user1_row["top_datasets_map"]

        assert len(user1_datasets) == 5  # user1 has 5 datasets
        assert user1_datasets[0]["dataset_urn"] == "dataset1"
        assert user1_datasets[0]["count"] == 100
        assert user1_datasets[0]["platform_urn"] == "urn:li:dataPlatform:snowflake"
        assert user1_datasets[1]["dataset_urn"] == "dataset2"
        assert user1_datasets[1]["count"] == 80
        assert user1_datasets[1]["platform_urn"] == "urn:li:dataPlatform:bigquery"
        assert user1_datasets[4]["dataset_urn"] == "dataset5"
        assert user1_datasets[4]["count"] == 20
        assert user1_datasets[4]["platform_urn"] == "urn:li:dataPlatform:mysql"

        # Check user1's total usage (100 + 80 + 60 + 40 + 20 = 300)
        assert user1_row["userUsageTotalPast30Days"] == 300

        # Check user1's platform usage totals
        user1_platform_pairs = user1_row["platform_usage_pairs"]

        # Convert pairs to dict for easier assertion
        user1_platform_totals = {
            pair["platform_urn"]: pair["platform_total"]
            for pair in user1_platform_pairs
        }
        expected_platform_totals = {
            "urn:li:dataPlatform:snowflake": 160.0,  # 100 + 60
            "urn:li:dataPlatform:bigquery": 80.0,
            "urn:li:dataPlatform:postgres": 40.0,
            "urn:li:dataPlatform:mysql": 20.0,
        }
        assert user1_platform_totals == expected_platform_totals

        # Check user2's top datasets
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )
        user2_datasets = user2_row["top_datasets_map"]

        assert len(user2_datasets) == 3  # user2 has 3 datasets
        assert user2_datasets[0]["dataset_urn"] == "dataset1"
        assert user2_datasets[0]["count"] == 150
        assert user2_datasets[0]["platform_urn"] == "urn:li:dataPlatform:snowflake"
        assert user2_datasets[1]["dataset_urn"] == "dataset6"
        assert user2_datasets[1]["count"] == 90
        assert user2_datasets[1]["platform_urn"] == "urn:li:dataPlatform:bigquery"

        # Check user2's total usage (150 + 90 + 30 = 270)
        assert user2_row["userUsageTotalPast30Days"] == 270

        # Check user2's platform usage totals
        user2_platform_pairs = user2_row["platform_usage_pairs"]

        # Convert pairs to dict for easier assertion
        user2_platform_totals = {
            pair["platform_urn"]: pair["platform_total"]
            for pair in user2_platform_pairs
        }
        expected_platform_totals = {
            "urn:li:dataPlatform:snowflake": 180.0,  # 150 + 30
            "urn:li:dataPlatform:bigquery": 90.0,
        }
        assert user2_platform_totals == expected_platform_totals

        # Check user3's top datasets
        user3_row = next(
            row for row in result_list if row["user"] == "user3@example.net"
        )
        user3_datasets = user3_row["top_datasets_map"]

        assert len(user3_datasets) == 1  # user3 has 1 dataset
        assert user3_datasets[0]["dataset_urn"] == "dataset8"
        assert user3_datasets[0]["count"] == 75
        assert user3_datasets[0]["platform_urn"] == "urn:li:dataPlatform:redshift"

        # Check user3's total usage (75)
        assert user3_row["userUsageTotalPast30Days"] == 75

        # Check user3's platform usage totals
        user3_platform_pairs = user3_row["platform_usage_pairs"]

        # Convert pairs to dict for easier assertion
        user3_platform_totals = {
            pair["platform_urn"]: pair["platform_total"]
            for pair in user3_platform_pairs
        }
        expected_platform_totals = {
            "urn:li:dataPlatform:redshift": 75.0,
        }
        assert user3_platform_totals == expected_platform_totals

    def test_top_n_filtering(
        self,
        source: DataHubUsageFeatureReportingSource,
        large_dataset_users_data: pl.LazyFrame,
    ) -> None:
        """Test that top_n parameter correctly limits the number of datasets returned."""
        # Test with default top_n=25
        result_lf = source._create_user_dataset_usage_map(large_dataset_users_data)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Check user1 (has 30 datasets but should only get top 25)
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        user1_datasets = user1_row["top_datasets_map"]

        assert len(user1_datasets) == 25
        # First dataset should be dataset1 with count 100
        assert user1_datasets[0]["dataset_urn"] == "dataset1"
        assert user1_datasets[0]["count"] == 100
        # Last dataset should be dataset25 with count 76
        assert user1_datasets[24]["dataset_urn"] == "dataset25"
        assert user1_datasets[24]["count"] == 76

        # Check user2 (has 5 datasets, should get all 5)
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )
        user2_datasets = user2_row["top_datasets_map"]

        assert len(user2_datasets) == 5

        # Check user totals
        # user1 total: sum of counts 100 down to 71 = 30 * (100 + 71) / 2 = 2565
        assert user1_row["userUsageTotalPast30Days"] == 2565
        # user2 total: 50 + 45 + 40 + 35 + 30 = 200
        assert user2_row["userUsageTotalPast30Days"] == 200

    def test_custom_top_n(
        self,
        source: DataHubUsageFeatureReportingSource,
        large_dataset_users_data: pl.LazyFrame,
    ) -> None:
        """Test custom top_n parameter."""
        # Test with top_n=3
        result_lf = source._create_user_dataset_usage_map(
            large_dataset_users_data, top_n=3
        )
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Check user1 (should only get top 3)
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        user1_datasets = user1_row["top_datasets_map"]

        assert len(user1_datasets) == 3
        assert user1_datasets[0]["dataset_urn"] == "dataset1"
        assert user1_datasets[0]["count"] == 100
        assert user1_datasets[2]["dataset_urn"] == "dataset3"
        assert user1_datasets[2]["count"] == 98

        # Check that total usage is still for all datasets (not just top 3)
        assert user1_row["userUsageTotalPast30Days"] == 2565

    def test_duplicate_user_dataset_aggregation(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that duplicate user-dataset combinations are properly aggregated."""
        # Create data with duplicate user-dataset combinations
        data = [
            {
                "user": "user1@company.com",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 50,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 30,
            },  # duplicate
            {
                "user": "user1@company.com",
                "urn": "dataset2",
                "platform": "bigquery",
                "count": 40,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset2",
                "platform": "bigquery",
                "count": 10,
            },  # duplicate
        ]
        users_lf = pl.LazyFrame(data)

        result_lf = source._create_user_dataset_usage_map(users_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        user1_row = result_list[0]
        user1_datasets = user1_row["top_datasets_map"]

        assert len(user1_datasets) == 2
        # dataset1 should have aggregated count of 80 (50 + 30)
        assert user1_datasets[0]["dataset_urn"] == "dataset1"
        assert user1_datasets[0]["count"] == 80
        # dataset2 should have aggregated count of 50 (40 + 10)
        assert user1_datasets[1]["dataset_urn"] == "dataset2"
        assert user1_datasets[1]["count"] == 50

        # Check total usage (80 + 50 = 130)
        assert user1_row["userUsageTotalPast30Days"] == 130

        # Check platform usage totals
        user1_platform_pairs = user1_row["platform_usage_pairs"]

        # Convert pairs to dict for easier assertion
        user1_platform_totals = {
            pair["platform_urn"]: pair["platform_total"]
            for pair in user1_platform_pairs
        }
        expected_platform_totals = {
            "urn:li:dataPlatform:snowflake": 80.0,
            "urn:li:dataPlatform:bigquery": 50.0,
        }
        assert user1_platform_totals == expected_platform_totals

    def test_empty_input(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test handling of empty input data."""
        empty_lf = pl.LazyFrame(
            [],
            schema={
                "user": pl.String,
                "urn": pl.String,
                "platform": pl.String,
                "count": pl.Int64,
            },
        )

        result_lf = source._create_user_dataset_usage_map(empty_lf)
        result_df = result_lf.collect()

        assert len(result_df) == 0
        expected_columns = {
            "user",
            "top_datasets_map",
            "userUsageTotalPast30Days",
            "platform_usage_pairs",
        }
        assert set(result_df.columns) == expected_columns

    def test_sorting_by_count(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test that datasets are properly sorted by count in descending order."""
        data = [
            {
                "user": "user1@company.com",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 10,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset2",
                "platform": "bigquery",
                "count": 100,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset3",
                "platform": "postgres",
                "count": 50,
            },
            {
                "user": "user1@company.com",
                "urn": "dataset4",
                "platform": "mysql",
                "count": 75,
            },
        ]
        users_lf = pl.LazyFrame(data)

        result_lf = source._create_user_dataset_usage_map(users_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        user1_datasets = result_list[0]["top_datasets_map"]

        # Check that datasets are sorted by count descending
        assert user1_datasets[0]["dataset_urn"] == "dataset2"  # count: 100
        assert user1_datasets[0]["count"] == 100
        assert user1_datasets[1]["dataset_urn"] == "dataset4"  # count: 75
        assert user1_datasets[1]["count"] == 75
        assert user1_datasets[2]["dataset_urn"] == "dataset3"  # count: 50
        assert user1_datasets[2]["count"] == 50
        assert user1_datasets[3]["dataset_urn"] == "dataset1"  # count: 10
        assert user1_datasets[3]["count"] == 10

        # Check total usage (100 + 75 + 50 + 10 = 235)
        user1_row = result_list[0]
        assert user1_row["userUsageTotalPast30Days"] == 235

    def test_filter_users_without_at_symbol(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that users without '@' symbol are filtered out."""
        data = [
            # Valid users with '@' symbol
            {
                "user": "user1@company.com",
                "urn": "dataset1",
                "platform": "snowflake",
                "count": 100,
            },
            {
                "user": "user2@domain.org",
                "urn": "dataset2",
                "platform": "bigquery",
                "count": 80,
            },
            # Invalid users without '@' symbol - these should be filtered out
            {
                "user": "service_account",
                "urn": "dataset3",
                "platform": "postgres",
                "count": 60,
            },
            {
                "user": "system_user",
                "urn": "dataset4",
                "platform": "mysql",
                "count": 40,
            },
            {
                "user": "anonymous",
                "urn": "dataset5",
                "platform": "redshift",
                "count": 20,
            },
        ]
        users_lf = pl.LazyFrame(data)

        result_lf = source._create_user_dataset_usage_map(users_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Should only have 2 users (those with '@' symbol)
        assert len(result_list) == 2

        # Check that only valid users are present
        user_names = {row["user"] for row in result_list}
        assert "user1@company.com" in user_names
        assert "user2@domain.org" in user_names
        assert "service_account" not in user_names
        assert "system_user" not in user_names
        assert "anonymous" not in user_names

        # Check the data for valid users
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )

        assert user1_row["userUsageTotalPast30Days"] == 100
        assert user2_row["userUsageTotalPast30Days"] == 80

        assert len(user1_row["top_datasets_map"]) == 1
        assert user1_row["top_datasets_map"][0]["dataset_urn"] == "dataset1"
        assert user1_row["top_datasets_map"][0]["count"] == 100

        assert len(user2_row["top_datasets_map"]) == 1
        assert user2_row["top_datasets_map"][0]["dataset_urn"] == "dataset2"
        assert user2_row["top_datasets_map"][0]["count"] == 80


class TestConvertTopDatasetsToDict:
    """Test cases for _convert_top_datasets_to_dict helper function."""

    def test_convert_top_datasets_to_dict_valid_data(self) -> None:
        """Test conversion of valid top datasets list to dictionary."""
        top_datasets_list = [
            {
                "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table1,PROD)",
                "count": 10,
            },
            {
                "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table2,PROD)",
                "count": 5,
            },
            {
                "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
                "count": 8,
            },
        ]

        result = DataHubUsageFeatureReportingSource._convert_top_datasets_to_dict(
            top_datasets_list
        )

        expected = {
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table1,PROD)": 10.0,
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table2,PROD)": 5.0,
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)": 8.0,
        }

        assert result == expected

    def test_convert_top_datasets_to_dict_empty_list(self) -> None:
        """Test conversion of empty list returns None."""
        result = DataHubUsageFeatureReportingSource._convert_top_datasets_to_dict([])
        assert result is None

    def test_convert_top_datasets_to_dict_none_input(self) -> None:
        """Test conversion of None input returns None."""
        result = DataHubUsageFeatureReportingSource._convert_top_datasets_to_dict(None)
        assert result is None


class TestGenRankAndPercentile:
    """Test cases for gen_rank_and_percentile method."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=False)

    def test_basic_rank_and_percentile(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test basic rank and percentile calculation."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 80},
            {"urn": "dataset3", "platform": "snowflake", "count": 60},
            {"urn": "dataset4", "platform": "snowflake", "count": 40},
            {"urn": "dataset5", "platform": "snowflake", "count": 20},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix="test_")
        result_df = result_lf.collect()

        # Check that rank and percentile columns are added
        assert "test_rank" in result_df.columns
        assert "test_rank_percentile" in result_df.columns

        result_list = result_df.to_dicts()

        # Check ranks (should be 1-5 for descending counts)
        dataset1_row = next(row for row in result_list if row["urn"] == "dataset1")
        dataset2_row = next(row for row in result_list if row["urn"] == "dataset2")
        dataset5_row = next(row for row in result_list if row["urn"] == "dataset5")

        assert dataset1_row["test_rank"] == 1  # Highest count
        assert dataset2_row["test_rank"] == 2  # Second highest
        assert dataset5_row["test_rank"] == 5  # Lowest count

        # Check percentiles (100% for rank 1, 0% for rank 5)
        assert dataset1_row["test_rank_percentile"] == 100.0  # Top rank
        assert dataset5_row["test_rank_percentile"] == 0.0  # Bottom rank
        assert dataset2_row["test_rank_percentile"] == 75.0  # Second from top

    def test_multiple_platforms(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test rank and percentile calculation across multiple platforms."""
        data = [
            # Snowflake platform
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 50},
            # BigQuery platform
            {"urn": "dataset3", "platform": "bigquery", "count": 200},
            {"urn": "dataset4", "platform": "bigquery", "count": 150},
            {"urn": "dataset5", "platform": "bigquery", "count": 100},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix="test_")
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Check Snowflake rankings (independent of BigQuery)
        snowflake_rows = [row for row in result_list if row["platform"] == "snowflake"]
        dataset1_row = next(row for row in snowflake_rows if row["urn"] == "dataset1")
        dataset2_row = next(row for row in snowflake_rows if row["urn"] == "dataset2")

        assert dataset1_row["test_rank"] == 1  # Best in snowflake
        assert dataset2_row["test_rank"] == 2  # Second in snowflake
        assert dataset1_row["test_rank_percentile"] == 100.0
        assert dataset2_row["test_rank_percentile"] == 0.0  # Only 2 items

        # Check BigQuery rankings (independent of Snowflake)
        bigquery_rows = [row for row in result_list if row["platform"] == "bigquery"]
        dataset3_row = next(row for row in bigquery_rows if row["urn"] == "dataset3")
        dataset4_row = next(row for row in bigquery_rows if row["urn"] == "dataset4")
        dataset5_row = next(row for row in bigquery_rows if row["urn"] == "dataset5")

        assert dataset3_row["test_rank"] == 1  # Best in bigquery
        assert dataset4_row["test_rank"] == 2  # Second in bigquery
        assert dataset5_row["test_rank"] == 3  # Third in bigquery
        assert dataset3_row["test_rank_percentile"] == 100.0
        assert dataset4_row["test_rank_percentile"] == 50.0
        assert dataset5_row["test_rank_percentile"] == 0.0

    def test_zero_counts(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test handling of zero counts."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 50},
            {"urn": "dataset3", "platform": "snowflake", "count": 0},
            {"urn": "dataset4", "platform": "snowflake", "count": 0},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix="test_")
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Zero counts should get 0 percentile regardless of rank
        zero_count_rows = [row for row in result_list if row["count"] == 0]
        for row in zero_count_rows:
            assert row["test_rank_percentile"] == 0.0

        # Non-zero counts should still get proper percentiles
        dataset1_row = next(row for row in result_list if row["urn"] == "dataset1")
        dataset2_row = next(row for row in result_list if row["urn"] == "dataset2")

        assert dataset1_row["test_rank_percentile"] == 100.0
        assert dataset2_row["test_rank_percentile"] == 66.66666666666667  # 2/3 * 100

    def test_single_item_platform(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test handling of platform with single item."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "bigquery", "count": 50},  # Single item
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix="test_")
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Single items should get 100% percentile (top of their platform)
        dataset1_row = next(row for row in result_list if row["urn"] == "dataset1")
        dataset2_row = next(row for row in result_list if row["urn"] == "dataset2")

        assert dataset1_row["test_rank"] == 1
        assert dataset1_row["test_rank_percentile"] == 100.0
        assert dataset2_row["test_rank"] == 1
        assert dataset2_row["test_rank_percentile"] == 100.0

    def test_custom_field_names(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test with custom field names."""
        data = [
            {"entity_id": "dataset1", "data_platform": "snowflake", "usage_count": 100},
            {"entity_id": "dataset2", "data_platform": "snowflake", "usage_count": 50},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(
            lf,
            count_field="usage_count",
            urn_field="entity_id",
            platform_field="data_platform",
            prefix="custom_",
        )
        result_df = result_lf.collect()

        # Check that custom field names work
        assert "custom_rank" in result_df.columns
        assert "custom_rank_percentile" in result_df.columns

        result_list = result_df.to_dicts()
        dataset1_row = next(
            row for row in result_list if row["entity_id"] == "dataset1"
        )
        dataset2_row = next(
            row for row in result_list if row["entity_id"] == "dataset2"
        )

        assert dataset1_row["custom_rank"] == 1
        assert dataset1_row["custom_rank_percentile"] == 100.0
        assert dataset2_row["custom_rank"] == 2
        assert dataset2_row["custom_rank_percentile"] == 0.0

    def test_no_prefix(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test with no prefix (None)."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 50},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix=None)
        result_df = result_lf.collect()

        # Check that columns are created with "None" prefix (this is the actual behavior)
        assert "Nonerank" in result_df.columns
        assert "Nonerank_percentile" in result_df.columns

    def test_duplicate_counts_max_rank(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test handling of duplicate counts with max rank method."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 100},  # Tie
            {"urn": "dataset3", "platform": "snowflake", "count": 50},
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.gen_rank_and_percentile(lf, "count", prefix="test_")
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # With method="max", tied items get the maximum rank in their tie group
        # So both 100s should get rank 2, and 50 gets rank 3
        dataset1_row = next(row for row in result_list if row["urn"] == "dataset1")
        dataset2_row = next(row for row in result_list if row["urn"] == "dataset2")
        dataset3_row = next(row for row in result_list if row["urn"] == "dataset3")

        assert dataset1_row["test_rank"] == 2  # max rank for tied values
        assert dataset2_row["test_rank"] == 2  # max rank for tied values
        assert dataset3_row["test_rank"] == 3

    def test_use_exp_cdf_override(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test overriding use_exp_cdf parameter."""
        data = [
            {"urn": "dataset1", "platform": "snowflake", "count": 100},
            {"urn": "dataset2", "platform": "snowflake", "count": 50},
        ]
        lf = pl.LazyFrame(data)

        # Override to use exp_cdf (even though config has it as False)
        result_lf = source.gen_rank_and_percentile(
            lf, "count", prefix="test_", use_exp_cdf=True
        )
        result_df = result_lf.collect()

        # Should still create the columns (exp_cdf function will be called)
        assert "test_rank" in result_df.columns
        assert "test_rank_percentile" in result_df.columns

    def test_empty_dataframe(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test handling of empty dataframe."""
        empty_lf = pl.LazyFrame(
            [],
            schema={
                "urn": pl.String,
                "platform": pl.String,
                "count": pl.Int64,
            },
        )

        result_lf = source.gen_rank_and_percentile(empty_lf, "count", prefix="test_")
        result_df = result_lf.collect()

        # Should have the new columns but no rows
        assert "test_rank" in result_df.columns
        assert "test_rank_percentile" in result_df.columns
        assert len(result_df) == 0


class TestConvertPlatformPairsToDict:
    """Test cases for _convert_platform_pairs_to_dict helper function."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=True)

    def test_convert_valid_platform_pairs(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test conversion of valid platform usage pairs to dictionary."""
        platform_pairs = [
            {"platform_urn": "urn:li:dataPlatform:snowflake", "platform_total": 150.0},
            {"platform_urn": "urn:li:dataPlatform:bigquery", "platform_total": 100.0},
            {"platform_urn": "urn:li:dataPlatform:postgres", "platform_total": 75.0},
        ]

        result = source._convert_platform_pairs_to_dict(platform_pairs)

        expected = {
            "urn:li:dataPlatform:snowflake": 150.0,
            "urn:li:dataPlatform:bigquery": 100.0,
            "urn:li:dataPlatform:postgres": 75.0,
        }

        assert result == expected

    def test_convert_empty_list(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test conversion of empty list returns None."""
        result = source._convert_platform_pairs_to_dict([])
        assert result is None

    def test_convert_none_input(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test conversion of None input returns None."""
        result = source._convert_platform_pairs_to_dict(None)
        assert result is None

    def test_filter_null_platform_urns(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that entries with None platform_urn are filtered out."""
        platform_pairs: List[Dict[str, Any]] = [
            {"platform_urn": "urn:li:dataPlatform:snowflake", "platform_total": 150.0},
            {"platform_urn": None, "platform_total": 100.0},  # Should be filtered
            {"platform_urn": "urn:li:dataPlatform:postgres", "platform_total": 75.0},
        ]

        result = source._convert_platform_pairs_to_dict(platform_pairs)

        expected = {
            "urn:li:dataPlatform:snowflake": 150.0,
            "urn:li:dataPlatform:postgres": 75.0,
        }

        assert result == expected

    def test_single_platform(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test conversion with single platform entry."""
        platform_pairs = [
            {"platform_urn": "urn:li:dataPlatform:snowflake", "platform_total": 200.0}
        ]

        result = source._convert_platform_pairs_to_dict(platform_pairs)

        expected = {"urn:li:dataPlatform:snowflake": 200.0}

        assert result == expected

    def test_all_platform_urns_null(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that when all platform_urns are None, returns empty dict."""
        platform_pairs = [
            {"platform_urn": None, "platform_total": 100.0},
            {"platform_urn": None, "platform_total": 50.0},
        ]

        result = source._convert_platform_pairs_to_dict(platform_pairs)
        assert result == {}

    def test_mixed_data_types(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test conversion with mixed data types for platform_total."""
        platform_pairs = [
            {
                "platform_urn": "urn:li:dataPlatform:snowflake",
                "platform_total": 150,
            },  # int
            {
                "platform_urn": "urn:li:dataPlatform:bigquery",
                "platform_total": 100.5,
            },  # float
        ]

        result = source._convert_platform_pairs_to_dict(platform_pairs)

        expected = {
            "urn:li:dataPlatform:snowflake": 150,
            "urn:li:dataPlatform:bigquery": 100.5,
        }

        assert result == expected

    def test_convert_with_custom_value_key(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test conversion with custom value_key parameter."""
        platform_percentiles = [
            {
                "platform_urn": "urn:li:dataPlatform:snowflake",
                "platform_rank_percentile": 85.5,
            },
            {
                "platform_urn": "urn:li:dataPlatform:bigquery",
                "platform_rank_percentile": 92.3,
            },
            {
                "platform_urn": "urn:li:dataPlatform:postgres",
                "platform_rank_percentile": 67.8,
            },
        ]

        # Test with custom value_key
        result = source._convert_platform_pairs_to_dict(
            platform_percentiles, value_key="platform_rank_percentile"
        )
        expected = {
            "urn:li:dataPlatform:snowflake": 85.5,
            "urn:li:dataPlatform:bigquery": 92.3,
            "urn:li:dataPlatform:postgres": 67.8,
        }
        assert result == expected

        # Test that default value_key still works with platform_total
        platform_totals = [
            {
                "platform_urn": "urn:li:dataPlatform:snowflake",
                "platform_total": 150.0,
            }
        ]
        result_default = source._convert_platform_pairs_to_dict(platform_totals)
        expected_default = {"urn:li:dataPlatform:snowflake": 150.0}
        assert result_default == expected_default


class TestAddPlatformUsagePercentiles:
    """Test cases for add_platform_usage_percentiles method."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=False)

    @pytest.fixture
    def sample_user_usage_data(self) -> pl.LazyFrame:
        """Create sample user usage data with platform_usage_pairs."""
        data = [
            {
                "user": "user1@company.com",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset1",
                        "count": 100,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    }
                ],
                "userUsageTotalPast30Days": 300,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 200.0,
                    },
                    {
                        "platform_urn": "urn:li:dataPlatform:bigquery",
                        "platform_total": 100.0,
                    },
                ],
            },
            {
                "user": "user2@domain.org",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset2",
                        "count": 150,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    }
                ],
                "userUsageTotalPast30Days": 250,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 150.0,
                    },
                    {
                        "platform_urn": "urn:li:dataPlatform:postgres",
                        "platform_total": 100.0,
                    },
                ],
            },
            {
                "user": "user3@example.net",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset3",
                        "count": 75,
                        "platform_urn": "urn:li:dataPlatform:redshift",
                    }
                ],
                "userUsageTotalPast30Days": 175,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                    {
                        "platform_urn": "urn:li:dataPlatform:bigquery",
                        "platform_total": 75.0,
                    },
                ],
            },
        ]
        return pl.LazyFrame(data)

    def test_basic_percentile_calculation(
        self,
        source: DataHubUsageFeatureReportingSource,
        sample_user_usage_data: pl.LazyFrame,
    ) -> None:
        """Test basic platform usage percentile calculation."""
        result_lf = source.add_platform_usage_percentiles(sample_user_usage_data)
        result_df = result_lf.collect()

        # Check that the new column is added
        assert "platform_usage_percentiles" in result_df.columns
        assert len(result_df) == 3

        result_list = result_df.to_dicts()

        # Check user1's platform percentiles
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        user1_percentiles = user1_row["platform_usage_percentiles"]

        # Convert to dict for easier assertion
        user1_percentile_dict = {
            pair["platform_urn"]: pair["platform_rank_percentile"]
            for pair in user1_percentiles
        }

        # For snowflake: user1=200, user2=150, user3=100 -> user1 should have 100% (highest)
        # For bigquery: user1=100, user3=75 -> user1 should have 100% (highest)
        assert user1_percentile_dict["urn:li:dataPlatform:snowflake"] == 100.0
        assert user1_percentile_dict["urn:li:dataPlatform:bigquery"] == 100.0

        # Check user2's platform percentiles
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )
        user2_percentiles = user2_row["platform_usage_percentiles"]

        user2_percentile_dict = {
            pair["platform_urn"]: pair["platform_rank_percentile"]
            for pair in user2_percentiles
        }

        # For snowflake: user1=200, user2=150, user3=100 -> user2 should have 50% (middle)
        # For postgres: only user2=100 -> user2 should have 100% (only user)
        assert user2_percentile_dict["urn:li:dataPlatform:snowflake"] == 50.0
        assert user2_percentile_dict["urn:li:dataPlatform:postgres"] == 100.0

        # Check user3's platform percentiles
        user3_row = next(
            row for row in result_list if row["user"] == "user3@example.net"
        )
        user3_percentiles = user3_row["platform_usage_percentiles"]

        user3_percentile_dict = {
            pair["platform_urn"]: pair["platform_rank_percentile"]
            for pair in user3_percentiles
        }

        # For snowflake: user1=200, user2=150, user3=100 -> user3 should have 0% (lowest)
        # For bigquery: user1=100, user3=75 -> user3 should have 0% (lowest)
        assert user3_percentile_dict["urn:li:dataPlatform:snowflake"] == 0.0
        assert user3_percentile_dict["urn:li:dataPlatform:bigquery"] == 0.0

    def test_single_user_per_platform(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that single users on a platform get 100% percentile."""
        data = [
            {
                "user": "user1@company.com",
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:unique1",
                        "platform_total": 100.0,
                    },
                ],
            },
            {
                "user": "user2@domain.org",
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:unique2",
                        "platform_total": 50.0,
                    },
                ],
            },
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.add_platform_usage_percentiles(lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Both users should have 100% percentile since they are the only users on their platforms
        for row in result_list:
            percentiles = row["platform_usage_percentiles"]
            for percentile_pair in percentiles:
                assert percentile_pair["platform_rank_percentile"] == 100.0

    def test_empty_platform_usage_pairs(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test handling of users with no platform usage pairs."""
        data = [
            {
                "user": "user1@company.com",
                "platform_usage_pairs": [],
            },
            {
                "user": "user2@domain.org",
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                ],
            },
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.add_platform_usage_percentiles(lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # user1 should have no percentiles (left join should keep the row but with null percentiles)
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )
        assert user1_row["platform_usage_percentiles"] is None

        # user2 should have percentiles
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )
        assert user2_row["platform_usage_percentiles"] is not None
        assert len(user2_row["platform_usage_percentiles"]) == 1

    def test_null_platform_urns_filtered(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that null platform URNs are filtered out."""
        data = [
            {
                "user": "user1@company.com",
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                    {
                        "platform_urn": None,
                        "platform_total": 50.0,
                    },  # Should be filtered
                ],
            },
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.add_platform_usage_percentiles(lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        user1_percentiles = result_list[0]["platform_usage_percentiles"]

        # Should only have one percentile entry (null platform_urn filtered out)
        assert len(user1_percentiles) == 1
        assert user1_percentiles[0]["platform_urn"] == "urn:li:dataPlatform:snowflake"

    def test_original_data_preserved(
        self,
        source: DataHubUsageFeatureReportingSource,
        sample_user_usage_data: pl.LazyFrame,
    ) -> None:
        """Test that original columns are preserved in the result."""
        result_lf = source.add_platform_usage_percentiles(sample_user_usage_data)
        result_df = result_lf.collect()

        # Check that all original columns are preserved
        original_columns = set(sample_user_usage_data.collect().columns)
        result_columns = set(result_df.columns)

        assert original_columns.issubset(result_columns)
        assert "platform_usage_percentiles" in result_columns

        # Check that original data is unchanged
        result_list = result_df.to_dicts()
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )

        assert user1_row["userUsageTotalPast30Days"] == 300
        assert len(user1_row["platform_usage_pairs"]) == 2

    def test_percentile_structure(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that the percentile structure has the correct fields."""
        data = [
            {
                "user": "user1@company.com",
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                ],
            },
        ]
        lf = pl.LazyFrame(data)

        result_lf = source.add_platform_usage_percentiles(lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        percentiles = result_list[0]["platform_usage_percentiles"]

        # Check structure of percentile entries
        assert isinstance(percentiles, list)
        assert len(percentiles) == 1

        percentile_entry = percentiles[0]
        assert "platform_urn" in percentile_entry
        assert "platform_rank_percentile" in percentile_entry
        assert isinstance(percentile_entry["platform_rank_percentile"], float)


class TestFilterUsers:
    """Test cases for _filter_users method."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=False)

    def test_filter_users_basic(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test basic filtering of users with @ symbol."""
        data = [
            {"user": "valid_user@company.com"},
            {"user": "another.user@domain.org"},
            {"user": "invalid_user_no_at"},
            {"user": "service_account"},
        ]
        users_lf = pl.LazyFrame(data)

        result_lf = source._filter_users(users_lf)
        result_df = result_lf.collect()
        result_users = [row["user"] for row in result_df.to_dicts()]

        # Should only include users with @ symbol
        assert "valid_user@company.com" in result_users
        assert "another.user@domain.org" in result_users
        assert "invalid_user_no_at" not in result_users
        assert "service_account" not in result_users

    def test_filter_users_excluded_patterns(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test filtering of users matching excluded patterns."""
        data = [
            {"user": "real_user@company.com"},
            {
                "user": "system@company.com"
            },  # Should be excluded if 'system' is in patterns
            {
                "user": "service@company.com"
            },  # Should be excluded if 'service' is in patterns
        ]
        users_lf = pl.LazyFrame(data)

        result_lf = source._filter_users(users_lf)
        result_df = result_lf.collect()
        result_users = [row["user"] for row in result_df.to_dicts()]

        # Should include real user but may exclude system/service users depending on EXCLUDED_PATTERNS
        assert "real_user@company.com" in result_users
        # Note: Whether system/service users are excluded depends on the actual EXCLUDED_PATTERNS

    def test_filter_users_empty_input(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test filtering with empty input."""
        empty_lf = pl.LazyFrame([], schema={"user": pl.String})

        result_lf = source._filter_users(empty_lf)
        result_df = result_lf.collect()

        assert len(result_df) == 0
        assert "user" in result_df.columns


class TestCombineUserUsageData:
    """Test cases for _combine_user_usage_data method."""

    @pytest.fixture
    def source(self) -> DataHubUsageFeatureReportingSource:
        """Create a DataHubUsageFeatureReportingSource instance for testing."""
        return create_test_source(use_exp_cdf=False)

    @pytest.fixture
    def dataset_usage_data(self) -> pl.LazyFrame:
        """Sample dataset usage data."""
        data = [
            {
                "user": "user1@company.com",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset1",
                        "count": 100,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    },
                    {
                        "dataset_urn": "dataset2",
                        "count": 80,
                        "platform_urn": "urn:li:dataPlatform:bigquery",
                    },
                ],
                "userUsageTotalPast30Days": 180,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                    {
                        "platform_urn": "urn:li:dataPlatform:bigquery",
                        "platform_total": 80.0,
                    },
                ],
            },
            {
                "user": "user2@domain.org",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset3",
                        "count": 150,
                        "platform_urn": "urn:li:dataPlatform:postgres",
                    }
                ],
                "userUsageTotalPast30Days": 150,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:postgres",
                        "platform_total": 150.0,
                    }
                ],
            },
        ]
        return pl.LazyFrame(data)

    @pytest.fixture
    def dashboard_usage_data(self) -> pl.LazyFrame:
        """Sample dashboard usage data."""
        data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 50,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:tableau",
                        "platform_total": 50.0,
                    }
                ],
            },
            {
                "user": "user3@example.net",  # New user only in dashboards
                "userUsageTotalPast30Days": 75,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:looker",
                        "platform_total": 75.0,
                    }
                ],
            },
        ]
        return pl.LazyFrame(data)

    @pytest.fixture
    def chart_usage_data(self) -> pl.LazyFrame:
        """Sample chart usage data."""
        data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 30,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 20.0,
                    },  # Overlaps with dataset
                    {
                        "platform_urn": "urn:li:dataPlatform:tableau",
                        "platform_total": 10.0,
                    },  # Overlaps with dashboard
                ],
            },
            {
                "user": "user2@domain.org",
                "userUsageTotalPast30Days": 25,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:powerbi",
                        "platform_total": 25.0,
                    }
                ],
            },
        ]
        return pl.LazyFrame(data)

    def test_basic_combination(
        self,
        source: DataHubUsageFeatureReportingSource,
        dataset_usage_data: pl.LazyFrame,
        dashboard_usage_data: pl.LazyFrame,
        chart_usage_data: pl.LazyFrame,
    ) -> None:
        """Test basic combination of user usage data from all three sources."""
        result_lf = source._combine_user_usage_data(
            dataset_usage_data, dashboard_usage_data, chart_usage_data
        )
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Should have 3 users (user1, user2, user3)
        assert len(result_list) == 3

        # Check user1 (appears in all three sources)
        user1_row = next(
            row for row in result_list if row["user"] == "user1@company.com"
        )

        # top_datasets_map should come from dataset usage
        expected_top_datasets = [
            {
                "dataset_urn": "dataset1",
                "count": 100,
                "platform_urn": "urn:li:dataPlatform:snowflake",
            },
            {
                "dataset_urn": "dataset2",
                "count": 80,
                "platform_urn": "urn:li:dataPlatform:bigquery",
            },
        ]
        assert user1_row["top_datasets_map"] == expected_top_datasets

        # userUsageTotalPast30Days should be sum: 180 + 50 + 30 = 260
        assert user1_row["userUsageTotalPast30Days"] == 260

        # platform_usage_pairs should be aggregated union
        # snowflake: 100 (dataset) + 20 (chart) = 120
        # bigquery: 80 (dataset only) = 80
        # tableau: 50 (dashboard) + 10 (chart) = 60
        expected_platform_pairs = [
            {"platform_urn": "urn:li:dataPlatform:snowflake", "platform_total": 120.0},
            {"platform_urn": "urn:li:dataPlatform:bigquery", "platform_total": 80.0},
            {"platform_urn": "urn:li:dataPlatform:tableau", "platform_total": 60.0},
        ]
        # Sort both lists by platform_urn for consistent comparison
        actual_platform_pairs = sorted(
            user1_row["platform_usage_pairs"], key=lambda x: str(x["platform_urn"])
        )
        expected_platform_pairs = sorted(
            expected_platform_pairs, key=lambda x: str(x["platform_urn"])
        )
        assert actual_platform_pairs == expected_platform_pairs

        # Check user2 (appears in dataset and chart)
        user2_row = next(
            row for row in result_list if row["user"] == "user2@domain.org"
        )

        # Should have top_datasets_map from dataset usage
        expected_top_datasets = [
            {
                "dataset_urn": "dataset3",
                "count": 150,
                "platform_urn": "urn:li:dataPlatform:postgres",
            }
        ]
        assert user2_row["top_datasets_map"] == expected_top_datasets

        # userUsageTotalPast30Days should be sum: 150 + 0 + 25 = 175
        assert user2_row["userUsageTotalPast30Days"] == 175

        # platform_usage_pairs should include both postgres and powerbi
        expected_platform_pairs = [
            {"platform_urn": "urn:li:dataPlatform:postgres", "platform_total": 150.0},
            {"platform_urn": "urn:li:dataPlatform:powerbi", "platform_total": 25.0},
        ]
        # Sort both lists by platform_urn for consistent comparison
        actual_platform_pairs = sorted(
            user2_row["platform_usage_pairs"], key=lambda x: str(x["platform_urn"])
        )
        expected_platform_pairs = sorted(
            expected_platform_pairs, key=lambda x: str(x["platform_urn"])
        )
        assert actual_platform_pairs == expected_platform_pairs

        # Check user3 (appears only in dashboard)
        user3_row = next(
            row for row in result_list if row["user"] == "user3@example.net"
        )

        # Should have null top_datasets_map (not in dataset usage)
        assert user3_row["top_datasets_map"] is None

        # userUsageTotalPast30Days should be: 0 + 75 + 0 = 75
        assert user3_row["userUsageTotalPast30Days"] == 75

        # platform_usage_pairs should only include looker
        expected_platform_pairs = [
            {"platform_urn": "urn:li:dataPlatform:looker", "platform_total": 75.0}
        ]
        assert user3_row["platform_usage_pairs"] == expected_platform_pairs

    def test_empty_sources(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test handling of empty data sources."""
        empty_schema: Dict[str, pl.DataType] = {
            "user": pl.String(),
            "top_datasets_map": pl.List(
                pl.Struct(
                    [
                        pl.Field("dataset_urn", pl.String),
                        pl.Field("count", pl.Int64),
                        pl.Field("platform_urn", pl.String),
                    ]
                )
            ),
            "userUsageTotalPast30Days": pl.Int64(),
            "platform_usage_pairs": pl.List(
                pl.Struct(
                    [
                        pl.Field("platform_urn", pl.String),
                        pl.Field("platform_total", pl.Float64),
                    ]
                )
            ),
        }

        dashboard_chart_schema: Dict[str, pl.DataType] = {
            "user": pl.String(),
            "userUsageTotalPast30Days": pl.Int64(),
            "platform_usage_pairs": pl.List(
                pl.Struct(
                    [
                        pl.Field("platform_urn", pl.String),
                        pl.Field("platform_total", pl.Float64),
                    ]
                )
            ),
        }

        empty_dataset_lf = pl.LazyFrame([], schema=empty_schema)
        empty_dashboard_lf = pl.LazyFrame([], schema=dashboard_chart_schema)
        empty_chart_lf = pl.LazyFrame([], schema=dashboard_chart_schema)

        result_lf = source._combine_user_usage_data(
            empty_dataset_lf, empty_dashboard_lf, empty_chart_lf
        )
        result_df = result_lf.collect()

        # Should return empty dataframe with correct columns
        assert len(result_df) == 0
        expected_columns = {
            "user",
            "top_datasets_map",
            "userUsageTotalPast30Days",
            "platform_usage_pairs",
        }
        assert set(result_df.columns) == expected_columns

    def test_no_overlap_users(self, source: DataHubUsageFeatureReportingSource) -> None:
        """Test combination when users don't overlap between sources."""
        dataset_data = [
            {
                "user": "dataset_user@company.com",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset1",
                        "count": 100,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    }
                ],
                "userUsageTotalPast30Days": 100,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    }
                ],
            }
        ]

        dashboard_data = [
            {
                "user": "dashboard_user@company.com",
                "userUsageTotalPast30Days": 50,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:tableau",
                        "platform_total": 50.0,
                    }
                ],
            }
        ]

        chart_data = [
            {
                "user": "chart_user@company.com",
                "userUsageTotalPast30Days": 25,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:powerbi",
                        "platform_total": 25.0,
                    }
                ],
            }
        ]

        dataset_lf = pl.LazyFrame(dataset_data)
        dashboard_lf = pl.LazyFrame(dashboard_data)
        chart_lf = pl.LazyFrame(chart_data)

        result_lf = source._combine_user_usage_data(dataset_lf, dashboard_lf, chart_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        # Should have all 3 users
        assert len(result_list) == 3

        user_names = {row["user"] for row in result_list}
        assert "dataset_user@company.com" in user_names
        assert "dashboard_user@company.com" in user_names
        assert "chart_user@company.com" in user_names

        # Check that each user only has their own data
        dataset_user_row = next(
            row for row in result_list if row["user"] == "dataset_user@company.com"
        )
        assert dataset_user_row["userUsageTotalPast30Days"] == 100
        expected_top_datasets = [
            {
                "dataset_urn": "dataset1",
                "count": 100,
                "platform_urn": "urn:li:dataPlatform:snowflake",
            }
        ]
        assert dataset_user_row["top_datasets_map"] == expected_top_datasets

        dashboard_user_row = next(
            row for row in result_list if row["user"] == "dashboard_user@company.com"
        )
        assert dashboard_user_row["userUsageTotalPast30Days"] == 50
        assert dashboard_user_row["top_datasets_map"] is None  # No dataset usage

        chart_user_row = next(
            row for row in result_list if row["user"] == "chart_user@company.com"
        )
        assert chart_user_row["userUsageTotalPast30Days"] == 25
        assert chart_user_row["top_datasets_map"] is None  # No dataset usage

    def test_null_platform_urns_filtered(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that null platform URNs are filtered out during aggregation."""
        dataset_data = [
            {
                "user": "user1@company.com",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset1",
                        "count": 100,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    }
                ],
                "userUsageTotalPast30Days": 100,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                    {
                        "platform_urn": None,
                        "platform_total": 50.0,
                    },  # Should be filtered
                ],
            }
        ]

        dashboard_data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 25,
                "platform_usage_pairs": [
                    {"platform_urn": None, "platform_total": 25.0}  # Should be filtered
                ],
            }
        ]

        chart_data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 10,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 10.0,
                    }
                ],
            }
        ]

        dataset_lf = pl.LazyFrame(dataset_data)
        dashboard_lf = pl.LazyFrame(dashboard_data)
        chart_lf = pl.LazyFrame(chart_data)

        result_lf = source._combine_user_usage_data(dataset_lf, dashboard_lf, chart_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        assert len(result_list) == 1

        user_row = result_list[0]
        assert user_row["userUsageTotalPast30Days"] == 135  # 100 + 25 + 10

        # Should only have snowflake platform (null ones filtered out)
        expected_platform_pairs = [
            {
                "platform_urn": "urn:li:dataPlatform:snowflake",
                "platform_total": 110.0,
            }  # 100 + 10
        ]
        assert user_row["platform_usage_pairs"] == expected_platform_pairs

    def test_platform_aggregation(
        self, source: DataHubUsageFeatureReportingSource
    ) -> None:
        """Test that platforms are correctly aggregated across sources."""
        dataset_data = [
            {
                "user": "user1@company.com",
                "top_datasets_map": [
                    {
                        "dataset_urn": "dataset1",
                        "count": 100,
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                    }
                ],
                "userUsageTotalPast30Days": 100,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 100.0,
                    },
                    {
                        "platform_urn": "urn:li:dataPlatform:postgres",
                        "platform_total": 50.0,
                    },
                ],
            }
        ]

        dashboard_data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 75,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:snowflake",
                        "platform_total": 25.0,
                    },  # Overlaps
                    {
                        "platform_urn": "urn:li:dataPlatform:tableau",
                        "platform_total": 50.0,
                    },  # New platform
                ],
            }
        ]

        chart_data = [
            {
                "user": "user1@company.com",
                "userUsageTotalPast30Days": 60,
                "platform_usage_pairs": [
                    {
                        "platform_urn": "urn:li:dataPlatform:postgres",
                        "platform_total": 30.0,
                    },  # Overlaps
                    {
                        "platform_urn": "urn:li:dataPlatform:tableau",
                        "platform_total": 20.0,
                    },  # Overlaps
                    {
                        "platform_urn": "urn:li:dataPlatform:powerbi",
                        "platform_total": 10.0,
                    },  # New platform
                ],
            }
        ]

        dataset_lf = pl.LazyFrame(dataset_data)
        dashboard_lf = pl.LazyFrame(dashboard_data)
        chart_lf = pl.LazyFrame(chart_data)

        result_lf = source._combine_user_usage_data(dataset_lf, dashboard_lf, chart_lf)
        result_df = result_lf.collect()
        result_list = result_df.to_dicts()

        assert len(result_list) == 1

        user_row = result_list[0]
        assert user_row["userUsageTotalPast30Days"] == 235  # 100 + 75 + 60

        # Check platform aggregation
        expected_platform_pairs = [
            {
                "platform_urn": "urn:li:dataPlatform:powerbi",
                "platform_total": 10.0,
            },  # 10 only
            {
                "platform_urn": "urn:li:dataPlatform:postgres",
                "platform_total": 80.0,
            },  # 50 + 30
            {
                "platform_urn": "urn:li:dataPlatform:snowflake",
                "platform_total": 125.0,
            },  # 100 + 25
            {
                "platform_urn": "urn:li:dataPlatform:tableau",
                "platform_total": 70.0,
            },  # 50 + 20
        ]
        # Sort both lists by platform_urn for consistent comparison
        actual_platform_pairs = sorted(
            user_row["platform_usage_pairs"], key=lambda x: str(x["platform_urn"])
        )
        expected_platform_pairs = sorted(
            expected_platform_pairs, key=lambda x: str(x["platform_urn"])
        )
        assert actual_platform_pairs == expected_platform_pairs
