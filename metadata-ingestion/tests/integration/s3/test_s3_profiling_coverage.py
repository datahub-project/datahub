"""Integration tests for S3 profiling to ensure code coverage of type-ignored lines.

This test file specifically targets code paths with type: ignore annotations
that need runtime execution to achieve coverage, particularly when profiling is enabled.
"""

from pathlib import Path

import pytest

# Check if profiling dependencies are available
try:
    import pydeequ  # noqa: F401
    import pyspark  # noqa: F401

    _PROFILING_ENABLED = True
except ImportError:
    _PROFILING_ENABLED = False

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.s3.source import S3Source


@pytest.fixture(autouse=True)
def stop_spark_context():
    """Stop any existing SparkContext before each test to ensure clean state."""
    if _PROFILING_ENABLED:
        try:
            from pyspark import SparkContext

            if SparkContext._active_spark_context is not None:
                SparkContext._active_spark_context.stop()
        except Exception:
            pass
    yield


@pytest.mark.integration
@pytest.mark.skipif(
    not _PROFILING_ENABLED,
    reason="PySpark not available, skipping profiling integration tests",
)
class TestS3ProfilingCoverage:
    """Integration tests to cover all profiling code paths with different data types."""

    def test_profiling_with_numeric_types(self, tmp_path: Path) -> None:
        """Test profiling with various numeric column types (int, float, double).

        This covers:
        - count/when/isnan/col operations for numeric null counts (lines 164-169)
        - isinstance checks for numeric types (lines 305-314)
        - Cardinality-based branching for UNIQUE/FEW/MANY (lines 315-337)
        """
        import pandas as pd

        # Create test data with different numeric types
        test_file = tmp_path / "numeric_data.csv"
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "int_col": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                "float_col": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0],
                "double_col": [
                    100.5,
                    200.5,
                    300.5,
                    400.5,
                    500.5,
                    600.5,
                    700.5,
                    800.5,
                    900.5,
                    1000.5,
                ],
                "category": [
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                    "A",
                    "B",
                ],  # FEW cardinality
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
                "include_field_mean_value": True,
                "include_field_median_value": True,
                "include_field_stddev_value": True,
                "include_field_quantiles": True,
                "include_field_histogram": True,
                "include_field_distinct_value_frequencies": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-numeric")
        source = S3Source.create(config_dict, ctx)

        # Execute profiling
        workunits = list(source.get_workunits())

        # Verify we got profile data
        assert len(workunits) > 0
        profile_workunits = [
            wu for wu in workunits if wu.metadata.aspectName == "datasetProfile"
        ]
        assert len(profile_workunits) > 0

    def test_profiling_with_string_types(self, tmp_path: Path) -> None:
        """Test profiling with string column types.

        This covers:
        - isinstance check for StringType (lines 341)
        - String column profiling for FEW cardinality (lines 342-351)
        - Non-numeric null count handling (lines 176-184)
        """
        import pandas as pd

        test_file = tmp_path / "string_data.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 21),
                "name": [f"User{i}" for i in range(1, 21)],
                "status": ["active", "inactive", "pending"] * 6
                + ["active", "inactive"],  # FEW values
                "code": ["A", "B", "C", "D", "E"] * 4,  # FEW values
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_distinct_value_frequencies": True,
                "include_field_sample_values": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-string")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0
        profile_workunits = [
            wu for wu in workunits if wu.metadata.aspectName == "datasetProfile"
        ]
        assert len(profile_workunits) > 0

    def test_profiling_with_date_timestamp_types(self, tmp_path: Path) -> None:
        """Test profiling with date and timestamp column types.

        This covers:
        - isinstance check for DateType/TimestampType (lines 353)
        - Date/timestamp profiling with min/max (lines 354-367)
        """
        import pandas as pd

        test_file = tmp_path / "date_data.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 11),
                "event_date": pd.date_range("2023-01-01", periods=10),
                "created_at": pd.date_range(
                    "2023-01-01 10:00:00", periods=10, freq="h"
                ),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-date")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_null_values(self, tmp_path: Path) -> None:
        """Test profiling with null values in numeric and non-numeric columns.

        This covers:
        - Null count calculation for numeric columns with isnan (lines 164-172)
        - Null count calculation for non-numeric columns (lines 176-184)
        - Null proportion calculation (lines 190-197)
        """
        import pandas as pd

        test_file = tmp_path / "null_data.csv"
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, None, 7, 8, None, 10],
                "amount": [
                    100.5,
                    None,
                    300.5,
                    None,
                    500.5,
                    600.5,
                    None,
                    800.5,
                    900.5,
                    None,
                ],
                "name": ["A", "B", None, "D", None, "F", "G", None, "I", "J"],
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_null_count": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-nulls")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_sample_values(self, tmp_path: Path) -> None:
        """Test profiling with sample values enabled.

        This covers:
        - Sample value collection when row_count < NUM_SAMPLE_ROWS (lines 210-212)
        - Sample value collection when row_count >= NUM_SAMPLE_ROWS (lines 214)
        - Sample value assignment to column profiles (lines 227-229)
        """
        import pandas as pd

        # Test with small dataset (< 20 rows)
        test_file_small = tmp_path / "small_data.csv"
        df_small = pd.DataFrame(
            {
                "id": range(1, 6),
                "value": ["A", "B", "C", "D", "E"],
            }
        )
        df_small.to_csv(test_file_small, index=False)

        # Test with large dataset (>= 20 rows)
        test_file_large = tmp_path / "large_data.csv"
        df_large = pd.DataFrame(
            {
                "id": range(1, 51),
                "value": [f"Val{i}" for i in range(1, 51)],
            }
        )
        df_large.to_csv(test_file_large, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_sample_values": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-samples")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_high_cardinality(self, tmp_path: Path) -> None:
        """Test profiling with high cardinality columns (MANY/VERY_MANY).

        This covers:
        - Numeric columns with MANY cardinality (lines 325-337)
        - All analyzer prep methods (min, max, mean, median, stdev, quantiles, histogram)
        """
        import pandas as pd

        test_file = tmp_path / "high_cardinality.csv"
        df = pd.DataFrame(
            {
                "unique_id": range(1, 1001),  # UNIQUE cardinality
                "amount": [i * 1.5 for i in range(1, 1001)],  # MANY cardinality
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
                "include_field_mean_value": True,
                "include_field_median_value": True,
                "include_field_stddev_value": True,
                "include_field_quantiles": True,
                "include_field_histogram": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-high-cardinality")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_low_cardinality(self, tmp_path: Path) -> None:
        """Test profiling with low cardinality columns (ONE/TWO/VERY_FEW/FEW).

        This covers:
        - Numeric columns with FEW cardinality using histograms (lines 315-324)
        - String columns with FEW cardinality using distinct value frequencies (lines 342-351)
        - Date columns with FEW cardinality (lines 359-367)
        """
        import pandas as pd

        test_file = tmp_path / "low_cardinality.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 101),
                "binary_flag": [0, 1] * 50,  # TWO values
                "rating": [1, 2, 3, 4, 5] * 20,  # FEW values
                "status": ["NEW", "ACTIVE", "CLOSED"] * 33 + ["NEW"],  # FEW values
                "event_date": pd.to_datetime(
                    ["2023-01-01", "2023-01-02", "2023-01-03"] * 33 + ["2023-01-01"]
                ),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_distinct_value_frequencies": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-low-cardinality")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_column_filtering(self, tmp_path: Path) -> None:
        """Test profiling with allow/deny patterns for columns.

        This covers:
        - Column filtering logic (lines 127-129)
        - columns_to_profile list building (lines 131-133)
        """
        import pandas as pd

        test_file = tmp_path / "filtered_columns.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 11),
                "public_field": range(10, 20),
                "sensitive_ssn": ["123-45-6789"] * 10,
                "sensitive_password": ["secret"] * 10,
                "normal_data": ["value"] * 10,
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profile_patterns": {
                "deny": ["sensitive_*"],
            },
            "profiling": {
                "enabled": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-filtered")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_max_fields_limit(self, tmp_path: Path) -> None:
        """Test profiling with max_number_of_fields_to_profile limit.

        This covers:
        - Field limiting logic (lines 135-149)
        - report_file_dropped call (lines 147-149)
        """
        import pandas as pd

        test_file = tmp_path / "many_columns.csv"
        # Create a dataset with 20 columns
        data = {f"col_{i}": range(1, 11) for i in range(20)}
        df = pd.DataFrame(data)
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "max_number_of_fields_to_profile": 5,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-max-fields")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0
        assert source.report.number_of_files_filtered > 0

    def test_profiling_with_table_level_only(self, tmp_path: Path) -> None:
        """Test profiling with profile_table_level_only enabled.

        This covers:
        - Early return when profile_table_level_only is True (lines 122-123)
        - Table-level stats only without column profiling
        """
        import pandas as pd

        test_file = tmp_path / "table_level_only.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 11),
                "value": range(10, 20),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "profile_table_level_only": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-table-only")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_extract_table_profiles_with_quantiles(
        self, tmp_path: Path
    ) -> None:
        """Test extract_table_profiles with quantile data.

        This covers:
        - Quantile extraction and processing (lines 446-456)
        - QuantileClass creation
        """
        import pandas as pd

        test_file = tmp_path / "quantile_data.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 101),
                "score": range(0, 100),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_quantiles": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-quantiles")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_extract_with_histogram_distinct(self, tmp_path: Path) -> None:
        """Test extract_table_profiles with histogram for distinct values.

        This covers:
        - Histogram processing for discrete data (lines 463-473)
        - distinctValueFrequencies creation
        """
        import pandas as pd

        test_file = tmp_path / "histogram_distinct.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 51),
                "category": ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5"] * 10,
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_distinct_value_frequencies": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-histogram-distinct")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_extract_with_histogram_continuous(self, tmp_path: Path) -> None:
        """Test extract_table_profiles with histogram for continuous data.

        This covers:
        - Histogram processing for continuous data (lines 475-479)
        - HistogramClass creation
        """
        import pandas as pd

        test_file = tmp_path / "histogram_continuous.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 201),
                "measurement": [i * 0.5 for i in range(1, 201)],
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "include_field_histogram": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-histogram-continuous")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0

    def test_profiling_with_all_options_enabled(self, tmp_path: Path) -> None:
        """Test profiling with all configuration options enabled.

        This is a comprehensive test that exercises all code paths to ensure
        maximum coverage of type-ignored lines.
        """
        import pandas as pd

        test_file = tmp_path / "comprehensive.csv"
        df = pd.DataFrame(
            {
                "id": range(1, 101),
                "int_unique": range(1, 101),  # UNIQUE
                "int_many": [i % 50 for i in range(1, 101)],  # MANY
                "int_few": [i % 3 for i in range(1, 101)],  # FEW
                "float_col": [i * 1.5 for i in range(1, 101)],
                "string_unique": [f"U{i}" for i in range(1, 101)],  # UNIQUE
                "string_few": ["A", "B", "C"] * 33 + ["A"],  # FEW
                "date_col": pd.date_range("2023-01-01", periods=100),
                "timestamp_col": pd.date_range(
                    "2023-01-01 10:00:00", periods=100, freq="h"
                ),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
                "profile_table_level_only": False,
                "include_field_null_count": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
                "include_field_mean_value": True,
                "include_field_median_value": True,
                "include_field_stddev_value": True,
                "include_field_quantiles": True,
                "include_field_histogram": True,
                "include_field_distinct_value_frequencies": True,
                "include_field_sample_values": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-comprehensive")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0
        profile_workunits = [
            wu for wu in workunits if wu.metadata.aspectName == "datasetProfile"
        ]
        assert len(profile_workunits) > 0

    def test_profiling_with_zero_row_count(self, tmp_path: Path) -> None:
        """Test profiling with empty dataset (row_count = 0).

        This covers:
        - Division by zero handling (lines 191, 297)
        - Empty dataset profiling
        """
        import pandas as pd

        test_file = tmp_path / "empty_data.csv"
        df = pd.DataFrame(
            {
                "id": pd.Series([], dtype=int),
                "value": pd.Series([], dtype=str),
            }
        )
        df.to_csv(test_file, index=False)

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {
                "enabled": True,
            },
        }

        ctx = PipelineContext(run_id="test-profiling-empty")
        source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())

        assert len(workunits) > 0
