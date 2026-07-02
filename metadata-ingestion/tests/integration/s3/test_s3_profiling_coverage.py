from pathlib import Path

import pandas as pd
import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.s3.source import S3Source
from datahub.metadata.schema_classes import MetadataChangeProposalClass


def _profile_workunits(workunits: list) -> list:
    """Return the datasetProfile workunits among the emitted workunits.

    These integration tests run the real DuckDB profiler end-to-end against
    local CSVs. Asserting on the datasetProfile aspect (rather than just
    ``len(workunits) > 0``) proves a profile was actually produced — a source
    that emitted only container/schema workunits but no profile would otherwise
    pass.
    """
    return [
        wu
        for wu in workunits
        if isinstance(
            wu.metadata,
            (MetadataChangeProposalClass, MetadataChangeProposalWrapper),
        )
        and wu.metadata.aspectName == "datasetProfile"
    ]


@pytest.mark.integration
class TestS3ProfilingCoverage:
    """Integration tests covering DuckDB profiling code paths across data types."""

    def test_profiling_with_numeric_types(self, tmp_path: Path) -> None:
        """Profiling numeric columns (int/float/double).

        Exercises min/max/mean/median/stddev, quantiles, histogram and
        distinct-value frequencies for numeric and low-cardinality columns.
        """

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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_string_types(self, tmp_path: Path) -> None:
        """Profiling string columns.

        Exercises distinct-value frequencies and sample values for
        low-cardinality text columns.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_date_timestamp_types(self, tmp_path: Path) -> None:
        """Profiling date and timestamp columns (min/max bounds)."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_null_values(self, tmp_path: Path) -> None:
        """Profiling columns containing nulls.

        Exercises null-count and null-proportion computation for numeric and
        text columns.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_sample_values(self, tmp_path: Path) -> None:
        """Profiling with sample values enabled.

        Covers both a small dataset (fewer rows than the sample limit) and a
        larger one.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_high_cardinality(self, tmp_path: Path) -> None:
        """Profiling high-cardinality numeric columns.

        Exercises all numeric stats (min/max/mean/median/stddev/quantiles/
        histogram) on UNIQUE and MANY cardinality columns.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_low_cardinality(self, tmp_path: Path) -> None:
        """Profiling low-cardinality columns.

        Exercises numeric histograms, string distinct-value frequencies, and
        low-cardinality date columns.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_column_filtering(self, tmp_path: Path) -> None:
        """Profiling honors profile_patterns deny rules for columns."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_max_fields_limit(self, tmp_path: Path) -> None:
        """Profiling with max_number_of_fields_to_profile limit.

        Truncating columns must surface a warning (the file is still profiled
        with fewer columns, so it must NOT be marked as filtered/not-ingested).
        """
        test_file = tmp_path / "many_columns.csv"
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(_profile_workunits(workunits)) > 0

        # Truncating columns emits a warning (not report_file_dropped, which would
        # wrongly count the file as filtered out).
        assert any(
            "max_number_of_fields_to_profile" in w.message
            for w in source.report.warnings
        )
        assert source.report.number_of_files_filtered == 0

    def test_profiling_with_table_level_only(self, tmp_path: Path) -> None:
        """Profiling with profile_table_level_only enabled.

        Emits table-level stats (row/column counts) without per-column
        profiling.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_extract_table_profiles_with_quantiles(
        self, tmp_path: Path
    ) -> None:
        """Quantile extraction (QuantileClass) for numeric columns."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_extract_with_histogram_distinct(self, tmp_path: Path) -> None:
        """Distinct-value-frequency extraction for discrete data."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_extract_with_histogram_continuous(self, tmp_path: Path) -> None:
        """Histogram extraction (HistogramClass) for continuous data."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_all_options_enabled(self, tmp_path: Path) -> None:
        """Comprehensive profiling run with every field-level option enabled."""
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        assert len(_profile_workunits(workunits)) > 0

    def test_profiling_with_zero_row_count(self, tmp_path: Path) -> None:
        """Profiling an empty dataset (row_count = 0) completes without error.

        Guards the division-by-zero path in null-proportion/cardinality math.
        """
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
        source: S3Source = S3Source.create(config_dict, ctx)

        workunits = list(source.get_workunits())
        assert len(source.report.failures) == 0
        assert len(source.report.warnings) == 0

        # An empty file must not crash profiling; we don't assert a profile is
        # emitted (behavior for 0-row tables is intentionally unconstrained here).
        assert len(workunits) > 0
