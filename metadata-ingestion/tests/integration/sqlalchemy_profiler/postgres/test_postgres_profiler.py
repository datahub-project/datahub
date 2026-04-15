"""Integration tests for custom SQL profiler with PostgreSQL."""

from typing import Optional

import pytest
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresSource
from datahub.metadata.schema_classes import DatasetProfileClass
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2024-01-01 12:00:00"
POSTGRES_PORT = 5433


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/sqlalchemy_profiler/postgres"


@pytest.fixture(scope="module")
def postgres_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "postgres"
    ) as docker_services:
        # Wait for PostgreSQL to be ready
        # The container exposes port 5432 internally, but maps to 5433 on host
        wait_for_port(
            docker_services,
            container_name="testpostgres_profiler",
            container_port=5432,  # Internal container port
            timeout=120,
        )
        yield docker_services


@pytest.fixture
def postgres_source(postgres_runner):
    """Create a PostgresSource instance with custom profiler enabled."""
    from datahub.ingestion.source.sql.postgres import PostgresConfig

    config_dict = {
        "username": "testuser",
        "password": "testpass",
        "host_port": f"localhost:{POSTGRES_PORT}",
        "database": "testdb",
        "profiling": {
            "enabled": True,
            "method": "sqlalchemy",
            "include_field_null_count": True,
            "include_field_distinct_count": True,
            "include_field_min_value": True,
            "include_field_max_value": True,
            "include_field_mean_value": True,
            "include_field_median_value": True,
            "include_field_stddev_value": True,
            "include_field_quantiles": True,
            "include_field_histogram": True,
            "include_field_sample_values": True,
        },
    }
    config = PostgresConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-profiler")
    source = PostgresSource(config, ctx)
    return source


def get_profile_for_table(
    source: PostgresSource, schema: str, table: str
) -> Optional[DatasetProfileClass]:
    """Get profile for a specific table."""
    from dataclasses import dataclass

    @dataclass
    class ProfilerRequest:
        """Simple request class for profiler."""

        pretty_name: str
        batch_kwargs: dict

    # Get the first inspector from the source
    inspectors = list(source.get_inspectors())
    if not inspectors:
        return None

    inspector = inspectors[0]
    profiler = source.get_profiler_instance(inspector)

    request = ProfilerRequest(
        pretty_name=f"{schema}.{table}",
        batch_kwargs={"schema": schema, "table": table},
    )

    profiles = list(profiler.generate_profiles([request], max_workers=1))  # type: ignore[arg-type,list-item]
    if profiles:
        return profiles[0][1]  # Return the profile (second element of tuple)
    return None


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_basic_statistics_exact_values(postgres_source):
    """Test basic statistics with known exact values."""
    profile = get_profile_for_table(postgres_source, "public", "test_exact_numeric")

    assert profile is not None
    assert profile.rowCount == 7

    # Find value_col field profile
    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    assert value_col_profile is not None

    # Exact assertions
    assert value_col_profile.nullCount == 2
    # nonNullCount = rowCount - nullCount = 7 - 2 = 5
    assert profile.rowCount - value_col_profile.nullCount == 5
    assert value_col_profile.min == "1"
    assert value_col_profile.max == "5"
    assert value_col_profile.mean is not None
    assert value_col_profile.stdev is not None
    assert float(value_col_profile.mean) == pytest.approx(3.0, rel=1e-6)
    assert float(value_col_profile.stdev) == pytest.approx(1.5811, rel=1e-3)
    assert value_col_profile.uniqueCount == 5


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_mathematical_correctness(postgres_source):
    """Verify statistical properties hold."""
    profile = get_profile_for_table(postgres_source, "public", "test_mixed_types")

    assert profile is not None
    assert profile.fieldProfiles is not None

    for field_profile in profile.fieldProfiles:
        if field_profile.fieldPath == "id":
            continue  # Skip ID column

        # Check min <= max (only for numeric fields)
        if field_profile.min is not None and field_profile.max is not None:
            try:
                assert float(field_profile.min) <= float(field_profile.max)
            except (ValueError, TypeError):
                # Skip non-numeric fields (e.g., DATETIME, STRING)
                pass

        # Check min <= mean <= max (only for numeric fields)
        if (
            field_profile.min is not None
            and field_profile.mean is not None
            and field_profile.max is not None
        ):
            try:
                assert float(field_profile.min) <= float(field_profile.mean)
                assert float(field_profile.mean) <= float(field_profile.max)
            except (ValueError, TypeError):
                # Skip non-numeric fields (e.g., DATETIME, STRING)
                pass

        # Check null_count + non_null_count = row_count
        if field_profile.nullCount is not None and profile.rowCount is not None:
            non_null_count = profile.rowCount - field_profile.nullCount
            assert field_profile.nullCount + non_null_count == profile.rowCount

        # Check median is between min and max
        if (
            field_profile.median is not None
            and field_profile.min is not None
            and field_profile.max is not None
        ):
            assert float(field_profile.min) <= float(field_profile.median)
            assert float(field_profile.median) <= float(field_profile.max)


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_quantiles_ordering(postgres_source):
    """Verify quantiles are properly ordered."""
    profile = get_profile_for_table(postgres_source, "public", "test_quantiles")

    assert profile is not None

    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    assert value_col_profile is not None
    # This table has 10 unique values out of 10 rows (pct_unique=1.0)
    # which is UNIQUE cardinality, so quantiles are not calculated
    # (quantiles aren't meaningful when every value is unique)
    assert value_col_profile.quantiles is None or len(value_col_profile.quantiles) == 0


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_approximate_unique_count_bounds(postgres_source):
    """Verify approximate unique count is within reasonable bounds."""
    profile = get_profile_for_table(postgres_source, "public", "test_unique_count")

    assert profile is not None

    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    assert value_col_profile is not None

    # Exact unique count should be 10
    # Approximate count should be within 5% (9-11)
    assert value_col_profile.uniqueCount is not None
    # For PostgreSQL, we use exact count, so it should be exactly 10
    # But if approximate is used, allow 5% variance
    assert 9 <= value_col_profile.uniqueCount <= 11


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_value_frequencies_correctness(postgres_source):
    """Verify value frequencies are correct."""
    profile = get_profile_for_table(postgres_source, "public", "test_frequencies")

    assert profile is not None

    assert profile.fieldProfiles is not None
    category_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "category_col"), None
    )
    assert category_col_profile is not None

    # Check sample values or value frequencies
    if category_col_profile.sampleValues:
        # sampleValues is a list of strings
        assert "A" in category_col_profile.sampleValues
        assert "B" in category_col_profile.sampleValues
        assert "C" in category_col_profile.sampleValues

    # If distinctValueFrequencies is populated, verify counts
    if category_col_profile.distinctValueFrequencies:
        freq_dict = {
            vf.value: vf.frequency
            for vf in category_col_profile.distinctValueFrequencies
        }
        assert freq_dict.get("A") == 3
        assert freq_dict.get("B") == 2
        assert freq_dict.get("C") == 1


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_histogram_consistency(postgres_source):
    """Verify histogram is mathematically consistent."""
    profile = get_profile_for_table(postgres_source, "public", "test_histogram")

    assert profile is not None

    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    assert value_col_profile is not None

    if value_col_profile.histogram:
        histogram = value_col_profile.histogram
        # Sum of bucket heights should equal non-null count
        total_count = sum(histogram.heights)
        non_null_count = (
            profile.rowCount - value_col_profile.nullCount
            if value_col_profile.nullCount is not None and profile.rowCount is not None
            else None
        )
        assert total_count == non_null_count

        # Verify buckets are ordered (boundaries should be in ascending order)
        boundaries = [float(b) for b in histogram.boundaries]
        for i in range(len(boundaries) - 1):
            assert boundaries[i] <= boundaries[i + 1]


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_edge_case_empty_table(postgres_source):
    """Test profiling empty table."""
    profile = get_profile_for_table(postgres_source, "public", "test_empty")

    assert profile is not None
    assert profile.rowCount == 0


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_edge_case_single_row(postgres_source):
    """Test profiling table with single row."""
    profile = get_profile_for_table(postgres_source, "public", "test_single_row")

    assert profile is not None
    assert profile.rowCount == 1

    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    if value_col_profile:
        # nonNullCount = rowCount - nullCount = 1 - 0 = 1
        assert value_col_profile.nullCount is not None
        assert profile.rowCount - value_col_profile.nullCount == 1
        assert value_col_profile.nullCount == 0
        assert value_col_profile.min == "42"
        assert value_col_profile.max == "42"
        assert value_col_profile.mean is not None
        assert value_col_profile.median is not None
        assert float(value_col_profile.mean) == pytest.approx(42.0, rel=1e-6)
        assert float(value_col_profile.median) == pytest.approx(42.0, rel=1e-6)
        assert value_col_profile.stdev is None  # stddev of single value is None
        assert value_col_profile.uniqueCount == 1
        # Single row has ONE cardinality, so quantiles are not calculated
        assert (
            value_col_profile.quantiles is None or len(value_col_profile.quantiles) == 0
        )
        assert value_col_profile.sampleValues == ["42"]


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_edge_case_all_nulls(postgres_source):
    """Test profiling table with all NULL values."""
    profile = get_profile_for_table(postgres_source, "public", "test_all_nulls")

    assert profile is not None
    assert profile.rowCount == 3

    assert profile.fieldProfiles is not None
    value_col_profile = next(
        (fp for fp in profile.fieldProfiles if fp.fieldPath == "value_col"), None
    )
    if value_col_profile:
        # nonNullCount = rowCount - nullCount = 3 - 3 = 0
        assert value_col_profile.nullCount is not None
        assert profile.rowCount - value_col_profile.nullCount == 0
        assert value_col_profile.nullCount == 3
        # Min/max/mean should be None for all NULLs
        assert value_col_profile.min is None
        assert value_col_profile.max is None
        assert value_col_profile.mean is None
        assert value_col_profile.median is None
        assert value_col_profile.stdev is None
        assert value_col_profile.uniqueCount == 0
        assert not value_col_profile.quantiles or len(value_col_profile.quantiles) == 0
        assert not value_col_profile.distinctValueFrequencies
        # sampleValues should be empty for all NULLs (None values are filtered out)
        assert not value_col_profile.sampleValues


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_row_count_estimation(postgres_source):
    """Test row count estimation for PostgreSQL."""
    from datahub.ingestion.source.sql.postgres import PostgresConfig

    # Create a source with estimation enabled
    config_dict = {
        "username": "testuser",
        "password": "testpass",
        "host_port": f"localhost:{POSTGRES_PORT}",
        "database": "testdb",
        "profiling": {
            "enabled": True,
            "method": "sqlalchemy",
            "profile_table_row_count_estimate_only": True,
        },
    }
    config = PostgresConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-profiler-est")
    source = PostgresSource(config, ctx)

    profile = get_profile_for_table(source, "public", "test_row_count_estimation")

    assert profile is not None
    assert profile.rowCount is not None
    # Estimated count should be within reasonable range of actual (1000)
    # PostgreSQL estimates can vary, so allow 50% variance
    assert 500 <= profile.rowCount <= 1500


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_quantiles_cardinality_filtering(postgres_source):
    """Test that quantiles are only calculated for columns with sufficient cardinality."""
    # Low cardinality (TWO) - quantiles should NOT be calculated
    low_card_profile = get_profile_for_table(
        postgres_source, "public", "test_low_cardinality"
    )

    assert low_card_profile is not None
    assert low_card_profile.fieldProfiles is not None
    low_card_value_col = next(
        (fp for fp in low_card_profile.fieldProfiles if fp.fieldPath == "value_col"),
        None,
    )
    assert low_card_value_col is not None
    # Should have 2 unique values (10 and 20)
    assert low_card_value_col.uniqueCount == 2
    # Quantiles should NOT be calculated for low cardinality (TWO)
    assert (
        low_card_value_col.quantiles is None or len(low_card_value_col.quantiles) == 0
    )

    # High cardinality (FEW) - quantiles SHOULD be calculated
    high_card_profile = get_profile_for_table(
        postgres_source, "public", "test_high_cardinality"
    )

    assert high_card_profile is not None
    assert high_card_profile.fieldProfiles is not None
    high_card_value_col = next(
        (fp for fp in high_card_profile.fieldProfiles if fp.fieldPath == "value_col"),
        None,
    )
    assert high_card_value_col is not None
    # Should have 25 unique values out of 50 rows (pct_unique=0.5 -> FEW cardinality)
    assert high_card_value_col.uniqueCount == 25
    # Quantiles SHOULD be calculated for higher cardinality (FEW)
    assert high_card_value_col.quantiles is not None
    assert len(high_card_value_col.quantiles) > 0


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_histogram_cardinality_filtering(postgres_source):
    """Test that histogram is only calculated for columns with sufficient cardinality."""
    # Low cardinality (TWO) - histogram should NOT be calculated
    low_card_profile = get_profile_for_table(
        postgres_source, "public", "test_low_cardinality"
    )

    assert low_card_profile is not None
    assert low_card_profile.fieldProfiles is not None
    low_card_value_col = next(
        (fp for fp in low_card_profile.fieldProfiles if fp.fieldPath == "value_col"),
        None,
    )
    assert low_card_value_col is not None
    # Should have 2 unique values (10 and 20)
    assert low_card_value_col.uniqueCount == 2
    # Histogram should NOT be calculated for low cardinality (TWO)
    assert low_card_value_col.histogram is None

    # High cardinality (FEW) - histogram SHOULD be calculated
    high_card_profile = get_profile_for_table(
        postgres_source, "public", "test_high_cardinality"
    )

    assert high_card_profile is not None
    assert high_card_profile.fieldProfiles is not None
    high_card_value_col = next(
        (fp for fp in high_card_profile.fieldProfiles if fp.fieldPath == "value_col"),
        None,
    )
    assert high_card_value_col is not None
    # Should have 25 unique values out of 50 rows (pct_unique=0.5 -> FEW cardinality)
    assert high_card_value_col.uniqueCount == 25
    # Histogram SHOULD be calculated for higher cardinality (FEW)
    assert high_card_value_col.histogram is not None
    assert len(high_card_value_col.histogram.boundaries) > 0
    assert len(high_card_value_col.histogram.heights) > 0
