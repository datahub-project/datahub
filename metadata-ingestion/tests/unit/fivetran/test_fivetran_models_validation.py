"""
Unit tests for Fivetran models and configuration validation.

Tests focus on business logic, edge cases, and data validation that affects connector behavior.
"""

from datahub.ingestion.source.fivetran.config import (
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.models import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)


class TestPlatformDetailValidation:
    """Test platform detail configuration validation and edge cases."""

    def test_platform_detail_with_database_name_validation(self) -> None:
        """Test that database names are handled correctly for URN generation."""
        # Test with various database name formats that could affect URN generation
        test_cases = [
            ("my-database", True),  # Hyphenated name
            ("my_database", True),  # Underscore name
            ("123database", True),  # Starting with number
            ("database.with.dots", True),  # Dots in name
            ("", True),  # Empty database name should be allowed
        ]

        for db_name, should_be_valid in test_cases:
            if should_be_valid:
                detail = PlatformDetail(
                    platform="postgres", database=db_name, env="PROD"
                )
                assert detail.database == db_name

    def test_platform_detail_environment_validation(self) -> None:
        """Test environment validation affects URN generation."""
        valid_envs = ["PROD", "DEV", "STAGING", "TEST", "QA"]

        for env in valid_envs:
            detail = PlatformDetail(platform="postgres", env=env)
            assert detail.env == env

    def test_platform_detail_schema_inclusion_logic(self) -> None:
        """Test schema inclusion logic that affects URN structure."""
        # When include_schema_in_urn is True, schema should be part of URN
        detail_with_schema = PlatformDetail(
            platform="postgres", include_schema_in_urn=True
        )
        assert detail_with_schema.include_schema_in_urn is True

        # When False, schema should not be part of URN
        detail_without_schema = PlatformDetail(
            platform="postgres", include_schema_in_urn=False
        )
        assert detail_without_schema.include_schema_in_urn is False


class TestConnectorModelValidation:
    """Test connector model validation and business logic."""

    def test_connector_with_invalid_sync_frequency(self) -> None:
        """Test connector validation with edge case sync frequencies."""
        # Very low sync frequency (should be allowed)
        connector_low = Connector(
            connector_id="test",
            connector_name="Test",
            connector_type="postgres",
            paused=False,
            sync_frequency=1,  # 1 minute
            destination_id="dest",
        )
        assert connector_low.sync_frequency == 1

        # Very high sync frequency (should be allowed)
        connector_high = Connector(
            connector_id="test",
            connector_name="Test",
            connector_type="postgres",
            paused=False,
            sync_frequency=525600,  # 1 year in minutes
            destination_id="dest",
        )
        assert connector_high.sync_frequency == 525600

    def test_connector_with_complex_lineage_relationships(self) -> None:
        """Test connector with complex lineage that could affect performance."""
        # Create many column lineages to test performance limits
        column_lineages = [
            ColumnLineage(
                source_column=f"source_col_{i}", destination_column=f"dest_col_{i}"
            )
            for i in range(100)  # Test with many columns
        ]

        table_lineage = TableLineage(
            source_table="large_source_table",
            destination_table="large_dest_table",
            column_lineage=column_lineages,
        )

        connector = Connector(
            connector_id="large_connector",
            connector_name="Large Connector",
            connector_type="postgres",
            paused=False,
            sync_frequency=1440,
            destination_id="dest",
            lineage=[table_lineage],
        )

        assert len(connector.lineage) == 1
        assert len(connector.lineage[0].column_lineage) == 100

    def test_connector_with_many_jobs(self) -> None:
        """Test connector with many jobs to validate performance."""
        # Create many jobs to test limits
        jobs = [
            Job(
                job_id=f"job_{i}",
                start_time=1234567890 + i,
                end_time=1234567890 + i + 60,
                status="SUCCESS" if i % 2 == 0 else "FAILED",
            )
            for i in range(50)  # Test with many jobs
        ]

        connector = Connector(
            connector_id="job_heavy_connector",
            connector_name="Job Heavy Connector",
            connector_type="postgres",
            paused=False,
            sync_frequency=1440,
            destination_id="dest",
            jobs=jobs,
        )

        assert len(connector.jobs) == 50
        # Test job status distribution
        success_jobs = [j for j in connector.jobs if j.status == "SUCCESS"]
        failed_jobs = [j for j in connector.jobs if j.status == "FAILED"]
        assert len(success_jobs) == 25
        assert len(failed_jobs) == 25

    def test_connector_user_id_none_handling(self) -> None:
        """Test that None user_id is handled correctly (important for API calls)."""
        connector = Connector(
            connector_id="test",
            connector_name="Test",
            connector_type="postgres",
            paused=False,
            sync_frequency=1440,
            destination_id="dest",
            user_id=None,
        )

        # This should not raise an error and user_id should be None
        assert connector.user_id is None


class TestLineageModelValidation:
    """Test lineage model validation and edge cases."""

    def test_column_lineage_with_special_characters(self) -> None:
        """Test column lineage with special characters that could break URNs."""
        special_cases = [
            ("source.col", "dest.col"),  # Dots
            ("source-col", "dest-col"),  # Hyphens
            ("source_col", "dest_col"),  # Underscores
            ("Source Col", "Dest Col"),  # Spaces
            ("source123", "dest456"),  # Numbers
        ]

        for source, dest in special_cases:
            col_lineage = ColumnLineage(source_column=source, destination_column=dest)
            assert col_lineage.source_column == source
            assert col_lineage.destination_column == dest

    def test_table_lineage_with_schema_qualified_names(self) -> None:
        """Test table lineage with schema-qualified table names."""
        table_lineage = TableLineage(
            source_table="source_schema.source_table",
            destination_table="dest_schema.dest_table",
            column_lineage=[],
        )

        assert "source_schema" in table_lineage.source_table
        assert "dest_schema" in table_lineage.destination_table

    def test_empty_lineage_collections(self) -> None:
        """Test that empty lineage collections are handled correctly."""
        # Empty column lineage should be fine
        table_lineage = TableLineage(
            source_table="source", destination_table="dest", column_lineage=[]
        )
        assert table_lineage.column_lineage == []

        # Connector with empty lineage should be fine
        connector = Connector(
            connector_id="test",
            connector_name="Test",
            connector_type="postgres",
            paused=False,
            sync_frequency=1440,
            destination_id="dest",
            lineage=[],
        )
        assert connector.lineage == []


class TestJobModelValidation:
    """Test job model validation and business logic."""

    def test_job_with_different_status_values(self) -> None:
        """Test job model with various status values that affect processing."""
        status_values = ["SUCCESS", "FAILED", "RUNNING", "PAUSED", "CANCELLED"]

        for status in status_values:
            job = Job(
                job_id=f"job_{status.lower()}",
                start_time=1234567890,
                end_time=1234567900,
                status=status,
            )
            assert job.status == status

    def test_job_timing_validation(self) -> None:
        """Test job timing logic that affects sync history processing."""
        # Normal case: end_time > start_time
        job_normal = Job(
            job_id="normal_job",
            start_time=1234567890,
            end_time=1234567900,  # 10 seconds later
            status="SUCCESS",
        )
        assert job_normal.end_time > job_normal.start_time

        # Edge case: same start and end time (instant job)
        job_instant = Job(
            job_id="instant_job",
            start_time=1234567890,
            end_time=1234567890,  # Same time
            status="SUCCESS",
        )
        assert job_instant.end_time == job_instant.start_time

        # Edge case: very long running job
        job_long = Job(
            job_id="long_job",
            start_time=1234567890,
            end_time=1234567890 + 86400,  # 24 hours later
            status="SUCCESS",
        )
        duration = job_long.end_time - job_long.start_time
        assert duration == 86400  # 24 hours in seconds
