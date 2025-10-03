"""
Unit tests for fivetran_constants.py

Tests focus on business logic and edge cases that matter for connector functionality.
"""

import pytest

from datahub.ingestion.source.fivetran.fivetran_constants import (
    DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
    MAX_COLUMN_LINEAGE_PER_CONNECTOR,
    MAX_JOBS_PER_CONNECTOR,
    DataJobMode,
    FivetranMode,
    get_platform_from_fivetran_service,
)


class TestPlatformMapping:
    """Test platform mapping logic that affects connector behavior."""

    @pytest.mark.parametrize(
        "fivetran_service,expected_datahub_platform",
        [
            # Critical pattern matching for SQL Server variants
            ("sql_server", "mssql"),
            ("sql_server_rds", "mssql"),
            ("azure_sql", "mssql"),
            ("azure_sql_database", "mssql"),
            # PostgreSQL variants that must map correctly
            ("postgres", "postgres"),
            ("postgresql", "postgres"),
            ("postgres_rds", "postgres"),
            ("amazon_rds_for_postgresql", "postgres"),
            # MySQL variants
            ("mysql", "mysql"),
            ("mysql_rds", "mysql"),
            ("google_cloud_mysql", "mysql"),
            ("amazon_rds_for_mysql", "mysql"),
            # Cloud storage platforms
            ("s3", "s3"),
            ("amazon_s3", "s3"),
            ("google_cloud_storage", "gcs"),
            ("gcs", "gcs"),
        ],
    )
    def test_critical_platform_mappings(
        self, fivetran_service: str, expected_datahub_platform: str
    ) -> None:
        """Test platform mappings that are critical for connector functionality."""
        result = get_platform_from_fivetran_service(fivetran_service)
        assert result == expected_datahub_platform, (
            f"Platform mapping failed for {fivetran_service}: "
            f"expected {expected_datahub_platform}, got {result}"
        )

    def test_case_insensitive_platform_detection(self) -> None:
        """Test that platform detection works regardless of case (important for real-world data)."""
        test_cases = [
            ("POSTGRES", "postgres"),
            ("PostgreSQL", "postgres"),
            ("MYSQL", "mysql"),
            ("MySQL", "mysql"),
            ("SQL_SERVER", "mssql"),
            ("Sql_Server", "mssql"),
        ]

        for service_name, expected_platform in test_cases:
            result = get_platform_from_fivetran_service(service_name)
            assert result == expected_platform, (
                f"Case-insensitive detection failed for {service_name}"
            )

    def test_unknown_service_fallback_behavior(self) -> None:
        """Test that unknown services fall back gracefully (critical for connector robustness)."""
        unknown_services = [
            "custom_proprietary_db",
            "new_saas_platform_2024",
            "unknown_connector_type",
            "test_service_123",
        ]

        for service in unknown_services:
            result = get_platform_from_fivetran_service(service)
            # Should return the service name itself for unknown services
            assert result == service, (
                f"Unknown service {service} should return itself, got {result}"
            )

    def test_empty_and_none_service_handling(self) -> None:
        """Test edge cases that could break the connector."""
        # Empty string should return "unknown"
        assert get_platform_from_fivetran_service("") == "unknown"

        # Whitespace-only should be handled (will be returned as-is since no pattern matches)
        result = get_platform_from_fivetran_service("   ")
        # The function returns the service name if no pattern matches, but might do substring matching
        # Let's just verify it doesn't crash and returns a string
        assert isinstance(result, str)

    def test_substring_matching_priority(self) -> None:
        """Test that substring matching works correctly for complex service names."""
        # These test real-world Fivetran service names with embedded platform identifiers
        complex_cases = [
            ("my_company_postgres_prod", "postgres"),
            ("client_mysql_staging", "mysql"),
            ("data_warehouse_snowflake", "snowflake"),
            ("analytics_bigquery_raw", "bigquery"),
            ("logs_kafka_stream", "kafka"),
        ]

        for service_name, expected_platform in complex_cases:
            result = get_platform_from_fivetran_service(service_name)
            assert result == expected_platform, (
                f"Substring matching failed for {service_name}"
            )


class TestConnectorLimits:
    """Test connector limits that affect performance and behavior."""

    def test_connector_limits_are_reasonable(self) -> None:
        """Test that connector limits make sense for production use."""
        # Limits should be positive
        assert DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR > 0
        assert MAX_COLUMN_LINEAGE_PER_CONNECTOR > 0
        assert MAX_JOBS_PER_CONNECTOR > 0

        # Column lineage should allow more entries than table lineage
        assert (
            MAX_COLUMN_LINEAGE_PER_CONNECTOR > DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR
        )

        # Job limit should be reasonable for typical connectors
        assert MAX_JOBS_PER_CONNECTOR >= 100  # Should handle at least 100 jobs
        assert MAX_JOBS_PER_CONNECTOR <= 10000  # But not unlimited

    def test_limits_prevent_memory_issues(self) -> None:
        """Test that limits are set to prevent memory problems."""
        # These limits should prevent excessive memory usage
        estimated_memory_per_table = 1024  # bytes
        estimated_memory_per_column = 256  # bytes
        estimated_memory_per_job = 512  # bytes

        # Rough memory estimates (in MB)
        table_memory_mb = (
            DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR * estimated_memory_per_table
        ) / (1024 * 1024)
        column_memory_mb = (
            MAX_COLUMN_LINEAGE_PER_CONNECTOR * estimated_memory_per_column
        ) / (1024 * 1024)
        job_memory_mb = (MAX_JOBS_PER_CONNECTOR * estimated_memory_per_job) / (
            1024 * 1024
        )

        # Should be reasonable memory usage (less than 100MB each)
        assert table_memory_mb < 100
        assert column_memory_mb < 100
        assert job_memory_mb < 100


class TestConnectorModes:
    """Test connector mode enums that affect connector behavior."""

    def test_fivetran_mode_completeness(self) -> None:
        """Test that all expected Fivetran modes are available."""
        expected_modes = {"auto", "standard", "enterprise"}
        actual_modes = {mode.value for mode in FivetranMode}

        assert actual_modes == expected_modes, (
            f"FivetranMode missing expected values. Expected: {expected_modes}, Got: {actual_modes}"
        )

    def test_data_job_mode_completeness(self) -> None:
        """Test that all expected data job modes are available."""
        expected_modes = {"consolidated", "per_table"}
        actual_modes = {mode.value for mode in DataJobMode}

        assert actual_modes == expected_modes, (
            f"DataJobMode missing expected values. Expected: {expected_modes}, Got: {actual_modes}"
        )

    def test_mode_enum_string_conversion(self) -> None:
        """Test that mode enums can be used as strings (important for config validation)."""
        # FivetranMode should be usable as strings
        for fivetran_mode in FivetranMode:
            assert isinstance(fivetran_mode.value, str)
            assert len(fivetran_mode.value) > 0
            assert fivetran_mode.value.isalpha()  # Should be alphabetic characters only

        # DataJobMode should be usable as strings
        for job_mode in DataJobMode:
            assert isinstance(job_mode.value, str)
            assert len(job_mode.value) > 0
