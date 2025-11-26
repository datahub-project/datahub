"""
Test comprehensive lineage functionality for Fivetran connector.

This module tests the lineage extraction capabilities including:
- Rich metadata extraction from both API and log tables
- Comprehensive platform detection
- Table and column lineage validation
- Error handling and validation
"""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.fivetran.config import PlatformDetail
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_constants import (
    get_platform_from_fivetran_service,
)
from datahub.ingestion.source.fivetran.models import (
    ColumnLineage,
    Connector,
    TableLineage,
)


class TestPlatformDetection:
    """Test platform detection using existing DataHub platform mapping."""

    def test_get_platform_from_fivetran_service_known_platforms(self):
        """Test platform detection for known platforms."""
        test_cases = [
            ("mysql", "mysql"),
            ("postgres", "postgres"),
            ("salesforce", "salesforce"),
            ("snowflake", "snowflake"),
            ("bigquery", "bigquery"),
            ("redshift", "redshift"),
            ("mongodb", "mongodb"),
            ("s3", "s3"),
            ("gcs", "gcs"),
        ]

        for input_service, expected_platform in test_cases:
            result = get_platform_from_fivetran_service(input_service)
            assert result == expected_platform, (
                f"Expected {expected_platform}, got {result} for {input_service}"
            )

    def test_get_platform_from_fivetran_service_unknown_platforms(self):
        """Test platform detection for unknown platforms."""
        unknown_services = ["unknown_service", "custom_connector", "proprietary_db"]

        for service in unknown_services:
            result = get_platform_from_fivetran_service(service)
            # Should return the service name itself for unknown platforms
            assert result == service.lower()

    def test_get_platform_from_fivetran_service_empty_input(self):
        """Test platform detection with empty input."""
        result = get_platform_from_fivetran_service("")
        assert result == "unknown"


class TestLineageValidation:
    """Test lineage validation and error handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_source = Mock(spec=FivetranSource)
        self.mock_source._validate_lineage_data = (
            FivetranSource._validate_lineage_data.__get__(self.mock_source)
        )

    def create_mock_connector(self, connector_id: str = "test_connector") -> Connector:
        """Create a mock connector for testing."""
        return Connector(
            connector_id=connector_id,
            connector_name="Test Connector",
            connector_type="mysql",
            paused=False,
            sync_frequency=60,
            destination_id="test_dest",
        )

    def create_mock_platform_detail(
        self, platform: str = "mysql", database: str = "test_db"
    ) -> PlatformDetail:
        """Create a mock platform detail for testing."""
        return PlatformDetail(
            platform=platform,
            database=database,
            env="PROD",
        )

    def test_validate_lineage_data_valid_lineage(self):
        """Test validation of valid lineage data."""
        connector = self.create_mock_connector()
        source_details = self.create_mock_platform_detail("mysql")
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="schema.table1",
            destination_table="dest_schema.table1",
            column_lineage=[
                ColumnLineage(
                    source_column="col1",
                    destination_column="col1",
                    source_column_type="VARCHAR",
                    destination_column_type="STRING",
                )
            ],
        )

        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is True

    def test_validate_lineage_data_missing_source_table(self):
        """Test validation fails for missing source table."""
        connector = self.create_mock_connector()
        source_details = self.create_mock_platform_detail()
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="",  # Empty source table
            destination_table="dest_schema.table1",
            column_lineage=[],
        )

        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is False

    def test_validate_lineage_data_missing_destination_table(self):
        """Test validation fails for missing destination table."""
        connector = self.create_mock_connector()
        source_details = self.create_mock_platform_detail()
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="schema.table1",
            destination_table="",  # Empty destination table
            column_lineage=[],
        )

        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is False

    def test_validate_lineage_data_missing_platforms(self):
        """Test validation fails for missing platform information."""
        connector = self.create_mock_connector()
        source_details = PlatformDetail(
            platform="", database="test_db"
        )  # Empty platform
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="schema.table1",
            destination_table="dest_schema.table1",
            column_lineage=[],
        )

        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is False

    def test_validate_lineage_data_invalid_column_lineage(self):
        """Test validation handles invalid column lineage."""
        connector = self.create_mock_connector()
        source_details = self.create_mock_platform_detail()
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="schema.table1",
            destination_table="dest_schema.table1",
            column_lineage=[
                ColumnLineage(
                    source_column="",  # Empty source column
                    destination_column="col1",
                )
            ],
        )

        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is False

    def test_validate_lineage_data_fivetran_system_columns_ignored(self):
        """Test that Fivetran system columns are properly handled."""
        connector = self.create_mock_connector()
        source_details = self.create_mock_platform_detail()
        dest_details = self.create_mock_platform_detail("snowflake")

        lineage = TableLineage(
            source_table="schema.table1",
            destination_table="dest_schema.table1",
            column_lineage=[
                ColumnLineage(
                    source_column="col1",
                    destination_column="_fivetran_synced",  # System column
                ),
                ColumnLineage(
                    source_column="col2",
                    destination_column="col2",  # Regular column
                ),
            ],
        )

        # Should still pass validation as system columns are handled
        result = self.mock_source._validate_lineage_data(
            connector, lineage, source_details, dest_details
        )
        assert result is True


class TestRichMetadataExtraction:
    """Test rich metadata extraction from TableLineage objects."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_source = Mock(spec=FivetranSource)
        self.mock_source._build_source_details = (
            FivetranSource._build_source_details.__get__(self.mock_source)
        )
        self.mock_source._build_destination_details = (
            FivetranSource._build_destination_details.__get__(self.mock_source)
        )
        self.mock_source._get_source_details = Mock()
        self.mock_source._get_destination_details = Mock()
        self.mock_source._detect_source_platform = Mock()

    def create_rich_table_lineage(self) -> TableLineage:
        """Create a TableLineage object with rich metadata."""
        return TableLineage(
            source_table="source_schema.source_table",
            destination_table="dest_schema.dest_table",
            column_lineage=[
                ColumnLineage(
                    source_column="id",
                    destination_column="id",
                    source_column_type="INT",
                    destination_column_type="NUMBER",
                ),
                ColumnLineage(
                    source_column="name",
                    destination_column="name",
                    source_column_type="VARCHAR(255)",
                    destination_column_type="STRING",
                ),
            ],
            # Rich metadata
            source_schema="source_schema",
            destination_schema="dest_schema",
            source_database="source_db",
            destination_database="dest_db",
            source_platform="mysql",
            destination_platform="snowflake",
            source_env="PROD",
            destination_env="PROD",
            connector_type_id="mysql",
            connector_name="MySQL Connector",
            destination_id="snowflake_dest",
        )

    def test_build_source_details_with_rich_metadata(self):
        """Test building source details using rich metadata from TableLineage."""
        connector = Connector(
            connector_id="test_connector",
            connector_name="Test Connector",
            connector_type="mysql",
            paused=False,
            sync_frequency=60,
            destination_id="test_dest",
        )

        lineage = self.create_rich_table_lineage()

        # Mock the fallback method
        fallback_details = PlatformDetail(platform="", database="")
        self.mock_source._get_source_details.return_value = fallback_details

        result = self.mock_source._build_source_details(connector, lineage)

        # Should use metadata from lineage
        assert result.platform == "mysql"
        assert result.database == "source_db"
        assert result.env == "PROD"

    def test_build_destination_details_with_rich_metadata(self):
        """Test building destination details using rich metadata from TableLineage."""
        connector = Connector(
            connector_id="test_connector",
            connector_name="Test Connector",
            connector_type="mysql",
            paused=False,
            sync_frequency=60,
            destination_id="test_dest",
        )

        lineage = self.create_rich_table_lineage()

        # Mock the fallback method
        fallback_details = PlatformDetail(platform="", database="")
        self.mock_source._get_destination_details.return_value = fallback_details

        result = self.mock_source._build_destination_details(connector, lineage)

        # Should use metadata from lineage
        assert result.platform == "snowflake"
        assert result.database == "dest_db"
        assert result.env == "PROD"

    def test_fallback_when_lineage_metadata_missing(self):
        """Test fallback to existing logic when lineage metadata is missing."""
        connector = Connector(
            connector_id="test_connector",
            connector_name="Test Connector",
            connector_type="mysql",
            paused=False,
            sync_frequency=60,
            destination_id="test_dest",
        )

        # Lineage with minimal metadata
        lineage = TableLineage(
            source_table="source_schema.source_table",
            destination_table="dest_schema.dest_table",
            column_lineage=[],
        )

        # Mock the fallback methods
        fallback_source = PlatformDetail(platform="mysql", database="fallback_db")
        fallback_dest = PlatformDetail(
            platform="snowflake", database="fallback_dest_db"
        )
        self.mock_source._get_source_details.return_value = fallback_source
        self.mock_source._get_destination_details.return_value = fallback_dest

        source_result = self.mock_source._build_source_details(connector, lineage)
        dest_result = self.mock_source._build_destination_details(connector, lineage)

        # Should use fallback values
        assert source_result.platform == "mysql"
        assert source_result.database == "fallback_db"
        assert dest_result.platform == "snowflake"
        assert dest_result.database == "fallback_dest_db"


class TestColumnLineageFeatures:
    """Test column lineage features with type information."""

    def test_column_lineage_with_types(self):
        """Test that column lineage includes type information."""
        column_lineage = ColumnLineage(
            source_column="user_id",
            destination_column="user_id",
            source_column_type="INT",
            destination_column_type="NUMBER(38,0)",
        )

        assert column_lineage.source_column == "user_id"
        assert column_lineage.destination_column == "user_id"
        assert column_lineage.source_column_type == "INT"
        assert column_lineage.destination_column_type == "NUMBER(38,0)"

    def test_column_lineage_without_types(self):
        """Test that column lineage works without type information."""
        column_lineage = ColumnLineage(
            source_column="user_name",
            destination_column="user_name",
        )

        assert column_lineage.source_column == "user_name"
        assert column_lineage.destination_column == "user_name"
        assert column_lineage.source_column_type is None
        assert column_lineage.destination_column_type is None


class TestTableLineageFeatures:
    """Test table lineage features with comprehensive metadata."""

    def test_table_lineage_with_full_metadata(self):
        """Test TableLineage with all metadata fields."""
        lineage = TableLineage(
            source_table="crm.users",
            destination_table="analytics.users",
            column_lineage=[
                ColumnLineage(
                    source_column="id",
                    destination_column="id",
                    source_column_type="INT",
                    destination_column_type="NUMBER",
                )
            ],
            source_schema="crm",
            destination_schema="analytics",
            source_database="production",
            destination_database="warehouse",
            source_platform="mysql",
            destination_platform="snowflake",
            source_env="PROD",
            destination_env="PROD",
            connector_type_id="mysql",
            connector_name="CRM MySQL Connector",
            destination_id="snowflake_warehouse",
        )

        assert lineage.source_table == "crm.users"
        assert lineage.destination_table == "analytics.users"
        assert len(lineage.column_lineage) == 1
        assert lineage.source_platform == "mysql"
        assert lineage.destination_platform == "snowflake"
        assert lineage.source_database == "production"
        assert lineage.destination_database == "warehouse"
        assert lineage.connector_type_id == "mysql"
        assert lineage.connector_name == "CRM MySQL Connector"

    def test_table_lineage_minimal_metadata(self):
        """Test TableLineage with minimal required metadata."""
        lineage = TableLineage(
            source_table="schema.table",
            destination_table="dest.table",
            column_lineage=[],
        )

        assert lineage.source_table == "schema.table"
        assert lineage.destination_table == "dest.table"
        assert lineage.column_lineage == []
        assert lineage.source_platform is None
        assert lineage.destination_platform is None
        assert lineage.source_database is None
        assert lineage.destination_database is None


if __name__ == "__main__":
    pytest.main([__file__])
