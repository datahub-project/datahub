import datetime
from unittest import TestCase
from unittest.mock import Mock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranAPIConfig,
    FivetranLogConfig,
    FivetranSourceConfig,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionConfig,
    FivetranConnectionDetails,
    FivetranConnectionSourceSyncDetails,
    FivetranConnectionStatus,
)


class TestFivetranGoogleSheetsIntegration(TestCase):
    """Test cases for Google Sheets integration in Fivetran source."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = FivetranSourceConfig(
            fivetran_log_config=FivetranLogConfig(
                destination_platform="snowflake",
                snowflake_destination_config={
                    "host_port": "test.snowflakecomputing.com",
                    "username": "test_user",
                    "password": "test_password",
                    "database": "test_db",
                    "warehouse": "test_warehouse",
                    "role": "test_role",
                    "log_schema": "test_log_schema",
                },
            ),
            api_config=FivetranAPIConfig(
                api_key="test_api_key", api_secret="test_api_secret"
            ),
            env="PROD",
            platform_instance="test_instance",
        )
        self.ctx = PipelineContext(run_id="test_run")

        # Mock the FivetranLogAPI to avoid real database connections
        with patch(
            "datahub.ingestion.source.fivetran.fivetran.FivetranLogAPI"
        ) as mock_log_api:
            mock_audit_log = Mock()
            mock_audit_log.get_user_email.return_value = "test@example.com"
            mock_audit_log.fivetran_log_database = "test_db"
            mock_log_api.return_value = mock_audit_log
            self.source = FivetranSource(self.config, self.ctx)

            # Mock the API client
            self.mock_api_client = Mock()
            self.source.api_client = self.mock_api_client

    def test_google_sheets_connector_detection(self):
        """Test detection of Google Sheets connectors."""
        connector = Connector(
            connector_id="test_gsheets_connector",
            connector_name="Google Sheets Test",
            connector_type=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            paused=False,
            sync_frequency=360,
            destination_id="test_destination",
            user_id="test_user",
            lineage=[],
            jobs=[],
        )

        # Mock the API client
        mock_api_client = Mock()
        self.source.api_client = mock_api_client

        # Mock the connection details response
        mock_connection_details = FivetranConnectionDetails(
            id="test_gsheets_connector",
            group_id="test_group",
            service="google_sheets",
            created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            succeeded_at=datetime.datetime(2025, 1, 1, 1, 0, 0),
            paused=False,
            sync_frequency=360,
            status=FivetranConnectionStatus(
                setup_state="connected",
                schema_status="ready",
                sync_state="paused",
                update_state="on_schedule",
                is_historical_sync=False,
                warnings=[],
            ),
            config=FivetranConnectionConfig(
                auth_type="ServiceAccount",
                sheet_id="https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0",
                named_range="Test_Range",
            ),
            source_sync_details=FivetranConnectionSourceSyncDetails(
                last_synced=datetime.datetime(2025, 1, 1, 1, 0, 0)
            ),
        )

        mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

        # Test the connector workunits generation
        workunits = list(self.source._get_connector_workunits(connector))

        # Should generate Google Sheets datasets
        assert len(workunits) >= 2  # At least the two Google Sheets datasets

        # Find the Google Sheets datasets
        gsheets_datasets = [
            wu
            for wu in workunits
            if hasattr(wu, "platform")
            and str(wu.platform)
            == f"urn:li:dataPlatform:{Constant.GOOGLE_SHEETS_CONNECTOR_TYPE}"
        ]
        assert len(gsheets_datasets) == 2

    def test_google_sheets_lineage_generation(self):
        """Test lineage generation for Google Sheets connectors."""
        connector = Connector(
            connector_id="test_gsheets_connector",
            connector_name="Google Sheets Test",
            connector_type=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            paused=False,
            sync_frequency=360,
            destination_id="test_destination",
            user_id="test_user",
            lineage=[
                TableLineage(
                    source_table="source_schema.source_table",
                    destination_table="dest_schema.dest_table",
                    column_lineage=[
                        ColumnLineage(
                            source_column="source_col", destination_column="dest_col"
                        )
                    ],
                )
            ],
            jobs=[],
        )

        # Mock the API client
        mock_api_client = Mock()
        self.source.api_client = mock_api_client

        # Mock the connection details response
        mock_connection_details = FivetranConnectionDetails(
            id="test_gsheets_connector",
            group_id="test_group",
            service="google_sheets",
            created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            succeeded_at=datetime.datetime(2025, 1, 1, 1, 0, 0),
            paused=False,
            sync_frequency=360,
            status=FivetranConnectionStatus(
                setup_state="connected",
                schema_status="ready",
                sync_state="paused",
                update_state="on_schedule",
                is_historical_sync=False,
                warnings=[],
            ),
            config=FivetranConnectionConfig(
                auth_type="ServiceAccount",
                sheet_id="https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0",
                named_range="Test_Range",
            ),
            source_sync_details=FivetranConnectionSourceSyncDetails(
                last_synced=datetime.datetime(2025, 1, 1, 1, 0, 0)
            ),
        )

        mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

        # Test lineage extension
        datajob = Mock()
        lineage_properties = self.source._extend_lineage(connector, datajob)

        # Check that the lineage properties include Google Sheets information
        assert "source.platform" in lineage_properties
        assert (
            lineage_properties["source.platform"]
            == Constant.GOOGLE_SHEETS_CONNECTOR_TYPE
        )

    def test_google_sheets_connector_type_constant(self):
        """Test that the Google Sheets connector type constant is correct."""
        assert Constant.GOOGLE_SHEETS_CONNECTOR_TYPE == "google_sheets"
