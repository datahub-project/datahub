import datetime
from typing import Optional
from unittest.mock import Mock, patch

import pytest
import requests

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
)


@pytest.fixture
def make_connection_details():
    """Factory fixture to create FivetranConnectionDetails with customizable fields."""

    def _make(
        connector_id: str = "test_connector",
        sheet_id: str = "https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0",
        named_range: str = "Test_Range",
        succeeded_at: Optional[datetime.datetime] = None,
    ) -> FivetranConnectionDetails:
        return FivetranConnectionDetails(
            id=connector_id,
            group_id="test_group",
            service="google_sheets",
            created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            succeeded_at=succeeded_at
            if succeeded_at is not None
            else datetime.datetime(2025, 1, 1, 1, 0, 0),
            paused=False,
            sync_frequency=360,
            config=FivetranConnectionConfig(
                auth_type="ServiceAccount",
                sheet_id=sheet_id,
                named_range=named_range,
            ),
        )

    return _make


class TestFivetranGoogleSheetsIntegration:
    """Test cases for Google Sheets integration in Fivetran source."""

    @pytest.fixture(autouse=True)
    def setup(self):
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

    def test_google_sheets_connector_detection(self, make_connection_details):
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

        mock_connection_details = make_connection_details(
            connector_id="test_gsheets_connector"
        )
        self.mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

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

    def test_google_sheets_lineage_generation(self, make_connection_details):
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

        mock_connection_details = make_connection_details(
            connector_id="test_gsheets_connector"
        )
        self.mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

        datajob = Mock()
        lineage_properties = self.source._extend_lineage(connector, datajob)

        assert "source.platform" in lineage_properties
        assert (
            lineage_properties["source.platform"]
            == Constant.GOOGLE_SHEETS_CONNECTOR_TYPE
        )

    def test_google_sheets_connector_type_constant(self):
        """Test that the Google Sheets connector type constant is correct."""
        assert Constant.GOOGLE_SHEETS_CONNECTOR_TYPE == "google_sheets"

    def test_get_gsheet_sheet_id_from_url_with_full_url(self, make_connection_details):
        """Test extraction of sheet ID from full Google Sheets URL."""
        gsheets_conn_details = make_connection_details(
            sheet_id="https://docs.google.com/spreadsheets/d/1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo/edit?gid=0#gid=0"
        )

        sheet_id = self.source._get_gsheet_sheet_id_from_url(gsheets_conn_details)
        assert sheet_id == "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo"

    def test_get_gsheet_sheet_id_from_url_with_plain_id(self, make_connection_details):
        """Test extraction of sheet ID when it's already just an ID."""
        gsheets_conn_details = make_connection_details(
            sheet_id="13aoqK7hn75-_fckhgfw10tU4yPTLwyrB8t_HkqnBG_A"
        )

        sheet_id = self.source._get_gsheet_sheet_id_from_url(gsheets_conn_details)
        assert sheet_id == "13aoqK7hn75-_fckhgfw10tU4yPTLwyrB8t_HkqnBG_A"

    def test_get_gsheet_sheet_id_from_url_returns_none_for_invalid_url(
        self, make_connection_details
    ):
        """Test that invalid URL formats return None."""
        gsheets_conn_details = make_connection_details(
            sheet_id="https://docs.google.com/invalid/path/format"
        )

        sheet_id = self.source._get_gsheet_sheet_id_from_url(gsheets_conn_details)
        assert sheet_id is None

    def test_get_gsheet_named_range_dataset_id(self, make_connection_details):
        """Test generation of named range dataset ID."""
        gsheets_conn_details = make_connection_details(
            sheet_id="1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo",
            named_range="Test_Range",
        )

        named_range_id = self.source._get_gsheet_named_range_dataset_id(
            gsheets_conn_details
        )
        assert (
            named_range_id == "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo.Test_Range"
        )

    def test_get_gsheet_named_range_dataset_id_returns_none_when_sheet_id_fails(
        self, make_connection_details
    ):
        """Test that named_range returns None when sheet_id extraction fails."""
        gsheets_conn_details = make_connection_details(
            sheet_id="https://docs.google.com/invalid/format"
        )

        result = self.source._get_gsheet_named_range_dataset_id(gsheets_conn_details)
        assert result is None

    def test_get_connection_details_by_id_api_error(self):
        """Test handling of API errors when fetching connection details."""
        self.mock_api_client.get_connection_details_by_id.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )

        result = self.source._get_connection_details_by_id("test_connector_id")
        assert result is None

    def test_get_connection_details_by_id_with_caching(self, make_connection_details):
        """Test that connection details are cached after first fetch."""
        gsheets_conn_details = make_connection_details(sheet_id="test_sheet_id")
        self.mock_api_client.get_connection_details_by_id.return_value = (
            gsheets_conn_details
        )

        # First call should fetch from API
        result1 = self.source._get_connection_details_by_id("test_connector")
        assert result1 == gsheets_conn_details
        assert self.mock_api_client.get_connection_details_by_id.call_count == 1

        # Second call should use cache
        result2 = self.source._get_connection_details_by_id("test_connector")
        assert result2 == gsheets_conn_details
        assert self.mock_api_client.get_connection_details_by_id.call_count == 1

    def test_get_connection_details_by_id_with_null_succeeded_at(
        self, make_connection_details
    ):
        """Test handling of connection details with null succeeded_at."""
        # Create with explicit None for succeeded_at
        gsheets_conn_details = FivetranConnectionDetails(
            id="test_connector",
            group_id="test_group",
            service="google_sheets",
            created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            succeeded_at=None,
            paused=False,
            sync_frequency=360,
            config=FivetranConnectionConfig(
                auth_type="ServiceAccount",
                sheet_id="test_sheet_id",
                named_range="Test_Range",
            ),
        )
        self.mock_api_client.get_connection_details_by_id.return_value = (
            gsheets_conn_details
        )

        result = self.source._get_connection_details_by_id("test_connector")
        assert result == gsheets_conn_details
        assert result.succeeded_at is None

    def test_connector_workunits_generates_warning_when_gsheets_details_invalid(
        self, make_connection_details
    ):
        """Test that a warning is generated when Google Sheets details can't be extracted."""
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

        # Mock API to return details with invalid sheet_id format
        mock_connection_details = make_connection_details(
            connector_id="test_gsheets_connector",
            sheet_id="https://docs.google.com/invalid/format",
        )
        self.mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

        workunits = list(self.source._get_connector_workunits(connector))

        # Should still generate dataflow/datajob but no Google Sheets datasets
        gsheets_datasets = [
            wu
            for wu in workunits
            if hasattr(wu, "platform") and "google_sheets" in str(wu.platform)
        ]
        assert len(gsheets_datasets) == 0

        # Verify warning was reported
        assert any(
            w.title == "Failed to generate entities for Google Sheets"
            for w in self.source.report._structured_logs.warnings
        )

    def test_lineage_not_generated_when_sheet_id_is_none(self, make_connection_details):
        """Test that lineage is skipped when sheet_id extraction fails."""
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
                    column_lineage=[],
                )
            ],
            jobs=[],
        )

        # Mock API to return details with invalid sheet_id format
        mock_connection_details = make_connection_details(
            connector_id="test_gsheets_connector",
            sheet_id="https://docs.google.com/invalid/format",
        )
        self.mock_api_client.get_connection_details_by_id.return_value = (
            mock_connection_details
        )

        datajob = Mock()
        self.source._extend_lineage(connector, datajob)

        # Verify that set_inlets was called with an empty list for Google Sheets
        # (no valid input_dataset_urn was created)
        datajob.set_inlets.assert_called()
        inlets = datajob.set_inlets.call_args[0][0]
        # All inlets should be empty since the only lineage is from invalid GSheets
        assert len(inlets) == 0
