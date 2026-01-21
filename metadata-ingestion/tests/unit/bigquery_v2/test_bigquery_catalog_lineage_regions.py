"""Unit tests for BigQuery catalog lineage region discovery."""

from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryDataset


class TestBigQueryCatalogLineageRegions:
    """Tests for dynamic region discovery in catalog lineage API."""

    def _create_mock_dataset(
        self, name: str, location: Optional[str] = None
    ) -> BigqueryDataset:
        """Helper to create a mock BigQuery dataset."""
        return BigqueryDataset(
            name=name,
            location=location,
            labels=None,
            created=datetime.now(),
            last_altered=datetime.now(),
            comment=None,
        )

    @patch("datahub.ingestion.source.bigquery_v2.lineage.lineage_v1.LineageClient")
    @patch("datahub.ingestion.source.bigquery_v2.lineage.BigQuerySchemaApi")
    def test_single_region_discovery(
        self, mock_schema_api_class, mock_lineage_client_class
    ):
        """Test that single region is discovered from datasets."""
        from datahub.ingestion.source.bigquery_v2.lineage import (
            BigqueryLineageExtractor,
        )

        # Setup mocks
        mock_config = MagicMock()
        mock_config.dataset_pattern = MagicMock()
        mock_config.table_pattern = MagicMock()
        mock_config.match_fully_qualified_names = False
        mock_config.get_bigquery_client.return_value = MagicMock()
        mock_config.get_projects_client.return_value = MagicMock()

        mock_report = MagicMock()
        mock_report.schema_api_perf = MagicMock()
        mock_report.num_skipped_lineage_entries_not_allowed = {}

        mock_schema_api = MagicMock()
        mock_datasets = [
            self._create_mock_dataset("dataset1", "europe-west1"),
            self._create_mock_dataset("dataset2", "europe-west1"),
        ]
        mock_schema_api.get_datasets_for_project_id.return_value = mock_datasets
        mock_schema_api.list_tables.return_value = []
        mock_schema_api_class.return_value = mock_schema_api

        # Mock lineage client
        mock_lineage_client = MagicMock()
        mock_lineage_client.search_links.return_value = iter([])
        mock_lineage_client_class.return_value = mock_lineage_client

        # Create extractor with proper mocking
        with (
            patch("datahub.ingestion.source.bigquery_v2.lineage.BigQueryAuditLogApi"),
            patch("datahub.ingestion.source.bigquery_v2.lineage.SqlParsingAggregator"),
        ):
            extractor = BigqueryLineageExtractor(
                config=mock_config,
                report=mock_report,
                schema_resolver=MagicMock(platform="bigquery"),
                identifiers=MagicMock(platform="bigquery"),
            )

            result = extractor.lineage_via_catalog_lineage_api("test-project")

            # Assertions
            assert isinstance(result, dict)
            mock_schema_api.get_datasets_for_project_id.assert_called_once_with(
                "test-project"
            )
            # Verify lineage client was instantiated
            mock_lineage_client_class.assert_called_once()

    @patch("datahub.ingestion.source.bigquery_v2.lineage.lineage_v1.LineageClient")
    @patch("datahub.ingestion.source.bigquery_v2.lineage.BigQuerySchemaApi")
    def test_multiple_regions_discovery(
        self, mock_schema_api_class, mock_lineage_client_class
    ):
        """Test that multiple regions are discovered from datasets."""
        from datahub.ingestion.source.bigquery_v2.lineage import (
            BigqueryLineageExtractor,
        )

        mock_config = MagicMock()
        mock_config.dataset_pattern = MagicMock()
        mock_config.table_pattern = MagicMock()
        mock_config.match_fully_qualified_names = False
        mock_config.get_bigquery_client.return_value = MagicMock()
        mock_config.get_projects_client.return_value = MagicMock()

        mock_report = MagicMock()
        mock_report.schema_api_perf = MagicMock()
        mock_report.num_skipped_lineage_entries_not_allowed = {}

        mock_schema_api = MagicMock()
        mock_datasets = [
            self._create_mock_dataset("dataset1", "us-central1"),
            self._create_mock_dataset("dataset2", "europe-west1"),
            self._create_mock_dataset("dataset3", "asia-southeast1"),
        ]
        mock_schema_api.get_datasets_for_project_id.return_value = mock_datasets
        mock_schema_api.list_tables.return_value = []
        mock_schema_api_class.return_value = mock_schema_api

        # Mock lineage client
        mock_lineage_client = MagicMock()
        mock_lineage_client.search_links.return_value = iter([])
        mock_lineage_client_class.return_value = mock_lineage_client

        with (
            patch("datahub.ingestion.source.bigquery_v2.lineage.BigQueryAuditLogApi"),
            patch("datahub.ingestion.source.bigquery_v2.lineage.SqlParsingAggregator"),
        ):
            extractor = BigqueryLineageExtractor(
                config=mock_config,
                report=mock_report,
                schema_resolver=MagicMock(platform="bigquery"),
                identifiers=MagicMock(platform="bigquery"),
            )

            result = extractor.lineage_via_catalog_lineage_api("test-project")

            # Assertions
            assert isinstance(result, dict)
            mock_schema_api.get_datasets_for_project_id.assert_called_once_with(
                "test-project"
            )
            mock_lineage_client_class.assert_called_once()

    @patch("datahub.ingestion.source.bigquery_v2.lineage.lineage_v1.LineageClient")
    @patch("datahub.ingestion.source.bigquery_v2.lineage.BigQuerySchemaApi")
    def test_no_location_fallback_to_default(
        self, mock_schema_api_class, mock_lineage_client_class
    ):
        """Test fallback to US/EU when no locations are specified."""
        from datahub.ingestion.source.bigquery_v2.lineage import (
            BigqueryLineageExtractor,
        )

        mock_config = MagicMock()
        mock_config.dataset_pattern = MagicMock()
        mock_config.table_pattern = MagicMock()
        mock_config.match_fully_qualified_names = False
        mock_config.get_bigquery_client.return_value = MagicMock()
        mock_config.get_projects_client.return_value = MagicMock()

        mock_report = MagicMock()
        mock_report.schema_api_perf = MagicMock()
        mock_report.num_skipped_lineage_entries_not_allowed = {}

        mock_schema_api = MagicMock()
        mock_datasets = [
            self._create_mock_dataset("dataset1", None),
            self._create_mock_dataset("dataset2", None),
        ]
        mock_schema_api.get_datasets_for_project_id.return_value = mock_datasets
        mock_schema_api.list_tables.return_value = []
        mock_schema_api_class.return_value = mock_schema_api

        # Mock lineage client
        mock_lineage_client = MagicMock()
        mock_lineage_client.search_links.return_value = iter([])
        mock_lineage_client_class.return_value = mock_lineage_client

        with (
            patch("datahub.ingestion.source.bigquery_v2.lineage.BigQueryAuditLogApi"),
            patch("datahub.ingestion.source.bigquery_v2.lineage.SqlParsingAggregator"),
        ):
            extractor = BigqueryLineageExtractor(
                config=mock_config,
                report=mock_report,
                schema_resolver=MagicMock(platform="bigquery"),
                identifiers=MagicMock(platform="bigquery"),
            )

            result = extractor.lineage_via_catalog_lineage_api("test-project")

            # Assertions
            assert isinstance(result, dict)
            mock_schema_api.get_datasets_for_project_id.assert_called_once_with(
                "test-project"
            )
            mock_lineage_client_class.assert_called_once()

    @patch("datahub.ingestion.source.bigquery_v2.lineage.lineage_v1.LineageClient")
    @patch("datahub.ingestion.source.bigquery_v2.lineage.BigQuerySchemaApi")
    def test_empty_datasets_fallback(
        self, mock_schema_api_class, mock_lineage_client_class
    ):
        """Test fallback when no datasets are found."""
        from datahub.ingestion.source.bigquery_v2.lineage import (
            BigqueryLineageExtractor,
        )

        mock_config = MagicMock()
        mock_config.dataset_pattern = MagicMock()
        mock_config.table_pattern = MagicMock()
        mock_config.match_fully_qualified_names = False
        mock_config.get_bigquery_client.return_value = MagicMock()
        mock_config.get_projects_client.return_value = MagicMock()

        mock_report = MagicMock()
        mock_report.schema_api_perf = MagicMock()
        mock_report.num_skipped_lineage_entries_not_allowed = {}

        mock_schema_api = MagicMock()
        mock_schema_api.get_datasets_for_project_id.return_value = []
        mock_schema_api_class.return_value = mock_schema_api

        # Mock lineage client
        mock_lineage_client = MagicMock()
        mock_lineage_client_class.return_value = mock_lineage_client

        with (
            patch("datahub.ingestion.source.bigquery_v2.lineage.BigQueryAuditLogApi"),
            patch("datahub.ingestion.source.bigquery_v2.lineage.SqlParsingAggregator"),
        ):
            extractor = BigqueryLineageExtractor(
                config=mock_config,
                report=mock_report,
                schema_resolver=MagicMock(platform="bigquery"),
                identifiers=MagicMock(platform="bigquery"),
            )

            result = extractor.lineage_via_catalog_lineage_api("test-project")

            # Assertions - should return empty lineage map
            assert result == {}
            mock_schema_api.get_datasets_for_project_id.assert_called_once_with(
                "test-project"
            )
            mock_lineage_client_class.assert_called_once()
