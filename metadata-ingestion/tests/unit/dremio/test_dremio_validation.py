from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioDataset,
    DremioDatasetType,
    DremioQuery,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioReflectionFiltering:
    """Test that Dremio reflections (_accelerator_ schema) are correctly filtered out."""

    @pytest.fixture
    def mock_config(self):
        return DremioSourceConfig(
            hostname="test-host",
            port=9047,
            tls=False,
            username="test-user",
            password="test-password",
        )

    @pytest.fixture
    def mock_dremio_source(self, mock_config, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        mock_ctx = Mock()
        mock_ctx.run_id = "test-run-id"
        mock_ctx.graph = None
        source = DremioSource(mock_config, mock_ctx)

        source.dremio_catalog = Mock(spec=DremioCatalog)
        source.dremio_catalog.dremio_api = Mock()

        return source

    def test_reflection_dataset_is_dropped(self, mock_dremio_source):
        """Test that datasets in _accelerator_ schema are dropped and reported."""
        reflection_dataset = Mock(spec=DremioDataset)
        reflection_dataset.path = ["_accelerator_", "reflection_123"]
        reflection_dataset.resource_name = "my_table_reflection"
        reflection_dataset.dataset_type = DremioDatasetType.TABLE

        workunits = list(mock_dremio_source.process_dataset(reflection_dataset))

        assert len(workunits) == 0, "Reflection should produce no workunits"
        assert len(mock_dremio_source.report.filtered) > 0, (
            "Report should track dropped reflection"
        )

    def test_normal_dataset_is_processed(self, mock_dremio_source):
        """Test that normal datasets (not in _accelerator_) are processed."""
        normal_dataset = Mock(spec=DremioDataset)
        normal_dataset.path = ["MySpace", "folder"]
        normal_dataset.resource_name = "my_table"
        normal_dataset.dataset_type = DremioDatasetType.TABLE
        normal_dataset.dataset_id = "test-id"
        normal_dataset.dataset_schema = None
        normal_dataset.dataset_name = "my_table"
        normal_dataset.parents = []
        normal_dataset.sql_definition = None
        normal_dataset.children_list = []
        normal_dataset.created_at = None

        # Mock the dremio_aspects to return an empty iterable
        mock_dremio_source.dremio_aspects.populate_dataset_mcp = Mock(
            return_value=iter([])
        )

        # Process the dataset
        list(mock_dremio_source.process_dataset(normal_dataset))

        # Should have processed the dataset (even if no workunits generated due to mocking)
        assert "myspace.folder.my_table" in mock_dremio_source.catalog_dataset_names, (
            "Normal dataset should be tracked in catalog"
        )

    def test_reflection_with_nested_path(self, mock_dremio_source):
        """Test that reflections in nested paths are still filtered."""
        reflection_dataset = Mock(spec=DremioDataset)
        reflection_dataset.path = ["_accelerator_", "subfolder", "nested"]
        reflection_dataset.resource_name = "reflection_table"
        reflection_dataset.dataset_type = DremioDatasetType.TABLE

        workunits = list(mock_dremio_source.process_dataset(reflection_dataset))

        assert len(workunits) == 0, "Nested reflection should be dropped"
        assert len(mock_dremio_source.report.filtered) > 0


class TestQueryLineageValidation:
    """Test query lineage format validation and suspicious pattern detection."""

    @pytest.fixture
    def mock_config(self):
        return DremioSourceConfig(
            hostname="test-host",
            port=9047,
            tls=False,
            username="test-user",
            password="test-password",
        )

    @pytest.fixture
    def mock_dremio_source(self, mock_config, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        mock_ctx = Mock()
        mock_ctx.run_id = "test-run-id"
        mock_ctx.graph = None
        source = DremioSource(mock_config, mock_ctx)

        source.dremio_catalog = Mock(spec=DremioCatalog)
        source.dremio_catalog.dremio_api = Mock()

        # Pre-populate catalog with known datasets
        source.catalog_dataset_names = {
            "myspace.folder.table1",
            "myspace.folder.table2",
            "source.schema.table3",
        }

        return source

    def test_query_with_catalog_dataset_no_warning(self, mock_dremio_source):
        """Test that queries referencing catalog datasets don't trigger warnings."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-123"
        query.query = "SELECT * FROM myspace.folder.table1"
        query.affected_dataset = "myspace.folder.result"
        query.queried_datasets = ["myspace.folder.table1"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) == initial_warning_count, (
            "No warning should be raised for catalog dataset"
        )

    def test_query_with_s3_path_triggers_warning(self, mock_dremio_source):
        """Test that queries with S3 paths trigger format mismatch warning."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-456"
        query.query = "SELECT * FROM s3_source.bucket.table"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["s3://my-bucket/path/to/data"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) > initial_warning_count, (
            "Warning should be raised for S3 path"
        )

        # Check that the warning message contains expected content
        warning_found = any(
            "S3 path" in warning.message
            or (warning.title and "S3 path" in warning.title)
            or any("S3 path" in ctx for ctx in warning.context)
            for warning in mock_dremio_source.report.warnings
        )
        assert warning_found, "Warning should mention S3 path"

    def test_query_with_hdfs_path_triggers_warning(self, mock_dremio_source):
        """Test that queries with HDFS paths trigger format mismatch warning."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-789"
        query.query = "SELECT * FROM hdfs_source.table"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["hdfs://namenode:9000/user/data"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) > initial_warning_count, (
            "Warning should be raised for HDFS path"
        )

        warning_found = any(
            "HDFS path" in warning.message
            or (warning.title and "HDFS path" in warning.title)
            or any("HDFS path" in ctx for ctx in warning.context)
            for warning in mock_dremio_source.report.warnings
        )
        assert warning_found, "Warning should mention HDFS path"

    def test_query_with_file_path_triggers_warning(self, mock_dremio_source):
        """Test that queries with file system paths trigger format mismatch warning."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-101"
        query.query = "SELECT * FROM nas.table"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["/mnt/data/warehouse/table"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) > initial_warning_count, (
            "Warning should be raised for file path"
        )

        warning_found = any(
            "file path" in warning.message
            or (warning.title and "file path" in warning.title)
            or any("file path" in ctx for ctx in warning.context)
            for warning in mock_dremio_source.report.warnings
        )
        assert warning_found, "Warning should mention file path"

    def test_query_with_versioned_reference_triggers_warning(self, mock_dremio_source):
        """Test that queries with @ versioned references trigger format mismatch warning."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-102"
        query.query = "SELECT * FROM source.table@branch"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["source.table@branch"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) > initial_warning_count, (
            "Warning should be raised for versioned reference"
        )

        warning_found = any(
            "versioned reference" in warning.message
            or (warning.title and "versioned reference" in warning.title)
            or any("versioned reference" in ctx for ctx in warning.context)
            for warning in mock_dremio_source.report.warnings
        )
        assert warning_found, "Warning should mention versioned reference"

    @patch("datahub.ingestion.source.dremio.dremio_source.logger")
    def test_query_with_unknown_dataset_debug_log(
        self, mock_logger, mock_dremio_source
    ):
        """Test that queries with unknown datasets (no suspicious pattern) only log debug."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-103"
        query.query = "SELECT * FROM otherspace.table"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["otherspace.unknown.table"]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) == initial_warning_count, (
            "No warning should be raised for non-suspicious unknown dataset"
        )

        # Verify debug log was called
        mock_logger.debug.assert_called()
        debug_message = str(mock_logger.debug.call_args)
        assert "not found in catalog" in debug_message

    def test_query_with_multiple_datasets_mixed(self, mock_dremio_source):
        """Test query with mix of catalog and suspicious datasets."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-104"
        query.query = "SELECT * FROM table1 JOIN external"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = [
            "myspace.folder.table1",  # In catalog
            "s3://external-bucket/data",  # Suspicious
        ]
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        # Should raise warning only for the suspicious dataset
        assert len(mock_dremio_source.report.warnings) > initial_warning_count, (
            "Warning should be raised for suspicious dataset"
        )

    def test_validation_case_insensitive(self, mock_dremio_source):
        """Test that validation is case-insensitive for dataset names."""
        query = Mock(spec=DremioQuery)
        query.job_id = "test-job-105"
        query.query = "SELECT * FROM MYSPACE.FOLDER.TABLE1"
        query.affected_dataset = "myspace.result"
        query.queried_datasets = ["MYSPACE.FOLDER.TABLE1"]  # Uppercase
        query.username = "test-user"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        initial_warning_count = len(mock_dremio_source.report.warnings)

        mock_dremio_source.process_query(query)

        assert len(mock_dremio_source.report.warnings) == initial_warning_count, (
            "Case-insensitive match should not trigger warning"
        )
